# Offset-index construction and serialization for the writer. The reader does not use
# page indexes, so it neither reads nor validates them.

function _writevarintsize(value::UInt64)
    bytes = Int64(1)
    while value >= 0x80
        value >>= 7
        bytes += 1
    end
    return bytes
end

function _writezigzagsize(value::Int32)
    encoded = UInt64(reinterpret(UInt32,
        xor(value << 1, value >> 31)))
    return _writevarintsize(encoded)
end

function _writezigzagsize(value::Int64)
    encoded = reinterpret(UInt64, xor(value << 1, value >> 63))
    return _writevarintsize(encoded)
end

function _writeoffsetindexencodedsize(index::Metadata.OffsetIndex)
    index.unencoded_byte_array_data_bytes === nothing || throw(AssertionError(
        "writer OffsetIndex unexpectedly carries byte-array size statistics"))
    isempty(index.unknown_fields) || throw(AssertionError(
        "writer OffsetIndex unexpectedly carries unknown fields"))
    count = length(index.page_locations)
    count <= typemax(Int32) || throw(LimitError(:container_elements,
        Int64(count), Int64(typemax(Int32))))
    bytes = Int64(2) # field header and struct stop
    bytes += 1 # list header
    count >= 15 && (bytes = Base.checked_add(bytes,
        _writevarintsize(UInt64(count))))
    for location in index.page_locations
        isempty(location.unknown_fields) || throw(AssertionError(
            "writer PageLocation unexpectedly carries unknown fields"))
        bytes = Base.checked_add(bytes, Int64(4)) # three headers and stop
        bytes = Base.checked_add(bytes, _writezigzagsize(location.offset))
        bytes = Base.checked_add(bytes,
            _writezigzagsize(location.compressed_page_size))
        bytes = Base.checked_add(bytes,
            _writezigzagsize(location.first_row_index))
    end
    return bytes
end

function _writeabsoluteoffsetindex(pages::ColumnPages, chunkoffset::Int64,
        rows::Int)
    rows > 0 || throw(AssertionError(
        "writer cannot index a zero-row column chunk"))
    isempty(pages.page_locations) && throw(AssertionError(
        "writer produced no data-page locations for a nonempty chunk"))
    locations = Metadata.PageLocation[]
    sizehint!(locations, length(pages.page_locations))
    previousend = chunkoffset
    previousrow = Int64(-1)
    chunkend = Base.checked_add(chunkoffset, Int64(length(pages.bytes)))
    for (index, relative) in enumerate(pages.page_locations)
        relative.offset >= 0 || throw(AssertionError(
            "writer produced a negative relative page offset"))
        relative.compressed_page_size > 0 || throw(AssertionError(
            "writer produced an empty data-page frame"))
        absolute = Base.checked_add(chunkoffset, relative.offset)
        frameend = Base.checked_add(absolute,
            Int64(relative.compressed_page_size))
        absolute >= previousend || throw(AssertionError(
            "writer data-page locations overlap or are out of order"))
        frameend <= chunkend || throw(AssertionError(
            "writer data-page location extends past its column chunk"))
        row = relative.first_row_index
        validrow = index == 1 ? row == 0 : row > previousrow
        validrow || throw(AssertionError(
            "writer data-page first-row indexes are not strictly increasing"))
        0 <= row < rows || throw(AssertionError(
            "writer data-page first-row index is outside its row group"))
        push!(locations, Metadata.PageLocation(offset=absolute,
            compressed_page_size=relative.compressed_page_size,
            first_row_index=row))
        previousend = frameend
        previousrow = row
    end
    return Metadata.OffsetIndex(page_locations=locations)
end

function _writecolumnchunkoffsetindex(chunk::Metadata.ColumnChunk,
        offset::Int64, length::Int32)
    return Metadata.ColumnChunk(
        file_path=chunk.file_path,
        file_offset=chunk.file_offset,
        meta_data=chunk.meta_data,
        offset_index_offset=offset,
        offset_index_length=length,
        column_index_offset=chunk.column_index_offset,
        column_index_length=chunk.column_index_length,
        crypto_metadata=chunk.crypto_metadata,
        encrypted_column_metadata=chunk.encrypted_column_metadata,
        unknown_fields=chunk.unknown_fields,
    )
end

function _writerowgroupcolumns(group::Metadata.RowGroup,
        columns::Vector{Metadata.ColumnChunk})
    return Metadata.RowGroup(
        columns=columns,
        total_byte_size=group.total_byte_size,
        num_rows=group.num_rows,
        sorting_columns=group.sorting_columns,
        file_offset=group.file_offset,
        total_compressed_size=group.total_compressed_size,
        ordinal=group.ordinal,
        unknown_fields=group.unknown_fields,
    )
end

function _writeencodeoffsetindex(index::Metadata.OffsetIndex,
        cumulative::Int64, limits::Limits)
    exact = _writeoffsetindexencodedsize(index)
    exact > 0 || throw(AssertionError("writer produced an empty OffsetIndex"))
    exact <= typemax(Int32) || throw(LimitError(:page_index_bytes,
        exact, Int64(typemax(Int32))))
    requested = try
        Base.checked_add(cumulative, exact)
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:page_index_bytes, typemax(Int64),
            limits.max_page_index_bytes))
    end
    _checklimit(:page_index_bytes, requested, limits.max_page_index_bytes)
    bytes = UInt8[]
    sizehint!(bytes, Int(exact))
    Thrift.encode!(Thrift.Writer(bytes), index)
    length(bytes) == exact || throw(AssertionError(
        "writer OffsetIndex size preflight did not match Compact Thrift"))
    return bytes, requested
end

function _writeoffsetindexsection!(output::Vector{UInt8},
        rowgroups::Vector{Metadata.RowGroup},
        indexes::Vector{Vector{Metadata.OffsetIndex}}, limits::Limits)
    length(rowgroups) == length(indexes) || throw(AssertionError(
        "writer row-group and offset-index counts differ"))
    startoutput = length(output)
    section = UInt8[]
    rebuilt = Metadata.RowGroup[]
    sizehint!(rebuilt, length(rowgroups))
    cumulative = Int64(0)
    for (group, groupindexes) in zip(rowgroups, indexes)
        length(group.columns) == length(groupindexes) || throw(
            AssertionError("writer column and OffsetIndex counts differ"))
        columns = Metadata.ColumnChunk[]
        sizehint!(columns, length(group.columns))
        for (chunk, index) in zip(group.columns, groupindexes)
            isempty(index.page_locations) && throw(AssertionError(
                "writer cannot serialize an empty OffsetIndex"))
            offset = Base.checked_add(Int64(startoutput),
                Int64(length(section)))
            bytes, cumulative = _writeencodeoffsetindex(index, cumulative,
                limits)
            append!(section, bytes)
            push!(columns, _writecolumnchunkoffsetindex(chunk, offset,
                Int32(length(bytes))))
        end
        push!(rebuilt, _writerowgroupcolumns(group, columns))
    end
    append!(output, section)
    return rebuilt
end
