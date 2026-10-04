# Offset-index construction, serialization, and bounded validation.

const _OffsetIndexRange = Tuple{Int64,Int64}

struct _PageIndexInterval
    first::Int64
    last::Int64
    index::Bool
end

struct _PageIndexRangePreflight
    groups::Int
    intervalcount::Int64
    cumulative::Int64
    overlaps_validated::Bool
end

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
        rows::Int, budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        rows > 0 || throw(AssertionError(
            "writer cannot index a zero-row column chunk"))
        isempty(pages.page_locations) && throw(AssertionError(
            "writer produced no data-page locations for a nonempty chunk"))
        count = length(pages.page_locations)
        _reservearray!(budget, Metadata.PageLocation, count)
        _reserveobjects!(budget, count + 1)
        locations = Metadata.PageLocation[]
        sizehint!(locations, count)
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
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
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
        cumulative::Int64, limits::Limits, budget::_LiveByteBudget)
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
    charge = _materializedsum(_materializedarraybytes(UInt8, exact),
        _MATERIALIZED_OBJECT_BYTES)
    _reserve!(budget, charge)
    bytes = try
        buffer = UInt8[]
        sizehint!(buffer, Int(exact))
        writer = Thrift.Writer(buffer)
        Thrift.encode!(writer, index)
        buffer
    catch
        _release!(budget, charge)
        rethrow()
    end
    length(bytes) == exact || begin
        _release!(budget, charge)
        throw(AssertionError(
            "writer OffsetIndex size preflight did not match Compact Thrift"))
    end
    return bytes, charge, requested
end

function _writeoffsetindexobsoletebytes(
        rowgroups::Vector{Metadata.RowGroup},
        indexes::Vector{Vector{Metadata.OffsetIndex}})
    bytes = _materializedarraybytes(Metadata.RowGroup, length(rowgroups))
    bytes = _materializedsum(bytes,
        _materializedproduct(length(rowgroups), _MATERIALIZED_OBJECT_BYTES))
    for group in rowgroups
        bytes = _materializedsum(bytes,
            _materializedarraybytes(Metadata.ColumnChunk,
                length(group.columns)))
        bytes = _materializedsum(bytes,
            _materializedproduct(length(group.columns),
                _MATERIALIZED_OBJECT_BYTES))
    end
    bytes = _materializedsum(bytes,
        _materializedarraybytes(Vector{Metadata.OffsetIndex},
            length(indexes)))
    for group in indexes
        bytes = _materializedsum(bytes,
            _materializedarraybytes(Metadata.OffsetIndex, length(group)))
        for index in group
            locations = length(index.page_locations)
            bytes = _materializedsum(bytes,
                _materializedarraybytes(Metadata.PageLocation, locations))
            bytes = _materializedsum(bytes,
                _materializedproduct(locations + 1,
                    _MATERIALIZED_OBJECT_BYTES))
        end
    end
    return bytes
end

function _writeoffsetindexsection!(output::Vector{UInt8},
        rowgroups::Vector{Metadata.RowGroup},
        indexes::Vector{Vector{Metadata.OffsetIndex}}, limits::Limits,
        budget::_LiveByteBudget)
    length(rowgroups) == length(indexes) || throw(AssertionError(
        "writer row-group and offset-index counts differ"))
    startbudget = _budgetused(budget)
    startoutput = length(output)
    try
        sectioncharge = _reservearray!(budget, UInt8, 0)
        section = UInt8[]
        _reservearray!(budget, Metadata.RowGroup, length(rowgroups))
        _reserveobjects!(budget, length(rowgroups))
        rebuilt = Metadata.RowGroup[]
        sizehint!(rebuilt, length(rowgroups))
        cumulative = Int64(0)
        for (group, groupindexes) in zip(rowgroups, indexes)
            length(group.columns) == length(groupindexes) || throw(
                AssertionError("writer column and OffsetIndex counts differ"))
            _reservearray!(budget, Metadata.ColumnChunk,
                length(group.columns))
            _reserveobjects!(budget, length(group.columns))
            columns = Metadata.ColumnChunk[]
            sizehint!(columns, length(group.columns))
            for (chunk, index) in zip(group.columns, groupindexes)
                isempty(index.page_locations) && throw(AssertionError(
                    "writer cannot serialize an empty OffsetIndex"))
                offset = Base.checked_add(Int64(startoutput),
                    Int64(length(section)))
                bytes, charge, cumulative = _writeencodeoffsetindex(index,
                    cumulative, limits, budget)
                try
                    growth = _reserveoutputgrowth!(budget, length(bytes))
                    sectioncharge = _materializedsum(sectioncharge, growth)
                    append!(section, bytes)
                finally
                    _release!(budget, charge)
                end
                push!(columns, _writecolumnchunkoffsetindex(chunk, offset,
                    Int32(length(bytes))))
            end
            push!(rebuilt, _writerowgroupcolumns(group, columns))
        end
        _reserveoutputgrowth!(budget, length(section))
        append!(output, section)
        section = nothing
        _release!(budget, sectioncharge)
        return rebuilt
    catch
        resize!(output, startoutput)
        used = _budgetused(budget)
        used > startbudget && _release!(budget, used - startbudget)
        rethrow()
    end
end

function _offsetindexcumulative(current::Int64, length::Int64,
        limits::Limits)
    requested = try
        Base.checked_add(current, length)
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:page_index_bytes, typemax(Int64),
            limits.max_page_index_bytes))
    end
    _checklimit(:page_index_bytes, requested, limits.max_page_index_bytes)
    return requested
end

function _pageindexrangeend(offset::Int64, length::Int64,
        message::String)
    return try
        Base.checked_add(offset, length)
    catch err
        err isa OverflowError || rethrow()
        throw(FormatError(message))
    end
end

function _pageindexintervalcount(current::Int64, additional::Int64,
        limits::Limits)
    current >= 0 && additional >= 0 || throw(AssertionError(
        "page-index interval counts must be nonnegative"))
    requested = try
        Base.checked_add(current, additional)
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64),
            limits.max_container_elements))
    end
    _checklimit(:container_elements, requested,
        limits.max_container_elements)
    return requested
end

function _offsetindexrange(file::File, chunk::Metadata.ColumnChunk,
        cumulative::Int64, limits::Limits)
    offset = chunk.offset_index_offset
    length = chunk.offset_index_length
    columnoffset = chunk.column_index_offset
    columnlength = chunk.column_index_length
    (columnoffset === nothing) == (columnlength === nothing) ||
        throw(FormatError(
            "column-index offset and length must be present together"))
    (offset === nothing) == (length === nothing) || throw(FormatError(
        "column chunk offset-index offset and length must be present together"))
    columnoffset !== nothing && offset === nothing && throw(FormatError(
        "column index is present without its required offset index"))
    offset === nothing && return nothing, cumulative
    offset >= 4 || throw(FormatError(
        "offset-index offset $offset is inside the file header"))
    length > 0 || throw(FormatError(
        "offset-index length must be positive, got $length"))
    stop = _pageindexrangeend(offset, Int64(length),
        "offset-index range overflows Int64")
    stop <= file.footer.offset || throw(FormatError(
        "offset-index range extends past the footer"))
    cumulative = _offsetindexcumulative(cumulative, Int64(length), limits)
    return (offset, Int64(length)), cumulative
end

function _columnindexrange(file::File, chunk::Metadata.ColumnChunk)
    offset = chunk.column_index_offset
    length = chunk.column_index_length
    (offset === nothing) == (length === nothing) || throw(FormatError(
        "column-index offset and length must be present together"))
    offset === nothing && return nothing
    chunk.offset_index_offset === nothing && throw(FormatError(
        "column index is present without its required offset index"))
    offset >= 4 || throw(FormatError(
        "column-index offset $offset is inside the file header"))
    length > 0 || throw(FormatError(
        "column-index length must be positive, got $length"))
    stop = _pageindexrangeend(offset, Int64(length),
        "column-index range overflows Int64")
    stop <= file.footer.offset || throw(FormatError(
        "column-index range extends past the footer"))
    return (Int64(offset), Int64(length))
end

function _decodeoffsetindex(file::File, offset::Int64, length::Int64,
        limits::Limits, budget::_LiveByteBudget)
    start = _budgetused(budget)
    temporary = Int64(0)
    try
        temporary = _reserveobjects!(budget, 2)
        total = _checkedsourcelength(file.source)
        bytes = _readrangeexact(file.source, total, offset, length)
        reader = Thrift.Reader(bytes; limits=limits, budget=budget)
        index = Thrift.decode(reader, Metadata.OffsetIndex)
        Thrift.remaining(reader) == 0 || throw(FormatError(
            "offset index has trailing Compact Thrift bytes"))
        _release!(budget, temporary)
        return index
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _validatepageindexintervals!(intervals::Vector{_PageIndexInterval})
    sort!(intervals; by=interval -> (interval.first, interval.last,
        interval.index))
    previousend = Int64(-1)
    for interval in intervals
        interval.last > interval.first || throw(AssertionError(
            "page-index preflight retained an empty interval"))
        interval.first >= previousend || throw(FormatError(
            "physical column, page-index, or bloom-filter storage ranges overlap"))
        previousend = interval.last
    end
    return
end

function _appendpageindexintervals!(
        intervals::Union{Nothing,Vector{_PageIndexInterval}}, file::File,
        chunk::Metadata.ColumnChunk, node::SchemaNode)
    md = _chunkmetadata(chunk, node)
    physicalstart, physicalstop = _chunkrange(md, file.footer.offset)
    range = chunk.offset_index_offset === nothing ? nothing :
        (Int64(chunk.offset_index_offset), Int64(chunk.offset_index_length))
    columnrange = chunk.column_index_offset === nothing ? nothing :
        (Int64(chunk.column_index_offset), Int64(chunk.column_index_length))
    bloomrange = _bloomfilterrange(md, file.footer.offset)
    intervals === nothing && return range
    physicalstop > physicalstart && push!(intervals,
        _PageIndexInterval(physicalstart, physicalstop, false))
    for (declared, message) in ((range, "offset-index range overflows Int64"),
            (columnrange, "column-index range overflows Int64"),
            (bloomrange, "bloom-filter range overflows Int64"))
        declared === nothing && continue
        first, length = declared
        last = _pageindexrangeend(first, length, message)
        push!(intervals, _PageIndexInterval(first, last, true))
    end
    return range
end

function _preflightoffsetindexdeclarations(file::File,
        metadata::Metadata.FileMetaData, schema::Schema, limits::Limits)
    groups = length(metadata.row_groups)
    totalchunks = Int64(0)
    intervalcount = Int64(0)
    cumulative = Int64(0)
    for (groupindex, group) in enumerate(metadata.row_groups)
        length(group.columns) == length(schema.leaves) || throw(FormatError(
            "row group $groupindex column count does not match the schema"))
        totalchunks = try
            Base.checked_add(totalchunks, Int64(length(group.columns)))
        catch err
            err isa OverflowError || rethrow()
            throw(LimitError(:container_elements, typemax(Int64),
                limits.max_container_elements))
        end
        _checklimit(:container_elements, totalchunks,
            limits.max_container_elements)
        for (chunk, node) in zip(group.columns, schema.leaves)
            md = _chunkmetadata(chunk, node)
            physicalstart, physicalstop = _chunkrange(md,
                file.footer.offset)
            range, cumulative = _offsetindexrange(file, chunk, cumulative,
                limits)
            columnrange = _columnindexrange(file, chunk)
            bloomrange = _bloomfilterrange(md, file.footer.offset)
            additional = Int64(physicalstop > physicalstart) +
                Int64(range !== nothing) + Int64(columnrange !== nothing) +
                Int64(bloomrange !== nothing)
            intervalcount = _pageindexintervalcount(intervalcount,
                additional, limits)
        end
    end
    return _PageIndexRangePreflight(groups, intervalcount, cumulative, false)
end

function _validatepageindexdeclarationoverlaps!(file::File,
        metadata::Metadata.FileMetaData, schema::Schema,
        preflight::_PageIndexRangePreflight, budget::_LiveByteBudget)
    preflight.overlaps_validated && return preflight
    intervalcharge = Int64(0)
    try
        intervalcharge = _reservearray!(budget, _PageIndexInterval,
            preflight.intervalcount)
        intervals = _PageIndexInterval[]
        sizehint!(intervals, Int(preflight.intervalcount))
        for group in metadata.row_groups
            for (chunk, node) in zip(group.columns, schema.leaves)
                _appendpageindexintervals!(intervals, file, chunk, node)
            end
        end
        length(intervals) == preflight.intervalcount || throw(AssertionError(
            "page-index declaration count changed during preflight"))
        _validatepageindexintervals!(intervals)
    finally
        iszero(intervalcharge) || _release!(budget, intervalcharge)
    end
    return _PageIndexRangePreflight(preflight.groups,
        preflight.intervalcount, preflight.cumulative, true)
end

function _preflightoffsetindexranges(file::File,
        metadata::Metadata.FileMetaData, schema::Schema, limits::Limits,
        budget::_LiveByteBudget, preflight::_PageIndexRangePreflight)
    groups = preflight.groups
    intervalcount = preflight.intervalcount
    cumulative = preflight.cumulative
    rangecharge = Int64(0)
    intervalcharge = Int64(0)
    try
        rangecharge = _reservearray!(budget,
            Vector{Union{Nothing,_OffsetIndexRange}}, groups)
        if !preflight.overlaps_validated
            intervalcharge = _reservearray!(budget, _PageIndexInterval,
                intervalcount)
        end
        intervals = preflight.overlaps_validated ? nothing :
            _PageIndexInterval[]
        intervals === nothing || sizehint!(intervals, Int(intervalcount))
        ranges = Vector{Union{Nothing,_OffsetIndexRange}}[]
        sizehint!(ranges, groups)
        for group in metadata.row_groups
            innercharge = _reservearray!(budget,
                Union{Nothing,_OffsetIndexRange}, length(group.columns))
            rangecharge = _materializedsum(rangecharge, innercharge)
            groupranges = Union{Nothing,_OffsetIndexRange}[]
            sizehint!(groupranges, length(group.columns))
            for (chunk, node) in zip(group.columns, schema.leaves)
                range = _appendpageindexintervals!(intervals, file, chunk,
                    node)
                push!(groupranges, range)
            end
            push!(ranges, groupranges)
        end
        if intervals !== nothing
            length(intervals) == intervalcount || throw(AssertionError(
                "page-index declaration count changed during materialization"))
            _validatepageindexintervals!(intervals)
        end
        _release!(budget, intervalcharge)
        intervalcharge = Int64(0)
        return ranges, cumulative, rangecharge
    catch
        iszero(intervalcharge) || _release!(budget, intervalcharge)
        iszero(rangecharge) || _release!(budget, rangecharge)
        rethrow()
    end
end

function _preflightoffsetindexranges(file::File,
        metadata::Metadata.FileMetaData, schema::Schema, limits::Limits,
        budget::_LiveByteBudget)
    preflight = _preflightoffsetindexdeclarations(file, metadata, schema,
        limits)
    return _preflightoffsetindexranges(file, metadata, schema, limits,
        budget, preflight)
end

function _offsetindexpagerows(frame::PageFrame,
        md::Metadata.ColumnMetaData, node::SchemaNode, limits::Limits,
        budget::_LiveByteBudget)
    kind = pagekind(frame)
    if kind === :data_v2
        header = frame.header.data_page_header_v2
        rows = Int64(header.num_rows)
        rows > 0 || throw(FormatError(
            "data page V2 has no rows and cannot be indexed"))
        entries = Int(header.num_values)
        if node.max_repetition_level == 0
            rows == entries || throw(FormatError(
                "flat data page V2 row and value counts differ"))
            return rows
        end
        entries > 0 || throw(FormatError(
            "nested data page V2 has no values"))
        _checklimit(:container_elements, entries,
            limits.max_container_elements)
        repetitionbytes = Int(header.repetition_levels_byte_length)
        repetitionbytes <= length(frame.payload) || throw(FormatError(
            "data page V2 repetition levels extend past its payload"))
        working = _materializedsum(
            _materializedarraybytes(UInt64, entries),
            _materializedproduct(2, _MATERIALIZED_OBJECT_BYTES))
        _reserve!(budget, working)
        try
            bytes = @view frame.payload[1:repetitionbytes]
            repetition = _decodelevelsv2(bytes, entries,
                Int(node.max_repetition_level), "repetition", limits)
            iszero(first(repetition)) || throw(FormatError(
                "nested data page V2 does not begin at a row boundary"))
            actual = Int64(Base.count(iszero, repetition))
            actual == rows || throw(FormatError(
                "data page V2 repetition levels do not match num_rows"))
        finally
            _release!(budget, working)
        end
        return rows
    end
    kind === :data_v1 || throw(AssertionError(
        "row counting requires a data page"))
    node.max_repetition_level == 0 && begin
        count = Int64(frame.header.data_page_header.num_values)
        count > 0 || throw(FormatError(
            "data page V1 has no values and cannot be indexed"))
        return count
    end
    header = frame.header.data_page_header
    count = Int(header.num_values)
    count > 0 || throw(FormatError(
        "data page V1 has no values and cannot begin a row"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    uncompressed = Int64(frame.header.uncompressed_page_size)
    working = _materializedsum(
        _materializedarraybytes(UInt8, uncompressed),
        _materializedarraybytes(UInt64, count))
    working = _materializedsum(working,
        _materializedproduct(3, _MATERIALIZED_OBJECT_BYTES))
    _reserve!(budget, working)
    try
        bytes = decompresspage(frame, md.codec; limits=limits, budget=budget)
        repetition, _ = _decodelevelv1(bytes, count,
            header.repetition_level_encoding,
            Int(node.max_repetition_level), 1, "repetition", limits)
        iszero(first(repetition)) || throw(FormatError(
            "nested data page V1 does not begin at a row boundary"))
        rows = Int64(Base.count(iszero, repetition))
        rows > 0 || throw(FormatError(
            "nested data page V1 has no row boundary"))
        return rows
    finally
        _release!(budget, working)
    end
end

function _offsetindexpageentries(frame::PageFrame, node::SchemaNode)
    kind = pagekind(frame)
    if kind === :data_v2
        header = frame.header.data_page_header_v2
        rows = Int64(header.num_rows)
        rows > 0 || throw(FormatError(
            "data page V2 has no rows and cannot be indexed"))
        entries = Int64(header.num_values)
        if node.max_repetition_level == 0
            rows == entries || throw(FormatError(
                "flat data page V2 row and value counts differ"))
        else
            entries > 0 || throw(FormatError(
                "nested data page V2 has no values"))
        end
        return entries
    end
    kind === :data_v1 || throw(AssertionError(
        "entry counting requires a data page"))
    entries = Int64(frame.header.data_page_header.num_values)
    entries > 0 || throw(FormatError(
        "data page V1 has no values and cannot be indexed"))
    return entries
end

function _validateoffsetindexsizes(index::Metadata.OffsetIndex,
        node::SchemaNode)
    sizes = index.unencoded_byte_array_data_bytes
    sizes === nothing && return
    node.element.type_ == Metadata.Type.BYTE_ARRAY || throw(FormatError(
        "offset-index byte-array sizes require a BYTE_ARRAY column"))
    length(sizes) == length(index.page_locations) || throw(FormatError(
        "offset-index byte-array size count does not match its page locations"))
    all(value -> value >= 0, sizes) || throw(FormatError(
        "offset-index byte-array sizes must be nonnegative"))
    return
end

function _validateoffsetindexframes(file::File,
        chunk::Metadata.ColumnChunk, node::SchemaNode, rows::Int64,
        index::Metadata.OffsetIndex, limits::Limits,
        budget::_LiveByteBudget)
    rows >= 0 || throw(FormatError(
        "offset index belongs to a negative-row row group"))
    md = _chunkmetadata(chunk, node)
    start, stop = _chunkrange(md, file.footer.offset)
    locations = index.page_locations
    _validateoffsetindexsizes(index, node)
    row = Int64(0)
    values = Int64(0)
    locationindex = 1
    position = start
    dictionaryseen = false
    dataseen = false
    indexseen = false
    framecount = Int64(0)
    while position < stop
        frame = readpage(file.source, position, stop, limits; budget=budget)
        try
            frameend = pageend(frame)
            frameend > position || throw(FormatError(
                "column chunk contains a nonadvancing page frame"))
            frameend <= stop || throw(FormatError(
                "page frame extends past the column chunk"))
            kind = pagekind(frame)
            dictionaryseen, dataseen, indexseen = _chunkpageoffsetstate(md,
                position, kind, dictionaryseen, dataseen, indexseen)
            _validatedpageentrycount(frame, limits)
            if kind === :data_v1 || kind === :data_v2
                locationindex <= length(locations) || throw(FormatError(
                    "offset index omits a data page"))
                location = locations[locationindex]
                location.offset == position || throw(FormatError(
                    "offset-index page offset does not match its physical frame"))
                location.compressed_page_size > 0 || throw(FormatError(
                    "offset-index page frame size must be positive"))
                Int64(location.compressed_page_size) == frameend - position ||
                    throw(FormatError(
                        "offset-index page size does not match its physical frame"))
                location.first_row_index == row || throw(FormatError(
                    "offset-index first-row value does not match its data pages"))
                pagevalues = _offsetindexpageentries(frame, node)
                nextvalues = try
                    Base.checked_add(values, pagevalues)
                catch err
                    err isa OverflowError || rethrow()
                    throw(FormatError(
                        "offset-index value count overflows Int64"))
                end
                nextvalues <= md.num_values || throw(FormatError(
                    "offset-index data pages exceed the column value count"))
                framecount = _nextpageframecount(framecount, limits)
                pagerows = _offsetindexpagerows(frame, md, node, limits,
                    budget)
                values = nextvalues
                row = try
                    Base.checked_add(row, pagerows)
                catch err
                    err isa OverflowError || rethrow()
                    throw(FormatError("offset-index row count overflows Int64"))
                end
                row <= rows || throw(FormatError(
                    "offset-index data pages exceed the row-group row count"))
                locationindex += 1
            else
                framecount = _nextpageframecount(framecount, limits)
            end
            position = frameend
        finally
            _release!(budget, frame.materializedcharge)
        end
    end
    position == stop || throw(FormatError(
        "column chunk page walk does not end at its declared boundary"))
    _validatechunkpageoffsets(md, dictionaryseen, dataseen, indexseen)
    locationindex == length(locations) + 1 || throw(FormatError(
        "offset index contains a location with no physical data page"))
    row == rows || throw(FormatError(
        "offset-index data-page rows do not match the row group"))
    values == md.num_values || throw(FormatError(
        "offset-index data-page values do not match the column chunk"))
    return
end

function _readoffsetindexpreflighted(file::File,
        chunk::Metadata.ColumnChunk,
        node::SchemaNode, rows::Int64,
        range::Union{Nothing,_OffsetIndexRange}, limits::Limits,
        budget::_LiveByteBudget)
    range === nothing && return nothing
    start = _budgetused(budget)
    try
        offset, length = range
        index = _decodeoffsetindex(file, offset, length, limits, budget)
        _validateoffsetindexframes(file, chunk, node, rows, index, limits,
            budget)
        return index
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _rangesoverlap(left::_OffsetIndexRange,
        right::_OffsetIndexRange)
    left[1] >= 0 && right[1] >= 0 || throw(FormatError(
        "page-index interval offset must be nonnegative"))
    left[2] >= 0 && right[2] >= 0 || throw(FormatError(
        "page-index interval length must be nonnegative"))
    leftstop = _pageindexrangeend(left[1], left[2],
        "page-index interval range overflows Int64")
    rightstop = _pageindexrangeend(right[1], right[2],
        "page-index interval range overflows Int64")
    return left[1] < rightstop && right[1] < leftstop
end

function _readoffsetindex(file::File, chunk::Metadata.ColumnChunk,
        node::SchemaNode, rows::Int64, cumulative::Int64, limits::Limits,
        budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        range, cumulative = _offsetindexrange(file, chunk, cumulative,
            limits)
        columnrange = _columnindexrange(file, chunk)
        range === nothing && return nothing, cumulative
        md = _chunkmetadata(chunk, node)
        physicalstart, physicalstop = _chunkrange(md, file.footer.offset)
        physical = (physicalstart, physicalstop - physicalstart)
        iszero(physical[2]) || !_rangesoverlap(range, physical) ||
            throw(FormatError(
                "offset-index range overlaps its physical column chunk"))
        columnrange === nothing || !_rangesoverlap(range, columnrange) ||
            throw(FormatError(
                "column-index and offset-index ranges overlap"))
        if columnrange !== nothing && !iszero(physical[2])
            !_rangesoverlap(columnrange, physical) || throw(FormatError(
                "column-index range overlaps its physical column chunk"))
        end
        rawcharge = _reservearray!(budget, UInt8, range[2])
        index = _readoffsetindexpreflighted(file, chunk, node, rows,
            range, limits, budget)
        _release!(budget, rawcharge)
        return index, cumulative
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _readoffsetindex(file::File, chunk::Metadata.ColumnChunk,
        node::SchemaNode, rows::Int64, limits::Limits,
        budget::_LiveByteBudget)
    index, _ = _readoffsetindex(file, chunk, node, rows, Int64(0), limits,
        budget)
    return index
end

function _readoffsetindexes(file::File,
        metadata::Metadata.FileMetaData, schema::Schema;
        limits::Limits=Limits(), budget::_LiveByteBudget=_LiveByteBudget(limits),
        preflight::Union{Nothing,_PageIndexRangePreflight}=nothing)
    start = _budgetused(budget)
    try
        length(metadata.row_groups) <= limits.max_container_elements ||
            throw(LimitError(:container_elements,
                Int64(length(metadata.row_groups)),
                limits.max_container_elements))
        _reservearray!(budget,
            Vector{Union{Nothing,Metadata.OffsetIndex}},
            length(metadata.row_groups))
        output = Vector{Union{Nothing,Metadata.OffsetIndex}}[]
        sizehint!(output, length(metadata.row_groups))
        ranges, cumulative, rangecharge = if preflight === nothing
            _preflightoffsetindexranges(file, metadata, schema, limits, budget)
        else
            _preflightoffsetindexranges(file, metadata, schema, limits, budget,
                preflight)
        end
        indexcharge = iszero(cumulative) ? Int64(0) :
            _reservearray!(budget, UInt8, cumulative)
        for (groupindex, (group, groupranges)) in enumerate(zip(
                metadata.row_groups, ranges))
            group.num_rows >= 0 || throw(FormatError(
                "row group $groupindex has a negative row count"))
            _reservearray!(budget, Union{Nothing,Metadata.OffsetIndex},
                length(group.columns))
            indexes = Union{Nothing,Metadata.OffsetIndex}[]
            sizehint!(indexes, length(group.columns))
            for (chunk, node, range) in zip(group.columns, schema.leaves,
                    groupranges)
                index = _readoffsetindexpreflighted(file, chunk, node,
                    group.num_rows, range, limits, budget)
                push!(indexes, index)
            end
            push!(output, indexes)
        end
        _release!(budget, _materializedsum(rangecharge, indexcharge))
        return output
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _readoffsetindexes(file::File,
        metadata::Metadata.FileMetaData, schema::Schema, limits::Limits,
        budget::_LiveByteBudget)
    return _readoffsetindexes(file, metadata, schema; limits=limits,
        budget=budget)
end

function _validateoffsetindexes!(file::File,
        metadata::Metadata.FileMetaData, schema::Schema, limits::Limits,
        budget::_LiveByteBudget,
        preflight::Union{Nothing,_PageIndexRangePreflight}=nothing)
    start = _budgetused(budget)
    try
        _readoffsetindexes(file, metadata, schema; limits=limits,
            budget=budget, preflight=preflight)
    finally
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
    end
    return
end
