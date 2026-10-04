# Flat column chunk decoding: V1/V2 data pages, levels, and value encodings
# (Parquet 2.13.0 README "Data Pages", "Nulls", and "Column chunks").

function _physicaleltype(type::Metadata.Type.T)
    type == Metadata.Type.BOOLEAN && return Bool
    type == Metadata.Type.INT32 && return Int32
    type == Metadata.Type.INT64 && return Int64
    type == Metadata.Type.FLOAT && return Float32
    type == Metadata.Type.DOUBLE && return Float64
    type == Metadata.Type.BYTE_ARRAY && return Vector{UInt8}
    type == Metadata.Type.FIXED_LEN_BYTE_ARRAY && return Vector{UInt8}
    type == Metadata.Type.INT96 && throw(UnsupportedFeatureError(
        "INT96 columns are not supported; the type is deprecated in Parquet"))
    throw(FormatError("unknown physical type $type"))
end

function _leafdensebytes(::Type{T}, count::Integer,
    md::Metadata.ColumnMetaData) where {T}
    bytes = _materializedarraybytes(T, count)
    T === Vector{UInt8} || return bytes
    payload = Int64(md.total_uncompressed_size)
    payload >= 0 || throw(FormatError("negative column chunk uncompressed size"))
    bytes = _materializedsum(bytes,
        _materializedproduct(count, _MATERIALIZED_ARRAY_HEADER_BYTES))
    return _materializedsum(bytes, payload)
end

function _leaflevelbytes(count::Integer)
    bytes = _materializedarraybytes(UInt64, count)
    return _materializedsum(bytes, _materializedarraybytes(UInt64, count))
end

function _leafchildbytes(::Type{T}, values::Vector{T}) where {T}
    return Int64(0)
end

function _leafchildbytes(::Type{Vector{UInt8}}, values::Vector{Vector{UInt8}})
    bytes = Int64(0)
    for value in values
        bytes = _materializedsum(bytes,
            _materializedarraybytes(UInt8, length(value)))
    end
    return bytes
end

function _leafretainedbytes(::Type{T}, entries::Integer,
    values::Vector{T}) where {T}
    bytes = _materializedsum(_leaflevelbytes(entries),
        _materializedarraybytes(T, entries))
    return _materializedsum(bytes, _leafchildbytes(T, values))
end

function _pageentrycount(frame::PageFrame)
    kind = pagekind(frame)
    kind === :data_v1 && return Int64(frame.header.data_page_header.num_values)
    kind === :data_v2 && return Int64(frame.header.data_page_header_v2.num_values)
    kind === :dictionary &&
        return Int64(frame.header.dictionary_page_header.num_values)
    return Int64(0)
end

function _validatedpageentrycount(frame::PageFrame, limits::Limits)
    entries = _pageentrycount(frame)
    entries >= 0 || throw(FormatError("negative page value count"))
    if pagekind(frame) === :dictionary
        _checkdictionarypageencoding(
            frame.header.dictionary_page_header.encoding)
        _checklimit(:container_elements, entries,
            limits.max_container_elements)
    end
    return entries
end

function _pageworkingbytes(::Type{T}, frame::PageFrame) where {T}
    count = _pageentrycount(frame)
    count >= 0 || throw(FormatError("negative page value count"))
    uncompressed = Int64(frame.header.uncompressed_page_size)
    uncompressed >= 0 || throw(FormatError("negative uncompressed page size"))
    bytes = _materializedproduct(4, _MATERIALIZED_OBJECT_BYTES)
    bytes = _materializedsum(bytes,
        _materializedarraybytes(UInt8, uncompressed))
    kind = pagekind(frame)
    if kind in (:data_v1, :data_v2)
        bytes = _materializedsum(bytes, _leaflevelbytes(count))
        bytes = _materializedsum(bytes, _materializedarraybytes(T, count))
        bytes = _materializedsum(bytes,
            _materializedarraybytes(UInt64, count))
    elseif kind === :dictionary
        bytes = _materializedsum(bytes, _materializedarraybytes(T, count))
    end
    if T === Vector{UInt8} && kind in (:data_v1, :data_v2, :dictionary)
        bytes = _materializedsum(bytes,
            _materializedproduct(count, _MATERIALIZED_ARRAY_HEADER_BYTES))
        bytes = _materializedsum(bytes, uncompressed)
    end
    return bytes
end

function _levelbitwidth(maxlevel::Integer)
    maxlevel == 0 && return 0
    return 64 - leading_zeros(UInt64(maxlevel))
end

function _chunkmetadata(chunk::Metadata.ColumnChunk, node::SchemaNode)
    chunk.file_path === nothing || throw(FormatError("column chunks stored in another file are not supported"))
    chunk.crypto_metadata === nothing && chunk.encrypted_column_metadata === nothing ||
        throw(FormatError("encrypted column chunks are not supported"))
    md = chunk.meta_data
    md === nothing && throw(FormatError("column chunk has no metadata"))
    md.type_ == node.element.type_ ||
        throw(FormatError("column chunk type $(md.type_) does not match the schema type $(node.element.type_)"))
    md.path_in_schema == node.path ||
        throw(FormatError("column chunk path $(md.path_in_schema) does not match the schema path $(node.path)"))
    md.num_values >= 0 || throw(FormatError("negative column chunk value count"))
    md.total_compressed_size >= 0 || throw(FormatError("negative column chunk size"))
    md.total_uncompressed_size >= 0 ||
        throw(FormatError("negative column chunk uncompressed size"))
    return md
end

# The chunk starts at its dictionary page, or at the earliest data/index page. Only the
# dictionary offset retains the historical zero sentinel.
function _chunkstart(md::Metadata.ColumnMetaData)
    data = Int64(md.data_page_offset)
    data >= 0 || throw(FormatError("negative data page offset $data"))
    dictionary = md.dictionary_page_offset
    dictionary === nothing || dictionary >= 0 || throw(FormatError(
        "negative dictionary page offset $dictionary"))
    index = md.index_page_offset
    index === nothing || index >= 4 || throw(FormatError(
        "index page offset $index is inside the file header"))
    if dictionary !== nothing && dictionary > 0
        dictionary >= 4 || throw(FormatError(
            "dictionary page offset $dictionary is inside the file header"))
        data == 0 || data >= dictionary || throw(FormatError(
            "data page offset $data precedes the dictionary page offset $dictionary"))
        index === nothing || index >= dictionary || throw(FormatError(
            "index page offset $index precedes the dictionary page"))
        return Int64(dictionary)
    end
    index === nothing && return data
    data > 0 || throw(FormatError(
        "index page offset is present for a chunk with no data page"))
    return min(data, Int64(index))
end

function _chunkrange(md::Metadata.ColumnMetaData, footeroffset::Int64)
    footeroffset >= 0 || throw(FormatError("negative footer offset $footeroffset"))
    start = _chunkstart(md)
    size = Int64(md.total_compressed_size)
    size >= 0 || throw(FormatError("negative column chunk size $size"))
    if size == 0
        iszero(md.data_page_offset) || throw(FormatError(
            "data page offset is present for an empty column chunk"))
        dictionary = md.dictionary_page_offset
        (dictionary === nothing || dictionary == 0) || throw(FormatError(
            "dictionary page offset is present for an empty column chunk"))
        md.index_page_offset === nothing || throw(FormatError(
            "index page offset is present for an empty column chunk"))
        return start, start
    end
    start >= 4 || throw(FormatError("column chunk offset $start is inside the file header"))
    size <= footeroffset - start || throw(FormatError("column chunk extends past the footer at $footeroffset"))
    stop = start + size
    data = Int64(md.data_page_offset)
    data == 0 || data < stop || throw(FormatError(
        "data page offset $(md.data_page_offset) is outside the column chunk"))
    index = md.index_page_offset
    index === nothing || index < stop || throw(FormatError(
        "index page offset $index is outside the column chunk"))
    return start, stop
end

function _chunkpageoffsetstate(md::Metadata.ColumnMetaData, position::Int64,
        kind::Symbol, dictionaryseen::Bool, dataseen::Bool,
        indexseen::Bool)
    dictionary = md.dictionary_page_offset
    realdictionary = dictionary !== nothing && dictionary > 0 ?
        Int64(dictionary) : nothing
    isdata = kind === :data_v1 || kind === :data_v2
    data = Int64(md.data_page_offset)
    legacydictionary = realdictionary === nothing &&
        kind === :dictionary && !dictionaryseen && !dataseen &&
        data > 0 && position == data && position == _chunkstart(md)
    if realdictionary !== nothing && position == realdictionary
        kind === :dictionary || throw(FormatError(
            "dictionary_page_offset does not point to a DICTIONARY_PAGE"))
    end
    if kind === :dictionary
        (realdictionary !== nothing && position == realdictionary) ||
            legacydictionary ||
            throw(FormatError(
                "dictionary page does not match dictionary_page_offset"))
        (!dictionaryseen && !dataseen) || throw(FormatError(
            "dictionary page is not the first physical page"))
        dictionaryseen = true
    end
    if data > 0 && position == data
        (isdata || legacydictionary) || throw(FormatError(
            "data_page_offset does not point to a data page"))
    end
    if isdata && !dataseen
        legacydata = realdictionary === nothing && dictionaryseen &&
            position > data
        (data > 0 && position == data) || legacydata || throw(FormatError(
            "first data page does not match data_page_offset"))
        dataseen = true
    end
    index = md.index_page_offset
    if index !== nothing && position == index
        kind === :index || throw(FormatError(
            "index_page_offset does not point to an INDEX_PAGE"))
        indexseen = true
    end
    return dictionaryseen, dataseen, indexseen
end

function _validatechunkpageoffsets(md::Metadata.ColumnMetaData,
        dictionaryseen::Bool, dataseen::Bool, indexseen::Bool)
    dictionary = md.dictionary_page_offset
    (dictionary === nothing || dictionary == 0 || dictionaryseen) ||
        throw(FormatError(
            "dictionary_page_offset does not identify a page frame"))
    (iszero(md.data_page_offset) || dataseen) || throw(FormatError(
        "data_page_offset does not identify a data page frame"))
    (md.index_page_offset === nothing || indexseen) || throw(FormatError(
        "index_page_offset does not identify a page frame"))
    return
end

function _fixedwidth(node::SchemaNode)
    node.element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY || return nothing
    return Int(node.element.type_length)
end

function _decodevalues(::Type{T}, bytes::AbstractVector{UInt8}, count::Int, ::Nothing,
    offset::Int, limits::Limits) where {T<:Union{Bool,Int32,Int64,Float32,Float64}}
    return decode_plain(T, bytes, count; offset=offset, limits=limits)
end

function _decodevalues(::Type{Vector{UInt8}}, bytes::AbstractVector{UInt8}, count::Int, ::Nothing,
    offset::Int, limits::Limits)
    return decode_plain_byte_array(bytes, count; offset=offset, limits=limits)
end

function _decodevalues(::Type{Vector{UInt8}}, bytes::AbstractVector{UInt8}, count::Int, width::Int,
    offset::Int, limits::Limits)
    matrix, position = decode_plain_fixed(bytes, count, width; offset=offset, limits=limits)
    values = Vector{Vector{UInt8}}(undef, count)
    for index in 1:count
        values[index] = matrix[:, index]
    end
    return values, position
end

function _validatelevels(levels::Vector{UInt64}, maxlevel::Int, name::String)
    atmaximum = 0
    for level in levels
        level <= maxlevel || throw(FormatError("$name level $level exceeds the maximum $maxlevel"))
        level == maxlevel && (atmaximum += 1)
    end
    return atmaximum
end

function _matrixvaluescharged(matrix::Matrix{UInt8},
    budget::_LiveByteBudget=_LiveByteBudget(Limits()))
    charge = _materializedarraybytes(Vector{UInt8}, size(matrix, 2))
    charge = _materializedsum(charge, _materializedproduct(size(matrix, 2),
        _materializedarraybytes(UInt8, size(matrix, 1))))
    _reserve!(budget, charge)
    values = Vector{Vector{UInt8}}(undef, size(matrix, 2))
    try
        for index in eachindex(values)
            values[index] = matrix[:, index]
        end
    catch
        _release!(budget, charge)
        rethrow()
    end
    return values, charge
end

function _matrixvalues(matrix::Matrix{UInt8},
    budget::_LiveByteBudget=_LiveByteBudget(Limits()))
    values, _ = _matrixvaluescharged(matrix, budget)
    return values
end

function _decodebooleanrle(bytes::AbstractVector{UInt8}, count::Int, offset::Int,
    limits::Limits)
    encoded, position = decode_hybrid(bytes, count, 1; offset=offset, length_prefix=true,
        limits=limits)
    values = Vector{Bool}(undef, count)
    for index in eachindex(encoded)
        encoded[index] <= 1 || throw(FormatError("RLE Boolean value $(encoded[index]) exceeds 1"))
        values[index] = !iszero(encoded[index])
    end
    return values, position
end

function _encodingerror(encoding::Metadata.Encoding.T, type)
    throw(FormatError("data page encoding $encoding is not valid for physical type $type"))
end

function _dictionaryselection(dictionary, indices::Vector{UInt64})
    count = UInt64(length(dictionary.values))
    for raw in indices
        raw < count || throw(FormatError(
            "dictionary index $raw is outside a dictionary of $count entries"))
    end
    return Int64(0)
end

function _dictionaryselection(dictionary::DecodedDictionary{Vector{UInt8}},
    indices::Vector{UInt64})
    count = UInt64(length(dictionary.values))
    bytes = Int64(0)
    for raw in indices
        raw < count || throw(FormatError(
            "dictionary index $raw is outside a dictionary of $count entries"))
        bytes = _materializedsum(bytes,
            length(dictionary.values[Int(raw) + 1]))
    end
    return bytes
end

function _decodedictionaryvaluesbudgeted(dictionary, bytes::AbstractVector{UInt8},
    count::Int, offset::Int, allowance::Integer, limits::Limits,
    budget::_LiveByteBudget)
    dictionary === nothing && throw(FormatError(
        "dictionary-encoded data page has no dictionary page"))
    indices, position = _decodedictionaryindices(bytes, count, offset, limits)
    selected = _dictionaryselection(dictionary, indices)
    expansion = max(Int64(0), selected - Int64(allowance))
    iszero(expansion) || _reserve!(budget, expansion)
    output = try
        _lookupdictionary(dictionary, indices)
    catch
        iszero(expansion) || _release!(budget, expansion)
        rethrow()
    end
    return output, position
end

function _decodeencodedvalues(::Type{T}, encoding::Metadata.Encoding.T,
    bytes::AbstractVector{UInt8}, count::Int, width, offset::Int, limits::Limits,
    budget::_LiveByteBudget) where {T}
    if encoding == Metadata.Encoding.PLAIN
        values, position = _decodevalues(T, bytes, count, width, offset, limits)
        return values, position, Int64(0)
    end
    _isdictionaryencoding(encoding) && return nothing
    if encoding == Metadata.Encoding.DELTA_BINARY_PACKED
        T <: Union{Int32,Int64} || return _encodingerror(encoding, T)
        return _decode_delta_binary_packed(T, bytes, count; offset=offset,
            limits=limits, budget=budget)
    elseif encoding == Metadata.Encoding.DELTA_LENGTH_BYTE_ARRAY
        T == Vector{UInt8} && width === nothing || return _encodingerror(encoding, T)
        return _decode_delta_length_byte_array(bytes, count; offset=offset,
            limits=limits, budget=budget)
    elseif encoding == Metadata.Encoding.DELTA_BYTE_ARRAY
        T == Vector{UInt8} || return _encodingerror(encoding, T)
        if width === nothing
            return _decode_delta_byte_array(bytes, count; offset=offset,
                limits=limits, budget=budget)
        end
        matrix, position, matrixcharge = _decode_delta_byte_array_fixed(bytes,
            count, width;
            offset=offset, limits=limits, budget=budget)
        values, valuescharge = try
            _matrixvaluescharged(matrix, budget)
        catch
            _release!(budget, matrixcharge)
            rethrow()
        end
        _release!(budget, matrixcharge)
        return values, position, valuescharge
    elseif encoding == Metadata.Encoding.BYTE_STREAM_SPLIT
        if T <: Union{Int32,Int64,Float32,Float64}
            values, position = decode_byte_stream_split(T, bytes, count;
                offset=offset, limits=limits)
            return values, position, Int64(0)
        elseif T == Vector{UInt8} && width !== nothing
            matrix, position = decode_byte_stream_split_fixed(bytes, count, width;
                offset=offset, limits=limits)
            values, valuescharge = _matrixvaluescharged(matrix, budget)
            return values, position, valuescharge
        end
        return _encodingerror(encoding, T)
    elseif encoding == Metadata.Encoding.RLE
        T == Bool || return _encodingerror(encoding, T)
        values, position = _decodebooleanrle(bytes, count, offset, limits)
        return values, position, Int64(0)
    end
    throw(FormatError("data page encoding $encoding is not supported"))
end

function _decodelevelv1(bytes::AbstractVector{UInt8}, count::Int,
    encoding::Metadata.Encoding.T, maxlevel::Int, offset::Int, name::String,
    limits::Limits)
    maxlevel == 0 && return zeros(UInt64, count), offset
    if encoding == Metadata.Encoding.RLE
        levels, position = decode_hybrid(bytes, count, _levelbitwidth(maxlevel); offset=offset,
            length_prefix=true, limits=limits)
    elseif encoding == Metadata.Encoding.BIT_PACKED
        levels, position = decode_bit_packed(bytes, count, _levelbitwidth(maxlevel);
            offset=offset, limits=limits)
    else
        throw(FormatError("$name level encoding $encoding is not supported"))
    end
    _validatelevels(levels, maxlevel, name)
    return levels, position
end

function _decodelevelsv1(bytes::AbstractVector{UInt8}, count::Int,
    header::Metadata.DataPageHeader, node::SchemaNode, limits::Limits)
    repetition, position = _decodelevelv1(bytes, count, header.repetition_level_encoding,
        Int(node.max_repetition_level), 1, "repetition", limits)
    definition, position = _decodelevelv1(bytes, count, header.definition_level_encoding,
        Int(node.max_definition_level), position, "definition", limits)
    present = _validatelevels(definition, Int(node.max_definition_level), "definition")
    return repetition, definition, present, position
end

function _decodelevelsv2(bytes::AbstractVector{UInt8}, count::Int, maxlevel::Int,
    name::String, limits::Limits)
    isempty(bytes) && maxlevel == 0 && return zeros(UInt64, count)
    levels, position = decode_hybrid(bytes, count, _levelbitwidth(maxlevel);
        offset=1, limits=limits)
    position == length(bytes) + 1 ||
        throw(FormatError("data page V2 $name levels have trailing bytes"))
    _validatelevels(levels, maxlevel, name)
    return levels
end

function _decodedatapage(::Type{T}, frame::PageFrame, md::Metadata.ColumnMetaData,
    node::SchemaNode, dictionary, remaining::Int, limits::Limits,
    budget::_LiveByteBudget) where {T}
    header = frame.header.data_page_header
    count = Int(header.num_values)
    count >= 0 || throw(FormatError("negative data page value count"))
    count <= remaining ||
        throw(FormatError("data pages carry more values than declared by the column chunk"))
    bytes = decompresspage(frame, md.codec; limits=limits, budget=budget)
    repetition, definition, present, position = _decodelevelsv1(bytes, count, header,
        node, limits)
    encoding = header.encoding
    decoded = _decodeencodedvalues(T, encoding, bytes, present, _fixedwidth(node),
        position, limits, budget)
    if decoded === nothing
        values, position = _decodedictionaryvaluesbudgeted(dictionary, bytes,
            present, position, frame.header.uncompressed_page_size, limits, budget)
    else
        values, position, _ = decoded
    end
    position == length(bytes) + 1 ||
        throw(FormatError("data page has $(length(bytes) - position + 1) bytes after its values"))
    return repetition, definition, values
end

function _v2values(frame::PageFrame, codec::Metadata.CompressionCodec.T,
        limits::Limits, budget::_LiveByteBudget)
    header = frame.header
    data = header.data_page_header_v2
    repetition = Int64(data.repetition_levels_byte_length)
    definition = Int64(data.definition_levels_byte_length)
    repetition >= 0 || throw(FormatError("negative data page V2 repetition-level byte length"))
    definition >= 0 || throw(FormatError("negative data page V2 definition-level byte length"))
    levels = repetition + definition
    compressed = Int64(header.compressed_page_size)
    uncompressed = Int64(header.uncompressed_page_size)
    levels <= compressed || throw(FormatError("data page V2 levels exceed its compressed size"))
    levels <= uncompressed || throw(FormatError("data page V2 levels exceed its uncompressed size"))
    repetition <= typemax(Int) && definition <= typemax(Int) && levels <= typemax(Int) ||
        throw(FormatError("data page V2 level lengths overflow Int"))
    repetitionlength = Int(repetition)
    definitionlength = Int(definition)
    levellength = Int(levels)
    repetitionbytes = @view frame.payload[1:repetitionlength]
    definitionbytes = @view frame.payload[(repetitionlength + 1):levellength]
    encoded = @view frame.payload[(levellength + 1):end]
    expected = Int(uncompressed - levels)
    actual = Int(compressed - levels)
    length(encoded) == actual || throw(FormatError("data page V2 value-section size is inconsistent"))
    compressedvalues = something(data.is_compressed, true)
    values = isempty(encoded) && expected == 0 ? UInt8[] :
        decompress(compressedvalues ? codec : Metadata.CompressionCodec.UNCOMPRESSED,
            encoded, expected; limits=limits, budget=budget)
    return repetitionbytes, definitionbytes, values
end

function _decodedatapagev2(::Type{T}, frame::PageFrame, md::Metadata.ColumnMetaData,
    node::SchemaNode, dictionary, remaining::Int, limits::Limits,
    budget::_LiveByteBudget) where {T}
    header = frame.header.data_page_header_v2
    count = Int(header.num_values)
    nulls = Int(header.num_nulls)
    rows = Int(header.num_rows)
    count >= 0 || throw(FormatError("negative data page V2 value count"))
    0 <= nulls <= count || throw(FormatError("invalid data page V2 null count $nulls for $count values"))
    rows >= 0 || throw(FormatError("negative data page V2 row count"))
    count <= remaining ||
        throw(FormatError("data pages carry more values than declared by the column chunk"))
    repetitionbytes, definitionbytes, bytes = _v2values(frame, md.codec,
        limits, budget)
    repetition = _decodelevelsv2(repetitionbytes, count,
        Int(node.max_repetition_level), "repetition", limits)
    definition = _decodelevelsv2(definitionbytes, count,
        Int(node.max_definition_level), "definition", limits)
    isempty(repetition) || iszero(first(repetition)) ||
        throw(FormatError("data page V2 starts with repetition level $(first(repetition))"))
    actualrows = Base.count(iszero, repetition)
    rows == actualrows ||
        throw(FormatError("data page V2 declares $rows rows but its repetition levels contain $actualrows"))
    present = _validatelevels(definition, Int(node.max_definition_level), "definition")
    count - present == nulls ||
        throw(FormatError("data page V2 declares $nulls nulls but its definition levels contain $(count - present)"))
    decoded = _decodeencodedvalues(T, header.encoding, bytes, present,
        _fixedwidth(node), 1, limits, budget)
    if decoded === nothing
        values, position = _decodedictionaryvaluesbudgeted(dictionary, bytes,
            present, 1, frame.header.uncompressed_page_size, limits, budget)
    else
        values, position, _ = decoded
    end
    position == length(bytes) + 1 ||
        throw(FormatError("data page V2 has $(length(bytes) - position + 1) bytes after its values"))
    return repetition, definition, values
end

function _appendleafpage!(repetition::Vector{UInt64}, definition::Vector{UInt64},
    values::Vector{T}, produced::Int, page) where {T}
    pagerepetition, pagedefinition, pagevalues = page
    entries = length(pagerepetition)
    length(pagedefinition) == entries ||
        throw(FormatError("data page repetition and definition counts differ"))
    copyto!(repetition, produced + 1, pagerepetition, 1, entries)
    copyto!(definition, produced + 1, pagedefinition, 1, entries)
    append!(values, pagevalues)
    return produced + entries
end

function _leafoperationcharge(budget::_LiveByteBudget, start::Int64)
    used = _budgetused(budget)
    used >= start || throw(AssertionError(
        "leaf decoding released bytes owned by its caller"))
    return used - start
end

function _transferleafcharge!(budget::_LiveByteBudget, start::Int64,
    releasing::Int64, retained::Int64)
    charge = _leafoperationcharge(budget, start)
    releasing <= charge || throw(AssertionError(
        "leaf decoding releases more bytes than it owns"))
    available = charge - releasing
    available >= retained || _reserve!(budget, retained - available)
    return
end

function _readleafstream(::Type{T}, src::AbstractSource, md::Metadata.ColumnMetaData,
    node::SchemaNode, start::Int64, stop::Int64, limits::Limits,
    budget::_LiveByteBudget;
    expected_rows=nothing) where {T}
    operationstart = _budgetused(budget)
    try
        count = Int(md.num_values)
        _reserve!(budget, _leaflevelbytes(count))
        _reserve!(budget, _leafdensebytes(T, count, md))
        repetition = Vector{UInt64}(undef, count)
        definition = Vector{UInt64}(undef, count)
        values = T[]
        sizehint!(values, count)
        retainedbase = _materializedsum(_leaflevelbytes(count),
            _materializedarraybytes(T, count))
        childbytes = Int64(0)
        position = start
        framecount = Int64(0)
        produced = 0
        dictionary = nothing
        dictionarycharge = Int64(0)
        seendata = false
        indexseen = false
        while position < stop
            frame = readpage(src, position, stop, limits; budget=budget)
            frameend = position
            workingcharge = Int64(0)
            try
                frameend = pageend(frame)
                frameend > position || throw(FormatError(
                    "column chunk contains a nonadvancing page frame"))
                frameend <= stop || throw(FormatError(
                    "page frame extends past the column chunk"))
                kind = pagekind(frame)
                dictionaryseen = dictionary !== nothing
                dictionaryseen, seendata, indexseen =
                    _chunkpageoffsetstate(md, position, kind,
                        dictionaryseen, seendata, indexseen)
                entries = _validatedpageentrycount(frame, limits)
                if kind === :data_v1 || kind === :data_v2
                    entries <= count - produced || throw(FormatError(
                        "data pages carry more values than declared by the column chunk"))
                end
                framecount = _nextpageframecount(framecount, limits)
                requested = _pageworkingbytes(T, frame)
                _reserve!(budget, requested)
                workingcharge = requested
                if kind === :data_v1
                    page = _decodedatapage(T, frame, md, node, dictionary,
                        count - produced, limits, budget)
                    childbytes = _materializedsum(childbytes,
                        _leafchildbytes(T, page[3]))
                    produced = _appendleafpage!(repetition, definition, values,
                        produced, page)
                    retained = _materializedsum(retainedbase, childbytes)
                    releasing = _materializedsum(workingcharge,
                        frame.materializedcharge)
                    _transferleafcharge!(budget, operationstart, releasing,
                        retained)
                    seendata = true
                elseif kind === :data_v2
                    page = _decodedatapagev2(T, frame, md, node, dictionary,
                        count - produced, limits, budget)
                    childbytes = _materializedsum(childbytes,
                        _leafchildbytes(T, page[3]))
                    produced = _appendleafpage!(repetition, definition, values,
                        produced, page)
                    retained = _materializedsum(retainedbase, childbytes)
                    releasing = _materializedsum(workingcharge,
                        frame.materializedcharge)
                    _transferleafcharge!(budget, operationstart, releasing,
                        retained)
                    seendata = true
                elseif kind === :dictionary
                    seendata && throw(FormatError(
                        "dictionary page follows a data page in the column chunk"))
                    dictionary === nothing || throw(FormatError(
                        "column chunk has more than one dictionary page"))
                    dictionary = _decodedictionarypage(T, frame, md, node,
                        limits, budget)
                    dictionarycharge = workingcharge
                    workingcharge = Int64(0)
                end
            finally
                iszero(workingcharge) || _release!(budget, workingcharge)
                _release!(budget, frame.materializedcharge)
            end
            position = frameend
        end
        position == stop || throw(FormatError(
            "column chunk page walk does not end at its declared boundary"))
        _validatechunkpageoffsets(md, dictionary !== nothing, seendata,
            indexseen)
        produced == count || throw(FormatError(
            "column chunk produced $produced of the $count declared values"))
        stream = LeafStream(repetition, definition, values,
            Int(node.max_repetition_level), Int(node.max_definition_level);
            expected_rows=expected_rows)
        retained = _leafretainedbytes(T, count, values)
        retained == _materializedsum(retainedbase, childbytes) ||
            throw(AssertionError("leaf retained-byte accounting is inconsistent"))
        if !iszero(dictionarycharge)
            _transferleafcharge!(budget, operationstart, dictionarycharge,
                retained)
            _release!(budget, dictionarycharge)
        end
        charge = _leafoperationcharge(budget, operationstart)
        charge >= retained || throw(AssertionError(
            "leaf decoding did not retain enough bytes for its result"))
        charge == retained || _release!(budget, charge - retained)
        return stream
    catch
        charge = _leafoperationcharge(budget, operationstart)
        iszero(charge) || _release!(budget, charge)
        rethrow()
    end
end

function _flatcolumn(stream::LeafStream{T}, node::SchemaNode,
    budget::_LiveByteBudget) where {T}
    node.max_repetition_level == 0 ||
        throw(FormatError("repeated columns are not supported yet"))
    node.max_definition_level == 0 && return stream.values
    outputcharge = _reservearray!(budget, Union{Missing,T}, length(stream))
    output = Vector{Union{Missing,T}}(undef, length(stream))
    try
        nextvalue = 1
        maxdefinition = UInt64(node.max_definition_level)
        for index in eachindex(stream.definition)
            if stream.definition[index] == maxdefinition
                output[index] = stream.values[nextvalue]
                nextvalue += 1
            else
                output[index] = missing
            end
        end
    catch
        _release!(budget, outputcharge)
        rethrow()
    end
    return output
end

function _flatcolumn(stream::LeafStream{T}, node::SchemaNode) where {T}
    return _flatcolumn(stream, node, _LiveByteBudget(Limits()))
end

function _flattenleafstream(stream::LeafStream, node::SchemaNode,
    budget::_LiveByteBudget)
    output = try
        _flatcolumn(stream, node, budget)
    catch
        _release!(budget, _leafretainedbytes(eltype(stream.values),
            length(stream), stream.values))
        rethrow()
    end
    _release!(budget, _leaflevelbytes(length(stream)))
    if output !== stream.values
        _release!(budget, _materializedarraybytes(eltype(stream.values),
            length(stream)))
    end
    return output
end

"""
    readleafstream(src, chunk, node, footeroffset; expected_rows, limits) -> LeafStream

Decode one physical column chunk into repetition levels, definition levels, and dense
present values. `expected_rows` optionally validates the number of zero repetition levels.
"""
function readleafstream(src::AbstractSource, chunk::Metadata.ColumnChunk,
    node::SchemaNode, footeroffset::Integer; expected_rows=nothing,
    limits::Limits=Limits(), budget::_LiveByteBudget=_LiveByteBudget(limits))
    md = _chunkmetadata(chunk, node)
    T = _physicaleltype(md.type_)
    md.num_values <= typemax(Int) || throw(FormatError("column chunk value count overflows"))
    _checklimit(:container_elements, md.num_values, limits.max_container_elements)
    typemin(Int64) <= footeroffset <= typemax(Int64) || throw(ArgumentError(
        "footer offset does not fit Int64"))
    footer = Int64(footeroffset)
    footer <= _checkedsourcelength(src) ||
        throw(FormatError("footer offset is past the end of the source"))
    start, stop = _chunkrange(md, footer)
    return _readleafstream(T, src, md, node, start, stop, limits, budget;
        expected_rows=expected_rows)
end

"""
    readcolumn(src, chunk, node, footeroffset; limits) -> Vector

Decode a flat (max repetition level 0) column chunk into a concrete
vector. Optional columns yield `Vector{Union{Missing,T}}`; required columns yield `Vector{T}`.
BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values are `Vector{UInt8}` at this physical layer.
"""
function readcolumn(src::AbstractSource, chunk::Metadata.ColumnChunk, node::SchemaNode, footeroffset::Integer;
    limits::Limits=Limits(), budget::_LiveByteBudget=_LiveByteBudget(limits))
    node.max_repetition_level == 0 ||
        throw(FormatError("repeated columns are not supported yet"))
    stream = readleafstream(src, chunk, node, footeroffset; limits=limits,
        budget=budget)
    return _flattenleafstream(stream, node, budget)
end

function _columnselection(metadata::Metadata.FileMetaData, schema::Schema,
    rowgroup::Integer, column::Integer)
    1 <= rowgroup <= length(metadata.row_groups) ||
        throw(ArgumentError("row group $rowgroup is out of range"))
    columns = metadata.row_groups[rowgroup].columns
    1 <= column <= length(schema.leaves) ||
        throw(ArgumentError("column $column is out of range"))
    length(columns) == length(schema.leaves) ||
        throw(FormatError("row group $rowgroup has $(length(columns)) column chunks for $(length(schema.leaves)) leaves"))
    return columns[column], schema.leaves[column]
end

function readleafstream(file::File, metadata::Metadata.FileMetaData, schema::Schema,
    rowgroup::Integer, column::Integer; expected_rows=nothing, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    chunk, node = _columnselection(metadata, schema, rowgroup, column)
    return readleafstream(file.source, chunk, node, file.footer.offset;
        expected_rows=expected_rows, limits=limits, budget=budget)
end

function readcolumn(file::File, metadata::Metadata.FileMetaData, schema::Schema, rowgroup::Integer,
    column::Integer; limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    chunk, node = _columnselection(metadata, schema, rowgroup, column)
    node.max_repetition_level == 0 ||
        throw(FormatError("repeated columns are not supported yet"))
    rows = metadata.row_groups[rowgroup].num_rows
    stream = readleafstream(file.source, chunk, node, file.footer.offset;
        expected_rows=rows, limits=limits, budget=budget)
    return _flattenleafstream(stream, node, budget)
end
