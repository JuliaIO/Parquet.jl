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
    type == Metadata.Type.INT96 && return Vector{UInt8}
    throw(FormatError("unknown physical type $type"))
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

function _levelbitwidth(maxlevel::Integer)
    maxlevel == 0 && return 0
    return 64 - leading_zeros(UInt64(maxlevel))
end

function _chunkmetadata(chunk::Metadata.ColumnChunk, node::SchemaNode)
    chunk.file_path === nothing || throw(FormatError("column chunks stored in another file are not supported"))
    chunk.crypto_metadata === nothing && chunk.encrypted_column_metadata === nothing ||
        throw(UnsupportedFeatureError("encrypted column chunks are not supported"))
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

# parquet-cpp before 1.4 (pyarrow < 0.9) wrote 0 for an absent index page, the same
# sentinel old writers used for the dictionary offset.
function _indexpageoffset(md::Metadata.ColumnMetaData)
    index = md.index_page_offset
    return index === nothing || iszero(index) ? nothing : Int64(index)
end

# The chunk starts at its dictionary page, or at the earliest data/index page. The
# dictionary and index offsets both retain the historical zero sentinel.
function _chunkstart(md::Metadata.ColumnMetaData)
    data = Int64(md.data_page_offset)
    data >= 0 || throw(FormatError("negative data page offset $data"))
    dictionary = md.dictionary_page_offset
    dictionary === nothing || dictionary >= 0 || throw(FormatError(
        "negative dictionary page offset $dictionary"))
    index = _indexpageoffset(md)
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
        _indexpageoffset(md) === nothing || throw(FormatError(
            "index page offset is present for an empty column chunk"))
        return start, start
    end
    start >= 4 || throw(FormatError("column chunk offset $start is inside the file header"))
    size <= footeroffset - start || throw(FormatError("column chunk extends past the footer at $footeroffset"))
    stop = start + size
    data = Int64(md.data_page_offset)
    data == 0 || data < stop || throw(FormatError(
        "data page offset $(md.data_page_offset) is outside the column chunk"))
    index = _indexpageoffset(md)
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
    index = _indexpageoffset(md)
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
    (_indexpageoffset(md) === nothing || indexseen) || throw(FormatError(
        "index_page_offset does not identify a page frame"))
    return
end

function _fixedwidth(node::SchemaNode)
    node.element.type_ == Metadata.Type.INT96 && return 12
    node.element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY || return nothing
    return Int(node.element.type_length)
end

# Per-value conversions from physical to logical values. One is chosen per column, so
# decoding loops are compiled for a concrete conversion and result type.
struct _DateValue end

function (::_DateValue)(value::Int32)
    return _fromparquetdate(value)
end

# TIMESTAMP values are their stored Int64 counts, so PLAIN pages copy straight into a
# column of them (`_bitcopy`).
struct _TimestampValue{L} end

function (::_TimestampValue{L})(value::Int64) where {L}
    return _timestampvalue(L, value)
end

struct _IntegerValue{L}
    element::Metadata.SchemaElement
end

function (convert::_IntegerValue{L})(value::Union{Int32,Int64}) where {L}
    L <: Signed && return _checkednarrow(L, value, convert.element, "INTEGER")
    sizeof(L) == sizeof(value) && return reinterpret(L, value)
    return _checkednarrow(L, value, convert.element, "unsigned INTEGER")
end

struct _TimeValue
    kind::_TimeLogicalKind
    element::Metadata.SchemaElement
end

function (convert::_TimeValue)(value::Union{Int32,Int64})
    return _fromparquettime(convert.kind, value, convert.element)
end

struct _Int96Value{P}
    element::Metadata.SchemaElement
end

function (convert::_Int96Value{P})(bytes::AbstractVector{UInt8}) where {P}
    return _fromint96(bytes, convert.element, P)::Durations.Timestamp{P}
end

# FIXED_LEN_BYTE_ARRAY values without an annotation read as `NTuple{N,UInt8}`, whose
# bytes PLAIN pages hold back to back.
struct _FixedBytesValue{N} end

function (::_FixedBytesValue{N})(bytes::Union{_ByteView,Vector{UInt8}}) where {N}
    length(bytes) == N || throw(FormatError(
        "fixed byte-array value has $(length(bytes)) bytes, expected $N"))
    return GC.@preserve bytes unsafe_load(Ptr{NTuple{N,UInt8}}(pointer(bytes)))
end

# Whether a conversion only reinterprets the bits of each physical value, so PLAIN
# values copy in bulk.
_bitcopy(convert) = false
_bitcopy(::_TimestampValue) = true

# Element types a read produces where the file leaves a choice: `int96` is the
# resolution of INT96 timestamps.
struct _ReadOptions
    int96::DataType
end

function _ReadOptions(; int96::Type=Dates.Nanosecond)
    int96 in (Dates.Nanosecond, Dates.Microsecond, Dates.Millisecond) ||
        throw(ArgumentError("int96 must be Dates.Nanosecond, Dates.Microsecond, or " *
            "Dates.Millisecond, got $int96"))
    return _ReadOptions(int96)
end

# Decimal, binary, and other annotations go through the general per-value conversion.
struct _LogicalValue{L,K}
    kind::K
    element::Metadata.SchemaElement
    limits::Limits
end

function (logical::_LogicalValue{L})(value) where {L}
    return convert(L, _logicalvalue(logical.kind, logical.element, value,
        logical.limits))::L
end

# The conversion of `element`'s physical values and the type it produces. STRING and
# ENUM values become DataStrings payloads, as do BYTE_ARRAY values without an
# annotation (`_StringPayloads`, whose column is a StringVector or BytesVector).
function _valueconverter(element::Metadata.SchemaElement, limits::Limits,
        options::_ReadOptions=_ReadOptions())
    physical = _physicaleltype(element.type_)
    kind = _logicalkind(element)
    if kind === nothing
        L = _logicaleltype(element, physical)
        L === DataStrings.DataBytes &&
            return _StringPayloads{false}(limits), DataStrings.StringPayload
        L <: NTuple && return _FixedBytesValue{fieldcount(L)}(), L
        return identity, physical
    end
    L = _logicaleltype(element, physical)
    kind in (:string, :enum) &&
        return _StringPayloads{true}(limits), DataStrings.StringPayload
    kind === :date && return _DateValue(), L
    if kind === :int96
        P = options.int96
        return _Int96Value{P}(element), Durations.Timestamp{P}
    end
    kind isa _TimestampLogicalKind && return _TimestampValue{L}(), L
    kind isa _IntegerLogicalKind && return _IntegerValue{L}(element), L
    kind isa _TimeLogicalKind && return _TimeValue(kind, element), L
    kind === :decimal && _preflightdecimalconversion(element, limits)
    return _LogicalValue{L,typeof(kind)}(kind, element, limits), L
end

# Scratch space one task reuses for every page it decodes. `dense` and `dictionary` hold
# one vector per value type: an optional page's present values and a chunk's dictionary.
mutable struct _PageBuffers
    page::Vector{UInt8}
    repetition::Vector{UInt16}
    definition::Vector{UInt16}
    indices::Vector{UInt32}
    dense::IdDict{Type,Vector}
    dictionary::IdDict{Type,Vector}
end

function _PageBuffers()
    return _PageBuffers(UInt8[], UInt16[], UInt16[], UInt32[], IdDict{Type,Vector}(),
        IdDict{Type,Vector}())
end

function _scratch(vectors::IdDict{Type,Vector}, ::Type{L}) where {L}
    return get!(() -> L[], vectors, L)::Vector{L}
end

# BYTE_ARRAY values as DataStrings payloads: up to 12 bytes inline, longer ones as views
# into a copy of the page they were decoded from, which `buffers` keeps. `page` is the
# current page's bytes; `kept` is the 0-based index of its copy in `buffers`, or -1
# before a value needed one. `V` is whether values must be UTF-8 (STRING and ENUM).
mutable struct _StringPayloads{V}
    limits::Limits
    buffers::Vector{Vector{UInt8}}
    page::_ByteView
    kept::Int
end

function _StringPayloads{V}(limits::Limits) where {V}
    return _StringPayloads{V}(limits, Vector{UInt8}[], _byteview(UInt8[]), -1)
end

function _isasciibytes(bytes::_ByteView)
    count = length(bytes)
    bits = UInt64(0)
    GC.@preserve bytes begin
        start = pointer(bytes)
        index = 0
        while index + 8 <= count
            bits |= unsafe_load(Ptr{UInt64}(start + index))
            index += 8
        end
        while index < count
            bits |= unsafe_load(start + index)
            index += 1
        end
    end
    return iszero(bits & 0x8080808080808080)
end

function _checkstringvalue(bytes::AbstractVector{UInt8}, limits::Limits)
    _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
    (bytes isa _ByteView && _isasciibytes(bytes)) || isvalid(String, bytes) ||
        throw(FormatError("STRING value contains invalid UTF-8"))
    return
end

function _checkpayloadvalue(strings::_StringPayloads{V},
        bytes::AbstractVector{UInt8}) where {V}
    V && return _checkstringvalue(bytes, strings.limits)
    _checklimit(:string_bytes, length(bytes), strings.limits.max_string_bytes)
    return
end

# A value inside the current page.
function (strings::_StringPayloads)(bytes::_ByteView)
    _checkpayloadvalue(strings, bytes)
    count = length(bytes)
    source = first(parentindices(bytes)[1])
    count <= DataStrings.INLINE_MAX &&
        return DataStrings.inline_payload(parent(bytes), source, count)
    page = strings.page
    parent(bytes) === parent(page) ||
        throw(AssertionError("STRING value is outside its page"))
    if strings.kept < 0
        push!(strings.buffers, copy(page))
        strings.kept = length(strings.buffers) - 1
    end
    position = source - first(parentindices(page)[1]) + 1
    return DataStrings.view_payload(strings.buffers[strings.kept + 1], position, count,
        strings.kept, position - 1)
end

# A value decoded into its own vector (DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY).
function (strings::_StringPayloads)(bytes::Vector{UInt8})
    _checkpayloadvalue(strings, bytes)
    count = length(bytes)
    count <= DataStrings.INLINE_MAX && return DataStrings.inline_payload(bytes, 1, count)
    push!(strings.buffers, bytes)
    return DataStrings.view_payload(bytes, 1, count, length(strings.buffers) - 1, 0)
end

# A page's decompressed bytes, in the reused page buffer (or the source, uncompressed).
function _pagebytes!(convert, buffers::_PageBuffers, codec::Metadata.CompressionCodec.T,
        payload::_ByteView, expected::Integer, limits::Limits)
    return _decompressinto!(buffers.page, codec, payload, expected, limits)
end

function _pagebytes!(strings::_StringPayloads, buffers::_PageBuffers,
        codec::Metadata.CompressionCodec.T, payload::_ByteView, expected::Integer,
        limits::Limits)
    bytes = _decompressinto!(buffers.page, codec, payload, expected, limits)
    strings.page = bytes
    strings.kept = -1
    return bytes
end

function _encodingerror(encoding::Metadata.Encoding.T, type)
    throw(FormatError("data page encoding $encoding is not valid for physical type $type"))
end

function _decodebooleanrle(bytes::_ByteView, count::Int, offset::Int)
    encoded, position = decode_hybrid(bytes, count, 1; offset=offset, length_prefix=true)
    values = Vector{Bool}(undef, count)
    for index in eachindex(encoded)
        encoded[index] <= 1 || throw(FormatError("RLE Boolean value $(encoded[index]) exceeds 1"))
        values[index] = !iszero(encoded[index])
    end
    return values, position
end

function _matrixvalues(matrix::Matrix{UInt8})
    return Vector{UInt8}[matrix[:, index] for index in axes(matrix, 2)]
end

# Values of the encodings other than PLAIN and dictionary as a physical vector, and the
# position after them.
function _decodeothervalues(::Type{T}, encoding::Metadata.Encoding.T,
    bytes::_ByteView, count::Int, width, offset::Int, limits::Limits) where {T}
    if encoding == Metadata.Encoding.DELTA_BINARY_PACKED
        T <: Union{Int32,Int64} || return _encodingerror(encoding, T)
        return decode_delta_binary_packed(T, bytes, count; offset=offset, limits=limits)
    elseif encoding == Metadata.Encoding.DELTA_LENGTH_BYTE_ARRAY
        T == Vector{UInt8} && width === nothing || return _encodingerror(encoding, T)
        return decode_delta_length_byte_array(bytes, count; offset=offset, limits=limits)
    elseif encoding == Metadata.Encoding.DELTA_BYTE_ARRAY
        T == Vector{UInt8} || return _encodingerror(encoding, T)
        width === nothing && return decode_delta_byte_array(bytes, count;
            offset=offset, limits=limits)
        matrix, position = decode_delta_byte_array_fixed(bytes, count, width;
            offset=offset, limits=limits)
        return _matrixvalues(matrix), position
    elseif encoding == Metadata.Encoding.BYTE_STREAM_SPLIT
        if T <: Union{Int32,Int64,Float32,Float64}
            return decode_byte_stream_split(T, bytes, count; offset=offset, limits=limits)
        elseif T == Vector{UInt8} && width !== nothing
            matrix, position = decode_byte_stream_split_fixed(bytes, count, width;
                offset=offset, limits=limits)
            return _matrixvalues(matrix), position
        end
        return _encodingerror(encoding, T)
    elseif encoding == Metadata.Encoding.RLE
        T == Bool || return _encodingerror(encoding, T)
        return _decodebooleanrle(bytes, count, offset)
    end
    throw(FormatError("data page encoding $encoding is not supported"))
end

function _convertvalue(::typeof(identity), value)
    return value
end

function _convertvalue(convert, value)
    return convert(value)
end

# Writes the `count` present values of one data page, encoded with `encoding` in
# `bytes[position:(stop - 1)]`, to `dest[(offset + 1):(offset + count)]`.
function _pagevalues!(dest::Vector{L}, offset::Int, count::Int, ::Type{P}, convert,
        width, encoding::Metadata.Encoding.T, bytes::_ByteView, position::Int,
        stop::Int, dictionary::Vector{L}, hasdictionary::Bool, buffers::_PageBuffers,
        limits::Limits) where {L,P}
    if encoding == Metadata.Encoding.PLAIN
        P === Vector{UInt8} && return _plainbytes!(dest, offset, count, convert, width,
            bytes, position, limits)
        return _plain!(dest, offset, count, P, convert, bytes, position)
    elseif _isdictionaryencoding(encoding)
        hasdictionary || throw(FormatError(
            "dictionary-encoded data page has no dictionary page"))
        return _dictionary!(dest, offset, count, dictionary, buffers.indices, bytes,
            position, stop)
    end
    values, position = _decodeothervalues(P, encoding, bytes, count, width, position,
        limits)
    _growto!(dest, offset + count)
    for index in 1:count
        dest[offset + index] = _convertvalue(convert, values[index])
    end
    return position
end

function _levelerror(levels::Vector{UInt16}, count::Int, maxlevel::Int, name::String)
    for index in 1:count
        level = levels[index]
        level <= maxlevel || throw(FormatError(
            "$name level $level exceeds the maximum $maxlevel"))
    end
    return
end

# Checks the first `count` levels against `maxlevel` and counts those at it.
function _validatelevels(levels::Vector{UInt16}, count::Int, maxlevel::Int, name::String)
    top = UInt16(maxlevel)
    exceeded = false
    atmaximum = 0
    @inbounds for index in 1:count
        level = levels[index]
        exceeded |= level > top
        atmaximum += level == top
    end
    exceeded && _levelerror(levels, count, maxlevel, name)
    return atmaximum
end

# Data page V1 levels: a length-prefixed RLE hybrid stream or deprecated BIT_PACKED.
function _levelsv1!(levels::Vector{UInt16}, bytes::_ByteView, position::Int, count::Int,
        encoding::Metadata.Encoding.T, maxlevel::Int, name::String, limits::Limits)
    maxlevel == 0 && return position
    bitwidth = _levelbitwidth(maxlevel)
    if encoding == Metadata.Encoding.RLE
        start, stop = _hybridbounds(bytes, position, true)
        _growto!(levels, _claimcapacity(count, stop - start))
        position = _decodehybrid!(levels, 0, count, bitwidth, bytes, start, stop)
        position == stop || throw(FormatError("hybrid stream has trailing bytes"))
    elseif encoding == Metadata.Encoding.BIT_PACKED
        values, position = decode_bit_packed(bytes, count, bitwidth; offset=position,
            limits=limits)
        _growto!(levels, count)
        copyto!(levels, 1, values, 1, count)
    else
        throw(FormatError("$name level encoding $encoding is not supported"))
    end
    _validatelevels(levels, count, maxlevel, name)
    return position
end

# Data page V2 levels; returns whether any were stored.
function _levelsv2!(levels::Vector{UInt16}, bytes::_ByteView, count::Int, maxlevel::Int,
        name::String)
    isempty(bytes) && maxlevel == 0 && return false
    _growto!(levels, _claimcapacity(count, length(bytes)))
    position = _decodehybrid!(levels, 0, count, _levelbitwidth(maxlevel), bytes, 1,
        length(bytes) + 1)
    position == length(bytes) + 1 ||
        throw(FormatError("data page V2 $name levels have trailing bytes"))
    _validatelevels(levels, count, maxlevel, name)
    return true
end

function _datapagev1!(sink, ::Type{P}, convert, frame::PageFrame,
        md::Metadata.ColumnMetaData, node::SchemaNode, dictionary::Vector,
        hasdictionary::Bool, remaining::Int, buffers::_PageBuffers,
        limits::Limits) where {P}
    header = frame.header.data_page_header
    count = Int(header.num_values)
    count >= 0 || throw(FormatError("negative data page value count"))
    count <= remaining ||
        throw(FormatError("data pages carry more values than declared by the column chunk"))
    bytes = _pagebytes!(convert, buffers, md.codec, _byteview(frame.payload),
        frame.header.uncompressed_page_size, limits)
    maxrepetition = Int(node.max_repetition_level)
    maxdefinition = Int(node.max_definition_level)
    position = _levelsv1!(buffers.repetition, bytes, 1, count,
        header.repetition_level_encoding, maxrepetition, "repetition", limits)
    position = _levelsv1!(buffers.definition, bytes, position, count,
        header.definition_level_encoding, maxdefinition, "definition", limits)
    present = maxdefinition == 0 ? count :
        _validatelevels(buffers.definition, count, maxdefinition, "definition")
    valuestart = position
    finish = _sinkpage!(sink, count, present, maxrepetition > 0, maxdefinition > 0,
        buffers) do dest, offset
        return _pagevalues!(dest, offset, present, P, convert, _fixedwidth(node),
            header.encoding, bytes, valuestart, length(bytes) + 1, dictionary,
            hasdictionary, buffers, limits)
    end
    finish == length(bytes) + 1 ||
        throw(FormatError("data page has $(length(bytes) - finish + 1) bytes after its values"))
    return count
end

# The level sections of a data page V2 (stored uncompressed) and its value section,
# decompressed into `buffers.page` when compressed.
function _v2sections!(convert, buffers::_PageBuffers, frame::PageFrame,
        codec::Metadata.CompressionCodec.T, limits::Limits)
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
    payload = _byteview(frame.payload)
    repetitionbytes = view(payload, 1:Int(repetition))
    definitionbytes = view(payload, (Int(repetition) + 1):Int(levels))
    encoded = view(payload, (Int(levels) + 1):length(payload))
    expected = Int(uncompressed - levels)
    length(encoded) == compressed - levels ||
        throw(FormatError("data page V2 value-section size is inconsistent"))
    # An empty value section is not a compressed stream.
    compressedvalues = something(data.is_compressed, true) && !(isempty(encoded) &&
        expected == 0)
    values = _pagebytes!(convert, buffers,
        compressedvalues ? codec : Metadata.CompressionCodec.UNCOMPRESSED, encoded,
        expected, limits)
    return repetitionbytes, definitionbytes, values
end

function _datapagev2!(sink, ::Type{P}, convert, frame::PageFrame,
        md::Metadata.ColumnMetaData, node::SchemaNode, dictionary::Vector,
        hasdictionary::Bool, remaining::Int, buffers::_PageBuffers,
        limits::Limits) where {P}
    header = frame.header.data_page_header_v2
    count = Int(header.num_values)
    rows = Int(header.num_rows)
    count >= 0 || throw(FormatError("negative data page V2 value count"))
    rows >= 0 || throw(FormatError("negative data page V2 row count"))
    count <= remaining ||
        throw(FormatError("data pages carry more values than declared by the column chunk"))
    repetitionbytes, definitionbytes, bytes = _v2sections!(convert, buffers, frame,
        md.codec, limits)
    maxrepetition = Int(node.max_repetition_level)
    maxdefinition = Int(node.max_definition_level)
    hasrepetition = _levelsv2!(buffers.repetition, repetitionbytes, count,
        maxrepetition, "repetition")
    hasdefinition = _levelsv2!(buffers.definition, definitionbytes, count,
        maxdefinition, "definition")
    actualrows = count
    if hasrepetition && count > 0
        first = buffers.repetition[1]
        iszero(first) || throw(FormatError(
            "data page V2 starts with repetition level $first"))
        actualrows = Base.count(iszero, view(buffers.repetition, 1:count))
    end
    rows == actualrows ||
        throw(FormatError("data page V2 declares $rows rows but its repetition levels contain $actualrows"))
    # num_nulls is advisory: writers disagree on whether empty and null ancestors count
    # (Arrow C++ counts only leaf-level nulls), so the definition levels decide.
    present = hasdefinition ?
        _validatelevels(buffers.definition, count, maxdefinition, "definition") : count
    position = _sinkpage!(sink, count, present, hasrepetition, hasdefinition,
        buffers) do dest, offset
        return _pagevalues!(dest, offset, present, P, convert, _fixedwidth(node),
            header.encoding, bytes, 1, length(bytes) + 1, dictionary, hasdictionary,
            buffers, limits)
    end
    position == length(bytes) + 1 ||
        throw(FormatError("data page V2 has $(length(bytes) - position + 1) bytes after its values"))
    return count
end

# Budget charge of one page: its decompressed bytes (twice for byte arrays, whose values
# may copy them) and its decoded entries.
function _pagecharge(::Type{P}, frame::PageFrame, entries::Int64) where {P}
    uncompressed = Int64(max(frame.header.uncompressed_page_size, 0))
    P === Vector{UInt8} || return uncompressed + 16 * entries
    return 2 * uncompressed + 64 * entries
end

# Decodes the pages of one column chunk in order, passing each data page's levels and
# values to `sink`. Page positions, the dictionary page, and the declared value count
# are validated against the chunk metadata.
function _readchunk!(sink, ::Type{P}, ::Type{L}, convert, src::AbstractSource,
        md::Metadata.ColumnMetaData, node::SchemaNode, start::Int64, stop::Int64,
        limits::Limits, budget::_ByteBudget, buffers::_PageBuffers) where {P,L}
    src isa MemorySource || (src = _ChunkSource(src, start, stop))
    count = Int(md.num_values)
    dictionary = _scratch(buffers.dictionary, L)
    hasdictionary = false
    seendata = false
    indexseen = false
    produced = 0
    framecount = Int64(0)
    position = start
    while position < stop
        frame = readpage(src, position, stop, limits)
        frameend = pageend(frame)
        frameend > position || throw(FormatError(
            "column chunk contains a nonadvancing page frame"))
        frameend <= stop || throw(FormatError(
            "page frame extends past the column chunk"))
        kind = pagekind(frame)
        dictionaryseen, seendata, indexseen = _chunkpageoffsetstate(md, position,
            kind, hasdictionary, seendata, indexseen)
        entries = _validatedpageentrycount(frame, limits)
        if kind === :data_v1 || kind === :data_v2
            entries <= count - produced || throw(FormatError(
                "data pages carry more values than declared by the column chunk"))
        end
        framecount = _nextpageframecount(framecount, limits)
        _charge!(budget, _pagecharge(P, frame, entries))
        if kind === :data_v1
            produced += _datapagev1!(sink, P, convert, frame, md, node, dictionary,
                hasdictionary, count - produced, buffers, limits)
        elseif kind === :data_v2
            produced += _datapagev2!(sink, P, convert, frame, md, node, dictionary,
                hasdictionary, count - produced, buffers, limits)
        elseif kind === :dictionary
            seendata && throw(FormatError(
                "dictionary page follows a data page in the column chunk"))
            hasdictionary && throw(FormatError(
                "column chunk has more than one dictionary page"))
            _dictionarypage!(dictionary, P, convert, frame, md, node, buffers, limits)
            hasdictionary = true
        end
        position = frameend
    end
    position == stop || throw(FormatError(
        "column chunk page walk does not end at its declared boundary"))
    _validatechunkpageoffsets(md, hasdictionary, seendata, indexseen)
    produced == count || throw(FormatError(
        "column chunk produced $produced of the $count declared values"))
    return
end

# Flat columns (no repetition): entries go to `output[(filled + 1):]`. `R` is whether
# the column is required; an optional page's present values decode into `dense` and are
# then spread out, with `_nullentry(O)` where the definition level is below the maximum.
mutable struct _FlatSink{O,L,R}
    output::Vector{O}
    filled::Int
    dense::Vector{L}
    maxdefinition::UInt16
end

function _nullentry(::Type{Union{Missing,L}}) where {L}
    return missing
end

function _nullentry(::Type{DataStrings.StringPayload})
    return DataStrings.PAYLOAD_MISSING
end

function _sinkpage!(decode, sink::_FlatSink{L,L,true}, count::Int, present::Int,
        hasrepetition::Bool, hasdefinition::Bool, buffers::_PageBuffers) where {L}
    position = decode(sink.output, sink.filled)
    sink.filled += count
    return position
end

function _sinkpage!(decode, sink::_FlatSink{O,L,false}, count::Int, present::Int,
        hasrepetition::Bool, hasdefinition::Bool, buffers::_PageBuffers) where {O,L}
    dense = sink.dense
    position = decode(dense, 0)
    output = sink.output
    offset = sink.filled
    _growto!(output, offset + count)
    if !hasdefinition || present == count
        @inbounds for index in 1:count
            output[offset + index] = dense[index]
        end
    else
        definition = buffers.definition
        top = sink.maxdefinition
        null = _nullentry(O)
        next = 0
        @inbounds for index in 1:count
            if definition[index] == top
                next += 1
                output[offset + index] = dense[next]
            else
                output[offset + index] = null
            end
        end
    end
    sink.filled += count
    return position
end

# Leaf streams: every entry's levels and the dense present values, appended page by
# page (and chunk by chunk when one sink reads several chunks).
struct _StreamSink{L}
    repetition::Vector{UInt16}
    definition::Vector{UInt16}
    values::Vector{L}
end

function _appendlevels!(levels::Vector{UInt16}, page::Vector{UInt16}, count::Int,
        stored::Bool)
    offset = length(levels)
    resize!(levels, offset + count)
    if stored
        copyto!(levels, offset + 1, page, 1, count)
    else
        fill!(view(levels, (offset + 1):(offset + count)), UInt16(0))
    end
    return
end

function _sinkpage!(decode, sink::_StreamSink, count::Int, present::Int,
        hasrepetition::Bool, hasdefinition::Bool, buffers::_PageBuffers)
    position = decode(sink.values, length(sink.values))
    _appendlevels!(sink.repetition, buffers.repetition, count, hasrepetition)
    _appendlevels!(sink.definition, buffers.definition, count, hasdefinition)
    return position
end

function _leafchunkrange(src::AbstractSource, md::Metadata.ColumnMetaData,
        footeroffset::Integer, limits::Limits)
    md.num_values <= typemax(Int) || throw(FormatError("column chunk value count overflows"))
    _checklimit(:container_elements, md.num_values, limits.max_container_elements)
    typemin(Int64) <= footeroffset <= typemax(Int64) || throw(ArgumentError(
        "footer offset does not fit Int64"))
    footer = Int64(footeroffset)
    footer <= _checkedsourcelength(src) ||
        throw(FormatError("footer offset is past the end of the source"))
    return _chunkrange(md, footer)
end

"""
    readleafstream(src, chunk, node, footeroffset; expected_rows, limits) -> LeafStream

Decode one physical column chunk into repetition levels, definition levels, and dense
present values. `expected_rows` optionally validates the number of zero repetition levels.
"""
function readleafstream(src::AbstractSource, chunk::Metadata.ColumnChunk,
    node::SchemaNode, footeroffset::Integer; expected_rows=nothing,
    limits::Limits=Limits(), budget::_ByteBudget=_ByteBudget(limits),
    buffers::_PageBuffers=_PageBuffers())
    md = _chunkmetadata(chunk, node)
    T = _physicaleltype(md.type_)
    start, stop = _leafchunkrange(src, md, footeroffset, limits)
    capacity = _claimcapacity(md.num_values, stop - start)
    sink = _StreamSink(sizehint!(UInt16[], capacity), sizehint!(UInt16[], capacity),
        sizehint!(T[], capacity))
    _readchunk!(sink, T, T, identity, src, md, node, start, stop, limits, budget,
        buffers)
    return LeafStream(sink.repetition, sink.definition, sink.values,
        Int(node.max_repetition_level), Int(node.max_definition_level);
        expected_rows=expected_rows)
end

# STRING payloads mark nulls themselves (`DataStrings.PAYLOAD_MISSING`).
function _flatoutputtype(::Type{L}, node::SchemaNode) where {L}
    L === DataStrings.StringPayload && return L
    return node.max_definition_level == 0 ? L : Union{Missing,L}
end

# Decodes a flat chunk into `output[(offset + 1):(offset + md.num_values)]`, growing
# `output` as pages decode.
function _readflatchunk!(output::Vector{O}, offset::Int, ::Type{P}, convert,
        src::AbstractSource, md::Metadata.ColumnMetaData, node::SchemaNode,
        start::Int64, stop::Int64, limits::Limits, budget::_ByteBudget,
        buffers::_PageBuffers) where {O,P}
    L = Base.nonmissingtype(O)
    sink = _FlatSink{O,L,node.max_definition_level == 0}(output, offset,
        _scratch(buffers.dense, L), UInt16(node.max_definition_level))
    _readchunk!(sink, P, L, convert, src, md, node, start, stop, limits, budget,
        buffers)
    return output
end

"""
    readcolumn(src, chunk, node, footeroffset; limits) -> Vector

Decode a flat (max repetition level 0) column chunk into a concrete
vector. Optional columns yield `Vector{Union{Missing,T}}`; required columns yield `Vector{T}`.
BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values are `Vector{UInt8}` at this physical layer.
"""
function readcolumn(src::AbstractSource, chunk::Metadata.ColumnChunk, node::SchemaNode, footeroffset::Integer;
    limits::Limits=Limits(), budget::_ByteBudget=_ByteBudget(limits),
    expected_rows=nothing)
    node.max_repetition_level == 0 ||
        throw(FormatError("repeated columns are not supported yet"))
    md = _chunkmetadata(chunk, node)
    T = _physicaleltype(md.type_)
    start, stop = _leafchunkrange(src, md, footeroffset, limits)
    count = Int(md.num_values)
    expected_rows === nothing || count == expected_rows || throw(FormatError(
        "leaf stream has $count rows but $expected_rows were expected"))
    output = sizehint!(_flatoutputtype(T, node)[], _claimcapacity(count, stop - start))
    return _readflatchunk!(output, 0, T, identity, src, md, node, start, stop, limits,
        budget, _PageBuffers())
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
    budget::_ByteBudget=_ByteBudget(limits), buffers::_PageBuffers=_PageBuffers())
    chunk, node = _columnselection(metadata, schema, rowgroup, column)
    return readleafstream(file.source, chunk, node, file.footer.offset;
        expected_rows=expected_rows, limits=limits, budget=budget, buffers=buffers)
end

function readcolumn(file::File, metadata::Metadata.FileMetaData, schema::Schema, rowgroup::Integer,
    column::Integer; limits::Limits=Limits(), budget::_ByteBudget=_ByteBudget(limits))
    chunk, node = _columnselection(metadata, schema, rowgroup, column)
    rows = metadata.row_groups[rowgroup].num_rows
    return readcolumn(file.source, chunk, node, file.footer.offset; limits=limits,
        budget=budget, expected_rows=rows)
end
