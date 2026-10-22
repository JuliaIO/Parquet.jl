const N5_MAGIC = UInt8[0x50, 0x41, 0x52, 0x31]
const N5_MAX_DECODE_VALUES = 1_048_576

struct N5DecodedFile
    metadata::MD.FileMetaData
    leaves::Vector{N5PhysicalLeaf}
    streams::Vector{N5LeafStream}
    pageversions::Vector{Symbol}
end

function _n5checkedint(value::Integer, label::String)
    try
        return Int(value)
    catch error
        error isa InexactError || error isa OverflowError || rethrow()
        throw(ArgumentError("N5 $label does not fit Int"))
    end
end

function _n5checkeduint64(value::Integer, label::String)
    try
        return UInt64(value)
    catch error
        error isa InexactError || error isa OverflowError || rethrow()
        throw(ArgumentError("N5 $label does not fit UInt64"))
    end
end

function _n5checkedadd(left::Int, right::Int, label::String)
    try
        return Base.checked_add(left, right)
    catch error
        error isa OverflowError || rethrow()
        throw(ArgumentError("N5 $label overflows Int"))
    end
end

function _n5checkedmul(left::Int, right::Int, label::String)
    try
        return Base.checked_mul(left, right)
    catch error
        error isa OverflowError || rethrow()
        throw(ArgumentError("N5 $label overflows Int"))
    end
end

function _n5inputlast(bytes::AbstractVector{UInt8}, last::Int, label::String)
    first = firstindex(bytes)
    minimumlast = first - 1
    minimumlast <= last <= lastindex(bytes) || throw(ArgumentError(
        "N5 $label is outside the input"))
    return last
end

function _n5rangelast(position::Int, lengthvalue::Int, limit::Int, label::String)
    position >= 1 || throw(ArgumentError("N5 $label starts before input"))
    lengthvalue >= 0 || throw(ArgumentError("N5 $label length is negative"))
    limit >= 0 || throw(ArgumentError("N5 $label bound is before input"))
    if iszero(lengthvalue)
        position <= limit || (limit < typemax(Int) && position == limit + 1) ||
            throw(ArgumentError("N5 $label starts after its bound"))
        return position - 1
    end
    position <= limit || throw(ArgumentError("N5 $label starts after its bound"))
    lengthvalue - 1 <= limit - position || throw(ArgumentError(
        "N5 $label exceeds its bound"))
    return position + lengthvalue - 1
end

function _n5framerange(bytes::AbstractVector{UInt8}, offset::Integer,
        framesize::Integer, limit::Int)
    offset >= 0 || throw(ArgumentError("N5 page offset is negative"))
    framesize > 0 || throw(ArgumentError("N5 page frame is empty"))
    _n5inputlast(bytes, limit, "page frame bound")
    offsetvalue = _n5checkedint(offset, "page offset")
    framevalue = _n5checkedint(framesize, "page frame size")
    first = _n5checkedadd(offsetvalue, 1, "page first byte")
    last = _n5rangelast(first, framevalue, limit, "page frame")
    return first, last
end

function _n5pushu32!(bytes::Vector{UInt8}, value::UInt32)
    for shift in (0, 8, 16, 24)
        push!(bytes, UInt8((value >> shift) & 0xff))
    end
    return
end

function _n5pushu64!(bytes::Vector{UInt8}, value::UInt64)
    for shift in (0, 8, 16, 24, 32, 40, 48, 56)
        push!(bytes, UInt8((value >> shift) & 0xff))
    end
    return
end

function _n5readu32(bytes::AbstractVector{UInt8}, position::Int, last::Int)
    _n5inputlast(bytes, last, "UInt32 frame")
    position >= firstindex(bytes) || throw(ArgumentError(
        "N5 UInt32 starts before input"))
    _n5rangelast(position, 4, last, "UInt32")
    value = UInt32(0)
    for offset in 0:3
        value |= UInt32(bytes[position + offset]) << (8 * offset)
    end
    return value, _n5checkedadd(position, 4, "UInt32 cursor")
end

function _n5readu64(bytes::AbstractVector{UInt8}, position::Int, last::Int)
    _n5inputlast(bytes, last, "UInt64 frame")
    position >= firstindex(bytes) || throw(ArgumentError(
        "N5 UInt64 starts before input"))
    _n5rangelast(position, 8, last, "UInt64")
    value = UInt64(0)
    for offset in 0:7
        value |= UInt64(bytes[position + offset]) << (8 * offset)
    end
    return value, _n5checkedadd(position, 8, "UInt64 cursor")
end

function _n5pushuleb!(bytes::Vector{UInt8}, value::UInt64)
    current = value
    while current >= 0x80
        push!(bytes, UInt8(current & 0x7f) | 0x80)
        current >>= 7
    end
    push!(bytes, UInt8(current))
    return
end

function _n5readuleb(bytes::AbstractVector{UInt8}, position::Int, last::Int)
    _n5inputlast(bytes, last, "hybrid header frame")
    position >= firstindex(bytes) || throw(ArgumentError(
        "N5 hybrid header starts before input"))
    value = UInt64(0)
    shift = 0
    cursor = position
    for _ in 1:10
        cursor <= last || throw(ArgumentError("N5 hybrid header is truncated"))
        byte = bytes[cursor]
        cursor += 1
        shift == 63 && byte > 0x01 && throw(ArgumentError(
            "N5 hybrid header overflows UInt64"))
        value |= UInt64(byte & 0x7f) << shift
        byte & 0x80 == 0 && return value, cursor
        shift += 7
    end
    throw(ArgumentError("N5 hybrid header is too long"))
end

function n5bitwidth(maximum::Integer)
    maximum >= 0 || throw(ArgumentError("N5 level maximum is negative"))
    iszero(maximum) && return 0
    maximumvalue = _n5checkeduint64(maximum, "level maximum")
    return 64 - leading_zeros(maximumvalue)
end

function _n5encoderuns(levels::AbstractVector{UInt64}, bitwidth::Int)
    isempty(levels) && return UInt8[]
    bytes = UInt8[]
    width = cld(bitwidth, 8)
    index = firstindex(levels)
    while index <= lastindex(levels)
        value = levels[index]
        (bitwidth == 64 || value < (UInt64(1) << bitwidth)) ||
            throw(ArgumentError(
                "N5 level $value does not fit bit width $bitwidth"))
        stop = index
        while stop < lastindex(levels) && levels[stop + 1] == value
            stop += 1
        end
        count = stop - index + 1
        _n5pushuleb!(bytes, UInt64(count) << 1)
        for offset in 0:(width - 1)
            push!(bytes, UInt8((value >> (8 * offset)) & 0xff))
        end
        index = stop + 1
    end
    return bytes
end

function n5encodehybrid(levels::AbstractVector{UInt64}, maximum::Integer;
        length_prefix::Bool)
    bitwidth = n5bitwidth(maximum)
    bitwidth == 0 && return UInt8[]
    payload = _n5encoderuns(levels, bitwidth)
    length(payload) <= typemax(UInt32) || throw(ArgumentError(
        "N5 hybrid payload exceeds UInt32"))
    length_prefix || return payload
    output = UInt8[]
    _n5pushu32!(output, UInt32(length(payload)))
    append!(output, payload)
    return output
end

function _n5rlevalue(bytes::AbstractVector{UInt8}, position::Int, last::Int,
        width::Int)
    _n5inputlast(bytes, last, "hybrid RLE frame")
    _n5rangelast(position, width, last, "hybrid RLE value")
    value = UInt64(0)
    for offset in 0:(width - 1)
        value |= UInt64(bytes[position + offset]) << (8 * offset)
    end
    return value, _n5checkedadd(position, width, "hybrid RLE cursor")
end

function _n5bitpackedvalue(bytes::AbstractVector{UInt8}, position::Int,
        bitoffset::Int, bitwidth::Int)
    value = UInt64(0)
    for bit in 0:(bitwidth - 1)
        absolute = bitoffset + bit
        byte = bytes[position + (absolute >> 3)]
        value |= UInt64((byte >> (absolute & 7)) & 0x01) << bit
    end
    return value
end

function n5decodehybrid(bytes::AbstractVector{UInt8}, position::Int, last::Int,
        count::Integer, maximum::Integer; length_prefix::Bool)
    count >= 0 || throw(ArgumentError("N5 hybrid count is negative"))
    count <= N5_MAX_DECODE_VALUES || throw(ArgumentError(
        "N5 hybrid count exceeds the focused decoder limit"))
    requested = Int(count)
    bitwidth = n5bitwidth(maximum)
    bitwidth == 0 && return fill(UInt64(0), requested), position
    _n5inputlast(bytes, last, "hybrid frame")
    payloadlast = last
    cursor = position
    if length_prefix
        lengthvalue, cursor = _n5readu32(bytes, cursor, last)
        payloadlength = _n5checkedint(lengthvalue, "hybrid payload length")
        payloadlast = _n5rangelast(cursor, payloadlength, last,
            "hybrid length prefix")
    end
    output = UInt64[]
    sizehint!(output, requested)
    width = cld(bitwidth, 8)
    while length(output) < requested
        header, cursor = _n5readuleb(bytes, cursor, payloadlast)
        if iszero(header & 0x01)
            run = _n5checkedint(header >> 1, "hybrid RLE run length")
            run > 0 || throw(ArgumentError("N5 hybrid RLE run is empty"))
            value, cursor = _n5rlevalue(bytes, cursor, payloadlast, width)
            value <= UInt64(maximum) || throw(ArgumentError(
                "N5 hybrid level exceeds its maximum"))
            append!(output, fill(value, min(run, requested - length(output))))
        else
            groups = _n5checkedint(header >> 1,
                "hybrid bit-packed group count")
            groups > 0 || throw(ArgumentError("N5 hybrid bit-packed run is empty"))
            values = _n5checkedmul(groups, 8, "hybrid bit-packed value count")
            payloadbytes = _n5checkedmul(groups, bitwidth,
                "hybrid bit-packed byte count")
            _n5rangelast(cursor, payloadbytes, payloadlast,
                "hybrid bit-packed run")
            take = min(values, requested - length(output))
            for index in 0:(take - 1)
                value = _n5bitpackedvalue(bytes, cursor, index * bitwidth,
                    bitwidth)
                value <= UInt64(maximum) || throw(ArgumentError(
                    "N5 hybrid level exceeds its maximum"))
                push!(output, value)
            end
            cursor = _n5checkedadd(cursor, payloadbytes,
                "hybrid bit-packed cursor")
        end
    end
    length_prefix && cursor != _n5checkedadd(payloadlast, 1,
        "hybrid payload end") && throw(ArgumentError(
        "N5 hybrid payload has trailing bytes"))
    return output, cursor
end

function _n5plainencode(values::Vector{Any}, leaf::N5PhysicalLeaf)
    bytes = UInt8[]
    physical = leaf.element.type_
    if physical == MD.Type.INT32
        for value in values
            _n5pushu32!(bytes, reinterpret(UInt32, Int32(value)))
        end
    elseif physical == MD.Type.INT64
        for value in values
            _n5pushu64!(bytes, reinterpret(UInt64, Int64(value)))
        end
    elseif physical == MD.Type.FLOAT
        for value in values
            _n5pushu32!(bytes, reinterpret(UInt32, Float32(value)))
        end
    elseif physical == MD.Type.DOUBLE
        for value in values
            _n5pushu64!(bytes, reinterpret(UInt64, Float64(value)))
        end
    elseif physical == MD.Type.BOOLEAN
        for base in 1:8:length(values)
            byte = UInt8(0)
            for offset in 0:min(7, length(values) - base)
                Bool(values[base + offset]) && (byte |= UInt8(1) << offset)
            end
            push!(bytes, byte)
        end
    elseif physical == MD.Type.BYTE_ARRAY
        for value in values
            data = value isa AbstractString ? codeunits(value) : value
            length(data) <= typemax(Int32) || throw(ArgumentError(
                "N5 byte array exceeds Int32"))
            _n5pushu32!(bytes, reinterpret(UInt32, Int32(length(data))))
            append!(bytes, data)
        end
    else
        throw(ArgumentError("unsupported N5 PLAIN physical type $physical"))
    end
    return bytes
end

function _n5plainisstring(element::MD.SchemaElement)
    logical = element.logicalType
    logical !== nothing && logical.STRING !== nothing && return true
    return element.converted_type == MD.ConvertedType.UTF8
end

function _n5plaindecode(bytes::AbstractVector{UInt8}, position::Int, last::Int,
        count::Int, leaf::N5PhysicalLeaf)
    values = []
    sizehint!(values, count)
    cursor = position
    physical = leaf.element.type_
    if physical == MD.Type.INT32
        for _ in 1:count
            raw, cursor = _n5readu32(bytes, cursor, last)
            push!(values, reinterpret(Int32, raw))
        end
    elseif physical == MD.Type.INT64
        for _ in 1:count
            raw, cursor = _n5readu64(bytes, cursor, last)
            push!(values, reinterpret(Int64, raw))
        end
    elseif physical == MD.Type.FLOAT
        for _ in 1:count
            raw, cursor = _n5readu32(bytes, cursor, last)
            push!(values, reinterpret(Float32, raw))
        end
    elseif physical == MD.Type.DOUBLE
        for _ in 1:count
            raw, cursor = _n5readu64(bytes, cursor, last)
            push!(values, reinterpret(Float64, raw))
        end
    elseif physical == MD.Type.BOOLEAN
        needed = cld(count, 8)
        _n5inputlast(bytes, last, "BOOLEAN frame")
        _n5rangelast(cursor, needed, last, "BOOLEAN payload")
        for index in 0:(count - 1)
            byte = bytes[cursor + (index >> 3)]
            push!(values, !iszero((byte >> (index & 7)) & 0x01))
        end
        cursor = _n5checkedadd(cursor, needed, "BOOLEAN cursor")
    elseif physical == MD.Type.BYTE_ARRAY
        stringvalue = _n5plainisstring(leaf.element)
        for _ in 1:count
            rawlength, cursor = _n5readu32(bytes, cursor, last)
            lengthvalue = Int(reinterpret(Int32, rawlength))
            lengthvalue >= 0 || throw(ArgumentError(
                "N5 BYTE_ARRAY length is negative"))
            datalast = _n5rangelast(cursor, lengthvalue, last,
                "BYTE_ARRAY payload")
            data = Vector{UInt8}(view(bytes, cursor:datalast))
            push!(values, stringvalue ? String(data) : data)
            cursor = _n5checkedadd(cursor, lengthvalue, "BYTE_ARRAY cursor")
        end
    else
        throw(ArgumentError("unsupported N5 PLAIN physical type $physical"))
    end
    return values, cursor
end

function _n5page(stream::N5LeafStream, leaf::N5PhysicalLeaf,
        pageversion::Symbol, headermutator::Function)
    repetition = n5encodehybrid(stream.repetition, stream.max_repetition;
        length_prefix=pageversion === :v1)
    definition = n5encodehybrid(stream.definition, stream.max_definition;
        length_prefix=pageversion === :v1)
    values = _n5plainencode(stream.values, leaf)
    payload = vcat(repetition, definition, values)
    entries = length(stream.repetition)
    entries <= typemax(Int32) || throw(ArgumentError("N5 page has too many entries"))
    if pageversion === :v1
        data = MD.DataPageHeader(num_values=Int32(entries),
            encoding=MD.Encoding.PLAIN,
            definition_level_encoding=MD.Encoding.RLE,
            repetition_level_encoding=MD.Encoding.RLE)
        header = MD.PageHeader(type_=MD.PageType.DATA_PAGE,
            uncompressed_page_size=Int32(length(payload)),
            compressed_page_size=Int32(length(payload)), data_page_header=data)
    elseif pageversion === :v2
        rows = count(iszero, stream.repetition)
        nulls = count(!=(UInt64(stream.max_definition)), stream.definition)
        data = MD.DataPageHeaderV2(num_values=Int32(entries),
            num_nulls=Int32(nulls), num_rows=Int32(rows),
            encoding=MD.Encoding.PLAIN,
            definition_levels_byte_length=Int32(length(definition)),
            repetition_levels_byte_length=Int32(length(repetition)),
            is_compressed=false)
        header = MD.PageHeader(type_=MD.PageType.DATA_PAGE_V2,
            uncompressed_page_size=Int32(length(payload)),
            compressed_page_size=Int32(length(payload)),
            data_page_header_v2=data)
    else
        throw(ArgumentError("unsupported N5 page version $pageversion"))
    end
    header = headermutator(header)
    header isa MD.PageHeader || throw(ArgumentError(
        "N5 page-header mutator returned $(typeof(header))"))
    headerbytes = TH.encode(header)
    return vcat(headerbytes, payload), length(headerbytes)
end

function _n5columnmetadata(stream::N5LeafStream, leaf::N5PhysicalLeaf,
        offset::Int64, framesize::Int64, uncompressed::Int64,
        pageversion::Symbol)
    encodings = MD.Encoding.T[MD.Encoding.PLAIN]
    (!iszero(stream.max_repetition) || !iszero(stream.max_definition)) &&
        pushfirst!(encodings, MD.Encoding.RLE)
    pagetype = pageversion === :v1 ? MD.PageType.DATA_PAGE :
        MD.PageType.DATA_PAGE_V2
    stats = MD.PageEncodingStats[MD.PageEncodingStats(page_type=pagetype,
        encoding=MD.Encoding.PLAIN, count=Int32(1))]
    return MD.ColumnMetaData(type_=leaf.element.type_, encodings=encodings,
        path_in_schema=leaf.path, codec=MD.CompressionCodec.UNCOMPRESSED,
        num_values=Int64(length(stream.repetition)),
        total_uncompressed_size=uncompressed,
        total_compressed_size=framesize, data_page_offset=offset,
        encoding_stats=stats)
end

function n5emitfile(schema::Vector{MD.SchemaElement},
        streams::Vector{N5LeafStream}, rows::Integer;
        pageversion::Symbol=:v1, headermutator::Function=identity)
    rows >= 0 || throw(ArgumentError("N5 row count is negative"))
    rowcount = _n5checkedint(rows, "row count")
    leaves = n5physicalleaves(schema)
    length(leaves) == length(streams) || throw(ArgumentError(
        "N5 schema and stream leaf counts differ"))
    body = copy(N5_MAGIC)
    if iszero(rowcount)
        all(stream -> isempty(stream.repetition) &&
            isempty(stream.definition) && isempty(stream.values), streams) ||
            throw(ArgumentError("N5 zero-row streams are not empty"))
        metadata = MD.FileMetaData(version=Int32(1), schema=schema,
            num_rows=Int64(0), row_groups=MD.RowGroup[],
            created_by="Parquet.jl N5 independent model")
        footer = TH.encode(metadata)
        append!(body, footer)
        _n5pushu32!(body, UInt32(length(footer)))
        append!(body, N5_MAGIC)
        return body
    end
    chunks = MD.ColumnChunk[]
    totaluncompressed = Int64(0)
    totalcompressed = Int64(0)
    for (leaf, stream) in zip(leaves, streams)
        leaf.max_repetition == stream.max_repetition || throw(ArgumentError(
            "N5 stream maximum repetition differs from schema"))
        leaf.max_definition == stream.max_definition || throw(ArgumentError(
            "N5 stream maximum definition differs from schema"))
        count(iszero, stream.repetition) == rowcount || throw(ArgumentError(
            "N5 stream row count differs"))
        offset = Int64(length(body))
        frame, headerlength = _n5page(stream, leaf, pageversion,
            headermutator)
        append!(body, frame)
        framesize = Int64(length(frame))
        uncompressed = Int64(headerlength) +
            Int64(length(frame) - headerlength)
        metadata = _n5columnmetadata(stream, leaf, offset, framesize,
            uncompressed, pageversion)
        push!(chunks, MD.ColumnChunk(file_offset=Int64(0),
            meta_data=metadata))
        totaluncompressed += uncompressed
        totalcompressed += framesize
    end
    rowgroup = MD.RowGroup(columns=chunks,
        total_byte_size=totaluncompressed, num_rows=Int64(rowcount),
        total_compressed_size=totalcompressed, file_offset=Int64(4),
        ordinal=Int16(0))
    metadata = MD.FileMetaData(version=Int32(1), schema=schema,
        num_rows=Int64(rowcount), row_groups=MD.RowGroup[rowgroup],
        created_by="Parquet.jl N5 independent model")
    footer = TH.encode(metadata)
    append!(body, footer)
    _n5pushu32!(body, UInt32(length(footer)))
    append!(body, N5_MAGIC)
    return body
end

function n5decodefooter(bytes::AbstractVector{UInt8})
    length(bytes) >= 12 || throw(ArgumentError("N5 Parquet file is too short"))
    bytes[1:4] == N5_MAGIC || throw(ArgumentError("N5 leading magic differs"))
    bytes[(end - 3):end] == N5_MAGIC || throw(ArgumentError(
        "N5 trailing magic differs"))
    filelast = lastindex(bytes)
    trailerstart = filelast - 7
    rawlength, _ = _n5readu32(bytes, trailerstart, filelast)
    footerlength = _n5checkedint(rawlength, "footer length")
    footerlength > 0 || throw(ArgumentError("N5 footer is empty"))
    footerlast = filelast - 8
    footerlength <= footerlast - 4 || throw(ArgumentError(
        "N5 footer starts before data"))
    footerstart = footerlast - footerlength + 1
    reader = TH.Reader(bytes, footerstart, footerlast)
    metadata = TH.decode(reader, MD.FileMetaData)
    TH.consumed(reader) == footerlength || throw(ArgumentError(
        "N5 footer has trailing Thrift bytes"))
    return metadata, footerstart - 1
end

function _n5decodepage(bytes::AbstractVector{UInt8}, offset::Int64,
        framesize::Int64, leaf::N5PhysicalLeaf; framelimit::Int=lastindex(bytes))
    first, last = _n5framerange(bytes, offset, framesize, framelimit)
    reader = TH.Reader(bytes, first, last)
    header = TH.decode(reader, MD.PageHeader)
    headerlength = TH.consumed(reader)
    payloadfirst = _n5checkedadd(first, headerlength, "page payload start")
    compressedsize = Int(header.compressed_page_size)
    compressedsize >= 0 || throw(ArgumentError(
        "N5 compressed page size is negative"))
    uncompressedsize = Int(header.uncompressed_page_size)
    uncompressedsize >= 0 || throw(ArgumentError(
        "N5 uncompressed page size is negative"))
    payloadlast = _n5rangelast(payloadfirst, compressedsize, last,
        "page payload")
    payloadlast == last || throw(ArgumentError(
        "N5 page compressed size differs from frame"))
    uncompressedsize == compressedsize ||
        throw(ArgumentError("N5 focused decoder requires uncompressed pages"))
    if header.type_ == MD.PageType.DATA_PAGE
        data = header.data_page_header
        data === nothing && throw(ArgumentError("N5 V1 page header is absent"))
        data.encoding == MD.Encoding.PLAIN || throw(ArgumentError(
            "N5 focused decoder requires PLAIN values"))
        data.repetition_level_encoding == MD.Encoding.RLE || throw(ArgumentError(
            "N5 focused decoder requires RLE repetition levels"))
        data.definition_level_encoding == MD.Encoding.RLE || throw(ArgumentError(
            "N5 focused decoder requires RLE definition levels"))
        entries = Int(data.num_values)
        0 <= entries <= N5_MAX_DECODE_VALUES || throw(ArgumentError(
            "N5 V1 value count exceeds the focused decoder limit"))
        cursor = payloadfirst
        repetition, cursor = n5decodehybrid(bytes, cursor, payloadlast,
            entries, leaf.max_repetition; length_prefix=true)
        definition, cursor = n5decodehybrid(bytes, cursor, payloadlast,
            entries, leaf.max_definition; length_prefix=true)
        dense = Base.count(==(UInt64(leaf.max_definition)), definition)
        values, cursor = _n5plaindecode(bytes, cursor, payloadlast, dense, leaf)
        cursor == _n5checkedadd(payloadlast, 1, "V1 payload end") ||
            throw(ArgumentError(
                "N5 V1 value payload has trailing bytes"))
        return N5LeafStream(repetition, definition, values,
            leaf.max_repetition, leaf.max_definition), :v1
    elseif header.type_ == MD.PageType.DATA_PAGE_V2
        data = header.data_page_header_v2
        data === nothing && throw(ArgumentError("N5 V2 page header is absent"))
        data.encoding == MD.Encoding.PLAIN || throw(ArgumentError(
            "N5 focused decoder requires PLAIN values"))
        data.is_compressed in (nothing, false) || throw(ArgumentError(
            "N5 focused decoder requires uncompressed V2 values"))
        entries = Int(data.num_values)
        0 <= entries <= N5_MAX_DECODE_VALUES || throw(ArgumentError(
            "N5 V2 value count exceeds the focused decoder limit"))
        rows = Int(data.num_rows)
        0 <= rows <= entries || throw(ArgumentError(
            "N5 V2 row count is outside the value count"))
        nulls = Int(data.num_nulls)
        0 <= nulls <= entries || throw(ArgumentError(
            "N5 V2 null count is outside the value count"))
        repetitionlength = Int(data.repetition_levels_byte_length)
        repetitionlength >= 0 || throw(ArgumentError(
            "N5 V2 repetition section length is negative"))
        definitionlength = Int(data.definition_levels_byte_length)
        definitionlength >= 0 || throw(ArgumentError(
            "N5 V2 definition section length is negative"))
        levellength = _n5checkedadd(repetitionlength, definitionlength,
            "V2 level section length")
        levellength <= payloadlast - payloadfirst + 1 || throw(ArgumentError(
            "N5 V2 level sections exceed payload"))
        repetitionlast = _n5rangelast(payloadfirst, repetitionlength,
            payloadlast, "V2 repetition section")
        definitionfirst = _n5checkedadd(repetitionlast, 1,
            "V2 definition section start")
        definitionlast = _n5rangelast(definitionfirst, definitionlength,
            payloadlast, "V2 definition section")
        repetition, repetitioncursor = n5decodehybrid(bytes, payloadfirst,
            repetitionlast, entries, leaf.max_repetition; length_prefix=false)
        repetitioncursor == _n5checkedadd(repetitionlast, 1,
            "V2 repetition section end") || throw(ArgumentError(
            "N5 V2 repetition section has trailing bytes"))
        definition, definitioncursor = n5decodehybrid(bytes, definitionfirst,
            definitionlast, entries, leaf.max_definition; length_prefix=false)
        definitioncursor == _n5checkedadd(definitionlast, 1,
            "V2 definition section end") || throw(ArgumentError(
            "N5 V2 definition section has trailing bytes"))
        dense = Base.count(==(UInt64(leaf.max_definition)), definition)
        valuesfirst = _n5checkedadd(definitionlast, 1,
            "V2 value section start")
        values, cursor = _n5plaindecode(bytes, valuesfirst,
            payloadlast, dense, leaf)
        cursor == _n5checkedadd(payloadlast, 1, "V2 payload end") ||
            throw(ArgumentError(
                "N5 V2 value payload has trailing bytes"))
        Base.count(iszero, repetition) == rows || throw(ArgumentError(
            "N5 V2 row count differs from repetition levels"))
        Base.count(!=(UInt64(leaf.max_definition)), definition) ==
            nulls || throw(ArgumentError(
            "N5 V2 null count differs from definition levels"))
        return N5LeafStream(repetition, definition, values,
            leaf.max_repetition, leaf.max_definition), :v2
    end
    throw(ArgumentError("N5 focused decoder found a non-data page"))
end

function n5decodefile(bytes::AbstractVector{UInt8})
    metadata, footeroffset = n5decodefooter(bytes)
    leaves = n5physicalleaves(metadata.schema)
    if isempty(metadata.row_groups)
        metadata.num_rows == 0 || throw(ArgumentError(
            "N5 file without row groups has nonzero rows"))
        streams = N5LeafStream[N5LeafStream(UInt64[], UInt64[], Any[],
            leaf.max_repetition, leaf.max_definition) for leaf in leaves]
        return N5DecodedFile(metadata, leaves, streams, Symbol[])
    end
    length(metadata.row_groups) == 1 || throw(ArgumentError(
        "N5 focused decoder needs one row group"))
    group = only(metadata.row_groups)
    length(group.columns) == length(leaves) || throw(ArgumentError(
        "N5 row-group column count differs from schema"))
    streams = N5LeafStream[]
    pageversions = Symbol[]
    for (chunk, leaf) in zip(group.columns, leaves)
        column = chunk.meta_data
        column === nothing && throw(ArgumentError("N5 column metadata is absent"))
        column.codec == MD.CompressionCodec.UNCOMPRESSED || throw(ArgumentError(
            "N5 focused decoder requires UNCOMPRESSED columns"))
        column.path_in_schema == leaf.path || throw(ArgumentError(
            "N5 column path differs from schema"))
        stream, pageversion = _n5decodepage(bytes, column.data_page_offset,
            column.total_compressed_size, leaf; framelimit=footeroffset)
        Int64(length(stream.repetition)) == column.num_values ||
            throw(ArgumentError("N5 column value count differs"))
        push!(streams, stream)
        push!(pageversions, pageversion)
    end
    group.num_rows == metadata.num_rows || throw(ArgumentError(
        "N5 row-group row count differs from footer"))
    return N5DecodedFile(metadata, leaves, streams, pageversions)
end
