# Page framing: PageHeader parsing, payload bounds, and CRC32 verification
# (Parquet 2.13.0 README "Data Pages"/"Column chunks"; PageHeader in parquet.thrift).

struct PageFrame{B<:AbstractVector{UInt8}}
    offset::Int64
    header::Metadata.PageHeader
    headerlength::Int
    payload::B
    materializedcharge::Int64
end

function pagekind(header::Metadata.PageHeader)
    type = header.type_
    type == Metadata.PageType.DATA_PAGE && return :data_v1
    type == Metadata.PageType.DATA_PAGE_V2 && return :data_v2
    type == Metadata.PageType.DICTIONARY_PAGE && return :dictionary
    type == Metadata.PageType.INDEX_PAGE && return :index
    return :unknown
end

function pagekind(frame::PageFrame)
    return pagekind(frame.header)
end

function _subheaderflags(header::Metadata.PageHeader)
    return (header.data_page_header !== nothing,
        header.index_page_header !== nothing,
        header.dictionary_page_header !== nothing,
        header.data_page_header_v2 !== nothing)
end

function _expectedsubheaders(kind::Symbol)
    kind === :data_v1 && return (true, false, false, false)
    kind === :index && return (false, true, false, false)
    kind === :dictionary && return (false, false, true, false)
    kind === :data_v2 && return (false, false, false, true)
    return nothing
end

function _validatev2header(header::Metadata.PageHeader)
    data = header.data_page_header_v2
    data.num_values >= 0 || throw(FormatError("negative data page V2 value count"))
    data.num_nulls >= 0 || throw(FormatError("negative data page V2 null count"))
    data.num_rows >= 0 || throw(FormatError("negative data page V2 row count"))
    data.num_nulls <= data.num_values ||
        throw(FormatError("data page V2 null count exceeds its value count"))
    data.num_rows <= data.num_values ||
        throw(FormatError("data page V2 row count exceeds its value count"))
    repetition = Int64(data.repetition_levels_byte_length)
    definition = Int64(data.definition_levels_byte_length)
    repetition >= 0 || throw(FormatError("negative data page V2 repetition-level byte length"))
    definition >= 0 || throw(FormatError("negative data page V2 definition-level byte length"))
    levels = repetition + definition
    levels <= header.compressed_page_size ||
        throw(FormatError("data page V2 levels exceed its compressed size"))
    levels <= header.uncompressed_page_size ||
        throw(FormatError("data page V2 levels exceed its uncompressed size"))
    return
end

function validatepageheader(header::Metadata.PageHeader)
    header.compressed_page_size >= 0 || throw(FormatError("negative compressed page size"))
    header.uncompressed_page_size >= 0 || throw(FormatError("negative uncompressed page size"))
    kind = pagekind(header)
    expected = _expectedsubheaders(kind)
    expected === nothing && return kind
    _subheaderflags(header) == expected ||
        throw(FormatError("page header fields do not match the page type $(header.type_)"))
    kind === :data_v2 && _validatev2header(header)
    return kind
end

function _pageheaderfailure(err, reader::Thrift.Reader, window::Int64,
        available::Int64, limits::Limits)
    clipped = window < available
    if err isa FormatError && clipped && Thrift.remaining(reader) == 0
        requested = window == typemax(Int64) ? window : window + 1
        throw(LimitError(:page_header_bytes, requested,
            limits.max_page_header_bytes))
    end
    throw(err)
end

function _nextpageframecount(count::Int64, limits::Limits)
    count >= 0 || throw(ArgumentError(
        "physical page frame count must be nonnegative"))
    requested = try
        Base.checked_add(count, Int64(1))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64),
            limits.max_container_elements))
    end
    _checklimit(:container_elements, requested,
        limits.max_container_elements)
    return requested
end

function _pageframeend(offset::Int64, headerlength::Integer,
        compressed::Integer)
    offset >= 0 || throw(FormatError("negative page frame offset"))
    headerlength >= 0 || throw(FormatError(
        "negative page header length"))
    compressed >= 0 || throw(FormatError(
        "negative compressed page size"))
    headerlength <= typemax(Int64) && compressed <= typemax(Int64) ||
        throw(FormatError("page frame end overflows Int64"))
    return try
        payload = Base.checked_add(offset, Int64(headerlength))
        Base.checked_add(payload, Int64(compressed))
    catch err
        err isa OverflowError || rethrow()
        throw(FormatError("page frame end overflows Int64"))
    end
end

"""
    readpageheader(src, offset, stop, limits) -> (header, headerlength)

Decode the PageHeader at zero-based `offset`, never reading at or past `stop` and never
decoding more than `limits.max_page_header_bytes`.
"""
function _readpageheader(src::AbstractSource, offset::Int64, stop::Int64,
    limits::Limits, budget::_LiveByteBudget)
    0 <= offset < stop || throw(FormatError("page header offset $offset is outside the column chunk"))
    total = _checkedsourcelength(src)
    stop <= total || throw(FormatError(
        "page read stop $stop is past the end of the source"))
    available = stop - offset
    window = min(Int64(limits.max_page_header_bytes), available)
    window > 0 || throw(LimitError(:page_header_bytes, 1, limits.max_page_header_bytes))
    temporary = _reserveobjects!(budget, 2)
    bytes = try
        _readrangeexact(src, total, offset, window)
    catch
        _release!(budget, temporary)
        rethrow()
    end
    reader = Thrift.Reader(bytes; limits=limits, budget=budget)
    header = try
        Thrift.decode(reader, Metadata.PageHeader)
    catch err
        charge = Thrift.materializedcharge(reader)
        _release!(budget, _materializedsum(temporary, charge))
        _pageheaderfailure(err, reader, window, available, limits)
    end
    charge = Thrift.materializedcharge(reader)
    _release!(budget, temporary)
    return header, Thrift.consumed(reader), charge, total
end

function readpageheader(src::AbstractSource, offset::Int64, stop::Int64,
    limits::Limits; budget::_LiveByteBudget=_LiveByteBudget(limits))
    header, headerlength, _, _ = _readpageheader(src, offset, stop, limits,
        budget)
    return header, headerlength
end

"""
    readpage(src, offset, stop, limits) -> PageFrame

Read one page whose header starts at `offset` and whose bytes must end at or before `stop`.
Both page sizes are charged to `limits.max_page_bytes` before the payload is read, and the
CRC32 of the on-disk payload is verified when the header carries one.
"""
function readpage(src::AbstractSource, offset::Int64, stop::Int64, limits::Limits;
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    header, headerlength, headercharge, total = _readpageheader(src, offset,
        stop, limits, budget)
    framecharge = Int64(0)
    try
        validatepageheader(header)
        compressed = Int64(header.compressed_page_size)
        payloadoffset = _pageframeend(offset, headerlength, Int64(0))
        compressed <= stop - payloadoffset || throw(FormatError(
            "page payload of $compressed bytes extends past the column chunk end"))
        _checklimit(:page_bytes, compressed, limits.max_page_bytes)
        _checklimit(:page_bytes, header.uncompressed_page_size,
            limits.max_page_bytes)
        framecharge = _reserveobjects!(budget, 2)
        payload = _readrangeexact(src, total, payloadoffset, compressed)
        crc = header.crc
        crc === nothing || verifypagechecksum(crc, payload; budget=budget)
        charge = _materializedsum(headercharge, framecharge)
        return PageFrame(offset, header, headerlength, payload, charge)
    catch
        _release!(budget, _materializedsum(headercharge, framecharge))
        rethrow()
    end
end

function pageend(frame::PageFrame)
    return _pageframeend(frame.offset, frame.headerlength,
        frame.header.compressed_page_size)
end

"""
    decompresspage(frame, codec; limits) -> bytes

Return the page payload decompressed to the exact size declared in its header.
"""
function decompresspage(frame::PageFrame, codec::Metadata.CompressionCodec.T;
        limits::Limits=Limits(),
        budget::Union{Nothing,_LiveByteBudget}=nothing)
    return decompress(codec, frame.payload, frame.header.uncompressed_page_size;
        limits=limits, budget=budget)
end
