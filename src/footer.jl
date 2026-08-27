const PARQUET_MAGIC = UInt8[0x50, 0x41, 0x52, 0x31]
const ENCRYPTED_MAGIC = UInt8[0x50, 0x41, 0x52, 0x45]

struct Footer{B<:AbstractVector{UInt8}}
    offset::Int64
    length::Int64
    encrypted::Bool
    bytes::B
end

mutable struct File{S<:AbstractSource,F<:Footer}
    source::S
    footer::F
    closed::Bool
end

function _readu32le(bytes::AbstractVector{UInt8}, offset::Int=1)
    checkbounds(bytes, offset:(offset + 3))
    value = UInt32(bytes[offset]) |
        UInt32(bytes[offset + 1]) << 8 |
        UInt32(bytes[offset + 2]) << 16 |
        UInt32(bytes[offset + 3]) << 24
    return value
end

function _magic(bytes::AbstractVector{UInt8}, offset::Int=1)
    checkbounds(bytes, offset:(offset + 3))
    first = bytes[offset]
    second = bytes[offset + 1]
    third = bytes[offset + 2]
    fourth = bytes[offset + 3]
    first == 0x50 && second == 0x41 && third == 0x52 && fourth == 0x31 &&
        return :plain
    first == 0x50 && second == 0x41 && third == 0x52 && fourth == 0x45 &&
        return :encrypted
    return :invalid
end

function readfooter(src::AbstractSource, limits::Limits=Limits())
    total = _checkedsourcelength(src)
    total >= 12 || throw(FormatError("file is shorter than the minimum 12 bytes"))
    leading = _readrangeexact(src, total, Int64(0), Int64(4))
    leadingmagic = _magic(leading)
    leadingmagic === :invalid && throw(FormatError("missing leading PAR1 or PARE magic"))
    trailer = _readrangeexact(src, total, total - 8, Int64(8))
    trailingmagic = _magic(trailer, 5)
    trailingmagic === :invalid && throw(FormatError("missing trailing PAR1 or PARE magic"))
    leadingmagic === trailingmagic || throw(FormatError("leading and trailing magic differ"))
    footerlength = Int64(_readu32le(trailer))
    footerlength <= total - 12 || throw(FormatError("footer length exceeds the file size"))
    _checklimit(:footer_bytes, footerlength, limits.max_footer_bytes)
    footeroffset = total - 8 - footerlength
    bytes = _readrangeexact(src, total, footeroffset, footerlength)
    return Footer(footeroffset, footerlength, leadingmagic === :encrypted, bytes)
end

function File(input; limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    src = source(input; budget=budget)
    footer = try
        readfooter(src, limits)
    catch
        input isa AbstractSource || close!(src)
        rethrow()
    end
    return File(src, footer, false)
end

function close!(file::File)
    file.closed && return
    file.closed = true
    close!(file.source)
    return
end

function Base.close(file::File)
    close!(file)
    return
end
