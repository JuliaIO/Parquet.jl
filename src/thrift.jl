module Thrift

import ..Parquet: Limits, FormatError, LimitError, _LiveByteBudget,
    _MATERIALIZED_OBJECT_BYTES, _checklimit, _materializedsum,
    _reserve!, _reservearray!, _reserveobjects!, _release!

# Thrift Compact Protocol type codes (field headers and container element types).
const STOP = 0x00
const BOOL_TRUE = 0x01
const BOOL_FALSE = 0x02
const BYTE = 0x03
const I16 = 0x04
const I32 = 0x05
const I64 = 0x06
const DOUBLE = 0x07
const BINARY = 0x08
const LIST = 0x09
const SET = 0x0a
const MAP = 0x0b
const STRUCT = 0x0c

abstract type ThriftEnum end

"""
    RawField

An undecoded Thrift field preserved for re-emission. `bytes` holds the exact encoded
field header followed by the payload; `headerlength` is the header size (0 when the field
was constructed without a verbatim header) and `previd` is the field id that preceded the
header, which the writer needs to re-emit the verbatim header byte-for-byte.
"""
struct RawField
    id::Int16
    type::UInt8
    previd::Int16
    headerlength::Int8
    bytes::Vector{UInt8}
end

function RawField(id::Integer, type::UInt8, payload::AbstractVector{UInt8})
    return RawField(Int16(id), type, Int16(0), Int8(0), Vector{UInt8}(payload))
end

function payload(field::RawField)
    return @view field.bytes[(Int(field.headerlength) + 1):end]
end

function Base.:(==)(a::RawField, b::RawField)
    return a.id == b.id && a.type == b.type && payload(a) == payload(b)
end

function Base.hash(x::RawField, h::UInt)
    return hash(payload(x), hash(x.type, hash(x.id, hash(:RawField, h))))
end

mutable struct Reader{B<:AbstractVector{UInt8}}
    const bytes::B
    const start::Int
    const last::Int
    const limits::Limits
    const budget::_LiveByteBudget
    pos::Int
    depth::Int
    headerpos::Int
    previd::Int16
    materialized::Int64
end

function Reader(bytes::AbstractVector{UInt8}, first::Integer, last::Integer;
        limits::Limits=Limits(), budget::_LiveByteBudget=_LiveByteBudget(limits))
    first >= firstindex(bytes) || throw(BoundsError(bytes, first))
    last <= lastindex(bytes) || throw(BoundsError(bytes, last))
    last >= first - 1 || throw(ArgumentError("Thrift reader range is reversed"))
    return Reader{typeof(bytes)}(bytes, Int(first), Int(last), limits, budget,
        Int(first), 0, Int(first), Int16(0), Int64(0))
end

function Reader(bytes::AbstractVector{UInt8}; limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    return Reader(bytes, firstindex(bytes), lastindex(bytes); limits=limits,
        budget=budget)
end

function materializedcharge(r::Reader)
    return r.materialized
end

function _readercharge!(r::Reader, bytes::Integer)
    _reserve!(r.budget, bytes)
    r.materialized = Base.checked_add(r.materialized, Int64(bytes))
    return Int64(bytes)
end

function _readerarray!(r::Reader, ::Type{T}, count::Integer;
    header::Bool=true) where {T}
    bytes = _reservearray!(r.budget, T, count; header=header)
    r.materialized = Base.checked_add(r.materialized, bytes)
    return bytes
end

function _readerobjects!(r::Reader, count::Integer=1)
    bytes = _reserveobjects!(r.budget, count)
    r.materialized = Base.checked_add(r.materialized, bytes)
    return bytes
end

function remaining(r::Reader)
    return r.last - r.pos + 1
end

function consumed(r::Reader)
    return r.pos - r.start
end

function _need(r::Reader, count::Integer)
    count <= remaining(r) && return
    throw(FormatError("Thrift data is truncated: $count bytes needed, $(remaining(r)) available"))
end

function readbyte(r::Reader)
    _need(r, 1)
    value = @inbounds r.bytes[r.pos]
    r.pos += 1
    return value
end

function _checkvarinttail(byte::UInt8, shift::Int, bits::Int)
    (byte & 0x80) == 0x00 || throw(FormatError("Thrift varint is longer than $(bits == 32 ? 5 : 10) bytes"))
    (byte & 0x7f) >> (bits - shift) == 0x00 || throw(FormatError("Thrift varint overflows $bits bits"))
    return
end

function readvarint(r::Reader, bits::Int)
    maxbytes = bits == 32 ? 5 : 10
    result = UInt64(0)
    shift = 0
    for i in 1:maxbytes
        byte = readbyte(r)
        i == maxbytes && _checkvarinttail(byte, shift, bits)
        result |= UInt64(byte & 0x7f) << shift
        (byte & 0x80) == 0x00 && return result
        shift += 7
    end
    throw(FormatError("Thrift varint is longer than $maxbytes bytes"))
end

function _zigzag32(raw::UInt64)
    value = UInt32(raw)
    return reinterpret(Int32, (value >> 1) ⊻ (-(value & 0x00000001)))
end

function _zigzag64(raw::UInt64)
    return reinterpret(Int64, (raw >> 1) ⊻ (-(raw & 0x0000000000000001)))
end

function readi8(r::Reader)
    return readbyte(r) % Int8
end

function readi16(r::Reader)
    value = readi32(r)
    typemin(Int16) <= value <= typemax(Int16) || throw(FormatError("Thrift i16 value $value is out of range"))
    return Int16(value)
end

function readi32(r::Reader)
    return _zigzag32(readvarint(r, 32))
end

function readi64(r::Reader)
    return _zigzag64(readvarint(r, 64))
end

function readdouble(r::Reader)
    _need(r, 8)
    value = UInt64(0)
    for i in 0:7
        value |= UInt64(@inbounds r.bytes[r.pos + i]) << (8 * i)
    end
    r.pos += 8
    return reinterpret(Float64, value)
end

function readbool(r::Reader)
    byte = readbyte(r)
    byte == BOOL_TRUE && return true
    byte == BOOL_FALSE && return false
    throw(FormatError("invalid Thrift boolean byte $byte"))
end

function _readsize(r::Reader, what::String)
    raw = readvarint(r, 32)
    raw <= typemax(Int32) || throw(FormatError("Thrift $what is negative"))
    return Int(raw)
end

function readbinarylength(r::Reader)
    count = _readsize(r, "binary length")
    _checklimit(:string_bytes, count, r.limits.max_string_bytes)
    _need(r, count)
    return count
end

function readbinary(r::Reader)
    count = readbinarylength(r)
    _readerarray!(r, UInt8, count)
    out = Vector{UInt8}(undef, count)
    count == 0 || copyto!(out, 1, r.bytes, r.pos, count)
    r.pos += count
    return out
end

function readstring(r::Reader)
    count = readbinarylength(r)
    _readercharge!(r, _materializedsum(_MATERIALIZED_OBJECT_BYTES, count))
    out = String(view(r.bytes, r.pos:(r.pos + count - 1)))
    r.pos += count
    return out
end

function _validtype(type::UInt8)
    return BOOL_TRUE <= type <= STRUCT
end

"""
    readfieldheader(r, lastid) -> (id, type)

Read a field header. Returns `(0, STOP)` at the end of a struct. The header position and
the preceding field id are recorded on the reader so unknown fields can be preserved.
"""
function readfieldheader(r::Reader, lastid::Int16)
    r.headerpos = r.pos
    r.previd = lastid
    byte = readbyte(r)
    byte == STOP && return (Int16(0), STOP)
    type = byte & 0x0f
    delta = byte >> 4
    _validtype(type) || throw(FormatError("invalid Thrift field type code $type"))
    delta == 0x00 && return (readi16(r), type)
    id = Int(lastid) + Int(delta)
    id <= typemax(Int16) || throw(FormatError("Thrift field id overflows Int16"))
    return (Int16(id), type)
end

# Minimum encoded size of one value of a type, used to bound container allocations.
function _minbytes(type::UInt8)
    type == DOUBLE && return 8
    return 1
end

function _checkcontainer(r::Reader, count::Int, minbytes::Int)
    _checklimit(:container_elements, count, r.limits.max_container_elements)
    _need(r, Int64(count) * Int64(minbytes))
    return
end

function readlistheader(r::Reader)
    byte = readbyte(r)
    type = byte & 0x0f
    _validtype(type) || throw(FormatError("invalid Thrift list element type code $type"))
    count = Int(byte >> 4)
    count == 15 && (count = _readsize(r, "list size"))
    _checkcontainer(r, count, _minbytes(type))
    return (count, type)
end

function readmapheader(r::Reader)
    count = _readsize(r, "map size")
    count == 0 && return (0, STOP, STOP)
    byte = readbyte(r)
    keytype = byte >> 4
    valuetype = byte & 0x0f
    _validtype(keytype) || throw(FormatError("invalid Thrift map key type code $keytype"))
    _validtype(valuetype) || throw(FormatError("invalid Thrift map value type code $valuetype"))
    _checkcontainer(r, count, _minbytes(keytype) + _minbytes(valuetype))
    return (count, keytype, valuetype)
end

function _enterdepth!(r::Reader)
    depth = r.depth + 1
    _checklimit(:metadata_depth, depth, r.limits.max_metadata_depth)
    r.depth = depth
    return
end

function enter!(r::Reader)
    depth = r.depth + 1
    _checklimit(:metadata_depth, depth, r.limits.max_metadata_depth)
    _readerobjects!(r)
    r.depth = depth
    return
end

function leave!(r::Reader)
    r.depth -= 1
    return
end

function skipfield!(r::Reader, type::UInt8)
    (type == BOOL_TRUE || type == BOOL_FALSE) && return
    skipvalue!(r, type)
    return
end

function skipvalue!(r::Reader, type::UInt8)
    if type == BOOL_TRUE || type == BOOL_FALSE
        readbool(r)
    elseif type == BYTE
        readbyte(r)
    elseif type == I16 || type == I32
        readvarint(r, 32)
    elseif type == I64
        readvarint(r, 64)
    elseif type == DOUBLE
        _need(r, 8)
        r.pos += 8
    elseif type == BINARY
        count = readbinarylength(r)
        r.pos += count
    elseif type == LIST || type == SET
        skiplist!(r)
    elseif type == MAP
        skipmap!(r)
    elseif type == STRUCT
        skipstruct!(r)
    else
        throw(FormatError("invalid Thrift type code $type"))
    end
    return
end

function skiplist!(r::Reader)
    count, type = readlistheader(r)
    _enterdepth!(r)
    for _ in 1:count
        skipvalue!(r, type)
    end
    leave!(r)
    return
end

function skipmap!(r::Reader)
    count, keytype, valuetype = readmapheader(r)
    _enterdepth!(r)
    for _ in 1:count
        skipvalue!(r, keytype)
        skipvalue!(r, valuetype)
    end
    leave!(r)
    return
end

function skipstruct!(r::Reader)
    _enterdepth!(r)
    lastid = Int16(0)
    while true
        id, type = readfieldheader(r, lastid)
        type == STOP && break
        lastid = id
        skipfield!(r, type)
    end
    leave!(r)
    return
end

function _copyrange(r::Reader, first::Int, last::Int)
    count = last - first + 1
    _readerarray!(r, UInt8, count)
    out = Vector{UInt8}(undef, count)
    count == 0 || copyto!(out, 1, r.bytes, first, count)
    return out
end

"""
    readrawfield(r, id, type)

Skip the payload of the field whose header was just read and return it as a `RawField`
carrying the verbatim header and payload bytes.
"""
function readrawfield(r::Reader, id::Int16, type::UInt8)
    headerpos = r.headerpos
    previd = r.previd
    start = r.pos
    skipfield!(r, type)
    bytes = _copyrange(r, headerpos, r.pos - 1)
    _readerobjects!(r)
    _readerarray!(r, RawField, 1)
    return RawField(id, type, previd, Int8(start - headerpos), bytes)
end

function pushunknown!(::Nothing, field::RawField)
    return RawField[field]
end

function pushunknown!(unknown::Vector{RawField}, field::RawField)
    push!(unknown, field)
    return unknown
end

function finishunknown(::Nothing)
    return ()
end

function finishunknown(unknown::Vector{RawField})
    return Tuple(unknown)
end

function missingfield(structname::Symbol, field::Symbol)
    throw(FormatError("Thrift struct $structname is missing required field $field"))
end

function typecode(::Type{Bool})
    return BOOL_TRUE
end

function typecode(::Type{Int8})
    return BYTE
end

function typecode(::Type{Int16})
    return I16
end

function typecode(::Type{Int32})
    return I32
end

function typecode(::Type{Int64})
    return I64
end

function typecode(::Type{Float64})
    return DOUBLE
end

function typecode(::Type{String})
    return BINARY
end

function typecode(::Type{Vector{UInt8}})
    return BINARY
end

function typecode(::Type{Vector{T}}) where {T}
    return LIST
end

function typecode(::Type{Vector{Pair{K,V}}}) where {K,V}
    return MAP
end

function matches(type::UInt8, ::Type{Bool})
    return type == BOOL_TRUE || type == BOOL_FALSE
end

function matches(type::UInt8, ::Type{T}) where {T}
    return type == typecode(T)
end

function readelement(r::Reader, ::Type{Bool})
    return readbool(r)
end

function readelement(r::Reader, ::Type{Int8})
    return readi8(r)
end

function readelement(r::Reader, ::Type{Int16})
    return readi16(r)
end

function readelement(r::Reader, ::Type{Int32})
    return readi32(r)
end

function readelement(r::Reader, ::Type{Int64})
    return readi64(r)
end

function readelement(r::Reader, ::Type{Float64})
    return readdouble(r)
end

function readelement(r::Reader, ::Type{String})
    return readstring(r)
end

function readelement(r::Reader, ::Type{Vector{UInt8}})
    return readbinary(r)
end

function readelement(r::Reader, ::Type{Vector{T}}) where {T}
    value = readlist(r, T)
    value === nothing && throw(FormatError("Thrift nested list element type mismatch"))
    return value
end

function readelement(r::Reader, ::Type{Vector{Pair{K,V}}}) where {K,V}
    value = readmap(r, K, V)
    value === nothing && throw(FormatError("Thrift nested map element type mismatch"))
    return value
end

"""
    readlist(r, T)

Decode a list or set whose elements decode to `T`. Returns `nothing` without consuming
input when the encoded element type does not match `T`.
"""
function readlist(r::Reader, ::Type{T}) where {T}
    start = r.pos
    count, type = readlistheader(r)
    matches(type, T) || (r.pos = start; return nothing)
    _enterdepth!(r)
    _readerarray!(r, T, count)
    out = Vector{T}(undef, count)
    for i in 1:count
        out[i] = readelement(r, T)
    end
    leave!(r)
    return out
end

function readmap(r::Reader, ::Type{K}, ::Type{V}) where {K,V}
    start = r.pos
    count, keytype, valuetype = readmapheader(r)
    if count > 0 && !(matches(keytype, K) && matches(valuetype, V))
        r.pos = start
        return nothing
    end
    _enterdepth!(r)
    _readerarray!(r, Pair{K,V}, count)
    out = Vector{Pair{K,V}}(undef, count)
    for i in 1:count
        key = readelement(r, K)
        out[i] = key => readelement(r, V)
    end
    leave!(r)
    return out
end

function decode end

function decode(bytes::AbstractVector{UInt8}, ::Type{T}; limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits)) where {T}
    reader = Reader(bytes; limits=limits, budget=budget)
    try
        return decode(reader, T)
    catch
        iszero(reader.materialized) || _release!(budget, reader.materialized)
        rethrow()
    end
end

struct _WriteCountOverflow <: Exception end

mutable struct _CountingBuffer
    count::Int64
end

function _writecount!(buffer::_CountingBuffer, bytes::Integer)
    bytes >= 0 || throw(ArgumentError("Thrift byte count must be nonnegative"))
    bytes <= typemax(Int64) || throw(_WriteCountOverflow())
    buffer.count = try
        Base.checked_add(buffer.count, Int64(bytes))
    catch err
        err isa OverflowError || rethrow()
        throw(_WriteCountOverflow())
    end
    return buffer
end

function Base.push!(buffer::_CountingBuffer, ::UInt8)
    return _writecount!(buffer, 1)
end

function Base.append!(buffer::_CountingBuffer,
    bytes::AbstractVector{UInt8})
    return _writecount!(buffer, length(bytes))
end

mutable struct _FixedBuffer
    bytes::Vector{UInt8}
    position::Int
end

function _fixedend(buffer::_FixedBuffer, count::Int)
    count >= 0 || throw(ArgumentError("Thrift byte count must be nonnegative"))
    stop = try
        Base.checked_add(buffer.position, count)
    catch err
        err isa OverflowError || rethrow()
        throw(AssertionError(
            "Compact Thrift encoding exceeded its counted byte size"))
    end
    stop <= length(buffer.bytes) || throw(AssertionError(
        "Compact Thrift encoding exceeded its counted byte size"))
    return stop
end

function Base.push!(buffer::_FixedBuffer, byte::UInt8)
    stop = _fixedend(buffer, 1)
    @inbounds buffer.bytes[stop] = byte
    buffer.position = stop
    return buffer
end

function Base.append!(buffer::_FixedBuffer,
    bytes::AbstractVector{UInt8})
    count = length(bytes)
    iszero(count) && return buffer
    stop = _fixedend(buffer, count)
    source = firstindex(bytes)
    destination = buffer.position + 1
    for offset in 0:(count - 1)
        @inbounds buffer.bytes[destination + offset] = bytes[source + offset]
    end
    buffer.position = stop
    return buffer
end

struct Writer{B}
    buffer::B
end

function Writer()
    return Writer(UInt8[])
end

function _encodedsize(value)
    buffer = _CountingBuffer(Int64(0))
    encode!(Writer(buffer), value)
    return buffer.count
end

function _encodefixed!(bytes::Vector{UInt8}, value)
    buffer = _FixedBuffer(bytes, 0)
    encode!(Writer(buffer), value)
    buffer.position == length(bytes) || throw(AssertionError(
        "Compact Thrift encoding wrote $(buffer.position) bytes after counting " *
        "$(length(bytes))"))
    return bytes
end

function writebyte!(w::Writer, byte::UInt8)
    push!(w.buffer, byte)
    return
end

function writevarint!(w::Writer, value::UInt64)
    while value >= 0x80
        push!(w.buffer, UInt8(value & 0x7f) | 0x80)
        value >>= 7
    end
    push!(w.buffer, UInt8(value))
    return
end

function writei8!(w::Writer, value::Int8)
    writebyte!(w, value % UInt8)
    return
end

function writei16!(w::Writer, value::Int16)
    writei32!(w, Int32(value))
    return
end

function writei32!(w::Writer, value::Int32)
    writevarint!(w, UInt64(reinterpret(UInt32, (value << 1) ⊻ (value >> 31))))
    return
end

function writei64!(w::Writer, value::Int64)
    writevarint!(w, reinterpret(UInt64, (value << 1) ⊻ (value >> 63)))
    return
end

function writedouble!(w::Writer, value::Float64)
    bits = reinterpret(UInt64, value)
    for i in 0:7
        push!(w.buffer, UInt8((bits >> (8 * i)) & 0xff))
    end
    return
end

function writebool!(w::Writer, value::Bool)
    writebyte!(w, value ? BOOL_TRUE : BOOL_FALSE)
    return
end

function writebinary!(w::Writer, bytes::AbstractVector{UInt8})
    length(bytes) <= typemax(Int32) || throw(ArgumentError("Thrift binary is longer than 2^31 - 1 bytes"))
    writevarint!(w, UInt64(length(bytes)))
    append!(w.buffer, bytes)
    return
end

function writestring!(w::Writer, value::AbstractString)
    writebinary!(w, codeunits(value))
    return
end

function writefieldheader!(w::Writer, lastid::Int16, id::Int16, type::UInt8)
    delta = Int(id) - Int(lastid)
    if 0 < delta <= 15
        push!(w.buffer, UInt8(delta << 4) | type)
    else
        push!(w.buffer, type)
        writei16!(w, id)
    end
    return id
end

function writestop!(w::Writer)
    writebyte!(w, STOP)
    return
end

function writelistheader!(w::Writer, count::Int, type::UInt8)
    count <= typemax(Int32) || throw(ArgumentError("Thrift list is longer than 2^31 - 1 elements"))
    if count < 15
        push!(w.buffer, UInt8(count << 4) | type)
    else
        push!(w.buffer, 0xf0 | type)
        writevarint!(w, UInt64(count))
    end
    return
end

function writelist!(w::Writer, values::Vector{T}) where {T}
    writelistheader!(w, length(values), typecode(T))
    for value in values
        writeelement!(w, value)
    end
    return
end

function writemap!(w::Writer, pairs::Vector{Pair{K,V}}) where {K,V}
    count = length(pairs)
    count <= typemax(Int32) || throw(ArgumentError("Thrift map is longer than 2^31 - 1 entries"))
    writevarint!(w, UInt64(count))
    count == 0 && return
    push!(w.buffer, UInt8(typecode(K) << 4) | typecode(V))
    for (key, value) in pairs
        writeelement!(w, key)
        writeelement!(w, value)
    end
    return
end

function writeelement!(w::Writer, value::Bool)
    writebool!(w, value)
    return
end

function writeelement!(w::Writer, value::Int8)
    writei8!(w, value)
    return
end

function writeelement!(w::Writer, value::Int16)
    writei16!(w, value)
    return
end

function writeelement!(w::Writer, value::Int32)
    writei32!(w, value)
    return
end

function writeelement!(w::Writer, value::Int64)
    writei64!(w, value)
    return
end

function writeelement!(w::Writer, value::Float64)
    writedouble!(w, value)
    return
end

function writeelement!(w::Writer, value::String)
    writestring!(w, value)
    return
end

function writeelement!(w::Writer, value::Vector{UInt8})
    writebinary!(w, value)
    return
end

function writeelement!(w::Writer, value::Vector{T}) where {T}
    writelist!(w, value)
    return
end

function writeelement!(w::Writer, value::Vector{Pair{K,V}}) where {K,V}
    writemap!(w, value)
    return
end

"""
    writeraw!(w, lastid, field) -> id

Re-emit a preserved field. The verbatim header is copied when the preceding field id
matches the one seen at decode time; otherwise a canonical header is synthesized.
"""
function writeraw!(w::Writer, lastid::Int16, field::RawField)
    if field.headerlength > 0 && lastid == field.previd
        append!(w.buffer, field.bytes)
        return field.id
    end
    writefieldheader!(w, lastid, field.id, field.type)
    append!(w.buffer, payload(field))
    return field.id
end

"""
    writeunknownafter!(w, unknown, index, lastid) -> (lastid, index)

Re-emit, in encounter order, the preserved fields that originally followed field `lastid`.
"""
@inline function writeunknownafter!(w::Writer, unknown::Tuple, index::Int,
    lastid::Int16)
    while index <= length(unknown)
        field = unknown[index]
        (field.headerlength > 0 && field.previd == lastid) || break
        lastid = writeraw!(w, lastid, field)
        index += 1
    end
    return (lastid, index)
end

@inline function writeunknownrest!(w::Writer, unknown::Tuple, index::Int,
    lastid::Int16)
    while index <= length(unknown)
        lastid = writeraw!(w, lastid, unknown[index])
        index += 1
    end
    return lastid
end

"""
    checkunion(name, known, unknown)

Reject a decoded Thrift union with more than one member (known or preserved unknown).
"""
function checkunion(structname::Symbol, known::Integer, unknown::Tuple)
    known + length(unknown) <= 1 && return
    throw(FormatError("Thrift union $structname has more than one member set"))
end

function checkunionargs(structname::Symbol, known::Integer, unknown::Tuple)
    known + length(unknown) <= 1 && return
    throw(ArgumentError("Thrift union $structname accepts at most one member"))
end

function encode! end

function encode(value)
    w = Writer()
    encode!(w, value)
    return w.buffer
end

function enumnames end

function name(x::ThriftEnum)
    for (value, symbol) in enumnames(typeof(x))
        value == x.value && return symbol
    end
    return nothing
end

function Base.show(io::IO, x::ThriftEnum)
    symbol = name(x)
    modname = nameof(parentmodule(typeof(x)))
    symbol === nothing && return print(io, modname, ".T(", x.value, ")")
    print(io, modname, ".", symbol)
    return
end

end
