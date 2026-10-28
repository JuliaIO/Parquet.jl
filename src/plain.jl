# Byte vectors the decoders read: page payloads and decompressed pages are always
# contiguous views of a `Vector{UInt8}`.
const _ByteView = SubArray{UInt8,1,Vector{UInt8},Tuple{UnitRange{Int}},true}

function _byteview(bytes::_ByteView)
    return bytes
end

function _byteview(bytes::Vector{UInt8})
    return view(bytes, 1:length(bytes))
end

function _byteview(bytes::AbstractVector{UInt8})
    return _byteview(_contiguous(bytes))
end

function _growto!(values::Vector, count::Int)
    length(values) < count && resize!(values, count)
    return values
end

function _requirebytes(bytes::AbstractVector{UInt8}, offset::Integer, count::Integer)
    offset >= 1 || throw(BoundsError(bytes, offset))
    count >= 0 || throw(ArgumentError("byte count must be nonnegative"))
    last = Base.checked_add(Int(offset) - 1, Int(count))
    last <= length(bytes) || throw(FormatError("truncated PLAIN value"))
    return
end

function _readlittle(::Type{U}, bytes::AbstractVector{UInt8}, offset::Int) where {U<:Unsigned}
    width = sizeof(U)
    _requirebytes(bytes, offset, width)
    value = zero(U)
    @inbounds for index in 0:(width - 1)
        value |= U(bytes[offset + index]) << (8 * index)
    end
    return value, offset + width
end

function _writelittle!(output::Vector{UInt8}, value::U) where {U<:Unsigned}
    @inbounds for index in 0:(sizeof(U) - 1)
        push!(output, UInt8((value >> (8 * index)) & U(0xff)))
    end
    return
end

function _plainbits(::Type{Int32})
    return UInt32
end

function _plainbits(::Type{Int64})
    return UInt64
end

function _plainbits(::Type{Float32})
    return UInt32
end

function _plainbits(::Type{Float64})
    return UInt64
end

function _fromplainbits(::Type{T}, value::U) where {T,U<:Unsigned}
    return reinterpret(T, value)
end

function _toplainbits(::Type{U}, value::T) where {U<:Unsigned,T}
    return reinterpret(U, value)
end

function decode_plain(::Type{T}, bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits()) where {T<:Union{Bool,Int32,Int64,Float32,Float64}}
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    output = T[]
    position = _plain!(output, 0, Int(count), T, identity, _byteview(bytes), Int(offset))
    return output, position
end

function decode_plain_byte_array(bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits())
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    output = Vector{UInt8}[]
    position = _plainbytes!(output, 0, Int(count), identity, nothing,
        _byteview(bytes), Int(offset), limits)
    return output, position
end

function decode_plain_fixed(bytes::AbstractVector{UInt8}, count::Integer, width::Integer;
    offset::Integer=1, limits::Limits=Limits())
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    width >= 0 || throw(ArgumentError("fixed byte-array width must be nonnegative"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    total = Base.checked_mul(Int(count), Int(width))
    position = Int(offset)
    _requirebytes(bytes, position, total)
    output = Matrix{UInt8}(undef, Int(width), Int(count))
    isempty(output) || copyto!(output, 1, bytes, position, total)
    return output, position + total
end

# The PLAIN decoders below write `count` values to `dest[(offset + 1):(offset + count)]`,
# applying `convert` to each physical value, and return the position after the values.
# `dest` grows only after the bytes for all `count` values are known to be present.

function _plain!(dest::Vector{L}, offset::Int, count::Int, ::Type{Bool}, convert::C,
        bytes::_ByteView, position::Int) where {L,C}
    total = cld(count, 8)
    _requirebytes(bytes, position, total)
    _growto!(dest, offset + count)
    @inbounds for index in 0:(count - 1)
        bit = bytes[position + (index >> 3)] & (UInt8(1) << (index & 7))
        dest[offset + index + 1] = convert(!iszero(bit))
    end
    return position + total
end

function _plain!(dest::Vector{L}, offset::Int, count::Int, ::Type{P}, convert::C,
        bytes::_ByteView, position::Int) where {L,P<:Union{Int32,Int64,Float32,Float64},C}
    total = Base.checked_mul(count, sizeof(P))
    _requirebytes(bytes, position, total)
    _growto!(dest, offset + count)
    GC.@preserve bytes dest begin
        source = Ptr{P}(pointer(bytes, position))
        if (L === P && convert === identity) || (_bitcopy(convert) && sizeof(L) == sizeof(P))
            unsafe_copyto!(Ptr{P}(pointer(dest, offset + 1)), source, count)
            _aftercopy!(convert, dest, offset, count)
        else
            @inbounds for index in 1:count
                dest[offset + index] = convert(ltoh(unsafe_load(source, index)))
            end
        end
    end
    return position + total
end

# BYTE_ARRAY values (`width === nothing`) carry a 4-byte little-endian length;
# FIXED_LEN_BYTE_ARRAY values are `width` bytes each. `convert` receives a view of the
# value's bytes and must copy what it keeps.
function _plainbytes!(dest::Vector{L}, offset::Int, count::Int, convert::C,
        width::Nothing, bytes::_ByteView, position::Int, limits::Limits) where {L,C}
    _requirebytes(bytes, position, Base.checked_mul(count, 4))
    _growto!(dest, offset + count)
    for index in 1:count
        raw = GC.@preserve bytes ltoh(unsafe_load(Ptr{UInt32}(pointer(bytes, position))))
        position += 4
        length = reinterpret(Int32, raw)
        length >= 0 || throw(FormatError("negative PLAIN byte-array length"))
        _checklimit(:string_bytes, length, limits.max_string_bytes)
        _requirebytes(bytes, position, length)
        dest[offset + index] = _bytevalue(convert, view(bytes, position:(position + length - 1)))
        position += length
        # The next length prefix must be present before it is loaded.
        index < count && _requirebytes(bytes, position, 4)
    end
    return position
end

function _plainbytes!(dest::Vector{L}, offset::Int, count::Int, convert::C,
        width::Int, bytes::_ByteView, position::Int, limits::Limits) where {L,C}
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    total = Base.checked_mul(count, width)
    _requirebytes(bytes, position, total)
    _growto!(dest, offset + count)
    # Julia's platforms are little-endian, so 128-bit integers copy as stored.
    if convert isa Union{_FixedBytesValue,_Int128Value} && sizeof(L) == width
        GC.@preserve bytes dest unsafe_copyto!(Ptr{UInt8}(pointer(dest, offset + 1)),
            pointer(bytes, position), total)
        return position + total
    end
    first = 1
    if convert isa _DecimalValue && width <= 8
        first = _plainshortdecimals!(dest, offset, count, convert, width, bytes, position)
        position += (first - 1) * width
    end
    for index in first:count
        dest[offset + index] = _bytevalue(convert, view(bytes, position:(position + width - 1)))
        position += width
    end
    return position
end

# FIXED_LEN_BYTE_ARRAY decimals of at most 8 bytes: each value is one unaligned 8-byte
# big-endian load, shifted down to its width (which sign-extends it). Returns the index of
# the first value left for the per-value path: those whose 8-byte load would run past
# `bytes`.
function _plainshortdecimals!(dest::Vector{D}, offset::Int, count::Int,
        convert::_DecimalValue{D,T}, width::Int, bytes::_ByteView,
        position::Int) where {D,T}
    T <: Union{Int32,Int64,Int128} || return 1
    fast = clamp(fld(length(bytes) - 8 - position + 1, width) + 1, 0, count)
    shift = 64 - 8width
    bound = Int128(convert.bound)
    GC.@preserve bytes begin
        base = pointer(bytes, position)
        @inbounds for index in 1:fast
            raw = bswap(unsafe_load(Ptr{UInt64}(base + (index - 1) * width)))
            value = reinterpret(Int64, raw) >> shift
            (value < bound) & (value > -bound) || _decimalrangeerror(convert)
            dest[offset + index] = reinterpret(D, value % T)
        end
    end
    return fast + 1
end

function _bytevalue(::typeof(identity), bytes::_ByteView)
    return collect(bytes)
end

function _bytevalue(convert, bytes::_ByteView)
    return convert(bytes)
end

function encode_plain(values::AbstractVector{Bool})
    output = zeros(UInt8, cld(length(values), 8))
    @inbounds for index in eachindex(values)
        values[index] || continue
        zeroindex = index - 1
        output[(zeroindex >> 3) + 1] |= UInt8(1) << (zeroindex & 7)
    end
    return output
end

function encode_plain(values::AbstractVector{T}) where {T<:Union{Int32,Int64,Float32,Float64}}
    output = UInt8[]
    sizehint!(output, Base.checked_mul(length(values), sizeof(T)))
    U = _plainbits(T)
    for value in values
        _writelittle!(output, _toplainbits(U, value))
    end
    return output
end

function encode_plain_byte_array(values)
    output = UInt8[]
    for value in values
        bytes = value isa AbstractString ? codeunits(value) : value
        length(bytes) <= typemax(Int32) || throw(ArgumentError("byte array exceeds Int32 length"))
        _writelittle!(output, reinterpret(UInt32, Int32(length(bytes))))
        append!(output, bytes)
    end
    return output
end

function encode_plain_fixed(values::AbstractMatrix{UInt8})
    return collect(vec(values))
end
