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

function decode_plain(::Type{Bool}, bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits())
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    bytecount = cld(Int(count), 8)
    position = Int(offset)
    _requirebytes(bytes, position, bytecount)
    output = Vector{Bool}(undef, Int(count))
    @inbounds for index in 0:(Int(count) - 1)
        output[index + 1] = !iszero(bytes[position + (index >> 3)] & (UInt8(1) << (index & 7)))
    end
    return output, position + bytecount
end

function decode_plain(::Type{T}, bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits()) where {T<:Union{Int32,Int64,Float32,Float64}}
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    position = Int(offset)
    total = Base.checked_mul(Int(count), sizeof(T))
    _requirebytes(bytes, position, total)
    output = Vector{T}(undef, Int(count))
    U = _plainbits(T)
    @inbounds for index in eachindex(output)
        bits, position = _readlittle(U, bytes, position)
        output[index] = _fromplainbits(T, bits)
    end
    return output, position
end

function decode_plain_byte_array(bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits())
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    position = Int(offset)
    _requirebytes(bytes, position, Base.checked_mul(Int(count), 4))
    output = Vector{Vector{UInt8}}(undef, Int(count))
    for index in eachindex(output)
        rawlength, position = _readlittle(UInt32, bytes, position)
        length = reinterpret(Int32, rawlength)
        length >= 0 || throw(FormatError("negative PLAIN byte-array length"))
        _checklimit(:string_bytes, length, limits.max_string_bytes)
        _requirebytes(bytes, position, length)
        output[index] = collect(@view bytes[position:(position + length - 1)])
        position += length
    end
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
