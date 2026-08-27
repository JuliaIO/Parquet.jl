# BYTE_STREAM_SPLIT (Encodings.md, Parquet 2.13.0): K byte streams of N values each.

function _splitbits(::Type{Int32})
    return UInt32
end

function _splitbits(::Type{Int64})
    return UInt64
end

function _splitbits(::Type{Float32})
    return UInt32
end

function _splitbits(::Type{Float64})
    return UInt64
end

function _splitbytes(count::Integer, width::Integer)
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    width >= 0 || throw(ArgumentError("byte width must be nonnegative"))
    (count == 0 || width == 0) && return 0
    count <= typemax(Int) ÷ width || throw(FormatError("BYTE_STREAM_SPLIT size overflows"))
    return Int(count) * Int(width)
end

function decode_byte_stream_split!(output::AbstractVector{T}, bytes::AbstractVector{UInt8};
    offset::Integer=1) where {T<:Union{Int32,Int64,Float32,Float64}}
    count = length(output)
    width = sizeof(T)
    total = _splitbytes(count, width)
    position = Int(offset)
    _requirebytes(bytes, position, total)
    U = _splitbits(T)
    base = firstindex(output)
    @inbounds for index in 0:(count - 1)
        value = zero(U)
        for stream in 0:(width - 1)
            value |= U(bytes[position + stream * count + index]) << (8 * stream)
        end
        output[base + index] = reinterpret(T, value)
    end
    return position + total
end

function decode_byte_stream_split(::Type{T}, bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits()) where {T<:Union{Int32,Int64,Float32,Float64}}
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    _checkbytes(count, sizeof(T), limits)
    output = Vector{T}(undef, _structural(count))
    position = decode_byte_stream_split!(output, bytes; offset=offset)
    return output, position
end

function decode_byte_stream_split_fixed(bytes::AbstractVector{UInt8}, count::Integer, width::Integer;
    offset::Integer=1, limits::Limits=Limits())
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    width > 0 || throw(FormatError("fixed byte-array width must be positive"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    total = _splitbytes(count, width)
    _checklimit(:page_bytes, total, limits.max_page_bytes)
    position = Int(offset)
    _requirebytes(bytes, position, total)
    size = _structural(count)
    fixedwidth = _structural(width)
    output = Matrix{UInt8}(undef, fixedwidth, size)
    @inbounds for index in 0:(size - 1), stream in 0:(fixedwidth - 1)
        output[stream + 1, index + 1] = bytes[position + stream * size + index]
    end
    return output, position + total
end

function encode_byte_stream_split(values::AbstractVector{T}) where {T<:Union{Int32,Int64,Float32,Float64}}
    count = length(values)
    width = sizeof(T)
    output = Vector{UInt8}(undef, _splitbytes(count, width))
    U = _splitbits(T)
    base = firstindex(values)
    @inbounds for index in 0:(count - 1)
        bits = reinterpret(U, values[base + index])
        for stream in 0:(width - 1)
            output[stream * count + index + 1] = UInt8((bits >> (8 * stream)) & 0xff)
        end
    end
    return output
end

function encode_byte_stream_split_fixed(values::AbstractMatrix{UInt8})
    width, count = size(values)
    width > 0 || throw(ArgumentError("fixed byte-array width must be positive"))
    output = Vector{UInt8}(undef, _splitbytes(count, width))
    @inbounds for index in 0:(count - 1), stream in 0:(width - 1)
        output[stream * count + index + 1] = values[stream + 1, index + 1]
    end
    return output
end
