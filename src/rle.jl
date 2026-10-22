function _readhybridvarint(bytes::AbstractVector{UInt8}, offset::Int)
    value = UInt64(0)
    position = offset
    for index in 0:9
        _requirebytes(bytes, position, 1)
        byte = bytes[position]
        position += 1
        index == 9 && byte > 0x01 && throw(FormatError("hybrid run header overflows UInt64"))
        value |= UInt64(byte & 0x7f) << (7 * index)
        iszero(byte & 0x80) && return value, position
    end
    throw(FormatError("unterminated hybrid run header"))
end

function _writehybridvarint!(output::Vector{UInt8}, value::UInt64)
    while value >= 0x80
        push!(output, UInt8(value & 0x7f) | 0x80)
        value >>= 7
    end
    push!(output, UInt8(value))
    return
end

function _readpackedvalue(bytes::AbstractVector{UInt8}, offset::Int, bitoffset::Int,
    bitwidth::Int)
    value = UInt64(0)
    bitwidth == 0 && return value
    @inbounds for bit in 0:(bitwidth - 1)
        absolute = bitoffset + bit
        byte = bytes[offset + (absolute >> 3)]
        value |= UInt64((byte >> (absolute & 7)) & 0x01) << bit
    end
    return value
end

# Writes `count` values of a bit-packed run of `bitwidth`-bit values starting at
# `bytes[base]` (least significant bit first) to `output[(offset + 1):(offset + count)]`.
# A value is read with one unaligned 8-byte load while those 8 bytes lie inside `bytes`.
function _unpack!(output::Vector{U}, offset::Int, count::Int, bytes::_ByteView,
        base::Int, bitwidth::Int) where {U<:Unsigned}
    if bitwidth == 0
        fill!(view(output, (offset + 1):(offset + count)), zero(U))
        return
    end
    mask = bitwidth == 64 ? typemax(UInt64) : (UInt64(1) << bitwidth) - 1
    room = length(bytes) - 7 - base
    fast = bitwidth > 56 || room < 0 ? 0 : min(count, (8 * room + 7) ÷ bitwidth + 1)
    GC.@preserve bytes begin
        pointer0 = pointer(bytes, base)
        @inbounds for index in 0:(fast - 1)
            bit = index * bitwidth
            word = ltoh(unsafe_load(Ptr{UInt64}(pointer0 + (bit >> 3))))
            output[offset + index + 1] = ((word >> (bit & 7)) & mask) % U
        end
    end
    @inbounds for index in fast:(count - 1)
        output[offset + index + 1] =
            _readpackedvalue(bytes, base, index * bitwidth, bitwidth) % U
    end
    return
end

# Decodes `count` RLE/bit-packed hybrid values of `bitwidth` bits from `bytes[position:]`
# into `output[(offset + 1):(offset + count)]`; the runs must end by `stop`. `output`
# grows run by run, so a count the runs do not back fails before it is allocated.
# `U` must hold `cld(bitwidth, 8)` bytes. Returns the position after the last run.
function _decodehybrid!(output::Vector{U}, offset::Int, count::Int, bitwidth::Int,
        bytes::_ByteView, position::Int, stop::Int) where {U<:Unsigned}
    filled = 0
    while filled < count
        position < stop || throw(FormatError("hybrid stream ended before all values"))
        header, position = _readhybridvarint(bytes, position)
        runs = header >> 1
        runs > 0 || throw(FormatError("zero-length hybrid run"))
        remaining = count - filled
        if isodd(header)
            runs <= UInt64(typemax(Int32) ÷ 8) ||
                throw(FormatError("bit-packed hybrid run exceeds Int32 values"))
            groups = Int(runs)
            payload = groups * bitwidth
            _requirebytes(bytes, position, payload)
            produced = min(8 * groups, remaining)
            _growto!(output, offset + filled + produced)
            _unpack!(output, offset + filled, produced, bytes, position, bitwidth)
            position += payload
        else
            runs <= UInt64(typemax(Int32)) ||
                throw(FormatError("RLE hybrid run exceeds Int32 values"))
            width = cld(bitwidth, 8)
            _requirebytes(bytes, position, width)
            value = UInt64(0)
            for index in 0:(width - 1)
                value |= UInt64(bytes[position + index]) << (8 * index)
            end
            position += width
            produced = min(Int(runs), remaining)
            _growto!(output, offset + filled + produced)
            fill!(view(output, (offset + filled + 1):(offset + filled + produced)),
                value % U)
        end
        position <= stop || throw(FormatError("hybrid run exceeds its declared length"))
        filled += produced
    end
    return position
end

# Start and end of a hybrid stream at `position`, after its 4-byte length when prefixed.
function _hybridbounds(bytes::AbstractVector{UInt8}, position::Int, length_prefix::Bool)
    length_prefix || return position, length(bytes) + 1
    rawlength, position = _readlittle(UInt32, bytes, position)
    bodylength = Int(rawlength)
    _requirebytes(bytes, position, bodylength)
    return position, position + bodylength
end

function decode_hybrid(bytes::AbstractVector{UInt8}, count::Integer, bitwidth::Integer;
    offset::Integer=1, length_prefix::Bool=false, limits::Limits=Limits())
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    0 <= bitwidth <= 64 || throw(ArgumentError("bit width must be between 0 and 64"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    data = _byteview(bytes)
    position, stop = _hybridbounds(data, Int(offset), length_prefix)
    output = UInt64[]
    position = _decodehybrid!(output, 0, Int(count), Int(bitwidth), data, position, stop)
    length_prefix && position != stop && throw(FormatError("hybrid stream has trailing bytes"))
    return output, position
end

function _setpackedvalue!(output::AbstractVector{UInt8}, bitoffset::Int, bitwidth::Int, value::UInt64)
    bitwidth == 0 && return
    @inbounds for bit in 0:(bitwidth - 1)
        iszero(value & (UInt64(1) << bit)) && continue
        absolute = bitoffset + bit
        output[(absolute >> 3) + 1] |= UInt8(1) << (absolute & 7)
    end
    return
end

function _hybridbody(values, bitwidth::Int)
    groups = cld(length(values), 8)
    groups == 0 && return UInt8[]
    output = UInt8[]
    _writehybridvarint!(output, (UInt64(groups) << 1) | 0x01)
    payloadstart = length(output)
    append!(output, zeros(UInt8, Base.checked_mul(groups, bitwidth)))
    limit = bitwidth == 64 ? typemax(UInt64) : (UInt64(1) << bitwidth) - 1
    for (index, rawvalue) in enumerate(values)
        rawvalue >= 0 || throw(ArgumentError("hybrid values must be nonnegative"))
        value = UInt64(rawvalue)
        value <= limit || throw(ArgumentError("hybrid value does not fit the bit width"))
        target = @view output[(payloadstart + 1):end]
        _setpackedvalue!(target, (index - 1) * bitwidth, bitwidth, value)
    end
    return output
end

function encode_hybrid(values, bitwidth::Integer; length_prefix::Bool=false)
    0 <= bitwidth <= 64 || throw(ArgumentError("bit width must be between 0 and 64"))
    body = _hybridbody(values, Int(bitwidth))
    length_prefix || return body
    length(body) <= typemax(UInt32) || throw(ArgumentError("hybrid stream exceeds UInt32 length"))
    output = UInt8[]
    _writelittle!(output, UInt32(length(body)))
    append!(output, body)
    return output
end

function decode_bit_packed(bytes::AbstractVector{UInt8}, count::Integer, bitwidth::Integer;
    offset::Integer=1, limits::Limits=Limits())
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    0 <= bitwidth <= 64 || throw(ArgumentError("bit width must be between 0 and 64"))
    count <= typemax(Int) || throw(FormatError("bit-packed value count overflows Int"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    size = Int(count)
    width = Int(bitwidth)
    totalbits = try
        Base.checked_mul(size, width)
    catch err
        err isa OverflowError || rethrow()
        throw(FormatError("bit-packed payload size overflows Int"))
    end
    bytecount = cld(totalbits, 8)
    position = Int(offset)
    _requirebytes(bytes, position, bytecount)
    output = Vector{UInt64}(undef, size)
    @inbounds for index in 0:(size - 1)
        value = UInt64(0)
        for bit in 0:(width - 1)
            absolute = index * width + bit
            packed = (bytes[position + (absolute >> 3)] >> (7 - (absolute & 7))) & 0x01
            value = (value << 1) | UInt64(packed)
        end
        output[index + 1] = value
    end
    padding = bytecount * 8 - totalbits
    if padding > 0
        mask = UInt8((UInt16(1) << padding) - 1)
        iszero(bytes[position + bytecount - 1] & mask) ||
            throw(FormatError("BIT_PACKED stream has nonzero padding bits"))
    end
    return output, position + bytecount
end
