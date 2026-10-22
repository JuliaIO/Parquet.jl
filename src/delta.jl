# DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, and DELTA_BYTE_ARRAY (Encodings.md, Parquet 2.13.0).
# Value reconstruction wraps in two's complement at the physical width, as the specification
# requires; only structural arithmetic (counts, sizes, positions) is checked.

const DELTA_BLOCK_SIZE = 128
const DELTA_MINIBLOCKS = 4
const DELTA_MINIBLOCK_VALUES = DELTA_BLOCK_SIZE ÷ DELTA_MINIBLOCKS

struct DeltaHeader
    blocksize::Int
    miniblocks::Int
    miniblockvalues::Int
    count::Int
    first::Int64
end

function _readuleb128(bytes::AbstractVector{UInt8}, offset::Int)
    value = UInt64(0)
    position = offset
    for index in 0:9
        _requirebytes(bytes, position, 1)
        byte = bytes[position]
        position += 1
        index == 9 && byte > 0x01 && throw(FormatError("delta varint overflows 64 bits"))
        value |= UInt64(byte & 0x7f) << (7 * index)
        iszero(byte & 0x80) && return value, position
    end
    throw(FormatError("delta varint is longer than 10 bytes"))
end

function _readzigzag(bytes::AbstractVector{UInt8}, offset::Int)
    raw, position = _readuleb128(bytes, offset)
    return reinterpret(Int64, (raw >> 1) ⊻ (-(raw & 0x01))), position
end

function _writeuleb128!(output::Vector{UInt8}, value::UInt64)
    while value >= 0x80
        push!(output, UInt8(value & 0x7f) | 0x80)
        value >>= 7
    end
    push!(output, UInt8(value))
    return
end

function _writezigzag!(output::Vector{UInt8}, value::Int32)
    _writeuleb128!(output, UInt64(reinterpret(UInt32, (value << 1) ⊻ (value >> 31))))
    return
end

function _writezigzag!(output::Vector{UInt8}, value::Int64)
    _writeuleb128!(output, reinterpret(UInt64, (value << 1) ⊻ (value >> 63)))
    return
end

function _checkedposition(position::Int, count::Integer)
    0 <= count <= typemax(Int) - position || throw(FormatError("delta stream position overflows"))
    return position + Int(count)
end

function _structural(value::Integer)
    0 <= value <= typemax(Int) || throw(FormatError("delta structural size $value overflows"))
    return Int(value)
end

# Charge `count` values of `width` bytes to the page limit without overflowing the product.
function _checkbytes(count::Integer, width::Integer, limits::Limits)
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    width > 0 || throw(ArgumentError("byte width must be positive"))
    count <= limits.max_page_bytes ÷ width && return Int64(count * width)
    requested = count <= typemax(Int64) ÷ width ? Int64(count * width) : typemax(Int64)
    throw(LimitError(:page_bytes, requested, limits.max_page_bytes))
end

function _deltaint(raw::UInt64, what::String)
    raw <= typemax(Int32) || throw(FormatError("delta $what $raw exceeds Int32"))
    return Int(raw)
end

function _checkdeltalayout(blocksize::Int, miniblocks::Int)
    blocksize > 0 && blocksize % 128 == 0 || throw(FormatError("delta block size $blocksize is not a positive multiple of 128"))
    0 < miniblocks <= blocksize || throw(FormatError("delta miniblock count $miniblocks is invalid for block size $blocksize"))
    blocksize % miniblocks == 0 || throw(FormatError("delta block size $blocksize is not divisible by $miniblocks miniblocks"))
    miniblockvalues = blocksize ÷ miniblocks
    miniblockvalues % 32 == 0 || throw(FormatError("delta miniblock holds $miniblockvalues values, not a multiple of 32"))
    return miniblockvalues
end

function _readdeltaheader(bytes::AbstractVector{UInt8}, offset::Int, count::Int, limits::Limits)
    rawblocksize, position = _readuleb128(bytes, offset)
    rawminiblocks, position = _readuleb128(bytes, position)
    rawcount, position = _readuleb128(bytes, position)
    first, position = _readzigzag(bytes, position)
    blocksize = _deltaint(rawblocksize, "block size")
    miniblocks = _deltaint(rawminiblocks, "miniblock count")
    total = _deltaint(rawcount, "value count")
    _checklimit(:container_elements, blocksize, limits.max_container_elements)
    _checklimit(:container_elements, miniblocks, limits.max_container_elements)
    _checklimit(:container_elements, total, limits.max_container_elements)
    miniblockvalues = _checkdeltalayout(blocksize, miniblocks)
    total == count || throw(FormatError("delta value count $total does not match the expected $count"))
    return DeltaHeader(blocksize, miniblocks, miniblockvalues, total, first), position
end

function _deltawidth(::Type{Int64})
    return 64
end

function _deltawidth(::Type{Int32})
    return 32
end

function _deltavalue(::Type{Int64}, accumulator::UInt64)
    return reinterpret(Int64, accumulator)
end

function _deltavalue(::Type{Int32}, accumulator::UInt64)
    return reinterpret(Int32, accumulator % UInt32)
end

function _deltasigned(::Type{Int64}, value::Int64, what::String)
    return value
end

function _deltasigned(::Type{Int32}, value::Int64, what::String)
    typemin(Int32) <= value <= typemax(Int32) || throw(FormatError("delta $what $value exceeds Int32"))
    return Int32(value)
end

function _deltamask(width::Int)
    width == 64 && return typemax(UInt64)
    return (UInt64(1) << width) - UInt64(1)
end

function _unpackdeltas!(::Type{B}, output::AbstractVector{T}, index::Int, count::Int,
    bytes::AbstractVector{UInt8}, offset::Int, width::Int, accumulator::UInt64,
    mindelta::UInt64) where {B<:Unsigned,T}
    mask = B(_deltamask(width))
    buffer = zero(B)
    bits = 0
    position = offset
    @inbounds for slot in 0:(count - 1)
        while bits < width
            buffer |= B(bytes[position]) << bits
            position += 1
            bits += 8
        end
        accumulator += mindelta + UInt64(buffer & mask)
        buffer >>= width
        bits -= width
        output[index + slot] = _deltavalue(T, accumulator)
    end
    return accumulator
end

function _unpackminiblock!(output::AbstractVector{T}, index::Int, count::Int,
    bytes::AbstractVector{UInt8}, offset::Int, width::Int, accumulator::UInt64,
    mindelta::UInt64) where {T}
    if width == 0
        @inbounds for slot in 0:(count - 1)
            accumulator += mindelta
            output[index + slot] = _deltavalue(T, accumulator)
        end
        return accumulator
    end
    width <= 56 && return _unpackdeltas!(UInt64, output, index, count, bytes, offset, width, accumulator, mindelta)
    return _unpackdeltas!(UInt128, output, index, count, bytes, offset, width, accumulator, mindelta)
end

# Width bytes of miniblocks that hold no values may be arbitrary; they are validated only when used.
function _readminiblockwidths!(widths::Vector{UInt8}, bytes::AbstractVector{UInt8}, offset::Int)
    _requirebytes(bytes, offset, length(widths))
    @inbounds for index in eachindex(widths)
        widths[index] = bytes[offset + index - 1]
    end
    return offset + length(widths)
end

function _miniblockwidth(::Type{T}, width::UInt8) where {T}
    width <= _deltawidth(T) || throw(FormatError("delta miniblock bit width $width exceeds the physical width $(_deltawidth(T))"))
    return Int(width)
end

function _miniblockpayload(header::DeltaHeader, width::Int, limits::Limits)
    payload = (Int64(header.miniblockvalues) * Int64(width)) >> 3
    _checklimit(:page_bytes, payload, limits.max_page_bytes)
    return _structural(payload)
end

function _decodedeltablock!(output::AbstractVector{T}, index::Int, bytes::AbstractVector{UInt8},
    offset::Int, header::DeltaHeader, widths::Vector{UInt8}, accumulator::UInt64,
    limits::Limits) where {T}
    signedmin, position = _readzigzag(bytes, offset)
    mindelta = reinterpret(UInt64, Int64(_deltasigned(T, signedmin, "minimum delta")))
    position = _readminiblockwidths!(widths, bytes, position)
    count = length(output)
    for miniblock in 1:header.miniblocks
        index > count && break
        width = _miniblockwidth(T, widths[miniblock])
        payload = _miniblockpayload(header, width, limits)
        _checkedposition(position, payload)
        _requirebytes(bytes, position, payload)
        available = min(header.miniblockvalues, count - index + 1)
        accumulator = _unpackminiblock!(output, index, available, bytes, position, width, accumulator, mindelta)
        position += payload
        index += available
    end
    return index, position, accumulator
end

function decode_delta_binary_packed!(output::AbstractVector{T}, bytes::AbstractVector{UInt8};
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits)) where {T<:Union{Int32,Int64}}
    count = length(output)
    header, position = _readdeltaheader(bytes, Int(offset), count, limits)
    count == 0 && return position
    first = _deltasigned(T, header.first, "first value")
    output[firstindex(output)] = first
    count == 1 && return position
    _requirebytes(bytes, position, header.miniblocks + 1)
    widthscharge = _reservearray!(budget, UInt8, header.miniblocks)
    widths = Vector{UInt8}(undef, header.miniblocks)
    try
        accumulator = reinterpret(UInt64, Int64(first))
        index = firstindex(output) + 1
        while index <= lastindex(output)
            index, position, accumulator = _decodedeltablock!(output, index,
                bytes, position, header, widths, accumulator, limits)
        end
    finally
        _release!(budget, widthscharge)
    end
    return position
end

function _decode_delta_binary_packed(::Type{T}, bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits)) where {T<:Union{Int32,Int64}}
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    _checkbytes(count, sizeof(T), limits)
    outputcharge = _reservearray!(budget, T, count)
    output = Vector{T}(undef, _structural(count))
    position = try
        decode_delta_binary_packed!(output, bytes; offset=offset, limits=limits,
            budget=budget)
    catch
        _release!(budget, outputcharge)
        rethrow()
    end
    return output, position, outputcharge
end

function decode_delta_binary_packed(::Type{T}, bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits)) where {T<:Union{Int32,Int64}}
    output, position, _ = _decode_delta_binary_packed(T, bytes, count;
        offset=offset, limits=limits, budget=budget)
    return output, position
end

function _bitwidth(value::UInt64)
    return 64 - leading_zeros(value)
end

function _packvalues!(output::Vector{UInt8}, values::AbstractVector{UInt64}, width::Int)
    width == 0 && return
    buffer = UInt128(0)
    bits = 0
    for value in values
        buffer |= UInt128(value) << bits
        bits += width
        while bits >= 8
            push!(output, UInt8(buffer & 0xff))
            buffer >>= 8
            bits -= 8
        end
    end
    bits > 0 && push!(output, UInt8(buffer & 0xff))
    return
end

function _encodedeltablock!(output::Vector{UInt8}, deltas::Vector{T}, count::Int,
    relative::Vector{UInt64}) where {T}
    mindelta = deltas[1]
    for index in 2:count
        mindelta = min(mindelta, deltas[index])
    end
    _writezigzag!(output, mindelta)
    for index in 1:DELTA_BLOCK_SIZE
        relative[index] = index <= count ? UInt64(reinterpret(unsigned(T), deltas[index] - mindelta)) : UInt64(0)
    end
    widths = zeros(UInt8, DELTA_MINIBLOCKS)
    for miniblock in 1:DELTA_MINIBLOCKS
        start = (miniblock - 1) * DELTA_MINIBLOCK_VALUES + 1
        start <= count || break
        widths[miniblock] = UInt8(_bitwidth(maximum(@view relative[start:(start + DELTA_MINIBLOCK_VALUES - 1)])))
    end
    append!(output, widths)
    for miniblock in 1:DELTA_MINIBLOCKS
        start = (miniblock - 1) * DELTA_MINIBLOCK_VALUES + 1
        start <= count || break
        _packvalues!(output, @view(relative[start:(start + DELTA_MINIBLOCK_VALUES - 1)]), Int(widths[miniblock]))
    end
    return
end

function encode_delta_binary_packed(values::AbstractVector{T}) where {T<:Union{Int32,Int64}}
    count = length(values)
    count <= typemax(Int32) || throw(ArgumentError("too many values for DELTA_BINARY_PACKED"))
    output = UInt8[]
    _writeuleb128!(output, UInt64(DELTA_BLOCK_SIZE))
    _writeuleb128!(output, UInt64(DELTA_MINIBLOCKS))
    _writeuleb128!(output, UInt64(count))
    _writezigzag!(output, count == 0 ? zero(T) : T(first(values)))
    count <= 1 && return output
    deltas = Vector{T}(undef, DELTA_BLOCK_SIZE)
    relative = Vector{UInt64}(undef, DELTA_BLOCK_SIZE)
    previous = T(first(values))
    index = firstindex(values) + 1
    while index <= lastindex(values)
        blockcount = min(DELTA_BLOCK_SIZE, lastindex(values) - index + 1)
        for slot in 1:blockcount
            value = T(values[index + slot - 1])
            deltas[slot] = value - previous
            previous = value
        end
        _encodedeltablock!(output, deltas, blockcount, relative)
        index += blockcount
    end
    return output
end

function _decode_delta_length_byte_array_offsets(bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    _checkbytes(count, 4, limits)
    size = _structural(count)
    lengthscharge = _reservearray!(budget, Int32, size)
    offsetscharge = try
        _reservearray!(budget, Int, size + 1)
    catch
        _release!(budget, lengthscharge)
        rethrow()
    end
    lengths = Vector{Int32}(undef, size)
    offsets = Vector{Int}(undef, size + 1)
    try
        position = decode_delta_binary_packed!(lengths, bytes; offset=offset,
            limits=limits, budget=budget)
        offsets[1] = position
        total = Int64(0)
        @inbounds for index in 1:size
            length = lengths[index]
            length >= 0 || throw(FormatError(
                "negative DELTA_LENGTH_BYTE_ARRAY length"))
            _checklimit(:string_bytes, length, limits.max_string_bytes)
            total += length
            _checklimit(:page_bytes, total, limits.max_page_bytes)
            offsets[index + 1] = _checkedposition(position, total)
        end
        last = _checkedposition(position, total)
        _requirebytes(bytes, position, last - position)
        _release!(budget, lengthscharge)
        return offsets, last, offsetscharge
    catch
        _release!(budget, _materializedsum(lengthscharge, offsetscharge))
        rethrow()
    end
end

function decode_delta_length_byte_array_offsets(bytes::AbstractVector{UInt8},
    count::Integer; offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    offsets, position, _ = _decode_delta_length_byte_array_offsets(bytes, count;
        offset=offset, limits=limits, budget=budget)
    return offsets, position
end

function _collectbytearrays(bytes::AbstractVector{UInt8}, offsets::Vector{Int},
    budget::_LiveByteBudget)
    count = length(offsets) - 1
    charge = _materializedarraybytes(Vector{UInt8}, count)
    for index in 1:count
        length = offsets[index + 1] - offsets[index]
        length >= 0 || throw(FormatError("byte-array offsets are not monotonic"))
        charge = _materializedsum(charge,
            _materializedarraybytes(UInt8, length))
    end
    _reserve!(budget, charge)
    output = Vector{Vector{UInt8}}(undef, count)
    try
        for index in eachindex(output)
            first = offsets[index]
            length = offsets[index + 1] - first
            value = Vector{UInt8}(undef, length)
            length == 0 || copyto!(value, 1, bytes, first, length)
            output[index] = value
        end
    catch
        _release!(budget, charge)
        rethrow()
    end
    return output, charge
end

function _decode_delta_length_byte_array(bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    offsets, position, offsetscharge = _decode_delta_length_byte_array_offsets(
        bytes, count; offset=offset, limits=limits, budget=budget)
    output, outputcharge = try
        _collectbytearrays(bytes, offsets, budget)
    catch
        _release!(budget, offsetscharge)
        rethrow()
    end
    _release!(budget, offsetscharge)
    return output, position, outputcharge
end

function decode_delta_length_byte_array(bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    output, position, _ = _decode_delta_length_byte_array(bytes, count;
        offset=offset, limits=limits, budget=budget)
    return output, position
end

function _bytearraylengths(values)
    lengths = Vector{Int32}(undef, length(values))
    for (index, value) in enumerate(values)
        bytes = value isa AbstractString ? codeunits(value) : value
        length(bytes) <= typemax(Int32) || throw(ArgumentError("byte array exceeds Int32 length"))
        lengths[index] = Int32(length(bytes))
    end
    return lengths
end

function encode_delta_length_byte_array(values)
    output = encode_delta_binary_packed(_bytearraylengths(values))
    for value in values
        append!(output, value isa AbstractString ? codeunits(value) : value)
    end
    return output
end

function _deltabytearraylayout(prefixes::Vector{Int32},
    suffixoffsets::Vector{Int}, limits::Limits, budget::_LiveByteBudget)
    count = length(prefixes)
    offsetscharge = _reservearray!(budget, Int, count + 1)
    offsets = Vector{Int}(undef, count + 1)
    try
        offsets[1] = 1
        previous = Int64(0)
        total = Int64(0)
        @inbounds for index in 1:count
            prefix = Int64(prefixes[index])
            0 <= prefix <= previous || throw(FormatError(
                "DELTA_BYTE_ARRAY prefix length $prefix exceeds the previous value"))
            length = prefix + (suffixoffsets[index + 1] - suffixoffsets[index])
            _checklimit(:string_bytes, length, limits.max_string_bytes)
            total += length
            _checklimit(:page_bytes, total, limits.max_page_bytes)
            offsets[index + 1] = _checkedposition(offsets[index], length)
            previous = length
        end
        return offsets, _structural(total), offsetscharge
    catch
        _release!(budget, offsetscharge)
        rethrow()
    end
end

function _fillbytearrays!(data::Vector{UInt8}, offsets::Vector{Int}, prefixes::Vector{Int32},
    bytes::AbstractVector{UInt8}, suffixoffsets::Vector{Int})
    @inbounds for index in eachindex(prefixes)
        prefix = Int(prefixes[index])
        target = offsets[index]
        prefix == 0 || copyto!(data, target, data, offsets[index - 1], prefix)
        suffixlength = suffixoffsets[index + 1] - suffixoffsets[index]
        suffixlength == 0 || copyto!(data, target + prefix, bytes, suffixoffsets[index], suffixlength)
    end
    return
end

function _decode_delta_byte_array_buffer(bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    count >= 0 || throw(ArgumentError("value count must be nonnegative"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    _checkbytes(count, 4, limits)
    size = _structural(count)
    prefixescharge = _reservearray!(budget, Int32, size)
    prefixes = Vector{Int32}(undef, size)
    suffixcharge = Int64(0)
    offsetscharge = Int64(0)
    datacharge = Int64(0)
    try
        position = decode_delta_binary_packed!(prefixes, bytes; offset=offset,
            limits=limits, budget=budget)
        suffixoffsets, position, suffixcharge =
            _decode_delta_length_byte_array_offsets(bytes, count; offset=position,
                limits=limits, budget=budget)
        offsets, total, offsetscharge = _deltabytearraylayout(prefixes,
            suffixoffsets, limits, budget)
        datacharge = _reservearray!(budget, UInt8, total)
        data = Vector{UInt8}(undef, total)
        _fillbytearrays!(data, offsets, prefixes, bytes, suffixoffsets)
        _release!(budget, _materializedsum(prefixescharge, suffixcharge))
        retained = _materializedsum(datacharge, offsetscharge)
        return data, offsets, position, retained, offsetscharge
    catch
        charge = _materializedsum(prefixescharge, suffixcharge)
        charge = _materializedsum(charge, offsetscharge)
        charge = _materializedsum(charge, datacharge)
        _release!(budget, charge)
        rethrow()
    end
end

function decode_delta_byte_array_buffer(bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    data, offsets, position, _, _ = _decode_delta_byte_array_buffer(bytes,
        count; offset=offset, limits=limits, budget=budget)
    return data, offsets, position
end

function _decode_delta_byte_array(bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    data, offsets, position, retained, _ = _decode_delta_byte_array_buffer(
        bytes, count; offset=offset, limits=limits, budget=budget)
    output, outputcharge = try
        _collectbytearrays(data, offsets, budget)
    catch
        _release!(budget, retained)
        rethrow()
    end
    _release!(budget, retained)
    return output, position, outputcharge
end

function decode_delta_byte_array(bytes::AbstractVector{UInt8}, count::Integer;
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    output, position, _ = _decode_delta_byte_array(bytes, count;
        offset=offset, limits=limits, budget=budget)
    return output, position
end

function _decode_delta_byte_array_fixed(bytes::AbstractVector{UInt8}, count::Integer,
    width::Integer;
    offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    width > 0 || throw(FormatError("fixed byte-array width must be positive"))
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    data, offsets, position, retained, offsetscharge =
        _decode_delta_byte_array_buffer(bytes, count; offset=offset,
            limits=limits, budget=budget)
    size = _structural(count)
    fixedwidth = _structural(width)
    objectcharge = Int64(0)
    try
        @inbounds for index in 1:size
            offsets[index + 1] - offsets[index] == width || throw(FormatError(
                "DELTA_BYTE_ARRAY value length differs from the fixed width $width"))
        end
        objectcharge = _reserveobjects!(budget)
        output = reshape(data, fixedwidth, size)
        _release!(budget, offsetscharge)
        outputcharge = _materializedsum(retained - offsetscharge, objectcharge)
        return output, position, outputcharge
    catch
        _release!(budget, _materializedsum(retained, objectcharge))
        rethrow()
    end
end

function decode_delta_byte_array_fixed(bytes::AbstractVector{UInt8}, count::Integer,
    width::Integer; offset::Integer=1, limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    output, position, _ = _decode_delta_byte_array_fixed(bytes, count, width;
        offset=offset, limits=limits, budget=budget)
    return output, position
end

function _commonprefix(previous::AbstractVector{UInt8}, current::AbstractVector{UInt8})
    limit = min(length(previous), length(current))
    index = 0
    @inbounds while index < limit && previous[index + 1] == current[index + 1]
        index += 1
    end
    return index
end

function encode_delta_byte_array(values)
    count = length(values)
    prefixes = Vector{Int32}(undef, count)
    suffixes = Vector{SubArray{UInt8,1,Vector{UInt8},Tuple{UnitRange{Int}},true}}(undef, count)
    previous = UInt8[]
    for (index, value) in enumerate(values)
        current = Vector{UInt8}(value isa AbstractString ? codeunits(value) : value)
        length(current) <= typemax(Int32) || throw(ArgumentError("byte array exceeds Int32 length"))
        prefix = _commonprefix(previous, current)
        prefixes[index] = Int32(prefix)
        suffixes[index] = @view current[(prefix + 1):end]
        previous = current
    end
    output = encode_delta_binary_packed(prefixes)
    append!(output, encode_delta_length_byte_array(suffixes))
    return output
end

function encode_delta_byte_array_fixed(values::AbstractMatrix{UInt8})
    size(values, 1) > 0 || throw(ArgumentError("fixed byte-array width must be positive"))
    return encode_delta_byte_array(eachcol(values))
end
