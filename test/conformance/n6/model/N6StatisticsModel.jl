module N6StatisticsModel

@enum PhysicalType begin
    PHYSICAL_BOOLEAN
    PHYSICAL_INT32
    PHYSICAL_INT64
    PHYSICAL_INT96
    PHYSICAL_FLOAT
    PHYSICAL_DOUBLE
    PHYSICAL_BYTE_ARRAY
    PHYSICAL_FIXED_LEN_BYTE_ARRAY
end

@enum LogicalType begin
    LOGICAL_NONE
    LOGICAL_STRING
    LOGICAL_ENUM
    LOGICAL_JSON
    LOGICAL_BSON
    LOGICAL_UUID
    LOGICAL_DECIMAL
    LOGICAL_SIGNED_INTEGER
    LOGICAL_UNSIGNED_INTEGER
    LOGICAL_DATE
    LOGICAL_TIME
    LOGICAL_TIMESTAMP
    LOGICAL_FLOAT16
    LOGICAL_INTERVAL
    LOGICAL_UNKNOWN
    LOGICAL_VARIANT
    LOGICAL_GEOMETRY
    LOGICAL_GEOGRAPHY
    LOGICAL_LIST
    LOGICAL_MAP
end

@enum TimeUnit begin
    TIME_MILLIS
    TIME_MICROS
    TIME_NANOS
end

@enum ComparatorKind begin
    COMPARATOR_SIGNED
    COMPARATOR_UNSIGNED
    COMPARATOR_UNSIGNED_BYTES
    COMPARATOR_DECIMAL
    COMPARATOR_BOOLEAN
    COMPARATOR_TYPE_FLOAT
    COMPARATOR_IEEE_FLOAT
    COMPARATOR_UNDEFINED
end

struct ModelFormatError <: Exception
    message::String
end

function Base.showerror(io::IO, err::ModelFormatError)
    print(io, err.message)
    return
end

struct LeafSpec
    physical::PhysicalType
    logical::LogicalType
    type_length::Union{Nothing,Int}
    bit_width::Union{Nothing,Int}
    precision::Union{Nothing,Int}
    time_unit::Union{Nothing,TimeUnit}
end

function LeafSpec(physical::PhysicalType; logical::LogicalType=LOGICAL_NONE,
        type_length::Union{Nothing,Integer}=nothing,
        bit_width::Union{Nothing,Integer}=nothing,
        precision::Union{Nothing,Integer}=nothing,
        time_unit::Union{Nothing,TimeUnit}=nothing)
    return LeafSpec(physical, logical,
        type_length === nothing ? nothing : Int(type_length),
        bit_width === nothing ? nothing : Int(bit_width),
        precision === nothing ? nothing : Int(precision), time_unit)
end

struct ModelLimits
    max_statistics_value_bytes::Int64
end

function ModelLimits(; max_statistics_value_bytes::Integer=4096)
    return ModelLimits(Int64(max_statistics_value_bytes))
end

abstract type ModelValue end

struct SignedValue <: ModelValue
    value::Int128
end

struct UnsignedValue <: ModelValue
    value::UInt64
end

function UnsignedValue(value::Unsigned)
    return UnsignedValue(UInt64(value))
end

struct BooleanValue <: ModelValue
    value::Bool
end

struct ByteValue <: ModelValue
    value::Vector{UInt8}
end

struct DecimalValue <: ModelValue
    value::Vector{UInt8}
end

struct FloatValue <: ModelValue
    width::UInt8
    bits::UInt64
end

@enum BoundState begin
    BOUND_ABSENT
    BOUND_UNKNOWN
    BOUND_KNOWN
end

struct DecodedBound
    state::BoundState
    value::Union{Nothing,ModelValue}
    reason::Symbol
    semantic_checked::Bool
end

function _known(value::ModelValue)
    return DecodedBound(BOUND_KNOWN, value, :known, true)
end

function _unknown(reason::Symbol, semantic_checked::Bool)
    return DecodedBound(BOUND_UNKNOWN, nothing, reason, semantic_checked)
end

function _readlittle(raw::AbstractVector{UInt8})
    value = UInt64(0)
    offset = 0
    for byte in raw
        value |= UInt64(byte) << offset
        offset += 8
    end
    return value
end

function _signed(bits::UInt64, width::Int)
    sign = UInt64(1) << (width - 1)
    bits & sign == 0 && return Int128(bits)
    return Int128(bits) - (Int128(1) << width)
end

function _fixedwidth(leaf::LeafSpec)
    leaf.physical == PHYSICAL_BOOLEAN && return 1
    leaf.physical in (PHYSICAL_INT32, PHYSICAL_FLOAT) && return 4
    leaf.physical in (PHYSICAL_INT64, PHYSICAL_DOUBLE) && return 8
    leaf.physical == PHYSICAL_INT96 && return 12
    if leaf.physical == PHYSICAL_FIXED_LEN_BYTE_ARRAY
        leaf.type_length === nothing && throw(ModelFormatError(
            "fixed byte-array leaf has no declared width"))
        leaf.type_length >= 0 || throw(ModelFormatError(
            "fixed byte-array leaf has a negative width"))
        return leaf.type_length
    end
    return nothing
end

function _checkstructure(raw::AbstractVector{UInt8}, leaf::LeafSpec)
    width = _fixedwidth(leaf)
    width === nothing && return
    length(raw) == width || throw(ModelFormatError(
        "fixed-width statistics bound has the wrong width"))
    return
end

function _validutf8(raw::AbstractVector{UInt8})
    return isvalid(String, raw)
end

function _skipjsonspace(data::Vector{UInt8}, position::Int, stop::Int)
    while position <= stop && data[position] in (0x20, 0x09, 0x0a, 0x0d)
        position += 1
    end
    return position
end

function _jsonhex(byte::UInt8)
    return (0x30 <= byte <= 0x39) || (0x41 <= byte <= 0x46) ||
        (0x61 <= byte <= 0x66)
end

function _jsonstring(data::Vector{UInt8}, position::Int, stop::Int)
    position <= stop && data[position] == 0x22 || return 0
    position += 1
    while position <= stop
        byte = data[position]
        byte == 0x22 && return position + 1
        byte < 0x20 && return 0
        if byte == 0x5c
            position += 1
            position <= stop || return 0
            escape = data[position]
            if escape == 0x75
                position + 4 <= stop || return 0
                for index in (position + 1):(position + 4)
                    _jsonhex(data[index]) || return 0
                end
                position += 5
                continue
            end
            escape in (0x22, 0x5c, 0x2f, 0x62, 0x66, 0x6e, 0x72, 0x74) ||
                return 0
        end
        position += 1
    end
    return 0
end

function _jsondigits(data::Vector{UInt8}, position::Int, stop::Int)
    start = position
    while position <= stop && 0x30 <= data[position] <= 0x39
        position += 1
    end
    return position == start ? 0 : position
end

function _jsonnumber(data::Vector{UInt8}, position::Int, stop::Int)
    position <= stop && data[position] == 0x2d && (position += 1)
    position <= stop || return 0
    if data[position] == 0x30
        position += 1
        position <= stop && 0x30 <= data[position] <= 0x39 && return 0
    elseif 0x31 <= data[position] <= 0x39
        position = _jsondigits(data, position, stop)
    else
        return 0
    end
    if position <= stop && data[position] == 0x2e
        position = _jsondigits(data, position + 1, stop)
        position == 0 && return 0
    end
    if position <= stop && data[position] in (0x65, 0x45)
        position += 1
        position <= stop && data[position] in (0x2b, 0x2d) && (position += 1)
        position = _jsondigits(data, position, stop)
        position == 0 && return 0
    end
    return position
end

function _jsonliteral(data::Vector{UInt8}, position::Int, stop::Int,
        literal::String)
    bytes = codeunits(literal)
    position + length(bytes) - 1 <= stop || return 0
    for (offset, byte) in enumerate(bytes)
        data[position + offset - 1] == byte || return 0
    end
    return position + length(bytes)
end

function _jsonarray(data::Vector{UInt8}, position::Int, stop::Int, depth::Int)
    position = _skipjsonspace(data, position + 1, stop)
    position <= stop && data[position] == 0x5d && return position + 1
    while position <= stop
        position = _jsonvalue(data, position, stop, depth + 1)
        position == 0 && return 0
        position = _skipjsonspace(data, position, stop)
        position <= stop || return 0
        data[position] == 0x5d && return position + 1
        data[position] == 0x2c || return 0
        position = _skipjsonspace(data, position + 1, stop)
    end
    return 0
end

function _jsonobject(data::Vector{UInt8}, position::Int, stop::Int, depth::Int)
    position = _skipjsonspace(data, position + 1, stop)
    position <= stop && data[position] == 0x7d && return position + 1
    while position <= stop
        position = _jsonstring(data, position, stop)
        position == 0 && return 0
        position = _skipjsonspace(data, position, stop)
        position <= stop && data[position] == 0x3a || return 0
        position = _skipjsonspace(data, position + 1, stop)
        position = _jsonvalue(data, position, stop, depth + 1)
        position == 0 && return 0
        position = _skipjsonspace(data, position, stop)
        position <= stop || return 0
        data[position] == 0x7d && return position + 1
        data[position] == 0x2c || return 0
        position = _skipjsonspace(data, position + 1, stop)
    end
    return 0
end

function _jsonvalue(data::Vector{UInt8}, position::Int, stop::Int, depth::Int)
    depth <= 64 || return 0
    position = _skipjsonspace(data, position, stop)
    position <= stop || return 0
    byte = data[position]
    byte == 0x22 && return _jsonstring(data, position, stop)
    byte == 0x5b && return _jsonarray(data, position, stop, depth)
    byte == 0x7b && return _jsonobject(data, position, stop, depth)
    byte == 0x74 && return _jsonliteral(data, position, stop, "true")
    byte == 0x66 && return _jsonliteral(data, position, stop, "false")
    byte == 0x6e && return _jsonliteral(data, position, stop, "null")
    return _jsonnumber(data, position, stop)
end

function _validjson(data::Vector{UInt8})
    isempty(data) && return false
    _validutf8(data) || return false
    position = _jsonvalue(data, firstindex(data), lastindex(data), 0)
    position == 0 && return false
    return _skipjsonspace(data, position, lastindex(data)) == lastindex(data) + 1
end

function _readbsoni32(data::Vector{UInt8}, position::Int, stop::Int)
    position + 3 <= stop || return nothing
    bits = UInt32(data[position]) | (UInt32(data[position + 1]) << 8) |
        (UInt32(data[position + 2]) << 16) | (UInt32(data[position + 3]) << 24)
    value = bits <= UInt32(typemax(Int32)) ? Int(bits) :
        Int(Int64(bits) - (Int64(1) << 32))
    return value
end

function _bsoncstring(data::Vector{UInt8}, position::Int, stop::Int)
    start = position
    while position <= stop && data[position] != 0x00
        position += 1
    end
    position <= stop || return 0
    _validutf8(view(data, start:(position - 1))) || return 0
    return position + 1
end

function _bsonregexoptions(data::Vector{UInt8}, position::Int, stop::Int)
    previous = UInt8(0)
    while position <= stop
        byte = data[position]
        byte == 0x00 && return position + 1
        byte in (0x69, 0x6d, 0x73, 0x75, 0x78) || return 0
        previous < byte || return 0
        previous = byte
        position += 1
    end
    return 0
end

function _bsonbinarysubtypevalid(subtype::UInt8)
    return subtype <= 0x09 || subtype >= 0x80
end

function _bsonarraykey(data::Vector{UInt8}, position::Int, stop::Int,
        index::Int)
    expected = codeunits(string(index))
    position + length(expected) <= stop || return false
    for (offset, byte) in enumerate(expected)
        data[position + offset - 1] == byte || return false
    end
    return data[position + length(expected)] == 0x00
end

function _bsonbytes(position::Int, count::Int, stop::Int)
    count >= 0 || return 0
    count <= stop - position + 1 || return 0
    return position + count
end

function _bsonstring(data::Vector{UInt8}, position::Int, stop::Int)
    count = _readbsoni32(data, position, stop)
    count === nothing && return 0
    count >= 1 || return 0
    start = position + 4
    finish = _bsonbytes(start, count, stop)
    finish == 0 && return 0
    data[finish - 1] == 0x00 || return 0
    _validutf8(view(data, start:(finish - 2))) || return 0
    return finish
end

function _bsonvalue(data::Vector{UInt8}, position::Int, stop::Int, kind::UInt8,
        depth::Int)
    kind == 0x01 && return _bsonbytes(position, 8, stop)
    kind == 0x02 && return _bsonstring(data, position, stop)
    kind == 0x03 && return _bsondocument(data, position, stop, depth + 1, false)
    kind == 0x04 && return _bsondocument(data, position, stop, depth + 1, true)
    if kind == 0x05
        count = _readbsoni32(data, position, stop)
        count === nothing && return 0
        count >= 0 || return 0
        position + 4 <= stop || return 0
        subtype = data[position + 4]
        _bsonbinarysubtypevalid(subtype) || return 0
        payload = position + 5
        finish = _bsonbytes(payload, count, stop)
        finish == 0 && return 0
        if subtype == 0x02
            count >= 4 || return 0
            oldcount = _readbsoni32(data, payload, finish - 1)
            oldcount == count - 4 || return 0
        end
        return finish
    end
    kind == 0x06 && return position
    kind == 0x07 && return _bsonbytes(position, 12, stop)
    if kind == 0x08
        position <= stop && data[position] in (0x00, 0x01) || return 0
        return position + 1
    end
    kind == 0x09 && return _bsonbytes(position, 8, stop)
    kind == 0x0a && return position
    if kind == 0x0b
        position = _bsoncstring(data, position, stop)
        position == 0 && return 0
        return _bsonregexoptions(data, position, stop)
    end
    if kind == 0x0c
        position = _bsonstring(data, position, stop)
        position == 0 && return 0
        return _bsonbytes(position, 12, stop)
    end
    kind in (0x0d, 0x0e) && return _bsonstring(data, position, stop)
    if kind == 0x0f
        count = _readbsoni32(data, position, stop)
        count === nothing && return 0
        count >= 14 || return 0
        finish = _bsonbytes(position, count, stop)
        finish == 0 && return 0
        cursor = _bsonstring(data, position + 4, finish - 1)
        cursor == 0 && return 0
        return _bsondocument(data, cursor, finish - 1, depth + 1, false) ==
            finish ? finish : 0
    end
    kind == 0x10 && return _bsonbytes(position, 4, stop)
    kind in (0x11, 0x12) && return _bsonbytes(position, 8, stop)
    kind == 0x13 && return _bsonbytes(position, 16, stop)
    kind in (0x7f, 0xff) && return position
    return 0
end

function _bsondocument(data::Vector{UInt8}, position::Int, stop::Int,
        depth::Int, isarray::Bool)
    depth <= 64 || return 0
    count = _readbsoni32(data, position, stop)
    count === nothing && return 0
    count >= 5 || return 0
    finish = _bsonbytes(position, count, stop)
    finish == 0 && return 0
    data[finish - 1] == 0x00 || return 0
    cursor = position + 4
    index = 0
    while cursor < finish - 1
        kind = data[cursor]
        isarray && !_bsonarraykey(data, cursor + 1, finish - 2, index) && return 0
        cursor = _bsoncstring(data, cursor + 1, finish - 2)
        cursor == 0 && return 0
        cursor = _bsonvalue(data, cursor, finish - 2, kind, depth)
        cursor == 0 && return 0
        index += 1
    end
    return cursor == finish - 1 ? finish : 0
end

function _validbson(data::Vector{UInt8})
    isempty(data) && return false
    return _bsondocument(data, firstindex(data), lastindex(data), 0, false) ==
        lastindex(data) + 1
end

function _normalizeddecimal(raw::Vector{UInt8})
    isempty(raw) && return UInt8[]
    first = 1
    while first < length(raw)
        byte = raw[first]
        next = raw[first + 1]
        byte == 0x00 && next < 0x80 && (first += 1; continue)
        byte == 0xff && next >= 0x80 && (first += 1; continue)
        break
    end
    return raw[first:end]
end

function _stripzerobytes(raw::Vector{UInt8})
    isempty(raw) && return raw
    first = findfirst(byte -> !iszero(byte), raw)
    first === nothing && return UInt8[0x00]
    return raw[first:end]
end

function _decimalmagnitude(raw::Vector{UInt8})
    normalized = _normalizeddecimal(raw)
    isempty(normalized) && return UInt8[]
    normalized[1] < 0x80 && return _stripzerobytes(copy(normalized))
    magnitude = [~byte for byte in normalized]
    carry = UInt16(1)
    for index in lastindex(magnitude):-1:firstindex(magnitude)
        value = UInt16(magnitude[index]) + carry
        magnitude[index] = UInt8(value & 0xff)
        carry = value >> 8
        iszero(carry) && break
    end
    return _stripzerobytes(magnitude)
end

function _smallintdigits(value::UInt32)
    digits = 1
    while value >= 10
        value ÷= 10
        digits += 1
    end
    return digits
end

function _decimaldigits(raw::Vector{UInt8})
    magnitude = _decimalmagnitude(raw)
    isempty(magnitude) && return 0
    limbs = UInt32[0]
    for byte in magnitude
        carry = UInt64(byte)
        for index in eachindex(limbs)
            value = UInt64(limbs[index]) * 256 + carry
            limbs[index] = UInt32(value % 1_000_000_000)
            carry = value ÷ 1_000_000_000
        end
        iszero(carry) || push!(limbs, UInt32(carry))
    end
    while length(limbs) > 1 && iszero(last(limbs))
        pop!(limbs)
    end
    return (length(limbs) - 1) * 9 + _smallintdigits(last(limbs))
end

function _decimalvalue(raw::Vector{UInt8}, precision::Union{Nothing,Int})
    isempty(raw) && return nothing
    precision === nothing && return nothing
    precision > 0 || return nothing
    _decimaldigits(raw) <= precision || return nothing
    return DecimalValue(_normalizeddecimal(raw))
end

function _integerbytes(raw::Vector{UInt8})
    return _normalizeddecimal(reverse(raw))
end

function _integerlogical(raw::Vector{UInt8}, leaf::LeafSpec)
    width = length(raw) * 8
    bits = _readlittle(raw)
    if leaf.logical == LOGICAL_UNSIGNED_INTEGER
        leaf.bit_width in (8, 16, 32, 64) || return nothing
        leaf.bit_width <= width || return nothing
        if leaf.bit_width < width
            bits < (UInt64(1) << leaf.bit_width) || return nothing
        end
        return UnsignedValue(bits)
    end
    value = _signed(bits, width)
    if leaf.logical == LOGICAL_SIGNED_INTEGER
        leaf.bit_width in (8, 16, 32, 64) || return nothing
        leaf.bit_width <= width || return nothing
        low = -(Int128(1) << (leaf.bit_width - 1))
        high = (Int128(1) << (leaf.bit_width - 1)) - 1
        low <= value <= high || return nothing
    end
    return SignedValue(value)
end

function _timevalid(value::Int128, unit::Union{Nothing,TimeUnit})
    unit === nothing && return false
    limit = unit == TIME_MILLIS ? Int128(86_400_000) :
        unit == TIME_MICROS ? Int128(86_400_000_000) :
        Int128(86_400_000_000_000)
    return 0 <= value < limit
end

function _floatvalue(raw::Vector{UInt8}, width::Int)
    return FloatValue(UInt8(width), _readlittle(raw))
end

function _logicalvalue(raw::Vector{UInt8}, leaf::LeafSpec)
    logical = leaf.logical
    if logical in (LOGICAL_STRING, LOGICAL_ENUM)
        return _validutf8(raw) ? ByteValue(raw) : nothing
    elseif logical == LOGICAL_JSON
        return _validjson(raw) ? ByteValue(raw) : nothing
    elseif logical == LOGICAL_BSON
        return _validbson(raw) ? ByteValue(raw) : nothing
    elseif logical == LOGICAL_UUID
        length(raw) == 16 || return nothing
        return ByteValue(raw)
    elseif logical == LOGICAL_FLOAT16
        length(raw) == 2 || return nothing
        return _floatvalue(raw, 16)
    elseif logical == LOGICAL_DECIMAL
        bytes = leaf.physical in (PHYSICAL_INT32, PHYSICAL_INT64) ?
            _integerbytes(raw) : raw
        return _decimalvalue(bytes, leaf.precision)
    elseif logical in (LOGICAL_SIGNED_INTEGER, LOGICAL_UNSIGNED_INTEGER)
        leaf.physical in (PHYSICAL_INT32, PHYSICAL_INT64) || return nothing
        return _integerlogical(raw, leaf)
    elseif logical in (LOGICAL_DATE, LOGICAL_TIMESTAMP)
        leaf.physical in (PHYSICAL_INT32, PHYSICAL_INT64) || return nothing
        return _integerlogical(raw, leaf)
    elseif logical == LOGICAL_TIME
        leaf.physical in (PHYSICAL_INT32, PHYSICAL_INT64) || return nothing
        value = _integerlogical(raw, leaf)
        value isa SignedValue || return nothing
        return _timevalid(value.value, leaf.time_unit) ? value : nothing
    end
    leaf.physical == PHYSICAL_BOOLEAN && return raw[1] in (0x00, 0x01) ?
        BooleanValue(raw[1] == 0x01) : nothing
    leaf.physical in (PHYSICAL_INT32, PHYSICAL_INT64) &&
        return _integerlogical(raw, leaf)
    leaf.physical == PHYSICAL_FLOAT && return _floatvalue(raw, 32)
    leaf.physical == PHYSICAL_DOUBLE && return _floatvalue(raw, 64)
    return ByteValue(raw)
end

function decode_bound(raw::AbstractVector{UInt8}, leaf::LeafSpec,
        limits::ModelLimits)
    limits.max_statistics_value_bytes >= 0 || throw(ArgumentError(
        "max_statistics_value_bytes must be nonnegative"))
    _checkstructure(raw, leaf)
    length(raw) > limits.max_statistics_value_bytes &&
        return _unknown(:over_limit, false)
    bytes = Vector{UInt8}(raw)
    value = _logicalvalue(bytes, leaf)
    value === nothing && return _unknown(:invalid_logical, true)
    return _known(value)
end

function _compare(left, right)
    left < right && return -1
    left > right && return 1
    return 0
end

function _comparebytes(left::Vector{UInt8}, right::Vector{UInt8})
    count = min(length(left), length(right))
    for index in 1:count
        left[index] == right[index] && continue
        return left[index] < right[index] ? -1 : 1
    end
    return _compare(length(left), length(right))
end

function _decimalbyte(raw::Vector{UInt8}, offset::Int, width::Int)
    padding = raw[1] >= 0x80 ? 0xff : 0x00
    skipped = width - length(raw)
    return offset <= skipped ? padding : raw[offset - skipped]
end

function _comparedecimal(left::DecimalValue, right::DecimalValue)
    lnegative = left.value[1] >= 0x80
    rnegative = right.value[1] >= 0x80
    lnegative != rnegative && return lnegative ? -1 : 1
    width = max(length(left.value), length(right.value))
    for offset in 1:width
        lbyte = _decimalbyte(left.value, offset, width)
        rbyte = _decimalbyte(right.value, offset, width)
        lbyte == rbyte && continue
        return lbyte < rbyte ? -1 : 1
    end
    return 0
end

function _floatmask(value::FloatValue)
    value.width == 64 && return typemax(UInt64)
    return (UInt64(1) << value.width) - 1
end

function _floatsignmask(value::FloatValue)
    return UInt64(1) << (value.width - 1)
end

function ieee_total_key(value::FloatValue)
    mask = _floatmask(value)
    bits = value.bits & mask
    sign = _floatsignmask(value)
    return bits & sign == 0 ? bits | sign : ~bits & mask
end

function float_isnan(value::FloatValue)
    if value.width == 16
        return value.bits & 0x7c00 == 0x7c00 && value.bits & 0x03ff != 0
    elseif value.width == 32
        return value.bits & 0x7f800000 == 0x7f800000 &&
            value.bits & 0x007fffff != 0
    elseif value.width == 64
        return value.bits & 0x7ff0000000000000 == 0x7ff0000000000000 &&
            value.bits & 0x000fffffffffffff != 0
    end
    throw(ArgumentError("unsupported floating width"))
end

function float_iszero(value::FloatValue)
    mask = xor(_floatmask(value), _floatsignmask(value))
    return value.bits & mask == 0
end

function compare_values(left::ModelValue, right::ModelValue,
        comparator::ComparatorKind)
    if comparator == COMPARATOR_SIGNED
        left isa SignedValue && right isa SignedValue || throw(ArgumentError(
            "signed comparator requires signed values"))
        return _compare(left.value, right.value)
    elseif comparator == COMPARATOR_UNSIGNED
        left isa UnsignedValue && right isa UnsignedValue || throw(ArgumentError(
            "unsigned comparator requires unsigned values"))
        return _compare(left.value, right.value)
    elseif comparator == COMPARATOR_BOOLEAN
        left isa BooleanValue && right isa BooleanValue || throw(ArgumentError(
            "Boolean comparator requires Boolean values"))
        return _compare(left.value, right.value)
    elseif comparator == COMPARATOR_UNSIGNED_BYTES
        left isa ByteValue && right isa ByteValue || throw(ArgumentError(
            "byte comparator requires byte values"))
        return _comparebytes(left.value, right.value)
    elseif comparator == COMPARATOR_DECIMAL
        left isa DecimalValue && right isa DecimalValue || throw(ArgumentError(
            "decimal comparator requires decimal values"))
        return _comparedecimal(left, right)
    elseif comparator in (COMPARATOR_TYPE_FLOAT, COMPARATOR_IEEE_FLOAT)
        left isa FloatValue && right isa FloatValue || throw(ArgumentError(
            "floating comparator requires floating values"))
        left.width == right.width || throw(ArgumentError(
            "floating widths do not match"))
        if comparator == COMPARATOR_TYPE_FLOAT
            (float_isnan(left) || float_isnan(right)) && throw(ArgumentError(
                "TYPE_ORDER cannot compare NaN bounds"))
            float_iszero(left) && float_iszero(right) && return 0
        end
        return _compare(ieee_total_key(left), ieee_total_key(right))
    end
    throw(ArgumentError("undefined comparator"))
end

function writer_bounds_allowed(lower::AbstractVector{UInt8},
        upper::AbstractVector{UInt8}, limits::ModelLimits)
    limits.max_statistics_value_bytes >= 0 || throw(ArgumentError(
        "max_statistics_value_bytes must be nonnegative"))
    length(lower) <= limits.max_statistics_value_bytes || return false
    return length(upper) <= limits.max_statistics_value_bytes
end

@enum DeclaredOrder begin
    ORDER_TYPE
    ORDER_IEEE
    ORDER_FUTURE
end

@enum BoundFamily begin
    FAMILY_NONE
    FAMILY_MODERN
    FAMILY_DEPRECATED
end

@enum Exactness begin
    EXACTNESS_UNKNOWN
    EXACTNESS_INEXACT
    EXACTNESS_EXACT
end

@enum OccupancyState begin
    OCCUPANCY_UNKNOWN
    OCCUPANCY_EMPTY
    OCCUPANCY_ALL_NAN
    OCCUPANCY_HAS_NON_NAN
end

@enum TrustState begin
    TRUST_TRUSTED
    TRUST_UNTRUSTED
end

Base.@kwdef struct RawStatistics
    modern_lower::Union{Nothing,Vector{UInt8}} = nothing
    modern_upper::Union{Nothing,Vector{UInt8}} = nothing
    deprecated_lower::Union{Nothing,Vector{UInt8}} = nothing
    deprecated_upper::Union{Nothing,Vector{UInt8}} = nothing
    null_count::Union{Nothing,Int64} = nothing
    nan_count::Union{Nothing,Int64} = nothing
    distinct_count::Union{Nothing,Int64} = nothing
    lower_exact::Union{Nothing,Bool} = nothing
    upper_exact::Union{Nothing,Bool} = nothing
end

struct CountFact
    known::Bool
    value::Int64
end

struct BoundFact
    state::BoundState
    value::Union{Nothing,ModelValue}
    raw::Union{Nothing,Vector{UInt8}}
    exactness::Exactness
    reason::Symbol
end

struct TrustDecision
    state::TrustState
    reason::Symbol
end

struct StatisticsResult
    lower::BoundFact
    upper::BoundFact
    null_count::CountFact
    nan_count::CountFact
    distinct_count::CountFact
    occupancy::OccupancyState
    family::BoundFamily
    comparator::ComparatorKind
    trust::TrustDecision
end

struct ExtremaSummary
    lower::Union{Nothing,ModelValue}
    upper::Union{Nothing,ModelValue}
    nan_count::Int64
    value_count::Int64
end

struct SemanticVersion
    major::Int
    minor::Int
    patch::Int
    unknown::String
    has_prerelease::Bool
    prerelease::Vector{String}
end

struct ParsedProducer
    parsed::Bool
    application::String
    version::Union{Nothing,SemanticVersion}
end

function _javadotsplit(label::AbstractString)
    isempty(label) && return [""]
    identifiers = String.(split(label, '.'; keepempty=true))
    while !isempty(identifiers) && isempty(last(identifiers))
        pop!(identifiers)
    end
    return identifiers
end

function _javaisspace(character::Char)
    return character in (' ', '\t', '\n', '\v', '\f', '\r')
end

function _javaasciistrip(text::AbstractString)
    return strip(_javaisspace, text)
end

function _javacontainslineterminator(text::AbstractString)
    return any(character -> character in ('\n', '\r', '\u0085', '\u2028',
        '\u2029'), text)
end

function _semver(text::AbstractString)
    matched = match(r"^([0-9]+)\.([0-9]+)\.([0-9]+)([^-+]*)?(?:-([^+]*))?(?:\+(.*))?$",
        text)
    matched === nothing && return nothing
    build = matched.captures[6]
    build !== nothing && _javacontainslineterminator(build) && return nothing
    major = tryparse(Int32, matched.captures[1])
    minor = tryparse(Int32, matched.captures[2])
    patch = tryparse(Int32, matched.captures[3])
    any(isnothing, (major, minor, patch)) && return nothing
    unknown = something(matched.captures[4], "")
    label = matched.captures[5]
    prerelease = label === nothing ? String[] :
        _javadotsplit(label)
    for identifier in prerelease
        occursin(r"^[0-9]+$", identifier) || continue
        tryparse(Int32, identifier) === nothing && return nothing
    end
    return SemanticVersion(Int(major), Int(minor), Int(patch), unknown,
        label !== nothing, prerelease)
end

function parse_created_by(created_by::Union{Nothing,AbstractString})
    created_by === nothing && return ParsedProducer(false, "", nothing)
    text = _javaasciistrip(String(created_by))
    isempty(text) && return ParsedProducer(false, "", nothing)
    matched = match(
        r"^(.*?)[ \t\n\x0b\f\r]+version[ \t\n\x0b\f\r]*(?:([^(]*?)[ \t\n\x0b\f\r]*(?:\([ \t\n\x0b\f\r]*build[ \t\n\x0b\f\r]*([^)]*?)[ \t\n\x0b\f\r]*\))?)?$",
        text)
    if matched !== nothing
        application = _javaasciistrip(matched.captures[1])
        isempty(application) && return ParsedProducer(false, "", nothing)
        _javacontainslineterminator(application) &&
            return ParsedProducer(false, "", nothing)
        rawversion = matched.captures[2]
        versiontext = rawversion === nothing ? "" : _javaasciistrip(rawversion)
        version = isempty(versiontext) ? nothing : _semver(versiontext)
        return ParsedProducer(true, application, version)
    end
    return ParsedProducer(false, "", nothing)
end

function parse_arrow_created_by(created_by::Union{Nothing,AbstractString})
    producer = parse_created_by(created_by)
    producer.parsed && return producer
    created_by === nothing && return producer
    application = _javaasciistrip(String(created_by))
    application in ("parquet-cpp", "parquet-mr") || return producer
    return ParsedProducer(true, application, nothing)
end

function _identifiercompare(left::String, right::String)
    lnumber = occursin(r"^[0-9]+$", left) ? tryparse(Int32, left) : nothing
    rnumber = occursin(r"^[0-9]+$", right) ? tryparse(Int32, right) : nothing
    lnumber !== nothing && rnumber !== nothing && return _compare(lnumber, rnumber)
    lnumber !== nothing && return -1
    rnumber !== nothing && return 1
    return _compare(left, right)
end

function _prereleasecompare(left::Vector{String}, right::Vector{String})
    for index in 1:min(length(left), length(right))
        compared = _identifiercompare(left[index], right[index])
        iszero(compared) || return compared
    end
    return _compare(length(left), length(right))
end

function _versioncompare(left::SemanticVersion, right::SemanticVersion)
    compared = _compare(left.major, right.major)
    iszero(compared) || return compared
    compared = _compare(left.minor, right.minor)
    iszero(compared) || return compared
    compared = _compare(left.patch, right.patch)
    iszero(compared) || return compared
    lunknown = !isempty(left.unknown)
    runknown = !isempty(right.unknown)
    lunknown != runknown && return lunknown ? -1 : 1
    left.has_prerelease != right.has_prerelease &&
        return left.has_prerelease ? -1 : 1
    return _prereleasecompare(left.prerelease, right.prerelease)
end

function _versionlt(left::SemanticVersion, right::SemanticVersion)
    return _versioncompare(left, right) < 0
end

function _parquet251affected(producer::ParsedProducer)
    producer.parsed || return true
    producer.application == "parquet-mr" || return false
    producer.version === nothing && return true
    fixed = SemanticVersion(1, 8, 0, "", false, String[])
    _versionlt(producer.version, fixed) || return false
    cdhstart = SemanticVersion(1, 5, 0, "", true, ["cdh5", "5", "0"])
    cdhend = SemanticVersion(1, 5, 0, "", false, String[])
    incdh = !_versionlt(producer.version, cdhstart) &&
        _versionlt(producer.version, cdhend)
    return !incdh
end

function _oldorderaffected(producer::ParsedProducer)
    cutoff = if producer.application == "parquet-cpp"
        SemanticVersion(1, 3, 0, "", false, String[])
    elseif producer.application == "parquet-mr"
        SemanticVersion(1, 10, 0, "", false, String[])
    else
        return false
    end
    producer.version === nothing && return true
    return _versionlt(producer.version, cutoff)
end

function _legacyorderissigned(comparator::ComparatorKind)
    return comparator in (COMPARATOR_SIGNED, COMPARATOR_BOOLEAN,
        COMPARATOR_DECIMAL, COMPARATOR_TYPE_FLOAT)
end

function _binaryleaf(leaf::LeafSpec)
    return leaf.physical in (PHYSICAL_BYTE_ARRAY, PHYSICAL_FIXED_LEN_BYTE_ARRAY)
end

function producer_decision(created_by::Union{Nothing,AbstractString},
        leaf::LeafSpec, comparator::ComparatorKind, family::BoundFamily,
        lower::Union{Nothing,AbstractVector{UInt8}},
        upper::Union{Nothing,AbstractVector{UInt8}},
        limits::ModelLimits=ModelLimits())
    limits.max_statistics_value_bytes >= 0 || throw(ArgumentError(
        "max_statistics_value_bytes must be nonnegative"))
    family == FAMILY_NONE && return TrustDecision(TRUST_TRUSTED, :no_bounds)
    java_producer = parse_created_by(created_by)
    if _binaryleaf(leaf) && _parquet251affected(java_producer)
        return TrustDecision(TRUST_UNTRUSTED, :parquet_251)
    end
    arrow_producer = parse_arrow_created_by(created_by)
    if _oldorderaffected(arrow_producer) && !_legacyorderissigned(comparator)
        equal = lower !== nothing && upper !== nothing &&
            length(lower) <= limits.max_statistics_value_bytes &&
            length(upper) <= limits.max_statistics_value_bytes && lower == upper
        equal || return TrustDecision(TRUST_UNTRUSTED, :legacy_wrong_order)
    end
    return TrustDecision(TRUST_TRUSTED, :trusted)
end

function _floatleaf(leaf::LeafSpec)
    return leaf.physical in (PHYSICAL_FLOAT, PHYSICAL_DOUBLE) ||
        leaf.logical == LOGICAL_FLOAT16
end

function _typecomparator(leaf::LeafSpec)
    leaf.logical in (LOGICAL_INTERVAL, LOGICAL_UNKNOWN, LOGICAL_VARIANT,
        LOGICAL_GEOMETRY, LOGICAL_GEOGRAPHY, LOGICAL_LIST, LOGICAL_MAP) &&
        return COMPARATOR_UNDEFINED
    leaf.physical == PHYSICAL_INT96 && return COMPARATOR_UNDEFINED
    leaf.logical == LOGICAL_UNSIGNED_INTEGER && return COMPARATOR_UNSIGNED
    leaf.logical in (LOGICAL_STRING, LOGICAL_ENUM, LOGICAL_JSON, LOGICAL_BSON,
        LOGICAL_UUID) && return COMPARATOR_UNSIGNED_BYTES
    leaf.logical == LOGICAL_DECIMAL && return COMPARATOR_DECIMAL
    leaf.logical == LOGICAL_FLOAT16 && return COMPARATOR_TYPE_FLOAT
    leaf.logical in (LOGICAL_SIGNED_INTEGER, LOGICAL_DATE, LOGICAL_TIME,
        LOGICAL_TIMESTAMP) && return COMPARATOR_SIGNED
    leaf.physical == PHYSICAL_BOOLEAN && return COMPARATOR_BOOLEAN
    leaf.physical in (PHYSICAL_INT32, PHYSICAL_INT64) && return COMPARATOR_SIGNED
    leaf.physical in (PHYSICAL_FLOAT, PHYSICAL_DOUBLE) &&
        return COMPARATOR_TYPE_FLOAT
    leaf.physical in (PHYSICAL_BYTE_ARRAY, PHYSICAL_FIXED_LEN_BYTE_ARRAY) &&
        return COMPARATOR_UNSIGNED_BYTES
    return COMPARATOR_UNDEFINED
end

function _deprecatedcompatible(comparator::ComparatorKind)
    return _legacyorderissigned(comparator)
end

function _countfact(value::Union{Nothing,Int64}, num_values::Int64,
        name::String)
    value === nothing && return CountFact(false, Int64(0))
    0 <= value <= num_values || throw(ModelFormatError(
        "$name is outside the column value count"))
    return CountFact(true, value)
end

function _counts(leaf::LeafSpec, num_values::Int64, stats::RawStatistics)
    num_values >= 0 || throw(ModelFormatError("column value count is negative"))
    nulls = _countfact(stats.null_count, num_values, "null_count")
    nans = _countfact(stats.nan_count, num_values, "nan_count")
    distinct = _countfact(stats.distinct_count, num_values, "distinct_count")
    nans.known && !_floatleaf(leaf) && throw(ModelFormatError(
        "nan_count is present on a non-floating leaf"))
    if nulls.known && nans.known
        nulls.value <= num_values - nans.value || throw(ModelFormatError(
            "null_count plus nan_count exceeds num_values"))
    end
    if nulls.known && distinct.known
        distinct.value <= num_values - nulls.value || throw(ModelFormatError(
            "distinct_count exceeds the non-null value count"))
    end
    occupancy = if iszero(num_values)
        OCCUPANCY_EMPTY
    elseif nulls.known && nulls.value == num_values
        OCCUPANCY_EMPTY
    elseif nulls.known && nans.known
        nonnull = num_values - nulls.value
        nonnull > 0 && nans.value == nonnull ? OCCUPANCY_ALL_NAN :
            OCCUPANCY_HAS_NON_NAN
    else
        OCCUPANCY_UNKNOWN
    end
    return nulls, nans, distinct, occupancy
end

function _selectedfamily(stats::RawStatistics)
    (stats.modern_lower !== nothing || stats.modern_upper !== nothing) &&
        return FAMILY_MODERN
    (stats.deprecated_lower !== nothing || stats.deprecated_upper !== nothing) &&
        return FAMILY_DEPRECATED
    return FAMILY_NONE
end

function _selectedraw(stats::RawStatistics, family::BoundFamily)
    family == FAMILY_MODERN && return stats.modern_lower, stats.modern_upper
    family == FAMILY_DEPRECATED &&
        return stats.deprecated_lower, stats.deprecated_upper
    return nothing, nothing
end

function _exactness(flag::Union{Nothing,Bool}, family::BoundFamily)
    family == FAMILY_DEPRECATED && return EXACTNESS_UNKNOWN
    flag === nothing && return EXACTNESS_UNKNOWN
    return flag ? EXACTNESS_EXACT : EXACTNESS_INEXACT
end

function _absentbound()
    return BoundFact(BOUND_ABSENT, nothing, nothing, EXACTNESS_UNKNOWN, :absent)
end

function _boundfact(raw::Union{Nothing,Vector{UInt8}}, leaf::LeafSpec,
        limits::ModelLimits, exactness::Exactness)
    raw === nothing && return _absentbound()
    decoded = decode_bound(raw, leaf, limits)
    return BoundFact(decoded.state, decoded.value, raw, exactness, decoded.reason)
end

function _invalidate(bound::BoundFact, reason::Symbol)
    bound.state != BOUND_KNOWN && return bound
    return BoundFact(BOUND_UNKNOWN, nothing, bound.raw, bound.exactness, reason)
end

function _invalidateboth(lower::BoundFact, upper::BoundFact, reason::Symbol)
    return _invalidate(lower, reason), _invalidate(upper, reason)
end

function _validateorders(orders::Union{Nothing,Vector{DeclaredOrder}},
        leaf::LeafSpec, leaf_index::Int, leaf_count::Int)
    orders === nothing && return nothing
    length(orders) == leaf_count || throw(ModelFormatError(
        "column_orders is not leaf aligned"))
    1 <= leaf_index <= leaf_count || throw(ArgumentError(
        "leaf index is outside the schema"))
    order = orders[leaf_index]
    order == ORDER_IEEE && !_floatleaf(leaf) && throw(ModelFormatError(
        "IEEE total order is present on a non-floating leaf"))
    return order
end

function _comparator(leaf::LeafSpec, family::BoundFamily,
        order::Union{Nothing,DeclaredOrder})
    typecomparator = _typecomparator(leaf)
    if family == FAMILY_MODERN
        order === nothing && return COMPARATOR_UNDEFINED, :missing_column_orders
        order == ORDER_FUTURE && return COMPARATOR_UNDEFINED, :unknown_column_order
        order == ORDER_IEEE && return COMPARATOR_IEEE_FLOAT, :known
        typecomparator == COMPARATOR_UNDEFINED &&
            return COMPARATOR_UNDEFINED, :undefined_type_order
        return typecomparator, :known
    elseif family == FAMILY_DEPRECATED
        _deprecatedcompatible(typecomparator) ||
            return COMPARATOR_UNDEFINED, :deprecated_order_mismatch
        return typecomparator, :known
    end
    return COMPARATOR_UNDEFINED, :no_bounds
end

function _floatnegative(value::FloatValue)
    return value.bits & _floatsignmask(value) != 0
end

function _widenzero(bound::BoundFact, lower::Bool)
    bound.state == BOUND_KNOWN || return bound
    value = bound.value
    value isa FloatValue || return bound
    float_iszero(value) || return bound
    needs = lower ? !_floatnegative(value) : _floatnegative(value)
    needs || return bound
    bits = lower ? value.bits | _floatsignmask(value) :
        value.bits & xor(_floatmask(value), _floatsignmask(value))
    exactness = bound.exactness == EXACTNESS_EXACT ? EXACTNESS_INEXACT :
        bound.exactness
    return BoundFact(BOUND_KNOWN, FloatValue(value.width, bits), bound.raw,
        exactness, :widened_zero)
end

function _boundisnan(bound::BoundFact)
    return bound.state == BOUND_KNOWN && bound.value isa FloatValue &&
        float_isnan(bound.value)
end

function _applyfloatrules(lower::BoundFact, upper::BoundFact,
        comparator::ComparatorKind, occupancy::OccupancyState)
    if comparator == COMPARATOR_TYPE_FLOAT
        if occupancy == OCCUPANCY_ALL_NAN &&
                (lower.state != BOUND_ABSENT || upper.state != BOUND_ABSENT)
            return _invalidateboth(lower, upper, :all_nan_type_order)
        end
        _boundisnan(lower) && (lower = _invalidate(lower, :nan_type_order))
        _boundisnan(upper) && (upper = _invalidate(upper, :nan_type_order))
        lower = _widenzero(lower, true)
        upper = _widenzero(upper, false)
        return lower, upper
    elseif comparator == COMPARATOR_IEEE_FLOAT
        if occupancy == OCCUPANCY_ALL_NAN
            contradiction = (lower.state == BOUND_KNOWN && !_boundisnan(lower)) ||
                (upper.state == BOUND_KNOWN && !_boundisnan(upper))
            contradiction && return _invalidateboth(lower, upper,
                :ieee_bound_kind_contradiction)
        elseif occupancy == OCCUPANCY_HAS_NON_NAN
            (_boundisnan(lower) || _boundisnan(upper)) &&
                return _invalidateboth(lower, upper,
                    :ieee_bound_kind_contradiction)
        else
            _boundisnan(lower) &&
                (lower = _invalidate(lower, :unproven_ieee_nan))
            _boundisnan(upper) &&
                (upper = _invalidate(upper, :unproven_ieee_nan))
        end
    end
    return lower, upper
end

function _checkboundorder(lower::BoundFact, upper::BoundFact,
        comparator::ComparatorKind)
    lower.state == BOUND_KNOWN && upper.state == BOUND_KNOWN ||
        return lower, upper
    compare_values(lower.value, upper.value, comparator) <= 0 &&
        return lower, upper
    return _invalidateboth(lower, upper, :contradictory_bounds)
end

function interpret_statistics(leaf::LeafSpec, num_values::Int64,
        stats::RawStatistics, orders::Union{Nothing,Vector{DeclaredOrder}};
        leaf_index::Int=1, leaf_count::Int=1,
        created_by::Union{Nothing,AbstractString}=nothing,
        limits::ModelLimits=ModelLimits())
    limits.max_statistics_value_bytes >= 0 || throw(ArgumentError(
        "max_statistics_value_bytes must be nonnegative"))
    nulls, nans, distinct, occupancy = _counts(leaf, num_values, stats)
    order = _validateorders(orders, leaf, leaf_index, leaf_count)
    family = _selectedfamily(stats)
    lowerraw, upperraw = _selectedraw(stats, family)
    lowerraw === nothing || _checkstructure(lowerraw, leaf)
    upperraw === nothing || _checkstructure(upperraw, leaf)
    comparator, orderreason = _comparator(leaf, family, order)
    lower = _boundfact(lowerraw, leaf, limits,
        _exactness(stats.lower_exact, family))
    upper = _boundfact(upperraw, leaf, limits,
        _exactness(stats.upper_exact, family))
    trust = producer_decision(created_by, leaf, comparator, family, lowerraw,
        upperraw, limits)
    if orderreason != :known && family != FAMILY_NONE
        lower, upper = _invalidateboth(lower, upper, orderreason)
    elseif trust.state == TRUST_UNTRUSTED
        lower, upper = _invalidateboth(lower, upper, trust.reason)
    elseif occupancy == OCCUPANCY_EMPTY
        lower, upper = _invalidateboth(lower, upper, :no_non_null_values)
    elseif comparator in (COMPARATOR_TYPE_FLOAT, COMPARATOR_IEEE_FLOAT)
        lower, upper = _applyfloatrules(lower, upper, comparator, occupancy)
    end
    if comparator != COMPARATOR_UNDEFINED
        lower, upper = _checkboundorder(lower, upper, comparator)
    end
    return StatisticsResult(lower, upper, nulls, nans, distinct, occupancy,
        family, comparator, trust)
end

function _summaryvalue(raw::AbstractVector{UInt8}, leaf::LeafSpec)
    _checkstructure(raw, leaf)
    value = _logicalvalue(Vector{UInt8}(raw), leaf)
    value === nothing && throw(ModelFormatError(
        "summary input is not a valid logical value"))
    return value
end

function _summarycandidate(value::ModelValue, comparator::ComparatorKind,
        has_non_nan::Bool)
    value isa FloatValue || return true
    float_isnan(value) || return true
    comparator == COMPARATOR_TYPE_FLOAT && return false
    return comparator == COMPARATOR_IEEE_FLOAT && !has_non_nan
end

function _summaryzeros(lower::ModelValue, upper::ModelValue,
        comparator::ComparatorKind)
    comparator == COMPARATOR_TYPE_FLOAT || return lower, upper
    lower isa FloatValue && upper isa FloatValue || return lower, upper
    if float_iszero(lower)
        lower = FloatValue(lower.width, lower.bits | _floatsignmask(lower))
    end
    if float_iszero(upper)
        upper = FloatValue(upper.width,
            upper.bits & xor(_floatmask(upper), _floatsignmask(upper)))
    end
    return lower, upper
end

function summarize_raw_values(leaf::LeafSpec,
        raw_values::AbstractVector{<:AbstractVector{UInt8}},
        comparator::ComparatorKind)
    values = ModelValue[]
    sizehint!(values, length(raw_values))
    nan_count = Int64(0)
    has_non_nan = false
    for raw in raw_values
        value = _summaryvalue(raw, leaf)
        push!(values, value)
        if value isa FloatValue && float_isnan(value)
            nan_count += 1
        else
            has_non_nan = true
        end
    end
    lower = nothing
    upper = nothing
    for value in values
        _summarycandidate(value, comparator, has_non_nan) || continue
        if lower === nothing
            lower = value
            upper = value
            continue
        end
        compare_values(value, lower, comparator) < 0 && (lower = value)
        compare_values(value, upper, comparator) > 0 && (upper = value)
    end
    if lower !== nothing
        lower, upper = _summaryzeros(lower, upper, comparator)
    end
    return ExtremaSummary(lower, upper, nan_count, Int64(length(values)))
end

function contains_value(summary::ExtremaSummary, value::ModelValue,
        comparator::ComparatorKind)
    summary.lower === nothing && return false
    summary.upper === nothing && return false
    if value isa FloatValue
        lowerisnan = summary.lower isa FloatValue && float_isnan(summary.lower)
        upperisnan = summary.upper isa FloatValue && float_isnan(summary.upper)
        valueisnan = float_isnan(value)
        if comparator == COMPARATOR_IEEE_FLOAT
            lowerisnan == upperisnan || return false
            valueisnan == lowerisnan || return false
        else
            valueisnan && return false
        end
    end
    compare_values(summary.lower, value, comparator) <= 0 || return false
    return compare_values(value, summary.upper, comparator) <= 0
end

end
