struct _StatisticOrderFact
    declared::Symbol
    comparison::Symbol
end

struct _StatisticBoundFact
    state::Symbol
    raw::Union{Nothing,Vector{UInt8}}
    exactness::Symbol
    reason::Symbol
    adjustment::Symbol
end

struct _StatisticCountFact
    state::Symbol
    value::Union{Nothing,Int64}
end

struct _StatisticTrustFact
    state::Symbol
    reason::Symbol
end

struct _ColumnStatisticFacts
    lower::_StatisticBoundFact
    upper::_StatisticBoundFact
    null_count::_StatisticCountFact
    nan_count::_StatisticCountFact
    distinct_count::_StatisticCountFact
    order::_StatisticOrderFact
    trust::_StatisticTrustFact
    family::Symbol
    comparison::Symbol
    occupancy::Symbol
end

struct _StatisticLeafSemantics
    comparison::Symbol
    logical::Any
    floating::Bool
end

struct _StatisticProducerVersion
    present::Bool
    major::UInt64
    minor::UInt64
    patch::UInt64
    prerelease::Bool
    cdh_exception::Bool
end

struct _StatisticProducer
    application::Symbol
    parsed::Bool
    version::_StatisticProducerVersion
end

const _NO_STATISTIC_VERSION = _StatisticProducerVersion(false, 0, 0, 0, false,
    false)

function _statisticlimit(limits::Limits)
    limit = limits.max_statistics_value_bytes
    limit >= 0 || throw(ArgumentError(
        "max_statistics_value_bytes must be nonnegative, got $limit"))
    return limit
end

function _leafstatisticsemantics(element::Metadata.SchemaElement)
    logical = _logicalkind(element)
    floating = element.type_ in (Metadata.Type.FLOAT, Metadata.Type.DOUBLE) ||
        logical === :float16
    if logical isa _IntegerLogicalKind
        comparison = logical.signed ? :signed : :unsigned
        return _StatisticLeafSemantics(comparison, logical, floating)
    elseif logical isa Union{_TimeLogicalKind,_TimestampLogicalKind}
        return _StatisticLeafSemantics(:signed, logical, floating)
    elseif logical in (:string, :enum, :uuid, :json, :bson)
        return _StatisticLeafSemantics(:unsigned_bytes, logical, floating)
    elseif logical === :date
        return _StatisticLeafSemantics(:signed, logical, floating)
    elseif logical === :decimal
        return _StatisticLeafSemantics(:decimal, logical, floating)
    elseif logical === :float16
        return _StatisticLeafSemantics(:floating, logical, floating)
    elseif logical in (:interval, :unknown)
        return _StatisticLeafSemantics(:undefined, logical, floating)
    elseif logical !== nothing
        return _StatisticLeafSemantics(:undefined, logical, floating)
    end
    annotated = element.logicalType !== nothing || element.converted_type !== nothing
    annotated && return _StatisticLeafSemantics(:undefined, nothing, floating)
    physical = element.type_
    physical == Metadata.Type.BOOLEAN &&
        return _StatisticLeafSemantics(:boolean, nothing, floating)
    physical in (Metadata.Type.INT32, Metadata.Type.INT64) &&
        return _StatisticLeafSemantics(:signed, nothing, floating)
    physical in (Metadata.Type.FLOAT, Metadata.Type.DOUBLE) &&
        return _StatisticLeafSemantics(:floating, nothing, floating)
    physical in (Metadata.Type.BYTE_ARRAY, Metadata.Type.FIXED_LEN_BYTE_ARRAY) &&
        return _StatisticLeafSemantics(:unsigned_bytes, nothing, floating)
    return _StatisticLeafSemantics(:undefined, nothing, floating)
end

function _columnorderdeclaration(order::Metadata.ColumnOrder)
    order.TYPE_ORDER !== nothing && return :type_order
    order.IEEE_754_TOTAL_ORDER !== nothing && return :ieee_total_order
    isempty(order.unknown_fields) &&
        throw(FormatError("ColumnOrder union has no member"))
    length(order.unknown_fields) == 1 ||
        throw(FormatError("ColumnOrder union has more than one unknown member"))
    return :unknown
end

function _validatecolumnorders(schema::Schema, leafindex::Int,
    orders::Union{Nothing,AbstractVector{Metadata.ColumnOrder}})
    orders === nothing && return :absent
    length(orders) == length(schema.leaves) || throw(FormatError(
        "column_orders has $(length(orders)) entries for $(length(schema.leaves)) leaves"))
    selected = :absent
    for index in eachindex(schema.leaves)
        semantics = _leafstatisticsemantics(schema.leaves[index].element)
        declaration = _columnorderdeclaration(orders[index])
        declaration === :ieee_total_order && !semantics.floating &&
            throw(FormatError("IEEE_754_TOTAL_ORDER is invalid for non-floating leaf " *
                "$(repr(schema.leaves[index].element.name))"))
        index == leafindex && (selected = declaration)
    end
    return selected
end

function _statisticorder(semantics::_StatisticLeafSemantics, declared::Symbol)
    comparison = if declared === :type_order
        semantics.comparison
    elseif declared === :ieee_total_order
        :ieee_total_order
    else
        :undefined
    end
    return _StatisticOrderFact(declared, comparison)
end

function _absentcount()
    return _StatisticCountFact(:absent, nothing)
end

function _validatedcount(name::Symbol, value::Union{Nothing,Int64}, total::Int64)
    value === nothing && return _absentcount()
    0 <= value <= total || throw(FormatError(
        "$name $value is outside the valid range 0:$total"))
    return _StatisticCountFact(:known, value)
end

function _checkedcountsum(left::Int64, right::Int64, label::String)
    return try
        Base.checked_add(left, right)
    catch err
        err isa OverflowError || rethrow()
        throw(FormatError("$label overflows Int64"))
    end
end

function _validatestatisticcounts(statistics::Union{Nothing,Metadata.Statistics},
    total::Int64, floating::Bool)
    statistics === nothing && return (_absentcount(), _absentcount(), _absentcount())
    nulls = _validatedcount(:null_count, statistics.null_count, total)
    nans = _validatedcount(:nan_count, statistics.nan_count, total)
    distinct = _validatedcount(:distinct_count, statistics.distinct_count, total)
    nans.state === :known && !floating && throw(FormatError(
        "nan_count is only valid for FLOAT, DOUBLE, and FLOAT16 leaves"))
    _validatecountrelationships(nulls, nans, distinct, total)
    return nulls, nans, distinct
end

function _validatecountrelationships(nulls::_StatisticCountFact,
    nans::_StatisticCountFact, distinct::_StatisticCountFact, total::Int64)
    if nulls.state === :known && nans.state === :known
        combined = _checkedcountsum(nulls.value::Int64, nans.value::Int64,
            "null_count + nan_count")
        combined <= total || throw(FormatError(
            "null_count + nan_count exceeds num_values $total"))
    end
    if nulls.state === :known && distinct.state === :known
        available = total - (nulls.value::Int64)
        (distinct.value::Int64) <= available || throw(FormatError(
            "distinct_count exceeds non-null value count $available"))
    end
    return
end

function _statisticoccupancy(nulls::_StatisticCountFact,
    nans::_StatisticCountFact, total::Int64, floating::Bool)
    iszero(total) && return :no_non_null
    if nulls.state === :known && nulls.value == total
        return :no_non_null
    end
    floating || return :unknown
    nulls.state === :known && nans.state === :known || return :unknown
    nonnull = total - (nulls.value::Int64)
    nonnull > 0 || return :no_non_null
    nancount = nans.value::Int64
    nancount == nonnull && return :all_nan
    nancount < nonnull && return :has_non_nan
    throw(AssertionError("validated floating counts have an impossible occupancy"))
end

function _readstatisticuint16(bytes::AbstractVector{UInt8})
    return UInt16(bytes[1]) | (UInt16(bytes[2]) << 8)
end

function _readstatisticuint32(bytes::AbstractVector{UInt8})
    return UInt32(bytes[1]) | (UInt32(bytes[2]) << 8) |
        (UInt32(bytes[3]) << 16) | (UInt32(bytes[4]) << 24)
end

function _readstatisticuint64(bytes::AbstractVector{UInt8})
    value = UInt64(0)
    for index in 8:-1:1
        value = (value << 8) | UInt64(bytes[index])
    end
    return value
end

function _statisticplainwidth(element::Metadata.SchemaElement)
    physical = element.type_
    physical == Metadata.Type.BOOLEAN && return 1
    physical in (Metadata.Type.INT32, Metadata.Type.FLOAT) && return 4
    physical in (Metadata.Type.INT64, Metadata.Type.DOUBLE) && return 8
    physical == Metadata.Type.INT96 && return 12
    physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
        return Int(something(element.type_length))
    return nothing
end

function _checkstatisticwidth(element::Metadata.SchemaElement,
    raw::Vector{UInt8})
    width = _statisticplainwidth(element)
    width === nothing && return
    length(raw) == width || throw(FormatError(
        "statistics bound for $(repr(element.name)) has $(length(raw)) bytes, expected $width"))
    return
end

function _statisticexactness(family::Symbol, raw, flag::Union{Nothing,Bool})
    raw === nothing && return :unknown
    family === :deprecated && return :unknown
    flag === nothing && return :unknown
    return flag ? :exact : :inexact
end

function _absentbound()
    return _StatisticBoundFact(:absent, nothing, :unknown, :absent, :none)
end

function _unknownbound(bound::_StatisticBoundFact, reason::Symbol)
    bound.state === :absent && return bound
    bound.state === :unknown && return bound
    return _StatisticBoundFact(:unknown, bound.raw, bound.exactness, reason, :none)
end

function _preparestatisticbound(element::Metadata.SchemaElement, raw,
    family::Symbol, flag::Union{Nothing,Bool}, limit::Int64)
    raw === nothing && return _absentbound()
    _checkstatisticwidth(element, raw)
    exactness = _statisticexactness(family, raw, flag)
    bound = _StatisticBoundFact(:known, raw, exactness, :valid, :none)
    length(raw) <= limit && return bound
    return _unknownbound(bound, :over_limit)
end

function _statisticsignedvalue(element::Metadata.SchemaElement,
    raw::AbstractVector{UInt8})
    element.type_ == Metadata.Type.INT32 &&
        return Int64(reinterpret(Int32, _readstatisticuint32(raw)))
    element.type_ == Metadata.Type.INT64 &&
        return reinterpret(Int64, _readstatisticuint64(raw))
    throw(ArgumentError("signed statistics comparison requires INT32 or INT64"))
end

function _statisticunsignedvalue(element::Metadata.SchemaElement,
    raw::AbstractVector{UInt8})
    element.type_ == Metadata.Type.INT32 && return UInt64(_readstatisticuint32(raw))
    element.type_ == Metadata.Type.INT64 && return _readstatisticuint64(raw)
    throw(ArgumentError("unsigned statistics comparison requires INT32 or INT64"))
end

function _statisticintegerfits(kind::_IntegerLogicalKind,
    element::Metadata.SchemaElement, raw::AbstractVector{UInt8})
    width = Int(kind.bitwidth)
    if kind.signed
        width == 64 && return true
        value = _statisticsignedvalue(element, raw)
        limit = Int64(1) << (width - 1)
        return -limit <= value < limit
    end
    width == 64 && return true
    value = _statisticunsignedvalue(element, raw)
    limit = (UInt64(1) << width) - UInt64(1)
    return value <= limit
end

function _statistictimevalid(kind::_TimeLogicalKind,
    element::Metadata.SchemaElement, raw::AbstractVector{UInt8})
    value = _statisticsignedvalue(element, raw)
    limit, _ = _timeparameters(kind.unit)
    return 0 <= value < limit
end

function _unsignedmagnitude(value::Int64)
    value >= 0 && return UInt64(value)
    return UInt64(-(value + 1)) + UInt64(1)
end

function _decimaldigitcount(value::Int64)
    magnitude = _unsignedmagnitude(value)
    magnitude == 0 && return 1
    digits = 0
    while magnitude != 0
        magnitude = div(magnitude, UInt64(10))
        digits += 1
    end
    return digits
end

function _decimalmagnitude!(output::Vector{UInt8}, raw::AbstractVector{UInt8})
    copyto!(output, raw)
    iszero(first(raw) & 0x80) && return output
    for index in eachindex(output)
        @inbounds output[index] = ~output[index]
    end
    for index in lastindex(output):-1:firstindex(output)
        @inbounds output[index] += UInt8(1)
        @inbounds iszero(output[index]) || break
    end
    return output
end

function _decimalchunkdigits(value::UInt64)
    value == 0 && return 1
    digits = 0
    while value != 0
        value = div(value, UInt64(10))
        digits += 1
    end
    return digits
end

function _dividedecimalchunk!(magnitude::Vector{UInt8}, start::Int)
    remainder = UInt64(0)
    divisor = UInt64(1_000_000_000)
    for index in start:lastindex(magnitude)
        current = (remainder << 8) | UInt64(magnitude[index])
        magnitude[index] = UInt8(div(current, divisor))
        remainder = rem(current, divisor)
    end
    while start <= lastindex(magnitude) && iszero(magnitude[start])
        start += 1
    end
    return start, remainder
end

function _decimaldigitcount(raw::AbstractVector{UInt8}, budget::_LiveByteBudget)
    isempty(raw) && return nothing
    charge = _reservearray!(budget, UInt8, length(raw))
    try
        magnitude = Vector{UInt8}(undef, length(raw))
        _decimalmagnitude!(magnitude, raw)
        start = firstindex(magnitude)
        while start <= lastindex(magnitude) && iszero(magnitude[start])
            start += 1
        end
        start > lastindex(magnitude) && return 1
        chunks = 0
        leading = UInt64(0)
        while start <= lastindex(magnitude)
            start, leading = _dividedecimalchunk!(magnitude, start)
            chunks += 1
        end
        return (chunks - 1) * 9 + _decimalchunkdigits(leading)
    finally
        _release!(budget, charge)
    end
end

function _statisticdecimalvalid(element::Metadata.SchemaElement,
    raw::AbstractVector{UInt8}, budget::_LiveByteBudget)
    precision, _ = something(_decimalparameters(element))
    physical = element.type_
    digits = if physical in (Metadata.Type.INT32, Metadata.Type.INT64)
        _decimaldigitcount(_statisticsignedvalue(element, raw))
    else
        _decimaldigitcount(raw, budget)
    end
    digits === nothing && return false
    return digits <= precision
end

function _optionallogicaldocumentvalid(kind::Symbol, raw::AbstractVector{UInt8},
    limits::Limits, budget::_LiveByteBudget)
    charge = _reserveobjects!(budget)
    try
        try
            kind === :json && _validatejson(raw, limits, FormatError)
            kind === :bson && _validatebson(raw, limits, FormatError)
            return true
        catch err
            err isa Union{FormatError,LimitError} && return false
            rethrow()
        end
    finally
        _release!(budget, charge)
    end
end

function _statisticsemanticvalid(semantics::_StatisticLeafSemantics,
    element::Metadata.SchemaElement, raw::AbstractVector{UInt8}, limits::Limits,
    budget::_LiveByteBudget)
    logical = semantics.logical
    logical isa _IntegerLogicalKind &&
        return _statisticintegerfits(logical, element, raw)
    logical isa _TimeLogicalKind &&
        return _statistictimevalid(logical, element, raw)
    logical === :decimal && return _statisticdecimalvalid(element, raw, budget)
    logical in (:string, :enum) && return isvalid(String, raw)
    logical in (:json, :bson) &&
        return _optionallogicaldocumentvalid(logical, raw, limits, budget)
    semantics.comparison === :boolean && return raw[1] in (0x00, 0x01)
    return true
end

function _validatestatisticbound(bound::_StatisticBoundFact,
    semantics::_StatisticLeafSemantics, element::Metadata.SchemaElement,
    limits::Limits, budget::_LiveByteBudget)
    bound.state === :known || return bound
    raw = bound.raw::Vector{UInt8}
    _statisticsemanticvalid(semantics, element, raw, limits, budget) && return bound
    return _unknownbound(bound, :invalid_value)
end

function _comparestatisticbytes(left::AbstractVector{UInt8},
    right::AbstractVector{UInt8})
    for (leftbyte, rightbyte) in zip(left, right)
        leftbyte < rightbyte && return Int8(-1)
        leftbyte > rightbyte && return Int8(1)
    end
    length(left) < length(right) && return Int8(-1)
    length(left) > length(right) && return Int8(1)
    return Int8(0)
end

function _normalizedsignedstart(bytes::AbstractVector{UInt8})
    start = firstindex(bytes)
    stop = lastindex(bytes)
    negative = !iszero(bytes[start] & 0x80)
    extension = negative ? UInt8(0xff) : UInt8(0x00)
    while start < stop && bytes[start] == extension
        nextnegative = !iszero(bytes[start + 1] & 0x80)
        nextnegative == negative || break
        start += 1
    end
    return start, negative
end

function _comparestatisticdecimalbytes(left::AbstractVector{UInt8},
    right::AbstractVector{UInt8})
    leftstart, leftnegative = _normalizedsignedstart(left)
    rightstart, rightnegative = _normalizedsignedstart(right)
    leftnegative != rightnegative && return leftnegative ? Int8(-1) : Int8(1)
    leftlength = lastindex(left) - leftstart + 1
    rightlength = lastindex(right) - rightstart + 1
    if leftlength != rightlength
        order = leftlength < rightlength ? Int8(-1) : Int8(1)
        return leftnegative ? -order : order
    end
    for offset in 0:(leftlength - 1)
        leftbyte = left[leftstart + offset]
        rightbyte = right[rightstart + offset]
        leftbyte < rightbyte && return Int8(-1)
        leftbyte > rightbyte && return Int8(1)
    end
    return Int8(0)
end

function _comparestatisticnumbers(left, right)
    left < right && return Int8(-1)
    left > right && return Int8(1)
    return Int8(0)
end

function _statisticfloatbits(element::Metadata.SchemaElement,
    raw::AbstractVector{UInt8})
    physical = element.type_
    physical == Metadata.Type.FLOAT && return _readstatisticuint32(raw)
    physical == Metadata.Type.DOUBLE && return _readstatisticuint64(raw)
    return _readstatisticuint16(raw)
end

function _statisticfloatvalue(bits::UInt16)
    return reinterpret(Float16, bits)
end

function _statisticfloatvalue(bits::UInt32)
    return reinterpret(Float32, bits)
end

function _statisticfloatvalue(bits::UInt64)
    return reinterpret(Float64, bits)
end

function _statisticfloatmasks(bits::UInt16)
    return UInt16(0x8000), UInt16(0x7c00), UInt16(0x03ff)
end

function _statisticfloatmasks(bits::UInt32)
    return UInt32(0x80000000), UInt32(0x7f800000), UInt32(0x007fffff)
end

function _statisticfloatmasks(bits::UInt64)
    return UInt64(0x8000000000000000), UInt64(0x7ff0000000000000),
        UInt64(0x000fffffffffffff)
end

function _statisticisnan(bits::Union{UInt16,UInt32,UInt64})
    _, exponent, fraction = _statisticfloatmasks(bits)
    return bits & exponent == exponent && !iszero(bits & fraction)
end

function _statisticiszero(bits::Union{UInt16,UInt32,UInt64})
    sign, _, _ = _statisticfloatmasks(bits)
    return iszero(bits & ~sign)
end

function _statisticisnegative(bits::Union{UInt16,UInt32,UInt64})
    sign, _, _ = _statisticfloatmasks(bits)
    return !iszero(bits & sign)
end

function _statisticieeekey(bits::T) where {T<:Union{UInt16,UInt32,UInt64}}
    sign, _, _ = _statisticfloatmasks(bits)
    return iszero(bits & sign) ? bits | sign : ~bits
end

function _adjustedstatisticfloat(bits::T, adjustment::Symbol) where
    {T<:Union{UInt16,UInt32,UInt64}}
    sign, _, _ = _statisticfloatmasks(bits)
    adjustment === :negative_zero && return _statisticfloatvalue(sign)
    adjustment === :positive_zero && return _statisticfloatvalue(zero(T))
    return _statisticfloatvalue(bits)
end

function _comparestatisticfloats(element::Metadata.SchemaElement,
    left::AbstractVector{UInt8}, right::AbstractVector{UInt8}, comparison::Symbol,
    leftadjustment::Symbol, rightadjustment::Symbol)
    leftbits = _statisticfloatbits(element, left)
    rightbits = _statisticfloatbits(element, right)
    comparison === :ieee_total_order && return _comparestatisticnumbers(
        _statisticieeekey(leftbits), _statisticieeekey(rightbits))
    (_statisticisnan(leftbits) || _statisticisnan(rightbits)) && return nothing
    leftvalue = _adjustedstatisticfloat(leftbits, leftadjustment)
    rightvalue = _adjustedstatisticfloat(rightbits, rightadjustment)
    return _comparestatisticnumbers(leftvalue, rightvalue)
end

function _comparestatisticvalues(element::Metadata.SchemaElement,
    left::AbstractVector{UInt8}, right::AbstractVector{UInt8}, comparison::Symbol;
    left_adjustment::Symbol=:none, right_adjustment::Symbol=:none)
    comparison === :unsigned_bytes && return _comparestatisticbytes(left, right)
    comparison === :signed && return _comparestatisticnumbers(
        _statisticsignedvalue(element, left), _statisticsignedvalue(element, right))
    comparison === :unsigned && return _comparestatisticnumbers(
        _statisticunsignedvalue(element, left), _statisticunsignedvalue(element, right))
    comparison === :boolean && return _comparestatisticnumbers(left[1], right[1])
    if comparison === :decimal
        element.type_ in (Metadata.Type.INT32, Metadata.Type.INT64) &&
            return _comparestatisticnumbers(_statisticsignedvalue(element, left),
                _statisticsignedvalue(element, right))
        return _comparestatisticdecimalbytes(left, right)
    end
    comparison in (:floating, :ieee_total_order) &&
        return _comparestatisticfloats(element, left, right, comparison,
            left_adjustment, right_adjustment)
    return nothing
end

function _statisticstokenat(bytes, position::Int, token::String)
    tokenbytes = codeunits(token)
    position + length(tokenbytes) - 1 <= length(bytes) || return false
    for offset in eachindex(tokenbytes)
        bytes[position + offset - 1] == tokenbytes[offset] || return false
    end
    return true
end

function _statisticsisspace(byte::UInt8)
    return byte in (0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x20)
end

function _statisticsskipspace(bytes, position::Int)
    while position <= length(bytes) && _statisticsisspace(bytes[position])
        position += 1
    end
    return position
end

function _statisticstrimspaceend(bytes, start::Int, stop::Int)
    while stop >= start && _statisticsisspace(bytes[stop])
        stop -= 1
    end
    return stop
end

function _statisticsrangematches(bytes, start::Int, stop::Int, token::String)
    stop - start + 1 == ncodeunits(token) || return false
    return _statisticstokenat(bytes, start, token)
end

function _statisticscontainslineterminator(bytes, start::Int, stop::Int)
    position = start
    while position <= stop
        byte = bytes[position]
        byte in (0x0a, 0x0d) && return true
        byte == 0xc2 && position < stop && bytes[position + 1] == 0x85 &&
            return true
        byte == 0xe2 && position + 2 <= stop && bytes[position + 1] == 0x80 &&
            bytes[position + 2] in (0xa8, 0xa9) && return true
        position += 1
    end
    return false
end

function _statisticsbuildsuffixvalid(bytes, opening::Int, stop::Int,
    internalclosing::Bool)
    bytes[stop] == 0x29 || return false
    internalclosing && return false
    position = _statisticsskipspace(bytes, opening + 1)
    return position + 4 <= stop && _statisticstokenat(bytes, position, "build")
end

function _statisticsfindjavamarker(bytes, start::Int, stop::Int)
    marker = 0
    versionstart = 0
    nextopening = 0
    nextopeningvalid = false
    internalclosing = false
    position = stop
    while position >= start
        byte = bytes[position]
        if byte == 0x29 && position < stop
            internalclosing = true
            position -= 1
            continue
        elseif byte == 0x28
            nextopening = position
            nextopeningvalid = _statisticsbuildsuffixvalid(bytes, position,
                stop, internalclosing)
            position -= 1
            continue
        elseif !_statisticsisspace(byte)
            position -= 1
            continue
        end
        runstop = position
        while position >= start && _statisticsisspace(bytes[position])
            position -= 1
        end
        token = runstop + 1
        token + 6 <= stop && _statisticstokenat(bytes, token, "version") ||
            continue
        if iszero(nextopening) || nextopeningvalid
            marker = position + 1
            versionstart = token + 7
        end
    end
    return marker, versionstart
end

function _statisticsjavasyntaxvalid(bytes, start::Int, stop::Int)
    opening = _statisticsfindboundedbyte(bytes, start, stop, 0x28)
    opening == 0 && return true
    position = _statisticsskipspace(bytes, opening + 1)
    position + 4 <= stop && _statisticstokenat(bytes, position, "build") ||
        return false
    position = _statisticsskipspace(bytes, position + 5)
    while position <= stop && bytes[position] != 0x29
        position += 1
    end
    return position == stop
end

function _statisticsparsedigits(bytes, position::Int, stop::Int)
    position <= stop && 0x30 <= bytes[position] <= 0x39 ||
        return UInt64(0), position, false
    value = UInt64(0)
    while position <= stop && 0x30 <= bytes[position] <= 0x39
        digit = UInt64(bytes[position] - 0x30)
        value <= div(typemax(UInt64) - digit, UInt64(10)) ||
            return UInt64(0), position, false
        value = value * UInt64(10) + digit
        position += 1
    end
    return value, position, true
end

function _statisticsparsetriplet(bytes, position::Int, stop::Int)
    major, position, ok = _statisticsparsedigits(bytes, position, stop)
    ok && position <= stop && bytes[position] == 0x2e ||
        return UInt64(0), UInt64(0), UInt64(0), position, false
    minor, position, ok = _statisticsparsedigits(bytes, position + 1, stop)
    ok && position <= stop && bytes[position] == 0x2e ||
        return UInt64(0), UInt64(0), UInt64(0), position, false
    patch, position, ok = _statisticsparsedigits(bytes, position + 1, stop)
    ok || return UInt64(0), UInt64(0), UInt64(0), position, false
    return major, minor, patch, position, true
end

function _statisticsfindboundedbyte(bytes, start::Int, stop::Int, byte::UInt8)
    for position in start:stop
        bytes[position] == byte && return position
    end
    return 0
end

function _statisticsidentifier(bytes, start::Int, stop::Int)
    start <= stop || return false, UInt64(0), true
    value = UInt64(0)
    maximum = UInt64(typemax(Int32))
    overflowed = false
    for position in start:stop
        byte = bytes[position]
        0x30 <= byte <= 0x39 || return false, UInt64(0), true
        overflowed && continue
        digit = UInt64(byte - 0x30)
        if value > div(maximum - digit, UInt64(10))
            overflowed = true
            continue
        end
        value = value * UInt64(10) + digit
    end
    return true, value, !overflowed
end

function _statisticsprereleasevalid(bytes, start::Int, stop::Int)
    position = start
    while true
        separator = _statisticsfindboundedbyte(bytes, position, stop, 0x2e)
        finish = separator == 0 ? stop : separator - 1
        _, _, valid = _statisticsidentifier(bytes, position, finish)
        valid || return false
        separator == 0 && return true
        position = separator + 1
    end
end

function _statisticsrangecomparison(bytes, start::Int, stop::Int, target::String)
    targetbytes = codeunits(target)
    position = start
    targetposition = firstindex(targetbytes)
    while position <= stop && targetposition <= lastindex(targetbytes)
        bytes[position] < targetbytes[targetposition] && return Int8(-1)
        bytes[position] > targetbytes[targetposition] && return Int8(1)
        position += 1
        targetposition += 1
    end
    position <= stop && return Int8(1)
    targetposition <= lastindex(targetbytes) && return Int8(-1)
    return Int8(0)
end

function _statisticsidentifiercomparison(bytes, start::Int, stop::Int,
    target::String, targetnumber::Union{Nothing,UInt64})
    numeric, value, valid = _statisticsidentifier(bytes, start, stop)
    valid || throw(AssertionError("validated prerelease identifier overflowed"))
    targetnumeric = targetnumber !== nothing
    numeric != targetnumeric && return numeric ? Int8(-1) : Int8(1)
    numeric && return _comparestatisticnumbers(value, targetnumber::UInt64)
    return _statisticsrangecomparison(bytes, start, stop, target)
end

function _statisticscdhlabelcomparison(bytes, start::Int, stop::Int)
    position = start
    for targetindex in 1:3
        separator = _statisticsfindboundedbyte(bytes, position, stop, 0x2e)
        finish = separator == 0 ? stop : separator - 1
        target = targetindex == 1 ? "cdh5" : targetindex == 2 ? "5" : "0"
        targetnumber = targetindex == 1 ? nothing :
            targetindex == 2 ? UInt64(5) : UInt64(0)
        compared = _statisticsidentifiercomparison(bytes, position, finish,
            target, targetnumber)
        iszero(compared) || return compared
        separator == 0 && return targetindex == 3 ? Int8(0) : Int8(-1)
        position = separator + 1
    end
    return Int8(1)
end

function _statisticstrimemptyidentifiers(bytes, start::Int, stop::Int)
    while stop >= start && bytes[stop] == 0x2e
        stop -= 1
    end
    return stop
end

function _statisticsversionsuffix(bytes, position::Int, stop::Int,
    major::UInt64, minor::UInt64, patch::UInt64)
    separator = 0
    for index in position:stop
        bytes[index] in (0x2b, 0x2d) || continue
        separator = index
        break
    end
    plus = _statisticsfindboundedbyte(bytes, position, stop, 0x2b)
    plus != 0 && _statisticscontainslineterminator(bytes, plus + 1, stop) &&
        return false, false, false
    unknownstop = separator == 0 ? stop : separator - 1
    unknown = position <= unknownstop
    separator == 0 && return true, unknown, false
    bytes[separator] == 0x2b && return true, unknown, false
    labelstop = plus == 0 ? stop : plus - 1
    labelstart = separator + 1
    _statisticsprereleasevalid(bytes, labelstart, labelstop) ||
        return false, false, false
    comparisonstop = _statisticstrimemptyidentifiers(bytes, labelstart,
        labelstop)
    cdh = major == 1 && minor == 5 && patch == 0 && !unknown &&
        _statisticscdhlabelcomparison(bytes, labelstart, comparisonstop) >= 0
    return true, true, cdh
end

function _statisticsversion(bytes, start::Int, stop::Int)
    start <= stop || return _NO_STATISTIC_VERSION
    major, minor, patch, position, ok = _statisticsparsetriplet(bytes, start, stop)
    ok || return _NO_STATISTIC_VERSION
    major <= typemax(Int32) && minor <= typemax(Int32) && patch <= typemax(Int32) ||
        return _NO_STATISTIC_VERSION
    valid, prerelease, cdh = _statisticsversionsuffix(bytes, position, stop,
        major, minor, patch)
    valid || return _NO_STATISTIC_VERSION
    return _StatisticProducerVersion(true, major, minor, patch, prerelease, cdh)
end

function _statisticsjavaproducer(createdby::Union{Nothing,String})
    createdby === nothing &&
        return _StatisticProducer(:missing, false, _NO_STATISTIC_VERSION)
    bytes = codeunits(createdby)
    start = _statisticsskipspace(bytes, 1)
    stop = _statisticstrimspaceend(bytes, start, length(bytes))
    start <= stop ||
        return _StatisticProducer(:missing, false, _NO_STATISTIC_VERSION)
    marker, versionstart = _statisticsfindjavamarker(bytes, start, stop)
    marker > start &&
        !_statisticscontainslineterminator(bytes, start, marker - 1) &&
        _statisticsjavasyntaxvalid(bytes, versionstart, stop) ||
        return _StatisticProducer(:unknown, false, _NO_STATISTIC_VERSION)
    application = if _statisticsrangematches(bytes, start, marker - 1, "parquet-mr")
        :parquet_mr
    elseif _statisticsrangematches(bytes, start, marker - 1, "parquet-cpp")
        :parquet_cpp
    else
        :other
    end
    opening = _statisticsfindboundedbyte(bytes, versionstart, stop, 0x28)
    versionstop = opening == 0 ? stop : opening - 1
    versionstart = _statisticsskipspace(bytes, versionstart)
    versionstop = _statisticstrimspaceend(bytes, versionstart, versionstop)
    version = _statisticsversion(bytes, versionstart, versionstop)
    return _StatisticProducer(application, true, version)
end

function _statisticsarrowproducer(createdby::Union{Nothing,String})
    producer = _statisticsjavaproducer(createdby)
    producer.parsed && return producer
    createdby === nothing && return producer
    bytes = codeunits(createdby)
    start = _statisticsskipspace(bytes, 1)
    stop = _statisticstrimspaceend(bytes, start, length(bytes))
    start <= stop || return producer
    application = if _statisticsrangematches(bytes, start, stop, "parquet-mr")
        :parquet_mr
    elseif _statisticsrangematches(bytes, start, stop, "parquet-cpp")
        :parquet_cpp
    else
        return producer
    end
    return _StatisticProducer(application, true, _NO_STATISTIC_VERSION)
end

function _statisticsversionbelow(version::_StatisticProducerVersion,
    major::UInt64, minor::UInt64, patch::UInt64)
    version.present || return true
    current = (version.major, version.minor, version.patch)
    cutoff = (major, minor, patch)
    current < cutoff && return true
    current > cutoff && return false
    return version.prerelease
end

function _statisticscdhexception(version::_StatisticProducerVersion)
    version.present || return false
    return version.cdh_exception
end

function _statisticsparquet251affected(producer::_StatisticProducer)
    producer.parsed || return true
    producer.application === :parquet_mr || return false
    _statisticscdhexception(producer.version) && return false
    return _statisticsversionbelow(producer.version, UInt64(1), UInt64(8), UInt64(0))
end

function _statisticsoldorderaffected(producer::_StatisticProducer)
    if producer.application === :parquet_cpp
        return _statisticsversionbelow(producer.version, UInt64(1), UInt64(3), UInt64(0)),
            :parquet_cpp_pre_1_3
    elseif producer.application === :parquet_mr
        return _statisticsversionbelow(producer.version, UInt64(1), UInt64(10), UInt64(0)),
            :parquet_mr_pre_1_10
    end
    return false, :trusted
end

function _statisticissignedcomparison(comparison::Symbol)
    return comparison in (:signed, :boolean, :decimal, :floating)
end

function _statisticproducertrust(element::Metadata.SchemaElement,
    createdby::Union{Nothing,String}, comparison::Symbol, family::Symbol, lower, upper,
    limit::Int64)
    family === :none && return _StatisticTrustFact(:trusted, :no_bounds)
    binary = element.type_ in (Metadata.Type.BYTE_ARRAY,
        Metadata.Type.FIXED_LEN_BYTE_ARRAY)
    java = _statisticsjavaproducer(createdby)
    if binary && _statisticsparquet251affected(java)
        return _StatisticTrustFact(:untrusted, :parquet_251)
    end
    _statisticissignedcomparison(comparison) &&
        return _StatisticTrustFact(:trusted, :trusted)
    arrow = _statisticsarrowproducer(createdby)
    affected, reason = _statisticsoldorderaffected(arrow)
    affected || return _StatisticTrustFact(:trusted, :trusted)
    lower !== nothing && upper !== nothing && length(lower) <= limit &&
        length(upper) <= limit && lower == upper &&
        return _StatisticTrustFact(:trusted, :affected_equal_bounds)
    return _StatisticTrustFact(:untrusted, reason)
end

function _statisticfamily(statistics::Union{Nothing,Metadata.Statistics})
    statistics === nothing && return :none
    statistics.min_value !== nothing && return :modern
    statistics.max_value !== nothing && return :modern
    statistics.min !== nothing && return :deprecated
    statistics.max !== nothing && return :deprecated
    return :none
end

function _statisticfamilyvalues(statistics::Union{Nothing,Metadata.Statistics},
    family::Symbol)
    family === :none && return nothing, nothing, nothing, nothing
    statistics = statistics::Metadata.Statistics
    family === :modern && return statistics.min_value, statistics.max_value,
        statistics.is_min_value_exact, statistics.is_max_value_exact
    return statistics.min, statistics.max, nothing, nothing
end

function _deprecatedstatisticscompatible(semantics::_StatisticLeafSemantics)
    return _statisticissignedcomparison(semantics.comparison)
end

function _invalidateboundpair(lower::_StatisticBoundFact,
    upper::_StatisticBoundFact, reason::Symbol)
    return _unknownbound(lower, reason), _unknownbound(upper, reason)
end

function _widenstatisticzero(bound::_StatisticBoundFact,
    adjustment::Symbol)
    return _StatisticBoundFact(bound.state, bound.raw, :inexact, :widened_zero,
        adjustment)
end

function _typeorderfloatingbounds(element::Metadata.SchemaElement,
    lower::_StatisticBoundFact, upper::_StatisticBoundFact, occupancy::Symbol)
    occupancy === :all_nan && (lower.state !== :absent || upper.state !== :absent) &&
        return _invalidateboundpair(lower, upper, :all_nan_type_order)
    if lower.state === :known
        bits = _statisticfloatbits(element, lower.raw::Vector{UInt8})
        _statisticisnan(bits) && (lower = _unknownbound(lower, :nan_type_order))
        lower.state === :known && _statisticiszero(bits) && !_statisticisnegative(bits) &&
            (lower = _widenstatisticzero(lower, :negative_zero))
    end
    if upper.state === :known
        bits = _statisticfloatbits(element, upper.raw::Vector{UInt8})
        _statisticisnan(bits) && (upper = _unknownbound(upper, :nan_type_order))
        upper.state === :known && _statisticiszero(bits) && _statisticisnegative(bits) &&
            (upper = _widenstatisticzero(upper, :positive_zero))
    end
    return lower, upper
end

function _ieeefloatingbounds(element::Metadata.SchemaElement,
    lower::_StatisticBoundFact, upper::_StatisticBoundFact, occupancy::Symbol)
    lowernan = lower.state === :known &&
        _statisticisnan(_statisticfloatbits(element, lower.raw::Vector{UInt8}))
    uppernan = upper.state === :known &&
        _statisticisnan(_statisticfloatbits(element, upper.raw::Vector{UInt8}))
    if occupancy === :all_nan
        (!lowernan && lower.state === :known || !uppernan && upper.state === :known) &&
            return _invalidateboundpair(lower, upper, :ieee_bound_kind)
    elseif occupancy === :has_non_nan
        (lowernan || uppernan) &&
            return _invalidateboundpair(lower, upper, :ieee_bound_kind)
    else
        lowernan && (lower = _unknownbound(lower, :unproven_ieee_nan))
        uppernan && (upper = _unknownbound(upper, :unproven_ieee_nan))
    end
    return lower, upper
end

function _normalizefloatingbounds(element::Metadata.SchemaElement,
    lower::_StatisticBoundFact, upper::_StatisticBoundFact, comparison::Symbol,
    occupancy::Symbol)
    comparison === :floating &&
        return _typeorderfloatingbounds(element, lower, upper, occupancy)
    comparison === :ieee_total_order &&
        return _ieeefloatingbounds(element, lower, upper, occupancy)
    return lower, upper
end

function _contradictorystatisticbounds(element::Metadata.SchemaElement,
    lower::_StatisticBoundFact, upper::_StatisticBoundFact, comparison::Symbol)
    lower.state === :known && upper.state === :known || return false
    order = _comparestatisticvalues(element, lower.raw::Vector{UInt8},
        upper.raw::Vector{UInt8}, comparison;
        left_adjustment=lower.adjustment, right_adjustment=upper.adjustment)
    order === nothing && return false
    return order > 0
end

function _statisticboundgate(family::Symbol, order::_StatisticOrderFact,
    semantics::_StatisticLeafSemantics, trust::_StatisticTrustFact,
    occupancy::Symbol)
    family === :none && return :none
    family === :modern && order.declared === :absent && return :missing_order
    family === :modern && order.declared === :unknown && return :unknown_order
    family === :modern && order.comparison === :undefined && return :undefined_order
    family === :deprecated && !_deprecatedstatisticscompatible(semantics) &&
        return :deprecated_order_mismatch
    trust.state === :untrusted && return trust.reason
    occupancy === :no_non_null && return :no_non_null
    return :valid
end

function _evaluatestatisticbounds(element::Metadata.SchemaElement,
    statistics::Union{Nothing,Metadata.Statistics}, family::Symbol,
    semantics::_StatisticLeafSemantics, order::_StatisticOrderFact,
    comparison::Symbol, trust::_StatisticTrustFact, occupancy::Symbol,
    limits::Limits, budget::_LiveByteBudget)
    rawlower, rawupper, lowerflag, upperflag =
        _statisticfamilyvalues(statistics, family)
    limit = limits.max_statistics_value_bytes
    lower = _preparestatisticbound(element, rawlower, family, lowerflag, limit)
    upper = _preparestatisticbound(element, rawupper, family, upperflag, limit)
    gate = _statisticboundgate(family, order, semantics, trust, occupancy)
    gate === :none && return lower, upper
    gate === :valid || return _invalidateboundpair(lower, upper, gate)
    lower = _validatestatisticbound(lower, semantics, element, limits, budget)
    upper = _validatestatisticbound(upper, semantics, element, limits, budget)
    lower, upper = _normalizefloatingbounds(element, lower, upper, comparison, occupancy)
    _contradictorystatisticbounds(element, lower, upper, comparison) &&
        return _invalidateboundpair(lower, upper, :contradictory_bounds)
    return lower, upper
end

function _checkstatisticmetadata(node::SchemaNode, metadata::Metadata.ColumnMetaData)
    metadata.type_ == node.element.type_ || throw(FormatError(
        "statistics column type $(metadata.type_) does not match schema type " *
        "$(node.element.type_)"))
    metadata.path_in_schema == node.path || throw(FormatError(
        "statistics column path $(metadata.path_in_schema) does not match schema path " *
        "$(node.path)"))
    metadata.num_values >= 0 || throw(FormatError("negative column chunk value count"))
    return
end

function _statisticsfacts(schema::Schema, leafindex::Integer,
    createdby::Union{Nothing,String},
    orders::Union{Nothing,AbstractVector{Metadata.ColumnOrder}},
    metadata::Metadata.ColumnMetaData; limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    _statisticlimit(limits)
    1 <= leafindex <= length(schema.leaves) || throw(ArgumentError(
        "leaf index $leafindex is outside 1:$(length(schema.leaves))"))
    index = Int(leafindex)
    declared = _validatecolumnorders(schema, index, orders)
    node = schema.leaves[index]
    _checkstatisticmetadata(node, metadata)
    semantics = _leafstatisticsemantics(node.element)
    order = _statisticorder(semantics, declared)
    statistics = metadata.statistics
    nulls, nans, distinct = _validatestatisticcounts(statistics,
        metadata.num_values, semantics.floating)
    occupancy = _statisticoccupancy(nulls, nans, metadata.num_values,
        semantics.floating)
    family = _statisticfamily(statistics)
    rawlower, rawupper, _, _ = _statisticfamilyvalues(statistics, family)
    selectedcomparison = if family === :modern
        order.comparison
    elseif family === :deprecated && _deprecatedstatisticscompatible(semantics)
        semantics.comparison
    else
        :undefined
    end
    trust = _statisticproducertrust(node.element, createdby,
        selectedcomparison, family, rawlower, rawupper,
        limits.max_statistics_value_bytes)
    lower, upper = _evaluatestatisticbounds(node.element, statistics, family,
        semantics, order, selectedcomparison, trust, occupancy, limits, budget)
    return _ColumnStatisticFacts(lower, upper, nulls, nans, distinct, order,
        trust, family, selectedcomparison, occupancy)
end
