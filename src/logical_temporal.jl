import Dates

const _TEMPORAL_MILLIS = UInt8(1)
const _TEMPORAL_MICROS = UInt8(2)
const _TEMPORAL_NANOS = UInt8(3)
const _MILLIS_PER_DAY = Int64(86_400_000)
const _MICROS_PER_DAY = Int64(86_400_000_000)
const _NANOS_PER_DAY = Int64(86_400_000_000_000)
const _PARQUET_DATETIME_EPOCH = Dates.value(Dates.DateTime(1970, 1, 1))

struct _TimeLogicalKind
    unit::UInt8
    is_adjusted_to_utc::Bool
end

struct _TimestampLogicalKind
    unit::UInt8
    is_adjusted_to_utc::Bool
end

struct _IntegerLogicalKind
    bitwidth::UInt8
    signed::Bool
end

function _temporalunit(unit::Metadata.TimeUnit)
    unit.MILLIS !== nothing && return _TEMPORAL_MILLIS
    unit.MICROS !== nothing && return _TEMPORAL_MICROS
    unit.NANOS !== nothing && return _TEMPORAL_NANOS
    return nothing
end

function _requiretemporalunit(element::Metadata.SchemaElement,
    annotation::Symbol, unit::Metadata.TimeUnit)
    value = _temporalunit(unit)
    value !== nothing && return value
    isempty(unit.unknown_fields) && throw(FormatError(
        "$annotation annotation on $(repr(element.name)) has no time unit"))
    throw(UnsupportedFeatureError(
        "$annotation annotation on $(repr(element.name)) uses an unknown time unit"))
end

function _temporalunitname(unit::UInt8)
    unit == _TEMPORAL_MILLIS && return :millis
    unit == _TEMPORAL_MICROS && return :micros
    unit == _TEMPORAL_NANOS && return :nanos
    throw(ArgumentError("unknown temporal unit code $unit"))
end

function _validatetimephysical(element::Metadata.SchemaElement, unit::UInt8)
    expected = unit == _TEMPORAL_MILLIS ? Metadata.Type.INT32 : Metadata.Type.INT64
    _requirelogicalphysical(element, expected)
    return
end

function _validateintegerphysical(element::Metadata.SchemaElement,
    kind::_IntegerLogicalKind)
    expected = kind.bitwidth == 64 ? Metadata.Type.INT64 : Metadata.Type.INT32
    _requirelogicalphysical(element, expected)
    return
end

function _modernintegerkind(element::Metadata.SchemaElement, integer::Metadata.IntType)
    width = Int(integer.bitWidth)
    width in (8, 16, 32, 64) ||
        throw(FormatError("INTEGER annotation on $(repr(element.name)) has invalid " *
            "bit width $width"))
    kind = _IntegerLogicalKind(UInt8(width), integer.isSigned)
    _validateintegerphysical(element, kind)
    return kind
end

function _legacyintegerkind(converted::Metadata.ConvertedType.T)
    converted == Metadata.ConvertedType.INT_8 && return _IntegerLogicalKind(8, true)
    converted == Metadata.ConvertedType.INT_16 && return _IntegerLogicalKind(16, true)
    converted == Metadata.ConvertedType.INT_32 && return _IntegerLogicalKind(32, true)
    converted == Metadata.ConvertedType.INT_64 && return _IntegerLogicalKind(64, true)
    converted == Metadata.ConvertedType.UINT_8 && return _IntegerLogicalKind(8, false)
    converted == Metadata.ConvertedType.UINT_16 && return _IntegerLogicalKind(16, false)
    converted == Metadata.ConvertedType.UINT_32 && return _IntegerLogicalKind(32, false)
    converted == Metadata.ConvertedType.UINT_64 && return _IntegerLogicalKind(64, false)
    return nothing
end

function _moderntemporallogicalkind(element::Metadata.SchemaElement,
    logical::Metadata.LogicalType)
    if logical.TIME !== nothing
        unit = _requiretemporalunit(element, :TIME, logical.TIME.unit)
        _validatetimephysical(element, unit)
        return _TimeLogicalKind(unit, logical.TIME.isAdjustedToUTC)
    elseif logical.TIMESTAMP !== nothing
        unit = _requiretemporalunit(element, :TIMESTAMP, logical.TIMESTAMP.unit)
        _requirelogicalphysical(element, Metadata.Type.INT64)
        return _TimestampLogicalKind(unit, logical.TIMESTAMP.isAdjustedToUTC)
    elseif logical.INTEGER !== nothing
        return _modernintegerkind(element, logical.INTEGER)
    end
    return nothing
end

function _legacytemporallogicalkind(element::Metadata.SchemaElement,
    converted::Metadata.ConvertedType.T)
    if converted == Metadata.ConvertedType.TIME_MILLIS
        _requirelogicalphysical(element, Metadata.Type.INT32)
        return _TimeLogicalKind(_TEMPORAL_MILLIS, true)
    elseif converted == Metadata.ConvertedType.TIME_MICROS
        _requirelogicalphysical(element, Metadata.Type.INT64)
        return _TimeLogicalKind(_TEMPORAL_MICROS, true)
    elseif converted == Metadata.ConvertedType.TIMESTAMP_MILLIS
        _requirelogicalphysical(element, Metadata.Type.INT64)
        return _TimestampLogicalKind(_TEMPORAL_MILLIS, true)
    elseif converted == Metadata.ConvertedType.TIMESTAMP_MICROS
        _requirelogicalphysical(element, Metadata.Type.INT64)
        return _TimestampLogicalKind(_TEMPORAL_MICROS, true)
    end
    kind = _legacyintegerkind(converted)
    kind === nothing && return nothing
    _validateintegerphysical(element, kind)
    return kind
end

_temporallogicalkind(element::Metadata.SchemaElement) =
    _compatiblekind(_temporalannotationkind, element)

function _temporalannotationkind(element::Metadata.SchemaElement)
    logical = element.logicalType
    logical !== nothing && return _moderntemporallogicalkind(element, logical)
    converted = element.converted_type
    converted === nothing && return nothing
    return _legacytemporallogicalkind(element, converted)
end

function _temporallogicalkind(node::SchemaNode)
    return _temporallogicalkind(node.element)
end

function _integerjuliatype(kind::_IntegerLogicalKind)
    if kind.signed
        kind.bitwidth == 8 && return Int8
        kind.bitwidth == 16 && return Int16
        kind.bitwidth == 32 && return Int32
        return Int64
    end
    kind.bitwidth == 8 && return UInt8
    kind.bitwidth == 16 && return UInt16
    kind.bitwidth == 32 && return UInt32
    return UInt64
end

function _temporaljuliatype(::_TimeLogicalKind)
    return Dates.Time
end

function _temporalperiod(unit::UInt8)
    unit == _TEMPORAL_MILLIS && return Dates.Millisecond
    unit == _TEMPORAL_MICROS && return Dates.Microsecond
    return Dates.Nanosecond
end

# TIMESTAMP reads as a `Durations.Timestamp` of its unit, or as a UTC
# `Durations.ZonedTimestamp` when it is adjusted to UTC (as legacy TIMESTAMP_MILLIS and
# TIMESTAMP_MICROS columns are).
function _temporaljuliatype(kind::_TimestampLogicalKind)
    P = _temporalperiod(kind.unit)
    kind.is_adjusted_to_utc && return Durations.ZonedTimestamp{P,:UTC}
    return Durations.Timestamp{P}
end

function _temporaljuliatype(kind::_IntegerLogicalKind)
    return _integerjuliatype(kind)
end

function _temporallogicaleltype(element::Metadata.SchemaElement, physical::Type)
    kind = _temporallogicalkind(element)
    kind === nothing && return physical
    return _temporaljuliatype(kind)
end

function _temporallogicaleltype(node::SchemaNode, physical::Type)
    return _temporallogicaleltype(node.element, physical)
end

function _timeparameters(unit::UInt8)
    unit == _TEMPORAL_MILLIS && return _MILLIS_PER_DAY, Int64(1_000_000)
    unit == _TEMPORAL_MICROS && return _MICROS_PER_DAY, Int64(1_000)
    return _NANOS_PER_DAY, Int64(1)
end

function _fromparquettime(kind::_TimeLogicalKind, value, element::Metadata.SchemaElement)
    expected = kind.unit == _TEMPORAL_MILLIS ? Int32 : Int64
    value isa expected ||
        throw(FormatError("TIME column $(repr(element.name)) contains a non-$expected value"))
    ticks = Int64(value)
    limit, scale = _timeparameters(kind.unit)
    ticks == limit && throw(UnsupportedFeatureError("TIME column $(repr(element.name)) " *
        "holds 24:00:00, which Dates.Time cannot represent; read the other columns with " *
        "drop=[:$(element.name)]"))
    0 <= ticks < limit ||
        throw(FormatError("TIME column $(repr(element.name)) is outside one day"))
    return Dates.Time(Dates.Nanosecond(ticks * scale))
end

function _toparquettime(kind::_TimeLogicalKind, value, element::Metadata.SchemaElement)
    value isa Dates.Time ||
        throw(ArgumentError("TIME column $(repr(element.name)) contains a non-Time value"))
    nanoseconds = Dates.value(value)
    _, scale = _timeparameters(kind.unit)
    rem(nanoseconds, scale) == 0 ||
        throw(ArgumentError("TIME column $(repr(element.name)) loses precision at " *
            "$(_temporalunitname(kind.unit)) resolution"))
    ticks = div(nanoseconds, scale)
    return kind.unit == _TEMPORAL_MILLIS ? Int32(ticks) : Int64(ticks)
end

function _toparquetdatetime(value::Dates.DateTime)
    ticks = try
        Base.checked_sub(Dates.value(value), _PARQUET_DATETIME_EPOCH)
    catch err
        err isa OverflowError || rethrow()
        throw(ArgumentError("TIMESTAMP value $(repr(value)) is outside the Int64 range"))
    end
    return ticks
end

# A timestamp of type `T` from its stored count of `T`'s unit since the Unix epoch (in
# UTC for a zoned type). Every Int64 count is a valid value.
function _timestampvalue(::Type{Durations.Timestamp{P}}, ticks::Int64) where {P}
    return Durations.Timestamp{P}(Dates.UTInstant(P(ticks)))
end

function _timestampvalue(::Type{Durations.ZonedTimestamp{P,Z}}, ticks::Int64) where {P,Z}
    return Durations.ZonedTimestamp{P,Z}(_timestampvalue(Durations.Timestamp{P}, ticks),
        Dates.UTC)
end

const _INT96_UNIX_EPOCH_DAY = 2_440_588
const _INT96_RESOLUTIONS = Union{Dates.Nanosecond,Dates.Microsecond,Dates.Millisecond}

# INT96 (Impala, Hive, Spark) stores 8 little-endian bytes of nanoseconds within the
# day, then a 4-byte signed Julian day number. It reads as a local `Timestamp{P}`, as
# pyarrow reads it. Microseconds follow Spark, whose own decoding wraps in Int64: Spark
# stores dates past year 294247 by that wraparound, and this recovers them.
function _fromint96(value, element::Metadata.SchemaElement,
        ::Type{P}=Dates.Nanosecond) where {P<:_INT96_RESOLUTIONS}
    value isa AbstractVector{UInt8} && length(value) == 12 || throw(FormatError(
        "INT96 column $(repr(element.name)) contains a value that is not 12 bytes"))
    nanoseconds = Int64(0)
    for index in 8:-1:1
        nanoseconds = (nanoseconds << 8) | Int64(value[index])
    end
    raw = UInt32(0)
    for index in 12:-1:9
        raw = (raw << 8) | UInt32(value[index])
    end
    days = Int64(reinterpret(Int32, raw)) - _INT96_UNIX_EPOCH_DAY
    sparkmicros = days * 86_400_000_000 + div(nanoseconds, 1000)
    if P === Dates.Nanosecond
        ticks = Int128(days) * _NANOS_PER_DAY + nanoseconds
        typemin(Int64) <= ticks <= typemax(Int64) || throw(UnsupportedFeatureError(
            "INT96 column $(repr(element.name)) holds $(_int96date(sparkmicros)), " *
            "outside the range of Timestamp{Nanosecond} (1677-09-21 to 2262-04-11); " *
            "read INT96 columns with int96=Dates.Microsecond"))
        return Durations.Timestamp{P}(Dates.UTInstant(P(Int64(ticks))))
    end
    scale = Dates.tons(oneunit(P)) ÷ 1000
    finer = rem(nanoseconds, 1000) != 0 ? Dates.Nanosecond : Dates.Microsecond
    finer === Dates.Microsecond && rem(sparkmicros, scale) == 0 ||
        throw(UnsupportedFeatureError("INT96 column $(repr(element.name)) holds a " *
            "timestamp finer than $(nameof(P)); read INT96 columns with " *
            "int96=Dates.$(nameof(finer))"))
    return Durations.Timestamp{P}(Dates.UTInstant(P(div(sparkmicros, scale))))
end

function _int96date(micros::Int64)
    days = fld(micros, Int64(86_400_000_000))
    return string(Dates.Date(Dates.UTD(days + _PARQUET_DATE_EPOCH)))
end

function _fromparquettimestamp(kind::_TimestampLogicalKind, value,
    element::Metadata.SchemaElement)
    value isa Int64 ||
        throw(FormatError("TIMESTAMP column $(repr(element.name)) contains a non-Int64 value"))
    return _timestampvalue(_temporaljuliatype(kind), value)
end

function _toparquettimestamp(kind::_TimestampLogicalKind, value,
    element::Metadata.SchemaElement)
    if value isa Union{Durations.Timestamp,Durations.ZonedTimestamp}
        zoned = value isa Durations.ZonedTimestamp
        zoned == kind.is_adjusted_to_utc || throw(ArgumentError(
            "TIMESTAMP column $(repr(element.name)) is " *
            (kind.is_adjusted_to_utc ? "" : "not ") * "adjusted to UTC; it holds " *
            (kind.is_adjusted_to_utc ? "ZonedTimestamp" : "Timestamp") * " values"))
        return _timestampticks(value, kind.unit, element)
    end
    value isa Dates.DateTime && kind.unit == _TEMPORAL_MILLIS && !kind.is_adjusted_to_utc &&
        return _toparquetdatetime(value)
    throw(ArgumentError("TIMESTAMP column $(repr(element.name)) contains a " *
        "$(typeof(value)) value, not a " *
        (kind.is_adjusted_to_utc ? "Durations.ZonedTimestamp" : "Durations.Timestamp")))
end

_timestampscale(::Durations.Timestamp{P}) where {P} = Int64(Dates.tons(oneunit(P)))
_timestampscale(value::Durations.ZonedTimestamp) = _timestampscale(value.utc)

# The count of `unit` ticks for a Durations timestamp, converted exactly.
function _timestampticks(value, unit::UInt8, element::Metadata.SchemaElement)
    count = Int64(Dates.value(value))
    _, target = _timeparameters(unit)
    scale = _timestampscale(value)
    if scale >= target
        ticks, overflow = Base.mul_with_overflow(count, div(scale, target))
        overflow && throw(ArgumentError("TIMESTAMP column $(repr(element.name)) value " *
            "$value overflows a $(_temporalunitname(unit)) count"))
        return ticks
    end
    ticks, remainder = divrem(count, div(target, scale))
    iszero(remainder) || throw(ArgumentError("TIMESTAMP column " *
        "$(repr(element.name)) loses precision at $(_temporalunitname(unit)) resolution"))
    return ticks
end

function _checkednarrow(::Type{T}, value::Integer, element::Metadata.SchemaElement,
    annotation::AbstractString) where {T<:Integer}
    typemin(T) <= value <= typemax(T) ||
        throw(FormatError("$annotation column $(repr(element.name)) value $value is out of range"))
    return T(value)
end

function _fromparquetinteger(kind::_IntegerLogicalKind, value,
    element::Metadata.SchemaElement)
    physical = kind.bitwidth == 64 ? Int64 : Int32
    value isa physical ||
        throw(FormatError("INTEGER column $(repr(element.name)) contains a non-$physical value"))
    T = _integerjuliatype(kind)
    kind.signed && return _checkednarrow(T, value, element, "INTEGER")
    kind.bitwidth == 32 && return reinterpret(UInt32, value)
    kind.bitwidth == 64 && return reinterpret(UInt64, value)
    return _checkednarrow(T, value, element, "unsigned INTEGER")
end

function _toparquetinteger(kind::_IntegerLogicalKind, value,
    element::Metadata.SchemaElement)
    T = _integerjuliatype(kind)
    value isa T ||
        throw(ArgumentError("INTEGER column $(repr(element.name)) contains a non-$T value"))
    kind.bitwidth == 64 && kind.signed && return Int64(value)
    kind.bitwidth == 64 && return reinterpret(Int64, value)
    kind.bitwidth == 32 && kind.signed && return Int32(value)
    kind.bitwidth == 32 && return reinterpret(Int32, value)
    return Int32(value)
end

function _temporallogicalvalue(kind::_TimeLogicalKind, element::Metadata.SchemaElement, value)
    ismissing(value) && return missing
    return _fromparquettime(kind, value, element)
end

function _temporallogicalvalue(kind::_TimestampLogicalKind,
    element::Metadata.SchemaElement, value)
    ismissing(value) && return missing
    return _fromparquettimestamp(kind, value, element)
end

function _temporallogicalvalue(kind::_IntegerLogicalKind,
    element::Metadata.SchemaElement, value)
    ismissing(value) && return missing
    return _fromparquetinteger(kind, value, element)
end

function _temporallogicalvalue(element::Metadata.SchemaElement, value)
    kind = _temporallogicalkind(element)
    kind === nothing && return value
    return _temporallogicalvalue(kind, element, value)
end

function _temporallogicalvalue(node::SchemaNode, value)
    return _temporallogicalvalue(node.element, value)
end

function _temporalphysicalvalue(kind::_TimeLogicalKind,
    element::Metadata.SchemaElement, value)
    ismissing(value) && return missing
    return _toparquettime(kind, value, element)
end

function _temporalphysicalvalue(kind::_TimestampLogicalKind,
    element::Metadata.SchemaElement, value)
    ismissing(value) && return missing
    return _toparquettimestamp(kind, value, element)
end

function _temporalphysicalvalue(kind::_IntegerLogicalKind,
    element::Metadata.SchemaElement, value)
    ismissing(value) && return missing
    return _toparquetinteger(kind, value, element)
end

function _temporalphysicalvalue(element::Metadata.SchemaElement, value)
    kind = _temporallogicalkind(element)
    kind === nothing && return value
    return _temporalphysicalvalue(kind, element, value)
end

function _temporalphysicalvalue(node::SchemaNode, value)
    return _temporalphysicalvalue(node.element, value)
end

function _temporallogicalvalues(element::Metadata.SchemaElement,
    values::AbstractVector; limits::Limits=Limits())
    kind = _temporallogicalkind(element)
    kind === nothing && return values
    _checklimit(:container_elements, length(values), limits.max_container_elements)
    output = _convertedvector(_temporaljuliatype(kind), values)
    for (index, value) in enumerate(values)
        output[index] = _temporallogicalvalue(kind, element, value)
    end
    return output
end

function _temporallogicalvalues(node::SchemaNode, values::AbstractVector;
    limits::Limits=Limits())
    return _temporallogicalvalues(node.element, values; limits=limits)
end

function _temporalphysicaltype(kind::_TimeLogicalKind)
    return kind.unit == _TEMPORAL_MILLIS ? Int32 : Int64
end

function _temporalphysicaltype(::_TimestampLogicalKind)
    return Int64
end

function _temporalphysicaltype(kind::_IntegerLogicalKind)
    return kind.bitwidth == 64 ? Int64 : Int32
end

function _temporalphysicalvalues(element::Metadata.SchemaElement,
    values::AbstractVector; limits::Limits=Limits())
    kind = _temporallogicalkind(element)
    kind === nothing && return values
    _checklimit(:container_elements, length(values), limits.max_container_elements)
    output = _convertedvector(_temporalphysicaltype(kind), values)
    for (index, value) in enumerate(values)
        output[index] = _temporalphysicalvalue(kind, element, value)
    end
    return output
end

function _temporalphysicalvalues(node::SchemaNode, values::AbstractVector;
    limits::Limits=Limits())
    return _temporalphysicalvalues(node.element, values; limits=limits)
end
