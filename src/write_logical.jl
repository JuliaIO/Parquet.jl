function _canonicaltimeunit(unit::UInt8)
    unit == _TEMPORAL_MILLIS &&
        return Metadata.TimeUnit(MILLIS=Metadata.MilliSeconds())
    unit == _TEMPORAL_MICROS &&
        return Metadata.TimeUnit(MICROS=Metadata.MicroSeconds())
    unit == _TEMPORAL_NANOS &&
        return Metadata.TimeUnit(NANOS=Metadata.NanoSeconds())
    throw(ArgumentError("unknown temporal unit code $unit"))
end

function _canonicalconverted(kind::_TimeLogicalKind)
    kind.unit == _TEMPORAL_MILLIS && return Metadata.ConvertedType.TIME_MILLIS
    kind.unit == _TEMPORAL_MICROS && return Metadata.ConvertedType.TIME_MICROS
    return nothing
end

function _canonicalconverted(kind::_TimestampLogicalKind)
    kind.unit == _TEMPORAL_MILLIS && return Metadata.ConvertedType.TIMESTAMP_MILLIS
    kind.unit == _TEMPORAL_MICROS && return Metadata.ConvertedType.TIMESTAMP_MICROS
    return nothing
end

function _canonicalconverted(kind::_IntegerLogicalKind)
    if kind.signed
        kind.bitwidth == 8 && return Metadata.ConvertedType.INT_8
        kind.bitwidth == 16 && return Metadata.ConvertedType.INT_16
        kind.bitwidth == 32 && return Metadata.ConvertedType.INT_32
        return Metadata.ConvertedType.INT_64
    end
    kind.bitwidth == 8 && return Metadata.ConvertedType.UINT_8
    kind.bitwidth == 16 && return Metadata.ConvertedType.UINT_16
    kind.bitwidth == 32 && return Metadata.ConvertedType.UINT_32
    return Metadata.ConvertedType.UINT_64
end

function _logicalwriteelement(name, physical::Metadata.Type.T, optional::Bool;
    width=nothing, logical=nothing, converted=nothing, scale=nothing,
    precision=nothing)
    repetition = optional ? Metadata.FieldRepetitionType.OPTIONAL :
        Metadata.FieldRepetitionType.REQUIRED
    return Metadata.SchemaElement(
        type_=physical,
        type_length=width,
        repetition_type=repetition,
        name=String(name),
        converted_type=converted,
        scale=scale,
        precision=precision,
        logicalType=logical,
    )
end

function _decimalwriteparameters(values::AbstractVector)
    D = Base.nonmissingtype(eltype(values))
    D <: DataDecimals.Decimal && return (Int32(Base.precision(D)), Int32(DataDecimals.scale(D)))
    scale = nothing
    precision = 1
    for value in values
        ismissing(value) && continue
        value isa DataDecimals.AbstractDecimal && (value = Decimal(BigInt(DataDecimals.unscaled(value)), DataDecimals.scale(value)))
        value isa Decimal || throw(ArgumentError(
            "DECIMAL columns must contain Decimal values or missing"))
        if scale === nothing
            scale = value.scale
        else
            value.scale == scale || throw(ArgumentError(
                "all values in a DECIMAL column must have the same scale"))
        end
        precision = max(precision, _decimaldigits(value.unscaled))
    end
    # Without values any annotation is exact; DECIMAL(1, 0) is the smallest.
    scale === nothing && (scale = Int32(0))
    precision = max(precision, Int(scale))
    precision <= typemax(Int32) || throw(ArgumentError(
        "DECIMAL precision exceeds Int32"))
    return Int32(precision), scale
end

# The fewest FIXED_LEN_BYTE_ARRAY bytes whose two's complement holds every
# `precision`-digit coefficient.
_decimalbytesfor(precision::Integer) =
    cld(ndigits(big(10)^precision - 1; base=2) + 1, 8)

const _DECIMAL_WIDTHS = ntuple(_decimalbytesfor, 76)

function _decimalwritewidth(precision::Int32, limits::Limits)
    precision >= 1 || throw(ArgumentError("DECIMAL precision must be positive"))
    width = precision <= 76 ? _DECIMAL_WIDTHS[precision] : _decimalbytesfor(precision)
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    width <= typemax(Int32) || throw(ArgumentError(
        "DECIMAL fixed width exceeds Int32"))
    return Int32(width)
end

function _decimalwriteelement(name, values::AbstractVector, optional::Bool,
    limits::Limits)
    precision, scale = _decimalwriteparameters(values)
    if precision <= 9
        physical = Metadata.Type.INT32
        width = nothing
    elseif precision <= 18
        physical = Metadata.Type.INT64
        width = nothing
    else
        physical = Metadata.Type.FIXED_LEN_BYTE_ARRAY
        width = _decimalwritewidth(precision, limits)
    end
    logical = Metadata.LogicalType(
        DECIMAL=Metadata.DecimalType(scale=scale, precision=precision))
    return _logicalwriteelement(name, physical, optional; width=width,
        logical=logical, converted=Metadata.ConvertedType.DECIMAL,
        scale=scale, precision=precision)
end
