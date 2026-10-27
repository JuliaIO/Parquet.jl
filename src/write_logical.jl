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

function _canonicaltemporallogical(element::Metadata.SchemaElement,
    kind::_TimeLogicalKind)
    logical = element.logicalType
    annotation = logical === nothing ? Metadata.TimeType(
        isAdjustedToUTC=kind.is_adjusted_to_utc,
        unit=_canonicaltimeunit(kind.unit)) : logical.TIME
    return Metadata.LogicalType(TIME=annotation), _canonicalconverted(kind)
end

function _canonicaltemporallogical(element::Metadata.SchemaElement,
    kind::_TimestampLogicalKind)
    logical = element.logicalType
    annotation = logical === nothing ? Metadata.TimestampType(
        isAdjustedToUTC=kind.is_adjusted_to_utc,
        unit=_canonicaltimeunit(kind.unit)) : logical.TIMESTAMP
    return Metadata.LogicalType(TIMESTAMP=annotation), _canonicalconverted(kind)
end

function _canonicaltemporallogical(element::Metadata.SchemaElement,
    kind::_IntegerLogicalKind)
    logical = element.logicalType
    annotation = logical === nothing ? Metadata.IntType(
        bitWidth=Int8(kind.bitwidth), isSigned=kind.signed) : logical.INTEGER
    return Metadata.LogicalType(INTEGER=annotation), _canonicalconverted(kind)
end

function _canonicalbinarylogical(element::Metadata.SchemaElement, kind::Symbol)
    logical = element.logicalType
    if kind === :enum
        annotation = logical === nothing ? Metadata.EnumType() : logical.ENUM
        return Metadata.LogicalType(ENUM=annotation), Metadata.ConvertedType.ENUM
    elseif kind === :json
        annotation = logical === nothing ? Metadata.JsonType() : logical.JSON
        return Metadata.LogicalType(JSON=annotation), Metadata.ConvertedType.JSON
    elseif kind === :bson
        annotation = logical === nothing ? Metadata.BsonType() : logical.BSON
        return Metadata.LogicalType(BSON=annotation), Metadata.ConvertedType.BSON
    elseif kind === :uuid
        return Metadata.LogicalType(UUID=logical.UUID), nothing
    elseif kind === :float16
        return Metadata.LogicalType(FLOAT16=logical.FLOAT16), nothing
    elseif kind === :unknown
        return Metadata.LogicalType(UNKNOWN=logical.UNKNOWN), nothing
    elseif kind === :interval
        return nothing, Metadata.ConvertedType.INTERVAL
    end
    throw(ArgumentError("unsupported binary logical kind $kind"))
end

function _canonicalannotation(element::Metadata.SchemaElement, kind)
    if kind === :string
        annotation = element.logicalType === nothing ? Metadata.StringType() :
            element.logicalType.STRING
        return Metadata.LogicalType(STRING=annotation), Metadata.ConvertedType.UTF8,
            nothing, nothing
    elseif kind === :date
        annotation = element.logicalType === nothing ? Metadata.DateType() :
            element.logicalType.DATE
        return Metadata.LogicalType(DATE=annotation), Metadata.ConvertedType.DATE,
            nothing, nothing
    elseif kind === :decimal
        precision, scale = something(_decimalparameters(element))
        annotation = element.logicalType === nothing ? Metadata.DecimalType(
            scale=scale, precision=precision) : element.logicalType.DECIMAL
        return Metadata.LogicalType(DECIMAL=annotation),
            Metadata.ConvertedType.DECIMAL, scale, precision
    elseif kind isa Union{_TimeLogicalKind,_TimestampLogicalKind,_IntegerLogicalKind}
        logical, converted = _canonicaltemporallogical(element, kind)
        return logical, converted, nothing, nothing
    end
    logical, converted = _canonicalbinarylogical(element, kind)
    return logical, converted, nothing, nothing
end

function _canonicalwriteelement(element::Metadata.SchemaElement)
    kind = _logicalkind(element)
    kind === nothing && return element
    logical, converted, scale, precision = _canonicalannotation(element, kind)
    return Metadata.SchemaElement(
        type_=element.type_,
        type_length=element.type_length,
        repetition_type=element.repetition_type,
        name=element.name,
        num_children=element.num_children,
        converted_type=converted,
        scale=scale,
        precision=precision,
        field_id=element.field_id,
        logicalType=logical,
        unknown_fields=element.unknown_fields,
    )
end

function _writescalarlogicalcolumn(name, values::AbstractVector,
    element::Metadata.SchemaElement, limits::Limits)
    element = _canonicalwriteelement(element)
    optional = Missing <: eltype(values)
    physical = _physicalvalues(element, values; limits=limits)
    definition = Int16(optional ? 1 : 0)
    schema = Metadata.SchemaElement[element]
    return WriteColumn(String(name), physical, element.type_, element.type_length,
        optional, element.logicalType, element.converted_type, String[String(name)],
        nothing, nothing, Int16(0), definition, length(values), schema)
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

# Columns whose annotation depends on their values: Parquet.Decimal and
# DataDecimals.DecimalValue (precision and scale).
function _logicalwritecolumn(name, values::AbstractVector, value_type::Type,
    limits::Limits)
    value_type <: Union{Decimal,DataDecimals.AbstractDecimal} || return nothing
    element = _decimalwriteelement(name, values, Missing <: eltype(values), limits)
    return _writescalarlogicalcolumn(name, values, element, limits)
end

function _unknowncolumn(name, values::AbstractVector, limits::Limits)
    element = _logicalwriteelement(name, Metadata.Type.INT32, true;
        logical=Metadata.LogicalType(UNKNOWN=Metadata.NullType()))
    return _writescalarlogicalcolumn(name, values, element, limits)
end
