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

function _binarywriteelement(name, value_type::Type, optional::Bool)
    if value_type == UUIDs.UUID
        return _logicalwriteelement(name, Metadata.Type.FIXED_LEN_BYTE_ARRAY, optional;
            width=Int32(16), logical=Metadata.LogicalType(UUID=Metadata.UUIDType()))
    elseif value_type == Float16
        return _logicalwriteelement(name, Metadata.Type.FIXED_LEN_BYTE_ARRAY, optional;
            width=Int32(2), logical=Metadata.LogicalType(FLOAT16=Metadata.Float16Type()))
    elseif value_type == JSONValue
        return _logicalwriteelement(name, Metadata.Type.BYTE_ARRAY, optional;
            logical=Metadata.LogicalType(JSON=Metadata.JsonType()),
            converted=Metadata.ConvertedType.JSON)
    elseif value_type == BSONValue
        return _logicalwriteelement(name, Metadata.Type.BYTE_ARRAY, optional;
            logical=Metadata.LogicalType(BSON=Metadata.BsonType()),
            converted=Metadata.ConvertedType.BSON)
    elseif value_type == Interval
        return _logicalwriteelement(name, Metadata.Type.FIXED_LEN_BYTE_ARRAY, optional;
            width=Int32(12), converted=Metadata.ConvertedType.INTERVAL)
    end
    return nothing
end

function _integerconverted(::Type{Int8})
    return Metadata.ConvertedType.INT_8
end

function _integerconverted(::Type{Int16})
    return Metadata.ConvertedType.INT_16
end

function _integerconverted(::Type{UInt8})
    return Metadata.ConvertedType.UINT_8
end

function _integerconverted(::Type{UInt16})
    return Metadata.ConvertedType.UINT_16
end

function _integerconverted(::Type{UInt32})
    return Metadata.ConvertedType.UINT_32
end

function _integerconverted(::Type{UInt64})
    return Metadata.ConvertedType.UINT_64
end

function _integerwriteelement(name, value_type::Type, optional::Bool)
    value_type in (Int8, Int16, UInt8, UInt16, UInt32, UInt64) || return nothing
    width = Int8(8 * sizeof(value_type))
    signed = value_type <: Signed
    physical = sizeof(value_type) <= 4 ? Metadata.Type.INT32 : Metadata.Type.INT64
    logical = Metadata.LogicalType(
        INTEGER=Metadata.IntType(bitWidth=width, isSigned=signed))
    return _logicalwriteelement(name, physical, optional; logical=logical,
        converted=_integerconverted(value_type))
end

function _timestampwriteadjustment(values::AbstractVector)
    adjusted = nothing
    for value in values
        ismissing(value) && continue
        value isa Timestamp || throw(ArgumentError(
            "TIMESTAMP columns must contain Timestamp values or missing"))
        if adjusted === nothing
            adjusted = value.is_adjusted_to_utc
        else
            value.is_adjusted_to_utc == adjusted || throw(ArgumentError(
                "all values in a TIMESTAMP column must use the same UTC adjustment"))
        end
    end
    adjusted === nothing && throw(ArgumentError(
        "cannot infer TIMESTAMP UTC adjustment from an empty or all-null column"))
    return adjusted
end

function _timestampwriteunitcode(value_type::Type)
    value_type == Timestamp{:millis} && return _TEMPORAL_MILLIS
    value_type == Timestamp{:micros} && return _TEMPORAL_MICROS
    value_type == Timestamp{:nanos} && return _TEMPORAL_NANOS
    throw(ArgumentError("unsupported TIMESTAMP element type $value_type"))
end

function _timestampwriteunit(value_type::Type)
    return _canonicaltimeunit(_timestampwriteunitcode(value_type))
end

function _temporalwriteelement(name, values::AbstractVector, value_type::Type,
    optional::Bool)
    integer = _integerwriteelement(name, value_type, optional)
    integer === nothing || return integer
    if value_type == Dates.Time
        unit = Metadata.TimeUnit(NANOS=Metadata.NanoSeconds())
        logical = Metadata.LogicalType(
            TIME=Metadata.TimeType(isAdjustedToUTC=false, unit=unit))
        return _logicalwriteelement(name, Metadata.Type.INT64, optional;
            logical=logical)
    elseif value_type == Dates.DateTime
        unit = Metadata.TimeUnit(MILLIS=Metadata.MilliSeconds())
        logical = Metadata.LogicalType(
            TIMESTAMP=Metadata.TimestampType(isAdjustedToUTC=false, unit=unit))
        return _logicalwriteelement(name, Metadata.Type.INT64, optional;
            logical=logical,
            converted=Metadata.ConvertedType.TIMESTAMP_MILLIS)
    elseif value_type <: Timestamp
        adjusted = _timestampwriteadjustment(values)
        unit = _timestampwriteunit(value_type)
        logical = Metadata.LogicalType(
            TIMESTAMP=Metadata.TimestampType(isAdjustedToUTC=adjusted, unit=unit))
        return _logicalwriteelement(name, Metadata.Type.INT64, optional;
            logical=logical,
            converted=_canonicalconverted(
                _TimestampLogicalKind(_timestampwriteunitcode(value_type), adjusted)))
    end
    return nothing
end

# `Decimal{P,S,T}` carries the complete annotation, so an empty or all-null column
# still has a schema.
function _decimalwriteparameters(value_type::Type)
    isconcretetype(value_type) || throw(ArgumentError(
        "DECIMAL columns require a concrete Decimals.Decimal{P,S,T} element type, " *
        "got $value_type"))
    return Int32(Base.precision(value_type)), Int32(Decimals.scale(value_type))
end

function _decimalwritewidth(precision::Int32, limits::Limits)
    width = setprecision(BigFloat, 256) do
        bits = ceil(Int64, BigFloat(precision) * log2(BigFloat(10)) + 1)
        return max(Int64(1), cld(bits, Int64(8)))
    end
    while _fixeddecimalprecision(width) < precision
        width = Base.checked_add(width, Int64(1))
    end
    while width > 1 && _fixeddecimalprecision(width - 1) >= precision
        width -= 1
    end
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    width <= typemax(Int32) || throw(ArgumentError(
        "DECIMAL fixed width exceeds Int32"))
    return Int32(width)
end

function _decimalwriteelement(name, value_type::Type, optional::Bool,
    limits::Limits)
    precision, scale = _decimalwriteparameters(value_type)
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

function _logicalwritecolumn(name, values::AbstractVector, value_type::Type,
    limits::Limits)
    optional = Missing <: eltype(values)
    element = _temporalwriteelement(name, values, value_type, optional)
    element = if element !== nothing
        element
    elseif value_type <: Decimal
        _decimalwriteelement(name, value_type, optional, limits)
    else
        _binarywriteelement(name, value_type, optional)
    end
    element === nothing && return nothing
    return _writescalarlogicalcolumn(name, values, element, limits)
end

function _unknowncolumn(name, values::AbstractVector, limits::Limits)
    element = _logicalwriteelement(name, Metadata.Type.INT32, true;
        logical=Metadata.LogicalType(UNKNOWN=Metadata.NullType()))
    return _writescalarlogicalcolumn(name, values, element, limits)
end
