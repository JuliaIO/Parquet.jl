abstract type _ScalarLogicalColumnSpec end

struct _EnumLogicalColumnSpec <: _ScalarLogicalColumnSpec end

struct _TimeLogicalColumnSpec <: _ScalarLogicalColumnSpec
    unit::UInt8
    adjusted::Bool
end

struct _TimestampLogicalColumnSpec <: _ScalarLogicalColumnSpec
    unit::UInt8
    adjusted::Bool
end

struct _DecimalLogicalColumnSpec <: _ScalarLogicalColumnSpec
    precision::Int32
    scale::Int32
end

"""
    LogicalColumn(values, logical; unit=nothing, adjusted=nothing,
        precision=nothing, scale=nothing)

Attach explicit Parquet scalar logical metadata to a vector used as a Tables.jl
column. Supported logical types are `:enum`, `:time`, `:timestamp`, and
`:decimal`. TIME and TIMESTAMP require `unit` and `adjusted`; DECIMAL requires
`precision` and `scale`. `adjusted` sets Parquet's `isAdjustedToUTC` flag.
"""
struct LogicalColumn{T,V<:AbstractVector,S<:_ScalarLogicalColumnSpec} <: AbstractVector{T}
    values::V
    spec::S
end

function _requirelogicalcolumnunset(kind::Symbol, option::Symbol, value)
    value === nothing && return
    throw(ArgumentError("$option is not valid for a $kind logical column"))
end

function _logicalcolumnunit(unit)
    unit isa Symbol || throw(ArgumentError(
        "logical column unit must be :millis, :micros, or :nanos"))
    unit === :millis && return _TEMPORAL_MILLIS
    unit === :micros && return _TEMPORAL_MICROS
    unit === :nanos && return _TEMPORAL_NANOS
    throw(ArgumentError("logical column unit must be :millis, :micros, or :nanos"))
end

function _logicalcolumnadjusted(adjusted)
    adjusted isa Bool || throw(ArgumentError(
        "TIME and TIMESTAMP logical columns require adjusted=true or adjusted=false"))
    return adjusted
end

function _logicalcolumnint32(value, option::Symbol)
    value isa Integer && !(value isa Bool) || throw(ArgumentError(
        "$option must be an integer"))
    typemin(Int32) <= value <= typemax(Int32) || throw(ArgumentError(
        "$option must fit in Int32"))
    return Int32(value)
end

function _logicalcolumnspec(logical, unit, adjusted, precision, scale)
    logical isa Symbol || throw(ArgumentError(
        "logical column type must be :enum, :time, :timestamp, or :decimal"))
    if logical === :enum
        _requirelogicalcolumnunset(logical, :unit, unit)
        _requirelogicalcolumnunset(logical, :adjusted, adjusted)
        _requirelogicalcolumnunset(logical, :precision, precision)
        _requirelogicalcolumnunset(logical, :scale, scale)
        return _EnumLogicalColumnSpec()
    elseif logical === :time
        _requirelogicalcolumnunset(logical, :precision, precision)
        _requirelogicalcolumnunset(logical, :scale, scale)
        return _TimeLogicalColumnSpec(_logicalcolumnunit(unit),
            _logicalcolumnadjusted(adjusted))
    elseif logical === :timestamp
        _requirelogicalcolumnunset(logical, :precision, precision)
        _requirelogicalcolumnunset(logical, :scale, scale)
        return _TimestampLogicalColumnSpec(_logicalcolumnunit(unit),
            _logicalcolumnadjusted(adjusted))
    elseif logical === :decimal
        _requirelogicalcolumnunset(logical, :unit, unit)
        _requirelogicalcolumnunset(logical, :adjusted, adjusted)
        decimalprecision = _logicalcolumnint32(precision, :precision)
        decimalscale = _logicalcolumnint32(scale, :scale)
        decimalprecision > 0 || throw(ArgumentError(
            "DECIMAL precision must be positive"))
        decimalprecision <= _DECIMAL_MAX_PRECISION || throw(ArgumentError(
            "DECIMAL precision must not exceed $_DECIMAL_MAX_PRECISION"))
        0 <= decimalscale <= decimalprecision || throw(ArgumentError(
            "DECIMAL scale must be between zero and precision"))
        return _DecimalLogicalColumnSpec(decimalprecision, decimalscale)
    end
    throw(ArgumentError(
        "logical column type must be :enum, :time, :timestamp, or :decimal"))
end

function _logicalcolumncanonicaltype(::_EnumLogicalColumnSpec)
    return String
end

function _logicalcolumncanonicaltype(::_TimeLogicalColumnSpec)
    return Dates.Time
end

function _logicalcolumncanonicaltype(spec::_TimestampLogicalColumnSpec)
    if spec.unit == _TEMPORAL_MILLIS
        spec.adjusted || return Dates.DateTime
        return Timestamp{:millis}
    end
    spec.unit == _TEMPORAL_MICROS && return Timestamp{:micros}
    return Timestamp{:nanos}
end

function _logicalcolumncanonicaltype(spec::_DecimalLogicalColumnSpec)
    return _decimaltype(spec.precision, spec.scale)
end

function _validatelogicalcolumnvaluetype(::_EnumLogicalColumnSpec, value_type::Type)
    value_type <: AbstractString && return
    throw(ArgumentError("ENUM logical columns require string values or missing"))
end

function _validatelogicalcolumnvaluetype(::_TimeLogicalColumnSpec, value_type::Type)
    value_type == Dates.Time && return
    throw(ArgumentError("TIME logical columns require Dates.Time values or missing"))
end

function _validatelogicalcolumnvaluetype(spec::_TimestampLogicalColumnSpec,
    value_type::Type)
    expected = _logicalcolumncanonicaltype(spec)
    value_type == expected && return
    throw(ArgumentError("$(_temporalunitname(spec.unit)) TIMESTAMP logical columns " *
        "require $expected values or missing"))
end

function _validatelogicalcolumnvaluetype(spec::_DecimalLogicalColumnSpec,
    value_type::Type)
    value_type <: Decimal && isconcretetype(value_type) || throw(ArgumentError(
        "DECIMAL logical columns require concrete Decimals.Decimal values or missing"))
    Decimals.scale(value_type) == spec.scale || throw(ArgumentError(
        "DECIMAL logical column requires scale $(spec.scale), got " *
        "$(Decimals.scale(value_type))"))
    return
end

function _logicalcolumneltype(values::AbstractVector, spec::_ScalarLogicalColumnSpec)
    source_type = eltype(values)
    source_type == Any && throw(ArgumentError(
        "logical column vectors must have a concrete logical element type"))
    source_type == Union{} && throw(ArgumentError(
        "logical column vectors must have a logical element type"))
    source_type == Missing &&
        return Union{Missing,_logicalcolumncanonicaltype(spec)}
    value_type = Base.nonmissingtype(source_type)
    _validatelogicalcolumnvaluetype(spec, value_type)
    return source_type
end

function LogicalColumn(values, logical; unit=nothing, adjusted=nothing,
    precision=nothing, scale=nothing)
    values isa AbstractVector || throw(ArgumentError(
        "LogicalColumn values must be an AbstractVector"))
    spec = _logicalcolumnspec(logical, unit, adjusted, precision, scale)
    T = _logicalcolumneltype(values, spec)
    return LogicalColumn{T,typeof(values),typeof(spec)}(values, spec)
end

function Base.IndexStyle(::Type{<:LogicalColumn{T,V}}) where {T,V}
    return IndexStyle(V)
end

function Base.size(column::LogicalColumn)
    return size(column.values)
end

function Base.axes(column::LogicalColumn)
    return axes(column.values)
end

function Base.length(column::LogicalColumn)
    return length(column.values)
end

function Base.getindex(column::LogicalColumn, index::Int)
    return column.values[index]
end

function Base.setindex!(column::LogicalColumn, value, index::Int)
    column.values[index] = value
    return value
end

function Base.parent(column::LogicalColumn)
    return column.values
end

function Base.copy(column::LogicalColumn{T,V,S}) where {T,V,S}
    values = copy(column.values)
    return LogicalColumn{T,typeof(values),S}(values, column.spec)
end

function _logicalcolumnwriteelement(name, ::_EnumLogicalColumnSpec, optional::Bool,
    ::Limits)
    logical = Metadata.LogicalType(ENUM=Metadata.EnumType())
    return _logicalwriteelement(name, Metadata.Type.BYTE_ARRAY, optional;
        logical=logical, converted=Metadata.ConvertedType.ENUM)
end

function _logicalcolumnwriteelement(name, spec::_TimeLogicalColumnSpec,
    optional::Bool, ::Limits)
    kind = _TimeLogicalKind(spec.unit, spec.adjusted)
    logical = Metadata.LogicalType(TIME=Metadata.TimeType(
        isAdjustedToUTC=spec.adjusted, unit=_canonicaltimeunit(spec.unit)))
    physical = spec.unit == _TEMPORAL_MILLIS ? Metadata.Type.INT32 : Metadata.Type.INT64
    return _logicalwriteelement(name, physical, optional; logical=logical,
        converted=_canonicalconverted(kind))
end

function _logicalcolumnwriteelement(name, spec::_TimestampLogicalColumnSpec,
    optional::Bool, ::Limits)
    kind = _TimestampLogicalKind(spec.unit, spec.adjusted)
    logical = Metadata.LogicalType(TIMESTAMP=Metadata.TimestampType(
        isAdjustedToUTC=spec.adjusted, unit=_canonicaltimeunit(spec.unit)))
    return _logicalwriteelement(name, Metadata.Type.INT64, optional; logical=logical,
        converted=_canonicalconverted(kind))
end

function _logicalcolumnwriteelement(name, spec::_DecimalLogicalColumnSpec,
    optional::Bool, limits::Limits)
    if spec.precision <= 9
        physical = Metadata.Type.INT32
        width = nothing
    elseif spec.precision <= 18
        physical = Metadata.Type.INT64
        width = nothing
    else
        physical = Metadata.Type.FIXED_LEN_BYTE_ARRAY
        width = _decimalwritewidth(spec.precision, limits)
        _checklimit(:decimal_bytes, width, limits.max_decimal_bytes)
    end
    logical = Metadata.LogicalType(DECIMAL=Metadata.DecimalType(
        scale=spec.scale, precision=spec.precision))
    return _logicalwriteelement(name, physical, optional; width=width,
        logical=logical, converted=Metadata.ConvertedType.DECIMAL,
        scale=spec.scale, precision=spec.precision)
end

function _writecolumn(name, column::LogicalColumn, limits::Limits)
    optional = Missing <: eltype(column)
    element = _logicalcolumnwriteelement(name, column.spec, optional, limits)
    return _writescalarlogicalcolumn(name, column, element, limits)
end
