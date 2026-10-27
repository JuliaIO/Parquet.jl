# Nested writer. The ordinary path infers a Parquet schema from each column's declared
# element type; the schema-bearing path reuses a Parquet.Table's stored schema. Both then
# read every value once and shred it into Dremel repetition and definition levels.

abstract type _NestedWriteShape end

# TIMESTAMP UTC adjustment and DECIMAL scale/precision come from the values themselves.
mutable struct _NestedWriteLeafAggregate
    adjusted::Union{Nothing,Bool}
    scale::Union{Nothing,Int32}
    precision::Int32
end

struct _NestedWriteLeafShape <: _NestedWriteShape
    name::String
    value_type::Type
    optional::Bool
    explicit::Union{Nothing,_ScalarLogicalColumnSpec}
    fixed_width::Union{Nothing,Int32}
    aggregate::_NestedWriteLeafAggregate
end

struct _NestedWriteStructShape <: _NestedWriteShape
    name::String
    value_type::Type
    optional::Bool
    names::Vector{String}
    children::Vector{_NestedWriteShape}
end

struct _NestedWriteListShape <: _NestedWriteShape
    name::String
    value_type::Type
    optional::Bool
    element::_NestedWriteShape
end

struct _NestedWriteMapShape <: _NestedWriteShape
    name::String
    value_type::Type
    optional::Bool
    key::_NestedWriteShape
    value::_NestedWriteShape
end

function _nestedwritedepthadd(depth::Int, increment::Int, limits::Limits)
    requested = try
        Base.checked_add(depth, increment)
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:metadata_depth, typemax(Int64),
            limits.max_metadata_depth))
    end
    return requested
end

function _nestedwritesplittype(declared::Type; key::Bool=false, path::String="")
    declared === Any && throw(ArgumentError(
        "Parquet field $(repr(path)) has element type Any; write a column with one " *
        "concrete element type"))
    declared === Union{} && throw(ArgumentError(
        "Parquet field $(repr(path)) has element type Union{}"))
    members = Base.uniontypes(declared)
    if length(members) == 1
        value_type = only(members)
        if value_type === Missing
            key && throw(ArgumentError("Parquet MAP key types cannot include Missing"))
            return Missing, true
        end
        value_type === Nothing && throw(ArgumentError(
            "nothing is not a Parquet null value; use missing"))
        return value_type, false
    end
    length(members) == 2 && Missing in members || throw(ArgumentError(
        "Parquet field $(repr(path)) has element type $declared; Parquet fields " *
        "need one type T or Union{Missing,T}"))
    key && throw(ArgumentError("Parquet MAP key types cannot include Missing"))
    value_type = members[1] === Missing ? members[2] : members[1]
    value_type === Nothing && throw(ArgumentError(
        "nothing is not a Parquet null value; use missing"))
    return value_type, true
end

function _nestedwriteisfixedtuple(::Type{T}) where {T}
    isconcretetype(T) || return false
    T <: Tuple || return false
    count = fieldcount(T)
    count > 0 || return false
    for field in fieldtypes(T)
        field === UInt8 || return false
    end
    return true
end

function _nestedwriteisscalar(value_type::Type)
    value_type === Missing && return true
    value_type in (Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32,
        UInt64, Float16, Float32, Float64, Dates.Date, Dates.Time,
        Dates.DateTime, Decimal, UUIDs.UUID, JSONValue, BSONValue, Interval) &&
        return true
    value_type <: Union{DataDecimals.AbstractDecimal,Durations.Duration} && return true
    value_type <: Timestamp && return true
    value_type <: AbstractString && return true
    value_type <: AbstractVector{UInt8} && return true
    return _nestedwriteisfixedtuple(value_type)
end

function _nestedwriteleafshape(name::String, value_type::Type, optional::Bool;
        explicit=nothing, fixed_width=nothing)
    aggregate = _NestedWriteLeafAggregate(nothing, nothing, Int32(1))
    if value_type <: DataDecimals.Decimal
        aggregate.scale = Int32(DataDecimals.scale(value_type))
        aggregate.precision = Int32(precision(value_type))
    end
    return _NestedWriteLeafShape(name, value_type, optional, explicit,
        fixed_width, aggregate)
end

function _nestedwritecheckconcrete(value_type::Type, label::AbstractString)
    isconcretetype(value_type) && return
    throw(ArgumentError("$label must use a concrete declared type, got $value_type"))
end

function _nestedwritelistshape(name::String, value_type::Type, optional::Bool,
        elementtype::Type, source, limits::Limits, depth::Int, path::String)
    element = _nestedwriteshape("element", elementtype, source, limits;
        depth=_nestedwritedepthadd(depth, 2, limits), path="$path.element")
    return _NestedWriteListShape(name, value_type, optional, element)
end

function _nestedwritemapshape(name::String, value_type::Type, optional::Bool,
        keytype::Type, valuetype::Type, keysource, valuesource, limits::Limits,
        depth::Int, path::String)
    childdepth = _nestedwritedepthadd(depth, 2, limits)
    key = _nestedwriteshape("key", keytype, keysource, limits; key=true,
        depth=childdepth, path="$path.key")
    value = _nestedwriteshape("value", valuetype, valuesource, limits;
        depth=childdepth, path="$path.value")
    return _NestedWriteMapShape(name, value_type, optional, key, value)
end

function _nestedwritestructshape(name::String, value_type::Type,
        optional::Bool, names::Vector{String}, types, sources, limits::Limits,
        depth::Int, path::String)
    isempty(names) && throw(ArgumentError(
        "zero-field structs cannot be written to Parquet"))
    _checklimit(:container_elements, length(names),
        limits.max_container_elements)
    childdepth = _nestedwritedepthadd(depth, 1, limits)
    children = _NestedWriteShape[]
    sizehint!(children, length(names))
    for index in eachindex(names)
        source = sources === nothing ? nothing : sources[index]
        push!(children, _nestedwriteshape(names[index], types[index], source,
            limits; depth=childdepth, path="$path.$(names[index])"))
    end
    return _NestedWriteStructShape(name, value_type, optional, names, children)
end

"""
    _nestedwriteshape(name, declared, source, limits; key=false, depth=2, path=name)

Return the writer shape for a field of declared type `declared`. `source` is the vector
that holds the field's values when it is a package vector (`StructVector`, `ListVector`,
`MapVector`, `LogicalColumn`, `FixedByteArrayVector`), whose layout then decides the shape;
otherwise `nothing`. `path` names the field in error messages.
"""
function _nestedwriteshape(name::String, declared::Type, source,
        limits::Limits; key::Bool=false, depth::Int=2, path::String=name)
    Base.@nospecialize declared source
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    value_type, optional = _nestedwritesplittype(declared; key=key, path=path)
    if source isa LogicalColumn
        key && optional && throw(ArgumentError(
            "Parquet MAP key types cannot include Missing"))
        return _nestedwriteleafshape(name, value_type, optional;
            explicit=source.spec)
    elseif source isa FixedByteArrayVector
        source.width > 0 || throw(ArgumentError(
            "fixed byte-array width must be positive"))
        return _nestedwriteleafshape(name, value_type, optional;
            fixed_width=source.width)
    elseif source isa StructVector
        (source.ranks !== nothing) == optional || throw(ArgumentError(
            "StructVector validity does not match its declared element type"))
        types = Type[eltype(child) for child in source.children]
        return _nestedwritestructshape(name, StructValue, optional,
            copy(source.names), types, copy(source.children), limits, depth, path)
    elseif source isa ListVector
        (source.validity !== nothing) == optional || throw(ArgumentError(
            "ListVector validity does not match its declared element type"))
        return _nestedwritelistshape(name, value_type, optional,
            eltype(source.values), source.values, limits, depth, path)
    elseif source isa MapVector
        (source.validity !== nothing) == optional || throw(ArgumentError(
            "MapVector validity does not match its declared element type"))
        values = source.values
        return _nestedwritemapshape(name, value_type, optional,
            eltype(source.keys), values === nothing ? Missing : eltype(values),
            source.keys, values, limits, depth, path)
    elseif value_type <: ListValue
        _nestedwritecheckconcrete(value_type, "Parquet list view type")
        return _nestedwritelistshape(name, value_type, optional,
            eltype(value_type), nothing, limits, depth, path)
    elseif _nestedwriteisscalar(value_type)
        value_type === Missing || _nestedwritecheckconcrete(value_type,
            "Parquet scalar type")
        return _nestedwriteleafshape(name, value_type, optional)
    elseif value_type <: StructValue
        throw(ArgumentError(
            "StructValue needs its owning StructVector to declare field names and types"))
    elseif value_type <: NamedTuple
        _nestedwritecheckconcrete(value_type, "Parquet struct type")
        names = String[String(field) for field in fieldnames(value_type)]
        return _nestedwritestructshape(name, value_type, optional, names,
            fieldtypes(value_type), nothing, limits, depth, path)
    elseif value_type <: MapValue
        _nestedwritecheckconcrete(value_type, "Parquet map view type")
        pairtype = eltype(value_type)
        pairtype <: Pair || throw(ArgumentError(
            "MapValue has no concrete key and value types"))
        return _nestedwritemapshape(name, value_type, optional,
            pairtype.parameters[1], pairtype.parameters[2], nothing, nothing,
            limits, depth, path)
    elseif value_type <: AbstractDict
        _nestedwritecheckconcrete(value_type, "Parquet map type")
        return _nestedwritemapshape(name, value_type, optional,
            Base.keytype(value_type), Base.valtype(value_type), nothing,
            nothing, limits, depth, path)
    elseif value_type <: AbstractVector
        _nestedwritecheckconcrete(value_type, "Parquet list type")
        return _nestedwritelistshape(name, value_type, optional,
            eltype(value_type), nothing, limits, depth, path)
    end
    throw(ArgumentError("Parquet cannot write field $(repr(path)) of type $value_type"))
end

# Value access shared by the aggregate scan and the shredder. Package views are read
# through their backing vectors directly.
function _nestedwritefield(value::NamedTuple, index::Int)
    return getfield(value, index)
end

function _nestedwritefield(value::StructValue, index::Int)
    return value.children[index][value.index]
end

function _nestedwritefield(value, ::Int)
    throw(ArgumentError(
        "Parquet struct fields require NamedTuple or StructValue rows, got $(typeof(value))"))
end

function _nestedwritefieldcount(value::NamedTuple)
    return nfields(value)
end

function _nestedwritefieldcount(value::StructValue)
    return length(value.children)
end

function _nestedwritefieldcount(value)
    return -1
end

# A list value as (backing vector, indices); package views skip their per-access checks.
function _nestedwriteitems(value::ListValue)
    return value.values, value.first:value.last
end

function _nestedwriteitems(value::AbstractVector)
    return value, eachindex(value)
end

function _nestedwriteitems(value)
    throw(ArgumentError("Parquet LIST values must be vectors, got $(typeof(value))"))
end

function _nestedwritepairs(value::AbstractDict)
    return value
end

function _nestedwritepairs(value)
    throw(ArgumentError(
        "Parquet MAP values must be dictionaries or MapValue views, got $(typeof(value))"))
end

function _nestedwriteneedsscan(shape::_NestedWriteLeafShape)
    shape.explicit === nothing || return false
    return shape.value_type <: Union{Timestamp,Decimal,DataDecimals.AbstractDecimal}
end

function _nestedwriteneedsscan(shape::_NestedWriteStructShape)
    return any(_nestedwriteneedsscan, shape.children)
end

function _nestedwriteneedsscan(shape::_NestedWriteListShape)
    return _nestedwriteneedsscan(shape.element)
end

function _nestedwriteneedsscan(shape::_NestedWriteMapShape)
    return _nestedwriteneedsscan(shape.key) || _nestedwriteneedsscan(shape.value)
end

function _nestedwritescan!(shape::_NestedWriteLeafShape, value)
    ismissing(value) && return
    aggregate = shape.aggregate
    if shape.value_type <: Union{Decimal,DataDecimals.AbstractDecimal}
        decimal = value isa DataDecimals.AbstractDecimal ?
            Decimal(BigInt(DataDecimals.unscaled(value)), DataDecimals.scale(value)) :
            value
        decimal isa Decimal || throw(ArgumentError(
            "DECIMAL field $(repr(shape.name)) contains $(typeof(value))"))
        if aggregate.scale === nothing
            aggregate.scale = decimal.scale
        elseif aggregate.scale != decimal.scale
            throw(ArgumentError("all values in DECIMAL field $(repr(shape.name)) " *
                "must use the same scale"))
        end
        digits = _decimaldigits(decimal.unscaled)
        digits <= typemax(Int32) || throw(ArgumentError(
            "DECIMAL precision exceeds Int32"))
        aggregate.precision = max(aggregate.precision, Int32(digits),
            decimal.scale)
    else
        value isa Timestamp || throw(ArgumentError(
            "TIMESTAMP field $(repr(shape.name)) contains $(typeof(value))"))
        if aggregate.adjusted === nothing
            aggregate.adjusted = value.is_adjusted_to_utc
        elseif aggregate.adjusted != value.is_adjusted_to_utc
            throw(ArgumentError("all values in TIMESTAMP field " *
                "$(repr(shape.name)) must use the same UTC adjustment"))
        end
    end
    return
end

function _nestedwritescan!(shape::_NestedWriteStructShape, value)
    ismissing(value) && return
    for (index, child) in enumerate(shape.children)
        _nestedwriteneedsscan(child) &&
            _nestedwritescan!(child, _nestedwritefield(value, index))
    end
    return
end

function _nestedwritescan!(shape::_NestedWriteListShape, value)
    ismissing(value) && return
    items, indices = _nestedwriteitems(value)
    for index in indices
        _nestedwritescan!(shape.element, items[index])
    end
    return
end

function _nestedwritescan!(shape::_NestedWriteMapShape, value)
    ismissing(value) && return
    scankey = _nestedwriteneedsscan(shape.key)
    scanvalue = _nestedwriteneedsscan(shape.value)
    if value isa MapValue
        for index in value.first:value.last
            scankey && _nestedwritescan!(shape.key, value.keys[index])
            scanvalue && value.values !== nothing &&
                _nestedwritescan!(shape.value, value.values[index])
        end
    else
        for pair in _nestedwritepairs(value)
            scankey && _nestedwritescan!(shape.key, pair.first)
            scanvalue && _nestedwritescan!(shape.value, pair.second)
        end
    end
    return
end

function _nestedwriteintegerlogical(name::String, value_type::Type,
        optional::Bool)
    element = _integerwriteelement(name, value_type, optional)
    element === nothing && throw(ArgumentError(
        "unsupported Parquet integer type $value_type"))
    return element
end

function _nestedwritetimestampelement(shape::_NestedWriteLeafShape)
    value_type = shape.value_type
    adjusted = something(shape.aggregate.adjusted,
        _timestampdefaultadjustment(value_type))
    unit = _timestampwriteunit(value_type)
    logical = Metadata.LogicalType(TIMESTAMP=Metadata.TimestampType(
        isAdjustedToUTC=adjusted, unit=unit))
    return _logicalwriteelement(shape.name, Metadata.Type.INT64, shape.optional;
        logical=logical,
        converted=_canonicalconverted(
            _TimestampLogicalKind(_timestampwriteunitcode(value_type), adjusted)))
end

function _nestedwritedecimalelement(shape::_NestedWriteLeafShape,
        limits::Limits)
    aggregate = shape.aggregate
    precision = aggregate.precision
    scale = something(aggregate.scale, Int32(0))
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
    logical = Metadata.LogicalType(DECIMAL=Metadata.DecimalType(
        scale=scale, precision=precision))
    return _logicalwriteelement(shape.name, physical, shape.optional;
        width=width, logical=logical, converted=Metadata.ConvertedType.DECIMAL,
        scale=scale, precision=precision)
end

function _nestedwriteleafelement(shape::_NestedWriteLeafShape,
        limits::Limits)
    shape.explicit === nothing || return _logicalcolumnwriteelement(shape.name,
        shape.explicit, shape.optional, limits)
    value_type = shape.value_type
    value_type === Missing && return _logicalwriteelement(shape.name,
        Metadata.Type.INT32, true;
        logical=Metadata.LogicalType(UNKNOWN=Metadata.NullType()))
    shape.fixed_width === nothing || return _logicalwriteelement(shape.name,
        Metadata.Type.FIXED_LEN_BYTE_ARRAY, shape.optional;
        width=shape.fixed_width)
    value_type == Dates.Date && return _logicalwriteelement(shape.name,
        Metadata.Type.INT32, shape.optional;
        logical=Metadata.LogicalType(DATE=Metadata.DateType()),
        converted=Metadata.ConvertedType.DATE)
    value_type in (Int8, Int16, UInt8, UInt16, UInt32, UInt64) &&
        return _nestedwriteintegerlogical(shape.name, value_type, shape.optional)
    if value_type == Dates.Time
        unit = Metadata.TimeUnit(NANOS=Metadata.NanoSeconds())
        logical = Metadata.LogicalType(TIME=Metadata.TimeType(
            isAdjustedToUTC=false, unit=unit))
        return _logicalwriteelement(shape.name, Metadata.Type.INT64,
            shape.optional; logical=logical)
    elseif value_type == Dates.DateTime
        unit = Metadata.TimeUnit(MILLIS=Metadata.MilliSeconds())
        logical = Metadata.LogicalType(TIMESTAMP=Metadata.TimestampType(
            isAdjustedToUTC=false, unit=unit))
        return _logicalwriteelement(shape.name, Metadata.Type.INT64,
            shape.optional; logical=logical,
            converted=Metadata.ConvertedType.TIMESTAMP_MILLIS)
    elseif value_type <: Timestamp
        return _nestedwritetimestampelement(shape)
    elseif value_type <: Union{Decimal,DataDecimals.AbstractDecimal}
        return _nestedwritedecimalelement(shape, limits)
    end
    binary = _binarywriteelement(shape.name, value_type, shape.optional)
    binary === nothing || return binary
    physical = _writetype(value_type)
    width = _writetypelength(value_type)
    logical, converted = _stringlogical(value_type)
    return _logicalwriteelement(shape.name, physical, shape.optional;
        width=width, logical=logical, converted=converted)
end

function _nestedwriterepetition(shape::_NestedWriteShape)
    return getfield(shape, :optional) ? Metadata.FieldRepetitionType.OPTIONAL :
        Metadata.FieldRepetitionType.REQUIRED
end

# Appends the SchemaElements of `shape` to `output` in depth-first order.
function _nestedwriteschema!(output::Vector{Metadata.SchemaElement},
        shape::_NestedWriteShape, limits::Limits)
    if shape isa _NestedWriteLeafShape
        push!(output, _nestedwriteleafelement(shape, limits))
    elseif shape isa _NestedWriteStructShape
        length(shape.children) <= typemax(Int32) || throw(ArgumentError(
            "Parquet struct has more than Int32 children"))
        push!(output, Metadata.SchemaElement(
            repetition_type=_nestedwriterepetition(shape), name=shape.name,
            num_children=Int32(length(shape.children))))
        for child in shape.children
            _nestedwriteschema!(output, child, limits)
        end
    elseif shape isa _NestedWriteListShape
        push!(output, Metadata.SchemaElement(
            repetition_type=_nestedwriterepetition(shape), name=shape.name,
            num_children=Int32(1), converted_type=Metadata.ConvertedType.LIST,
            logicalType=Metadata.LogicalType(LIST=Metadata.ListType())))
        push!(output, Metadata.SchemaElement(
            repetition_type=Metadata.FieldRepetitionType.REPEATED,
            name="list", num_children=Int32(1)))
        _nestedwriteschema!(output, shape.element, limits)
    else
        mapshape = shape::_NestedWriteMapShape
        push!(output, Metadata.SchemaElement(
            repetition_type=_nestedwriterepetition(mapshape),
            name=mapshape.name, num_children=Int32(1),
            converted_type=Metadata.ConvertedType.MAP,
            logicalType=Metadata.LogicalType(MAP=Metadata.MapType())))
        push!(output, Metadata.SchemaElement(
            repetition_type=Metadata.FieldRepetitionType.REPEATED,
            name="key_value", num_children=Int32(2)))
        _nestedwriteschema!(output, mapshape.key, limits)
        _nestedwriteschema!(output, mapshape.value, limits)
    end
    _checklimit(:container_elements, length(output),
        limits.max_container_elements)
    return output
end

function _nestedwriteschema(shape::_NestedWriteShape, limits::Limits)
    return _nestedwriteschema!(Metadata.SchemaElement[], shape, limits)
end

function _nestedwritefixedpayload(element::Metadata.SchemaElement, value,
        limits::Limits)
    width = element.type_length
    width === nothing && throw(ArgumentError(
        "fixed byte-array field $(repr(element.name)) has no width"))
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    length(value) == width || throw(ArgumentError(
        "fixed byte-array field $(repr(element.name)) has a value with " *
        "the wrong width"))
    for byte in value
        byte isa UInt8 || throw(ArgumentError(
            "fixed byte-array field $(repr(element.name)) contains a non-byte value"))
    end
    return
end

function _nestedwritedecimalcheck(element::Metadata.SchemaElement, value,
        limits::Limits)
    value isa DataDecimals.AbstractDecimal && (value = Decimal(
        BigInt(DataDecimals.unscaled(value)), DataDecimals.scale(value)))
    value isa Decimal || throw(ArgumentError(
        "DECIMAL field $(repr(element.name)) contains a non-Decimal value"))
    precision, scale = something(_decimalparameters(element))
    value.scale == scale || throw(ArgumentError(
        "DECIMAL field $(repr(element.name)) requires scale $scale, got " *
        "$(value.scale)"))
    _checkdecimalvalue(value.unscaled, precision, element.name, ArgumentError)
    physical = element.type_
    if physical == Metadata.Type.INT32
        typemin(Int32) <= value.unscaled <= typemax(Int32) || throw(ArgumentError(
            "DECIMAL value does not fit in INT32"))
        return
    elseif physical == Metadata.Type.INT64
        typemin(Int64) <= value.unscaled <= typemax(Int64) || throw(ArgumentError(
            "DECIMAL value does not fit in INT64"))
        return
    end
    width = physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY ?
        Int(element.type_length) : _twoscomplementwidth(value.unscaled)
    _checklimit(:decimal_bytes, width, limits.max_decimal_bytes)
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    return
end

function _nestedwritebinarycheck(kind::Symbol,
        element::Metadata.SchemaElement, value, limits::Limits)
    if kind === :enum
        value isa AbstractString || throw(ArgumentError(
            "ENUM field $(repr(element.name)) contains a non-string value"))
        bytes = codeunits(value)
        _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
        isvalid(String, bytes) || throw(ArgumentError(
            "ENUM field $(repr(element.name)) contains invalid UTF-8"))
    elseif kind === :uuid
        value isa UUIDs.UUID || throw(ArgumentError(
            "UUID field $(repr(element.name)) contains a non-UUID value"))
    elseif kind === :float16
        value isa Float16 || throw(ArgumentError(
            "FLOAT16 field $(repr(element.name)) contains a non-Float16 value"))
    elseif kind === :json
        value isa JSONValue || throw(ArgumentError(
            "JSON field $(repr(element.name)) contains an untagged value"))
        _validatejson(value.bytes, limits, ArgumentError)
    elseif kind === :bson
        value isa BSONValue || throw(ArgumentError(
            "BSON field $(repr(element.name)) contains an untagged value"))
        _validatebson(value.bytes, limits, ArgumentError)
    elseif kind === :interval
        value isa Union{Interval,Durations.Duration} || throw(ArgumentError(
            "INTERVAL field $(repr(element.name)) contains a non-Interval value"))
    elseif kind === :unknown
        throw(ArgumentError(
            "UNKNOWN field $(repr(element.name)) can contain only missing"))
    else
        throw(ArgumentError("unsupported binary logical kind $kind"))
    end
    return
end

# Validates one present value against its leaf element and returns its physical value;
# BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values become private byte vectors.
function _nestedwritephysical(kind, element::Metadata.SchemaElement, value,
        limits::Limits)
    if kind isa Union{_TimeLogicalKind,_TimestampLogicalKind,_IntegerLogicalKind}
        return _temporalphysicalvalue(kind, element, value)
    elseif kind === :string
        value isa AbstractString || throw(ArgumentError(
            "STRING field $(repr(element.name)) contains a non-string value"))
        return _toparquetstring(value, limits)
    elseif kind === :date
        value isa Dates.Date || throw(ArgumentError(
            "DATE field $(repr(element.name)) contains a non-Date value"))
        return _toparquetdate(value)
    elseif kind === :decimal
        _nestedwritedecimalcheck(element, value, limits)
    elseif kind isa Symbol
        _nestedwritebinarycheck(kind, element, value, limits)
    else
        physical = element.type_
        if physical == Metadata.Type.BYTE_ARRAY
            value isa AbstractVector{UInt8} || throw(ArgumentError(
                "BYTE_ARRAY field $(repr(element.name)) contains a non-byte-array value"))
            _checklimit(:string_bytes, length(value), limits.max_string_bytes)
            return UInt8[byte for byte in value]
        elseif physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY
            value isa Union{Tuple,AbstractVector} || throw(ArgumentError(
                "fixed byte-array field $(repr(element.name)) contains $(typeof(value))"))
            _nestedwritefixedpayload(element, value, limits)
            return UInt8[byte for byte in value]
        end
        expected = _physicaleltype(physical)
        value isa expected || throw(ArgumentError(
            "physical field $(repr(element.name)) contains $(typeof(value)); " *
            "expected $expected"))
        return value
    end
    physical = _physicalvalue(kind, element, value, limits)
    element.type_ in (Metadata.Type.BYTE_ARRAY,
        Metadata.Type.FIXED_LEN_BYTE_ARRAY) || return physical
    physical isa Vector{UInt8} && physical !== value && return physical
    bytes = physical isa AbstractString ? codeunits(physical) : physical
    return UInt8[byte for byte in bytes]
end

# One leaf's level streams and dense values, in schema leaf order. `K` is the leaf's
# logical kind and `V` the type its present values must have. STRING and ENUM values
# are kept as strings; other BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values are copied.
struct _NestedWriteLeaf{T,K,V}
    repetition::Vector{UInt64}
    definition::Vector{UInt64}
    values::Vector{T}
    element::Metadata.SchemaElement
    kind::K
    limits::Limits
end

function _nestedwriteleaf(plan::_NestedLeafPlan, value_type::Type,
        limits::Limits)
    element = plan.source.element
    element.type_ == Metadata.Type.INT96 && throw(UnsupportedFeatureError(
        "writing INT96 is not supported; write the table's columns (for example " *
        "Tables.columntable(table)) to store $(repr(element.name)) as an INT64 " *
        "nanosecond TIMESTAMP"))
    kind = _logicalkind(element)
    T = kind in (:string, :enum) && value_type <: AbstractString &&
        isconcretetype(value_type) ? value_type : _physicaleltype(element.type_)
    return _NestedWriteLeaf{T,typeof(kind),value_type}(UInt64[], UInt64[], T[],
        element, kind, limits)
end

function _nestedwritephysical(leaf::_NestedWriteLeaf{T}, value) where {T}
    if T <: AbstractString
        bytes = codeunits(value)
        _checklimit(:string_bytes, length(bytes), leaf.limits.max_string_bytes)
        isvalid(String, bytes) || throw(ArgumentError(
            "$(uppercase(String(leaf.kind))) field $(repr(leaf.element.name)) " *
            "contains invalid UTF-8"))
        return value
    end
    return _nestedwritephysical(leaf.kind, leaf.element, value, leaf.limits)
end

function _nestedwritemarker!(leaf::_NestedWriteLeaf, repetition::UInt64,
        definition::UInt64)
    push!(leaf.repetition, repetition)
    push!(leaf.definition, definition)
    return
end

# Shredding node: a compiled plan and the declared type its values must have, with the
# plan's leaf range and definition levels copied into concrete fields for the hot path.
struct _NestedWriteNode
    plan::_NestedPlan
    value_type::Type
    children::Vector{_NestedWriteNode}
    leaves::UnitRange{Int32}
    absent::UInt64
    present::UInt64
end

function _NestedWriteNode(plan::_NestedPlan, value_type::Type,
        children::Vector{_NestedWriteNode})
    return _NestedWriteNode(plan, value_type, children,
        getfield(plan, :leaf_range), UInt64(getfield(plan, :parent_definition)),
        UInt64(getfield(plan, :present_definition)))
end

function _nestedwritemissing!(leaves::Vector{Any}, node::_NestedWriteNode,
        repetition::UInt64)
    node.absent == node.present && _nestedwriterequirederror(node)
    for index in node.leaves
        _nestedwritemarker!(leaves[Int(index)], repetition, node.absent)
    end
    return
end

function _nestedwritepresent!(leaves::Vector{Any}, node::_NestedWriteNode,
        repetition::UInt64)
    for index in node.leaves
        _nestedwritemarker!(leaves[Int(index)], repetition, node.present)
    end
    return
end

function _nestedwriterequirederror(node::_NestedWriteNode)
    throw(ArgumentError("required Parquet field " *
        "$(repr(join(getfield(node.plan, :source).path, "."))) is missing"))
end

function _nestedwritepush!(leaf::_NestedWriteLeaf{T,K,V}, node::_NestedWriteNode,
        value, repetition::UInt64) where {T,K,V}
    if ismissing(value)
        node.absent == node.present && _nestedwriterequirederror(node)
        _nestedwritemarker!(leaf, repetition, node.absent)
        return
    end
    value isa V || throw(ArgumentError(
        "Parquet field $(repr(leaf.element.name)) contains $(typeof(value)); " *
        "expected $V"))
    physical = _nestedwritephysical(leaf, value)
    push!(leaf.repetition, repetition)
    push!(leaf.definition, node.present)
    push!(leaf.values, physical)
    return
end

function _nestedwriteleafof(leaves::Vector{Any}, node::_NestedWriteNode)
    return leaves[Int(first(node.leaves))]
end

function _nestedwriteshred!(leaves::Vector{Any}, node::_NestedWriteNode, value,
        repetition::UInt64)
    plan = node.plan
    if plan isa _NestedLeafPlan
        _nestedwritepush!(_nestedwriteleafof(leaves, node), node, value,
            repetition)
        return
    elseif ismissing(value)
        _nestedwritemissing!(leaves, node, repetition)
        return
    end
    value === nothing && throw(ArgumentError(
        "nothing is not a Parquet null value; use missing"))
    value isa node.value_type || throw(ArgumentError(
        "Parquet field $(repr(join(getfield(plan, :source).path, "."))) contains " *
        "$(typeof(value)); expected $(node.value_type)"))
    if plan isa _NestedStructPlan
        _nestedwritefieldcount(value) == length(node.children) || throw(ArgumentError(
            "Parquet struct field $(repr(join(plan.source.path, "."))) " *
            "has $(_nestedwritefieldcount(value)) fields; expected $(length(node.children))"))
        for (index, child) in enumerate(node.children)
            _nestedwriteshred!(leaves, child, _nestedwritefield(value, index),
                repetition)
        end
    elseif plan isa _NestedListPlan
        items, indices = _nestedwriteitems(value)
        if isempty(indices)
            _nestedwritepresent!(leaves, node, repetition)
            return
        end
        element = only(node.children)
        repeated = UInt64(plan.repetition_level)
        if element.plan isa _NestedLeafPlan
            _nestedwritepushitems!(_nestedwriteleafof(leaves, element), element,
                items, indices, repetition, repeated)
        else
            for index in indices
                _nestedwriteshred!(leaves, element, items[index], repetition)
                repetition = repeated
            end
        end
    else
        _nestedwriteshredmap!(leaves, node, plan::_NestedMapPlan, value,
            repetition)
    end
    return
end

# Consecutive entries of one repeated leaf: the first continues the parent's repetition
# level, the rest repeat at the list's level.
function _nestedwritepushitems!(leaf::_NestedWriteLeaf, node::_NestedWriteNode,
        items, indices, repetition::UInt64, repeated::UInt64)
    for index in indices
        _nestedwritepush!(leaf, node, items[index], repetition)
        repetition = repeated
    end
    return
end

function _nestedwriteshredmap!(leaves::Vector{Any}, node::_NestedWriteNode,
        plan::_NestedMapPlan, value, repetition::UInt64)
    count = value isa MapValue ? value.last - value.first + 1 :
        length(_nestedwritepairs(value))
    if iszero(count)
        _nestedwritepresent!(leaves, node, repetition)
        return
    end
    key = node.children[1]
    item = length(node.children) == 2 ? node.children[2] : nothing
    repeated = UInt64(plan.repetition_level)
    if key.plan isa _NestedLeafPlan && (item === nothing || item.plan isa _NestedLeafPlan)
        # Leaf keys and values take one typed loop.
        keyleaf = _nestedwriteleafof(leaves, key)
        itemleaf = item === nothing ? nothing : _nestedwriteleafof(leaves, item)
        if value isa MapValue
            _nestedwritepushentries!(keyleaf, key, itemleaf, item, value.keys,
                value.values, value.first:value.last, repetition, repeated)
        else
            _nestedwritepushentries!(keyleaf, key, itemleaf, item, value,
                repetition, repeated)
        end
    elseif value isa MapValue
        values = value.values
        for index in value.first:value.last
            _nestedwriteshredentry!(leaves, key, item, value.keys[index],
                values === nothing ? missing : values[index], repetition)
            repetition = repeated
        end
    else
        for pair in value
            _nestedwriteshredentry!(leaves, key, item, pair.first, pair.second,
                repetition)
            repetition = repeated
        end
    end
    return
end

function _nestedwritekeycheck(key, node::_NestedWriteNode)
    ismissing(key) && throw(ArgumentError("Parquet MAP key " *
        "$(repr(join(getfield(node.plan, :source).path, "."))) is missing"))
    return
end

function _nestedwriteshredentry!(leaves::Vector{Any}, key::_NestedWriteNode,
        item, entrykey, entryvalue, repetition::UInt64)
    _nestedwritekeycheck(entrykey, key)
    _nestedwriteshred!(leaves, key, entrykey, repetition)
    item === nothing || _nestedwriteshred!(leaves, item, entryvalue, repetition)
    return
end

function _nestedwritepushentries!(keyleaf::_NestedWriteLeaf, key::_NestedWriteNode,
        itemleaf, item, keys, values, indices, repetition::UInt64,
        repeated::UInt64)
    for index in indices
        entrykey = keys[index]
        _nestedwritekeycheck(entrykey, key)
        _nestedwritepush!(keyleaf, key, entrykey, repetition)
        itemleaf === nothing || _nestedwritepush!(itemleaf, item,
            values === nothing ? missing : values[index], repetition)
        repetition = repeated
    end
    return
end

function _nestedwritepushentries!(keyleaf::_NestedWriteLeaf, key::_NestedWriteNode,
        itemleaf, item, pairs::AbstractDict, repetition::UInt64,
        repeated::UInt64)
    for pair in pairs
        _nestedwritekeycheck(pair.first, key)
        _nestedwritepush!(keyleaf, key, pair.first, repetition)
        itemleaf === nothing || _nestedwritepush!(itemleaf, item, pair.second,
            repetition)
        repetition = repeated
    end
    return
end

# Shreds whole top-level columns. Leaf columns take a typed loop.
function _nestedwriteshredcolumn!(leaves::Vector{Any}, node::_NestedWriteNode,
        column::AbstractVector)
    if node.plan isa _NestedLeafPlan
        leaf = _nestedwriteleafof(leaves, node)
        sizehint!(leaf.repetition, length(column))
        sizehint!(leaf.definition, length(column))
        _nestedwritepushitems!(leaf, node, column, eachindex(column),
            UInt64(0), UInt64(0))
        return
    end
    for value in column
        _nestedwriteshred!(leaves, node, value, UInt64(0))
    end
    return
end

function _nestedwriteleaves(semantic::_NestedSchemaPlan,
        nodes::Vector{_NestedWriteNode}, limits::Limits)
    types = Vector{Type}(undef, length(semantic.leaves))
    collect = function (node)
        if node.plan isa _NestedLeafPlan
            types[Int(first(node.leaves))] = node.value_type
        else
            foreach(collect, node.children)
        end
        return
    end
    foreach(collect, nodes)
    return Any[_nestedwriteleaf(semantic.leaves[index], types[index], limits)
        for index in eachindex(semantic.leaves)]
end

function _nestedwriteshredall(semantic::_NestedSchemaPlan,
        nodes::Vector{_NestedWriteNode}, columns, limits::Limits)
    leaves = _nestedwriteleaves(semantic, nodes, limits)
    for (node, column) in zip(nodes, columns)
        _nestedwriteshredcolumn!(leaves, node, column)
    end
    return leaves
end

function _nestedwritecolumn(leaf::_NestedWriteLeaf, plan::_NestedLeafPlan,
        rows::Int)
    node = plan.source
    element = node.element
    optional = element.repetition_type == Metadata.FieldRepetitionType.OPTIONAL
    return WriteColumn(element.name, leaf.values, element.type_,
        element.type_length, optional, element.logicalType,
        element.converted_type, copy(node.path), leaf.repetition,
        leaf.definition, node.max_repetition_level,
        node.max_definition_level, rows, Metadata.SchemaElement[])
end

# Groups shredded leaves into one field plan per top-level schema child.
function _nestedwritefinishfields(fragments, semantic::_NestedSchemaPlan,
        leaves::Vector{Any}, rows::Int)
    fields = WriteFieldPlan[]
    for (fragment, child) in zip(fragments, semantic.root.children)
        range = _nestedleafrange(child)
        isempty(range) && throw(ArgumentError(
            "zero-leaf fields cannot be written to Parquet"))
        columns = WriteColumn[_nestedwritecolumn(leaves[Int(index)],
            semantic.leaves[Int(index)], rows) for index in range]
        push!(fields, WriteFieldPlan(fragment, columns))
    end
    return fields
end

function _nestedwritebind(shape::_NestedWriteShape, plan::_NestedPlan)
    if shape isa _NestedWriteLeafShape
        return _NestedWriteNode(plan, shape.value_type, _NestedWriteNode[])
    elseif shape isa _NestedWriteStructShape
        structplan = plan::_NestedStructPlan
        children = _NestedWriteNode[_nestedwritebind(child, childplan)
            for (child, childplan) in zip(shape.children, structplan.children)]
        return _NestedWriteNode(plan, shape.value_type, children)
    elseif shape isa _NestedWriteListShape
        listplan = plan::_NestedListPlan
        return _NestedWriteNode(plan, shape.value_type,
            _NestedWriteNode[_nestedwritebind(shape.element, listplan.element)])
    end
    mapshape = shape::_NestedWriteMapShape
    mapplan = plan::_NestedMapPlan
    return _NestedWriteNode(plan, mapshape.value_type, _NestedWriteNode[
        _nestedwritebind(mapshape.key, mapplan.key),
        _nestedwritebind(mapshape.value, something(mapplan.value))])
end

function _nestedwriteinputname(column::Pair)
    return String(first(column))
end

function _nestedwriteinputname(column)
    hasproperty(column, :name) || throw(ArgumentError(
        "nested writer input columns need name and values fields"))
    return String(getproperty(column, :name))
end

function _nestedwriteinputvalues(column::Pair)
    return last(column)
end

function _nestedwriteinputvalues(column)
    hasproperty(column, :values) || throw(ArgumentError(
        "nested writer input columns need name and values fields"))
    return getproperty(column, :values)
end

function _nestedwritevalidatedinputs(input_columns::AbstractVector, rows::Int,
        limits::Limits)
    isempty(input_columns) && throw(ArgumentError(
        "a Parquet table must have at least one column"))
    _checklimit(:container_elements, length(input_columns),
        limits.max_container_elements)
    names = String[]
    values = AbstractVector[]
    seen = Set{String}()
    for column in input_columns
        name = _nestedwriteinputname(column)
        occursin('\0', name) && throw(ArgumentError(
            "Parquet top-level field names cannot contain NUL"))
        name in seen && throw(ArgumentError(
            "Parquet column names must be unique"))
        push!(seen, name)
        columnvalues = _nestedwriteinputvalues(column)
        columnvalues isa AbstractVector || throw(ArgumentError(
            "Parquet columns must be vectors"))
        count = _nestedvectorcount(columnvalues, "Parquet column $(repr(name))")
        count == rows || throw(ArgumentError(
            "Parquet column $(repr(name)) has $count rows; expected $rows"))
        push!(names, name)
        push!(values, columnvalues)
    end
    return names, values
end

function _nestedwritecompile(fragments::Vector{Vector{Metadata.SchemaElement}},
        limits::Limits)
    length(fragments) <= typemax(Int32) || throw(ArgumentError(
        "Parquet schema has more than Int32 top-level fields"))
    elements = Metadata.SchemaElement[Metadata.SchemaElement(
        name="schema", num_children=Int32(length(fragments)))]
    for fragment in fragments
        append!(elements, fragment)
        _checklimit(:container_elements, length(elements),
            limits.max_container_elements)
    end
    schema = Schema(elements; limits=limits)
    return _nestedplan(schema; limits=limits)
end

"""
    _nestedwritefields(input_columns, rows, limits; preflight=nothing)

Infer a schema from each column's declared element type, then shred every value once.
`input_columns` holds `name => values` pairs. `preflight`, when given, is called with the
compiled schema plan before any value is read.
"""
function _nestedwritefields(input_columns::AbstractVector, rows::Integer,
        limits::Limits; preflight=nothing)
    rows >= 0 || throw(ArgumentError("Parquet row count must be nonnegative"))
    rowcount = Int(rows)
    _checklimit(:container_elements, rowcount, limits.max_container_elements)
    names, values = _nestedwritevalidatedinputs(input_columns, rowcount,
        limits)
    shapes = _NestedWriteShape[_nestedwriteshape(names[index],
        eltype(values[index]), values[index], limits)
        for index in eachindex(names, values)]
    for (shape, column) in zip(shapes, values)
        _nestedwriteneedsscan(shape) || continue
        for value in column
            _nestedwritescan!(shape, value)
        end
    end
    fragments = Vector{Metadata.SchemaElement}[_nestedwriteschema(shape, limits)
        for shape in shapes]
    semantic = _nestedwritecompile(fragments, limits)
    preflight === nothing || preflight(semantic)
    nodes = _NestedWriteNode[_nestedwritebind(shape, plan)
        for (shape, plan) in zip(shapes, semantic.root.children)]
    leaves = _nestedwriteshredall(semantic, nodes, values, limits)
    return _nestedwritefinishfields(fragments, semantic, leaves, rowcount)
end

# Schema-bearing writes of a Parquet.Table keep its stored schema elements exactly.
struct _ProvenanceWriteFields <: AbstractVector{WriteFieldPlan}
    fields::Vector{WriteFieldPlan}
    elements::Vector{Metadata.SchemaElement}
    schema::Schema
end

function Base.IndexStyle(::Type{_ProvenanceWriteFields})
    return IndexLinear()
end

function Base.size(fields::_ProvenanceWriteFields)
    return size(fields.fields)
end

function Base.getindex(fields::_ProvenanceWriteFields, index::Int)
    return fields.fields[index]
end

function _provenancenode(plan::_NestedPlan)
    isempty(_nestedleafrange(plan)) && throw(ArgumentError(
        "schema-bearing writes do not support zero-leaf group " *
        repr(join(getfield(plan, :source).path, "."))))
    if plan isa _NestedLeafPlan
        element = plan.source.element
        return _NestedWriteNode(plan,
            _logicaleltype(element, _physicaleltype(element.type_)),
            _NestedWriteNode[])
    elseif plan isa _NestedStructPlan
        return _NestedWriteNode(plan, StructValue,
            _NestedWriteNode[_provenancenode(child) for child in plan.children])
    elseif plan isa _NestedListPlan
        return _NestedWriteNode(plan, ListValue,
            _NestedWriteNode[_provenancenode(plan.element)])
    end
    mapplan = plan::_NestedMapPlan
    children = _NestedWriteNode[_provenancenode(mapplan.key)]
    mapplan.value === nothing || push!(children,
        _provenancenode(something(mapplan.value)))
    return _NestedWriteNode(plan, MapValue, children)
end

function _provenancewritefields(table::Table, limits::Limits, encoding,
        dictionary::Bool)
    rows = table.rows
    rows >= 0 || throw(ArgumentError("Parquet row count must be nonnegative"))
    _checklimit(:container_elements, rows, limits.max_container_elements)
    elements = copy(table.metadata.schema)
    schema = Schema(elements; limits=limits)
    semantic = _nestedplan(schema; limits=limits)
    children = semantic.root.children
    names = Tables.columnnames(table)
    columns = Base.values(table.columns)
    length(columns) == length(children) || throw(ArgumentError(
        "table columns no longer match the stored Parquet schema"))
    for (index, child) in enumerate(children)
        String(names[index]) == child.source.element.name || throw(ArgumentError(
            "table column name or order no longer matches the stored Parquet " *
            "schema at position $index"))
        length(columns[index]) == rows || throw(ArgumentError(
            "table column $(repr(child.source.element.name)) has " *
            "$(length(columns[index])) values for $rows rows"))
    end
    _preflightwriteencoding(semantic, encoding, dictionary)
    nodes = _NestedWriteNode[_provenancenode(child) for child in children]
    leaves = _nestedwriteshredall(semantic, nodes, columns, limits)
    fragments = (Metadata.SchemaElement[] for _ in children)
    fields = _nestedwritefinishfields(fragments, semantic, leaves, rows)
    return _ProvenanceWriteFields(fields, elements, schema), rows
end

function _writeplanleaves(fields::_ProvenanceWriteFields, schema::Schema,
        rows::Int, limits::Limits)
    return _writeplanleaves(fields.fields, schema, rows, limits)
end

function _writeplan(fields::_ProvenanceWriteFields, rows::Int, limits::Limits)
    rows >= 0 || throw(ArgumentError("Parquet row count must be nonnegative"))
    leaves = _writeplanleaves(fields.fields, fields.schema, rows, limits)
    rowgroups = iszero(rows) ? WriteRowGroupPlan[] :
        WriteRowGroupPlan[WriteRowGroupPlan(rows, leaves)]
    return WritePlan(fields.elements, fields.schema, rows, rowgroups)
end
