# Nested writer. The ordinary path infers a Parquet schema from each column's declared
# element type; the schema-bearing path reuses a Parquet.Table's stored schema. Both then
# read every value once and shred it into Dremel repetition and definition levels.

abstract type _NestedWriteShape end

# DECIMAL scale/precision come from the values themselves.
mutable struct _NestedWriteLeafAggregate
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

function _nestedwritesplittype(declared::Type; key::Bool=false, path::String="",
        nullable::Bool=false)
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
        return value_type, nullable && !key
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

function _nestedwriteisscalar(value_type::Type)
    value_type === Missing && return true
    # Abstract scalar types are scalars here, so the concrete-type check names them.
    value_type <: Union{AbstractString,AbstractVector{UInt8},DataDecimals.AbstractDecimal,
        Durations.Timestamp,Durations.ZonedTimestamp,Dates.Period} &&
        return true
    _writemap(value_type) === nothing || return true
    return _writevaluescanned(value_type)
end

function _nestedwriteleafshape(name::String, value_type::Type, optional::Bool;
        explicit=nothing, fixed_width=nothing)
    aggregate = _NestedWriteLeafAggregate(nothing, Int32(1))
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
        elementtype::Type, source, limits::Limits, depth::Int, path::String,
        nullable::Bool)
    element = _nestedwriteshape("element", elementtype, source, limits;
        depth=_nestedwritedepthadd(depth, 2, limits), path="$path.element",
        nullable=nullable)
    return _NestedWriteListShape(name, value_type, optional, element)
end

function _nestedwritemapshape(name::String, value_type::Type, optional::Bool,
        keytype::Type, valuetype::Type, keysource, valuesource, limits::Limits,
        depth::Int, path::String, nullable::Bool)
    childdepth = _nestedwritedepthadd(depth, 2, limits)
    key = _nestedwriteshape("key", keytype, keysource, limits; key=true,
        depth=childdepth, path="$path.key")
    value = _nestedwriteshape("value", valuetype, valuesource, limits;
        depth=childdepth, path="$path.value", nullable=nullable)
    return _NestedWriteMapShape(name, value_type, optional, key, value)
end

function _nestedwritestructshape(name::String, value_type::Type,
        optional::Bool, names::Vector{String}, types, sources, limits::Limits,
        depth::Int, path::String, nullable::Bool)
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
            limits; depth=childdepth, path="$path.$(names[index])", nullable=nullable))
    end
    return _NestedWriteStructShape(name, value_type, optional, names, children)
end

"""
    _nestedwriteshape(name, declared, source, limits; key=false, depth=2, path=name,
        nullable=false)

Return the writer shape for a field of declared type `declared`. `source` is the vector
that holds the field's values when it is a package vector (`StructVector`, `ListVector`,
`MapVector`, `LogicalColumn`, `FixedByteArrayVector`), whose layout then decides the shape;
otherwise `nothing`. `path` names the field in error messages. With `nullable=true` every
field is optional except map keys.
"""
Base.@nospecializeinfer function _nestedwriteshape(name::String, declared::Type, source,
        limits::Limits; key::Bool=false, depth::Int=2, path::String=name,
        nullable::Bool=false)
    Base.@nospecialize declared source
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    value_type, optional = _nestedwritesplittype(declared; key=key, path=path,
        nullable=nullable)
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
        types = Type[_eltypeof(child) for child in source.children]
        return _nestedwritestructshape(name, value_type, optional,
            copy(source.names), types, copy(source.children), limits, depth, path, nullable)
    elseif source isa ListVector
        return _nestedwritelistshape(name, value_type, optional,
            _eltypeof(source.values), source.values, limits, depth, path, nullable)
    elseif source isa MapVector
        return _nestedwritemapshape(name, value_type, optional,
            _eltypeof(source.keys), _eltypeof(source.values), source.keys, source.values,
            limits, depth, path, nullable)
    elseif value_type <: ListValue
        _nestedwritecheckconcrete(value_type, "Parquet list view type")
        return _nestedwritelistshape(name, value_type, optional,
            eltype(value_type), nothing, limits, depth, path, nullable)
    elseif _nestedwriteisscalar(value_type)
        value_type === Missing || _nestedwritecheckconcrete(value_type,
            "Parquet scalar type")
        return _nestedwriteleafshape(name, value_type, optional)
    elseif value_type <: StructValue
        # Rows of one column share their struct's children, which declare its fields.
        row = source isa AbstractVector ? _barrier(findfirst, !ismissing, source) : nothing
        row === nothing && throw(ArgumentError("Parquet field $(repr(path)) holds " *
            "StructValue rows but no present row declares their fields"))
        sample = source[row]
        return _nestedwritestructshape(name, value_type, optional, copy(sample.names),
            Type[_eltypeof(child) for child in sample.children], copy(sample.children),
            limits, depth, path, nullable)
    elseif value_type <: NamedTuple
        _nestedwritecheckconcrete(value_type, "Parquet struct type")
        names = String[String(field) for field in fieldnames(value_type)]
        return _nestedwritestructshape(name, value_type, optional, names,
            fieldtypes(value_type), nothing, limits, depth, path, nullable)
    elseif value_type <: MapValue
        _nestedwritecheckconcrete(value_type, "Parquet map view type")
        pairtype = eltype(value_type)
        pairtype <: Pair || throw(ArgumentError(
            "MapValue has no concrete key and value types"))
        return _nestedwritemapshape(name, value_type, optional,
            pairtype.parameters[1], pairtype.parameters[2], nothing, nothing,
            limits, depth, path, nullable)
    elseif value_type <: AbstractDict
        _nestedwritecheckconcrete(value_type, "Parquet map type")
        return _nestedwritemapshape(name, value_type, optional,
            Base.keytype(value_type), Base.valtype(value_type), nothing,
            nothing, limits, depth, path, nullable)
    elseif value_type <: AbstractVector{<:Pair}
        _nestedwritecheckconcrete(value_type, "Parquet map type")
        entry = eltype(value_type)
        _nestedwritecheckconcrete(entry, "Parquet map entry type")
        return _nestedwritemapshape(name, value_type, optional, fieldtype(entry, 1),
            fieldtype(entry, 2), nothing, nothing, limits, depth, path, nullable)
    elseif value_type <: AbstractVector
        _nestedwritecheckconcrete(value_type, "Parquet list type")
        return _nestedwritelistshape(name, value_type, optional,
            eltype(value_type), nothing, limits, depth, path, nullable)
    elseif value_type <: Tuple
        return _nestedwritelistshape(name, value_type, optional,
            _tupleelementtype(value_type, path), nothing, limits, depth, path, nullable)
    end
    throw(_writeunsupported(value_type, path))
end

# The element type of a vector that `_nestedwriteshape` sees untyped.
_eltypeof(values::AbstractVector) = _barrier(eltype, values)::Type

# Tuples write as lists, so their fields need one element type: types that are equal up
# to missing, also inside nested tuples (a tuple value belongs to the wider tuple type).
function _tupleelementtype(T::Type, path::String)
    fields = fieldtypes(T)
    !isempty(fields) && !Base.isvarargtype(T.parameters[end]) || throw(ArgumentError(
        "Parquet field $(repr(path)) has tuple type $T; tuples write as lists and need " *
        "a fixed, nonzero number of elements"))
    element = Union{}
    for F in fields
        element = _tupleunify(element, F)
        element === nothing && throw(ArgumentError(
            "Parquet field $(repr(path)) has tuple type $T; tuples write as lists and " *
            "need one element type, or use a NamedTuple for a struct"))
    end
    return element
end

function _tupleunify(A::Type, B::Type)
    A === Union{} && return B
    B === Union{} && return A
    A == B && return A
    # Removing Missing from an abstract type such as Any leaves it unchanged.
    S, U = Base.nonmissingtype(A), Base.nonmissingtype(B)
    if (S, U) != (A, B)
        T = _tupleunify(S, U)
        return T === nothing ? nothing : Union{Missing,T}
    end
    A <: Tuple && B <: Tuple && A isa DataType && B isa DataType &&
        !isempty(A.parameters) && !Base.isvarargtype(A.parameters[end]) &&
        !isempty(B.parameters) && !Base.isvarargtype(B.parameters[end]) &&
        fieldcount(A) == fieldcount(B) || return nothing
    fields = map(_tupleunify, fieldtypes(A), fieldtypes(B))
    any(isnothing, fields) && return nothing
    return Tuple{fields...}
end

# Value access shared by the decimal scan and the shredder.
_structfield(value::NamedTuple, ::Val{K}) where {K} = getfield(value, K)
_structfield(value::StructValue, ::Val{K}) where {K} = value.children[K][value.index]
_structfield(value, ::Val) = throw(ArgumentError(
    "Parquet struct fields require NamedTuple or StructValue rows, got $(typeof(value))"))

# A row of a read struct column (NamedTuple rows), read field by field from the column's
# children instead of being built as a row.
struct _StructRef{V<:StructVector}
    column::V
    index::Int
end

# Element `index` of `items` for the shredder.
@inline _element(items, index) = @inbounds items[index]
@inline _element(items::StructVector{<:Union{Missing,NamedTuple}}, index::Int) =
    _StructRef(items, index)

@inline _structfield(ref::_StructRef{<:StructVector{T}}, ::Val{K}) where {T,K} =
    (@inbounds ref.column.children[K])[ref.index]::fieldtype(Base.nonmissingtype(T), K)

_structfieldcount(value::NamedTuple) = nfields(value)
_structfieldcount(ref::_StructRef) = length(ref.column.children)
_structfieldcount(value::StructValue) = length(value.children)
_structfieldcount(value) = -1

# A list value as (items, indices). Tuples write as lists too.
_listentries(value::Union{AbstractVector,Tuple}) = (value, eachindex(value))
_listentries(value) = throw(ArgumentError(
    "Parquet LIST values must be vectors or tuples, got $(typeof(value))"))

_mappairs(value::Union{AbstractDict,AbstractVector{<:Pair}}) = value
_mappairs(value) = throw(ArgumentError("Parquet MAP values must be dictionaries or " *
    "vectors of pairs, got $(typeof(value))"))

function _nestedwriteneedsscan(shape::_NestedWriteLeafShape)
    shape.explicit === nothing || return false
    return _writevaluescanned(shape.value_type)
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
    aggregate.precision = max(aggregate.precision, Int32(digits), decimal.scale)
    return
end

function _nestedwritescan!(shape::_NestedWriteStructShape, value)
    ismissing(value) && return
    for (index, child) in enumerate(shape.children)
        _nestedwriteneedsscan(child) &&
            _nestedwritescan!(child, _structfield(value, Val(index)))
    end
    return
end

function _nestedwritescan!(shape::_NestedWriteListShape, value)
    ismissing(value) && return
    items, indices = _listentries(value)
    for index in indices
        _nestedwritescan!(shape.element, items[index])
    end
    return
end

function _nestedwritescan!(shape::_NestedWriteMapShape, value)
    ismissing(value) && return
    scankey = _nestedwriteneedsscan(shape.key)
    scanvalue = _nestedwriteneedsscan(shape.value)
    for pair in _mappairs(value)
        scankey && _nestedwritescan!(shape.key, pair.first)
        scanvalue && _nestedwritescan!(shape.value, pair.second)
    end
    return
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
    map = _writemap(value_type)
    map === nothing || return _writeelement(map, shape.name, shape.optional)
    value_type <: Union{Decimal,DataDecimals.AbstractDecimal} &&
        return _nestedwritedecimalelement(shape, limits)
    throw(_writeunsupported(value_type, shape.name))
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
        _checklimit(:string_bytes, ncodeunits(value), limits.max_string_bytes)
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
        value isa Durations.Duration || throw(ArgumentError(
            "INTERVAL field $(repr(element.name)) contains a non-Duration value"))
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
        physical == Metadata.Type.INT64 && value isa Dates.Period &&
            return Int64(Dates.value(value))
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

# A leaf whose element is exactly what `_writemap(V)` writes converts with that map;
# others (schema-bearing rewrites) convert by their stored annotation.
function _nestedwriteleafmap(element::Metadata.SchemaElement, value_type::Type)
    isconcretetype(value_type) || return nothing
    map = _writemap(value_type)
    map === nothing && return nothing
    map.physical == element.type_ && map.type_length == element.type_length &&
        map.logical == element.logicalType && map.converted == element.converted_type &&
        map.scale == element.scale && map.precision == element.precision || return nothing
    return map
end

# A field's schema plan bound to the type its values must have at each node, with the
# plan's leaf range and definition levels; `_shredtree` turns it into the typed tree the
# shredder walks.
struct _NestedWriteNode
    plan::_NestedPlan
    value_type::Type
    children::Vector{_NestedWriteNode}
    leaves::UnitRange{Int32}
    absent::UInt16
    present::UInt16
end

function _NestedWriteNode(plan::_NestedPlan, value_type::Type,
        children::Vector{_NestedWriteNode})
    return _NestedWriteNode(plan, value_type, children,
        getfield(plan, :leaf_range), UInt16(getfield(plan, :parent_definition)),
        UInt16(getfield(plan, :present_definition)))
end

## Shredding

# A field's values are shredded in one pass over its rows (each row is read once) into
# level streams and present values per leaf. The walk follows a tree whose node types
# carry the field's structure, so for one column type every step compiles to direct
# calls: leaves index a tuple of typed value vectors, struct fields are type parameters.
# Fields wider than these limits use untyped vectors instead of tuples, and nodes deeper
# than _SHRED_TYPED_DEPTH hold their children untyped, so node types stay small.
const _SHRED_TUPLE_LEAVES = 64
const _SHRED_TUPLE_FIELDS = 32
const _SHRED_TYPED_DEPTH = 8

struct _ShredLeaf{I,V}
    absent::UInt16
    present::UInt16
    name::String
end

struct _ShredField{K,N}
    node::N
end

struct _ShredStruct{T,C}
    absent::UInt16
    present::UInt16
    leaves::UnitRange{Int}
    name::String
    fields::C
end

struct _ShredList{T,E}
    absent::UInt16
    present::UInt16
    repeated::UInt16
    leaves::UnitRange{Int}
    name::String
    element::E
end

# `value` is `nothing` for a map without a value field.
struct _ShredMap{T,K,V}
    absent::UInt16
    present::UInt16
    repeated::UInt16
    leaves::UnitRange{Int}
    name::String
    key::K
    value::V
end

# Level streams of every leaf (indexed at run time by null and empty markers), and the
# present physical values of each leaf with the callables that convert them. `R` is false
# when no leaf repeats, so no repetition levels are kept.
struct _ShredState{R,V,C}
    repetition::Vector{Vector{UInt16}}
    definition::Vector{Vector{UInt16}}
    values::V
    converts::C
end

_ShredState{R}(repetition, definition, values::V, converts::C) where {R,V,C} =
    _ShredState{R,V,C}(repetition, definition, values, converts)

# One leaf's shredded rows.
struct _ShreddedLeaf{V<:AbstractVector}
    repetition::Vector{UInt16}
    definition::Vector{UInt16}
    values::V
end

function _shredtree(node::_NestedWriteNode, base::Int, depth::Int=0)
    plan = node.plan
    leaves = (Int(first(node.leaves)) - base):(Int(last(node.leaves)) - base)
    name = join(_plansource(plan).path, ".")
    typed = depth < _SHRED_TYPED_DEPTH
    T = typed ? node.value_type : Any
    if plan isa _NestedLeafPlan
        return _ShredLeaf{first(leaves),node.value_type}(node.absent, node.present, name)
    elseif plan isa _NestedStructPlan
        fields = Any[_shredfield(index, _shredtree(child, base, depth + 1), typed)
            for (index, child) in enumerate(node.children)]
        packed = typed && length(fields) <= _SHRED_TUPLE_FIELDS ? Tuple(fields) : fields
        return _ShredStruct{T,typeof(packed)}(node.absent, node.present, leaves, name,
            packed)
    elseif plan isa _NestedListPlan
        element = _shredtree(only(node.children), base, depth + 1)
        return _ShredList{T,typed ? typeof(element) : Any}(node.absent, node.present,
            UInt16(plan.repetition_level), leaves, name, element)
    end
    mapplan = plan::_NestedMapPlan
    key = _shredtree(node.children[1], base, depth + 1)
    value = length(node.children) == 2 ? _shredtree(node.children[2], base, depth + 1) :
        nothing
    return _ShredMap{T,typed ? typeof(key) : Any,typed ? typeof(value) : Any}(
        node.absent, node.present, UInt16(mapplan.repetition_level), leaves, name, key,
        value)
end

_shredfield(index::Int, node, typed::Bool) =
    _ShredField{index,typed ? typeof(node) : Any}(node)

@noinline function _shredrequired(name::String)
    throw(ArgumentError("required Parquet field $(repr(name)) is missing"))
end

@noinline function _shredtypeerror(name::String, value, expected::Type)
    throw(ArgumentError("Parquet field $(repr(name)) contains $(typeof(value)); " *
        "expected $expected"))
end

@inline function _shredlevels!(state::_ShredState{R}, leaf::Int, repetition::UInt16,
        definition::UInt16) where {R}
    R && @inbounds push!(state.repetition[leaf], repetition)
    @inbounds push!(state.definition[leaf], definition)
    return
end

# Every leaf below a missing node gets the node's absent level, and every leaf below an
# empty list or map its present level.
function _shredmarker!(state::_ShredState, node, repetition::UInt16, definition::UInt16)
    for leaf in node.leaves
        _shredlevels!(state, leaf, repetition, definition)
    end
    return
end

function _shredabsent!(state::_ShredState, node, repetition::UInt16)
    node.absent == node.present && _shredrequired(node.name)
    _shredmarker!(state, node, repetition, node.absent)
    return
end

@inline function _shred!(state::_ShredState, node::_ShredLeaf{I,V}, value,
        repetition::UInt16) where {I,V}
    if ismissing(value)
        node.absent == node.present && _shredrequired(node.name)
        _shredlevels!(state, I, repetition, node.absent)
    else
        value isa V || _shredtypeerror(node.name, value, V)
        _shredlevels!(state, I, repetition, node.present)
        _appendvalue!(state.values[I], state.converts[I](value))
    end
    return
end

_shredmissing(value) = ismissing(value)
_shredmissing(ref::_StructRef{<:StructVector{T}}) where {T} =
    Missing <: T && !@inbounds(ref.column.validity[ref.index])

_shredrowtype(value) = typeof(value)
_shredrowtype(::_StructRef{<:StructVector{T}}) where {T} = Base.nonmissingtype(T)

function _shred!(state::_ShredState, node::_ShredStruct{T}, value,
        repetition::UInt16) where {T}
    _shredmissing(value) && return _shredabsent!(state, node, repetition)
    _shredrowtype(value) <: T || _shredtypeerror(node.name, value, T)
    count = _structfieldcount(value)
    count == length(node.fields) || throw(ArgumentError("Parquet struct field " *
        "$(repr(node.name)) has $count fields; expected $(length(node.fields))"))
    _shredfields!(state, node.fields, value, repetition)
    return
end

@inline _shredfields!(::_ShredState, ::Tuple{}, value, ::UInt16) = nothing
@inline function _shredfields!(state::_ShredState, fields::Tuple, value,
        repetition::UInt16)
    _shredfield!(state, first(fields), value, repetition)
    _shredfields!(state, Base.tail(fields), value, repetition)
    return
end
function _shredfields!(state::_ShredState, fields::Vector{Any}, value,
        repetition::UInt16)
    for field in fields
        _shredfield!(state, field, value, repetition)
    end
    return
end

@inline _shredfield!(state::_ShredState, field::_ShredField{K}, value,
    repetition::UInt16) where {K} =
    _shred!(state, field.node, _structfield(value, Val(K)), repetition)

# A field of a read struct row comes straight from the child vector its type reads into.
@inline function _shredfield!(state::_ShredState, field::_ShredField{K},
        ref::_StructRef{<:StructVector{T}}, repetition::UInt16) where {K,T}
    child = @inbounds ref.column.children[K]
    C = _columntype(fieldtype(Base.nonmissingtype(T), K))
    if child isa C
        _shred!(state, field.node, _element(child::C, ref.index), repetition)
    else
        _shred!(state, field.node, _structfield(ref, Val(K)), repetition)
    end
    return
end

function _shred!(state::_ShredState, node::_ShredList{T}, value,
        repetition::UInt16) where {T}
    ismissing(value) && return _shredabsent!(state, node, repetition)
    value isa T || _shredtypeerror(node.name, value, T)
    _shredlist!(state, node, value, repetition)
    return
end

# A read list row hands over its column's element vector when that is the vector its
# element type reads into.
function _shredlist!(state::_ShredState, node::_ShredList, row::ListValue{T},
        repetition::UInt16) where {T}
    items = row.values
    C = _columntype(T)
    if items isa C
        _shreditems!(state, node, items::C, row.first:row.last, repetition)
    else
        _shreditems!(state, node, row, eachindex(row), repetition)
    end
    return
end

function _shredlist!(state::_ShredState, node::_ShredList, value, repetition::UInt16)
    items, indices = _listentries(value)
    _shreditems!(state, node, items, indices, repetition)
    return
end

function _shreditems!(state::_ShredState, node::_ShredList, items, indices,
        repetition::UInt16)
    isempty(indices) && return _shredmarker!(state, node, repetition, node.present)
    element = node.element
    repeated = node.repeated
    for index in indices
        _shred!(state, element, _element(items, index), repetition)
        repetition = repeated
    end
    return
end

function _shred!(state::_ShredState, node::_ShredMap{T}, value,
        repetition::UInt16) where {T}
    ismissing(value) && return _shredabsent!(state, node, repetition)
    value isa T || _shredtypeerror(node.name, value, T)
    if isempty(_mappairs(value))
        _shredmarker!(state, node, repetition, node.present)
    else
        _shredentries!(state, node, value, repetition)
    end
    return
end

function _shredentries!(state::_ShredState, node::_ShredMap, value::AbstractDict,
        repetition::UInt16)
    for (key, item) in value
        _shredentry!(state, node, key, item, repetition)
        repetition = node.repeated
    end
    return
end

function _shredentries!(state::_ShredState, node::_ShredMap, row::MapValue{K,V},
        repetition::UInt16) where {K,V}
    keys = row.keys
    values = row.values
    CK = _columntype(K)
    CV = _columntype(V)
    keys isa CK && values isa CV || return _shredpairs!(state, node, row, repetition)
    _shredentries!(state, node, keys::CK, values::CV, row.first:row.last, repetition)
    return
end

function _shredentries!(state::_ShredState, node::_ShredMap, keys::AbstractVector,
        values::AbstractVector, indices::UnitRange{Int}, repetition::UInt16)
    for index in indices
        _shredentry!(state, node, _element(keys, index), _element(values, index),
            repetition)
        repetition = node.repeated
    end
    return
end

_shredentries!(state::_ShredState, node::_ShredMap, value::AbstractVector,
    repetition::UInt16) = _shredpairs!(state, node, value, repetition)

function _shredpairs!(state::_ShredState, node::_ShredMap, value::AbstractVector,
        repetition::UInt16)
    for index in eachindex(value)
        entry = @inbounds value[index]
        _shredentry!(state, node, entry.first, entry.second, repetition)
        repetition = node.repeated
    end
    return
end

@inline function _shredentry!(state::_ShredState, node::_ShredMap, key, item,
        repetition::UInt16)
    ismissing(key) && throw(ArgumentError("Parquet MAP key $(repr(node.name)) is missing"))
    _shred!(state, node.key, key, repetition)
    node.value === nothing || _shred!(state, node.value, item, repetition)
    return
end

function _shredcolumn!(state::_ShredState, tree, column::AbstractVector,
        rows::AbstractUnitRange{Int})
    for index in rows
        _shred!(state, tree, _element(column, index), 0x0000)
    end
    return state
end

## Leaf values

# Converts a value by its leaf's stored annotation (schema-bearing rewrites).
struct _StoredConvert{K}
    kind::K
    element::Metadata.SchemaElement
    limits::Limits
end

(convert::_StoredConvert)(value) =
    _nestedwritephysical(convert.kind, convert.element, value, convert.limits)

# The callable that turns a leaf's present values into physical values, and their type.
function _leafconvert(plan::_NestedLeafPlan, value_type::Type, limits::Limits)
    element = plan.source.element
    # Byte limits and UTF-8 are checked as values reach their pages.
    kind = _logicalkind(element)
    map = _nestedwriteleafmap(element, value_type)
    map === nothing || return map.convert, _writephysicaltype(map)
    kind in (:string, :enum) && value_type <: AbstractString && isconcretetype(value_type) &&
        return identity, value_type
    return _StoredConvert(kind, element, limits), _physicaleltype(element.type_)
end

function _nestedwriteleafnodes!(output::Vector{_NestedWriteNode}, node::_NestedWriteNode)
    if node.plan isa _NestedLeafPlan
        push!(output, node)
    else
        foreach(child -> _nestedwriteleafnodes!(output, child), node.children)
    end
    return output
end

function _shredsetup(node::_NestedWriteNode, limits::Limits)
    leafnodes = _nestedwriteleafnodes!(_NestedWriteNode[], node)
    converts = Any[]
    types = Type[]
    for leaf in leafnodes
        convert, P = _leafconvert(leaf.plan::_NestedLeafPlan, leaf.value_type, limits)
        push!(converts, convert)
        push!(types, P)
    end
    repeated = any(leaf -> leaf.plan.source.max_repetition_level > 0, leafnodes)
    return converts, types, repeated, _shredtree(node, Int(first(node.leaves)) - 1)
end

"""
    _nestedwriterows(semantic, node, column, limits) -> Vector{_ShreddedLeaf}

Shred every value of `column`, the rows of one top-level field bound to `node`, into level
streams and present physical values for the field's leaves, in schema order.
"""
function _nestedwriterows(semantic::_NestedSchemaPlan, node::_NestedWriteNode,
        column::AbstractVector, limits::Limits)
    converts, types, repeated, tree = _shredsetup(node, limits)
    count = length(converts)
    values = Any[sizehint!(P[], length(column)) for P in types]
    levels() = [sizehint!(UInt16[], length(column)) for _ in 1:count]
    typed = count <= _SHRED_TUPLE_LEAVES
    state = _ShredState{repeated}(levels(), levels(), typed ? Tuple(values) : values,
        typed ? Tuple(converts) : converts)
    _barrier(_shredcolumn!, state, tree, column, eachindex(column))
    return _ShreddedLeaf[_ShreddedLeaf(state.repetition[index], state.definition[index],
        state.values[index]) for index in 1:count]
end

# Fields with at most this many leaves shred straight into their leaves' page buffers;
# wider ones shred a row group into level streams first, then encode leaf by leaf.
const _FUSED_LEAVES = 16

"""
    _encodenested(semantic, node, column, specs, options) -> Vector{_EncodedChunk}

Encode the column chunks of one nested field's leaves (`specs`, in schema order) for the
rows `column`.
"""
Base.@nospecializeinfer function _encodenested(semantic::_NestedSchemaPlan,
        node::_NestedWriteNode, column::AbstractVector, rows::UnitRange{Int},
        specs::Vector{_LeafSpec}, options::_ChunkOptions)
    # Only the shredding loop specializes on the column type.
    Base.@nospecialize column
    if length(specs) > _FUSED_LEAVES
        leaves = _barrier(_nestedwriterows, semantic, node, view(column, rows),
            options.limits)::Vector{_ShreddedLeaf}
        return _EncodedChunk[_encodechunk(spec, _LeveledRows(leaf.repetition,
            leaf.definition, leaf.values, length(rows)), options)
            for (spec, leaf) in zip(specs, leaves)]
    end
    converts, _, repeated, tree = _shredsetup(node, options.limits)
    scratches = _nestedscratches(length(specs))
    chunks = Tuple(Any[_ChunkWriter(spec, options, scratch)
        for (spec, scratch) in zip(specs, scratches)])
    state = _ShredState{repeated}(
        Vector{UInt16}[(chunk::_ChunkWriter).repetitions for chunk in chunks],
        Vector{UInt16}[(chunk::_ChunkWriter).definitions for chunk in chunks],
        map(chunk -> chunk.values, chunks), Tuple(converts))
    # The chunks' buffer types and the column's type are known at run time.
    _barrier(_shredpages!, state, tree, column, rows, chunks, options)
    return _EncodedChunk[_barrier(_finishchunk, chunk)::_EncodedChunk for chunk in chunks]
end

# Buffers for the leaves of nested fields, one set per leaf position, reused by the
# fields and row groups a task encodes.
function _nestedscratches(count::Int)
    pool = get!(() -> _EncodeScratch[], task_local_storage(),
        :ParquetNestedScratch)::Vector{_EncodeScratch}
    while length(pool) < count
        push!(pool, _newscratch())
    end
    return pool
end

# A leaf's page ends at a row boundary once it holds `pagerows` rows or its buffers reach
# `pagesize` bytes; `starts[k]` is the first row of leaf k's open page.
function _shredpages!(state::_ShredState, tree, column::AbstractVector,
        rows::UnitRange{Int}, chunks::Tuple, options::_ChunkOptions)
    starts = ones(Int, length(chunks))
    total = length(rows)
    row = 0
    for index in rows
        _shred!(state, tree, _element(column, index), 0x0000)
        row += 1
        _flushpages!(chunks, starts, 1, row, total, options, row == total)
    end
    return
end

@inline _flushpages!(::Tuple{}, ::Vector{Int}, ::Int, ::Int, ::Int, ::_ChunkOptions,
    ::Bool) = nothing
@inline function _flushpages!(chunks::Tuple, starts::Vector{Int}, leaf::Int, row::Int,
        total::Int, options::_ChunkOptions, last::Bool)
    chunk = first(chunks)
    start = @inbounds starts[leaf]
    rows = row - start + 1
    if last || rows >= options.pagerows || _bufferbytes(chunk.values) +
            2 * (length(chunk.definitions) + length(chunk.repetitions)) >= options.pagesize
        _flushpage!(chunk, rows, start - 1)
        start == 1 && _hintoutput!(chunk, total, rows)
        @inbounds starts[leaf] = row + 1
    end
    _flushpages!(Base.tail(chunks), starts, leaf + 1, row, total, options, last)
    return
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

# The values a stored leaf takes back: what it reads as, including the time zone of a
# UTC-adjusted timestamp and the duration unit of an INT64 column that an ARROW:schema
# gives on read.
_provenancevaluetype(::Type{Durations.ZonedTimestamp{P,Z}}) where {P,Z} =
    Durations.ZonedTimestamp{P}
_provenancevaluetype(::Type{Int64}) = Union{Int64,Dates.Second,Dates.Millisecond,
    Dates.Microsecond,Dates.Nanosecond}
_provenancevaluetype(T::Type) = T

function _provenancenode(plan::_NestedPlan)
    isempty(_nestedleafrange(plan)) && throw(ArgumentError(
        "schema-bearing writes do not support zero-leaf group " *
        repr(join(_plansource(plan).path, "."))))
    if plan isa _NestedLeafPlan
        element = plan.source.element
        return _NestedWriteNode(plan,
            _provenancevaluetype(_logicaleltype(element, _physicaleltype(element.type_))),
            _NestedWriteNode[])
    elseif plan isa _NestedStructPlan
        return _NestedWriteNode(plan, Union{NamedTuple,StructValue},
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
