# A logical type map can be provided during schema construction.
# It contains mapping of a column to a logical type and the converter function to be applied.
# Columns can be indentified either by their actual type or column name (the full path in the schema)
const TLogicalTypeMap = Dict{Union{Int32,Vector{String}},Tuple{DataType,Function}}

# schema and helper methods
mutable struct Schema
    schema::Vector{SchemaElement}
    map_logical_types::TLogicalTypeMap
    graph::Dict{Int,Vector{Int}}
    name_lookup::Dict{Vector{String},Int}
    type_lookup::Dict{Vector{String},Union{DataType,Union}}
    nttype_lookup::Dict{Vector{String},Union{DataType,Union}}

    function Schema(elems::Vector{SchemaElement}, map_logical_types::TLogicalTypeMap=TLogicalTypeMap())
        name_lookup = Dict{Vector{String},Int}()
        graph = Dict{Int,Vector{Int}}(1 => [])
        name_stack = String[]
        nchildren_stack = Int[]
        idx_stack = Vector{Int}()
        base_idx = 1

        for idx in 1:length(elems)
            sch = elems[idx]
            nested_name = [name_stack; sch.name]
            name_lookup[nested_name] = idx

            if !isempty(idx_stack)
                parent_idx = idx_stack[end]
                if haskey(graph, parent_idx)
                    push!(graph[parent_idx], idx)
                else
                    graph[parent_idx] = [idx]
                end
            else
                base_idx != idx && push!(graph[base_idx], idx)
            end

            if !haskey(map_logical_types, nested_name)
                if is_logical_string(sch)
                    map_logical_types[nested_name] = (String, logical_string)
                elseif is_logical_timestamp(sch)
                    map_logical_types[nested_name] = (DateTime, logical_timestamp)
                elseif is_logical_decimal(sch)
                    map_logical_types[nested_name] = map_logical_decimal(sch.precision, sch.scale)
                end
            end

            if (idx > 1) && (num_children(sch) > 0)
                push!(nchildren_stack, sch.num_children)
                push!(name_stack, sch.name)
                push!(idx_stack, idx)
            elseif !isempty(nchildren_stack)
                while !isempty(nchildren_stack) && nchildren_stack[end] == 1
                    pop!(nchildren_stack)
                    pop!(name_stack)
                    pop!(idx_stack)
                end
                if !isempty(nchildren_stack)
                    nchildren_stack[end] -= 1
                end
            end
        end
        new(elems, map_logical_types, graph, name_lookup, Dict{Vector{String},Union{DataType,Union}}(), Dict{Vector{String},Union{DataType,Union}}())
    end
end

leafname(schname::T) where {T<:AbstractVector{String}} = [schname[end]]

parentname(schname::T) where {T<:AbstractVector{String}} = istoplevel(schname) ? schname : schname[1:(end-1)]

istoplevel(schname::Vector) = length(schname) == 1

elem(sch::Schema, schname::T) where {T<:AbstractVector{String}} = sch.schema[sch.name_lookup[schname]]
elemindex(sch::Schema, schname::T) where {T<:AbstractVector{String}} = sch.name_lookup[schname]

isrepetitiontype(schelem::SchemaElement, repetition_type) = hasproperty(schelem, :repetition_type) && (schelem.repetition_type == repetition_type)

isrequired(sch::Schema, schname::T) where {T<:AbstractVector{String}} = isrequired(elem(sch, schname))
isrequired(schelem::SchemaElement) = isrepetitiontype(schelem, FieldRepetitionType.REQUIRED)

isoptional(sch::Schema, schname::T) where {T<:AbstractVector{String}} = isoptional(elem(sch, schname))
isoptional(schelem::SchemaElement) = isrepetitiontype(schelem, FieldRepetitionType.OPTIONAL)

isrepeated(sch::Schema, schname::T) where {T<:AbstractVector{String}} = isrepeated(elem(sch, schname))
isrepeated(schelem::SchemaElement) = isrepetitiontype(schelem, FieldRepetitionType.REPEATED)

is_logical_string(sch::SchemaElement) = hasproperty(sch, :_type) && (sch._type === _Type.BYTE_ARRAY) && ((hasproperty(sch, :converted_type) && (sch.converted_type === ConvertedType.UTF8)) || (hasproperty(sch, :logicalType) && hasproperty(sch.logicalType, :STRING)))

# converted_type is usually not set for INT96 types, but they are used exclusively used for timestamps only
is_logical_timestamp(sch::SchemaElement) = hasproperty(sch, :_type) && (sch._type === _Type.INT96)

function is_logical_decimal(sch::SchemaElement)
    if hasproperty(sch, :_type)
        if (sch._type === _Type.FIXED_LEN_BYTE_ARRAY) || (sch._type === _Type.INT64)
            if (hasproperty(sch, :converted_type) && (sch.converted_type === ConvertedType.DECIMAL)) || (hasproperty(sch, :logicalType) && hasproperty(sch.logicalType, :DECIMAL))
                return true
            end
        end
    end
    false
end

function path_in_schema(sch::Schema, schelem::SchemaElement)
    for (n, v) in sch.name_lookup
        (sch.schema[v] === schelem) && return n
    end
    error("schema element not found in schema")
end

function logical_converter(sch::Schema, schname::T) where {T<:AbstractVector{String}}
    elem = sch.schema[sch.name_lookup[schname]]

    if schname in keys(sch.map_logical_types)
        _logical_type, converter = sch.map_logical_types[schname]
        return converter
    elseif hasproperty(elem, :_type) && (elem._type in keys(sch.map_logical_types))
        _logical_type, converter = sch.map_logical_types[elem._type]
        return converter
    else
        return identity
    end
end

function logical_convert(sch::Schema, schname::T, val) where {T<:AbstractVector{String}}
    elem = sch.name_lookup[schname]

    if schname in keys(sch.map_logical_types)
        logical_type, converter = sch.map_logical_types[schname]
        converter(val)::logical_type
    elseif hasproperty(elem, :_type) && (elem._type in keys(sch.map_logical_types))
        logical_type, converter = sch.map_logical_types[elem._type]
        converter(val)::logical_type
    else
        val
    end
end

elemtype(sch::Schema, schname::T) where {T<:AbstractVector{String}} =
    get!(sch.type_lookup, schname) do
        elem = sch.schema[sch.name_lookup[schname]]

        if schname in keys(sch.map_logical_types)
            logical_type, _converter = sch.map_logical_types[schname]
            logical_type
        elseif hasproperty(elem, :_type) && (elem._type in keys(sch.map_logical_types))
            logical_type, _converter = sch.map_logical_types[elem._type]
            logical_type
        else
            elemtype(elem)
        end
    end
function elemtype(schelem::SchemaElement)
    jtype = Nothing

    if hasproperty(schelem, :_type)
        jtype = PLAIN_JTYPES[schelem._type+1]
    else
        jtype = Dict{Symbol,Any} # this is a nested type
    end

    if (hasproperty(schelem, :_type) && (schelem._type == _Type.BYTE_ARRAY || schelem._type == _Type.FIXED_LEN_BYTE_ARRAY)) ||
       (hasproperty(schelem, :repetition_type) && (schelem.repetition_type == FieldRepetitionType.REPEATED))  # array type
        jtype = Vector{jtype}
    end

    jtype
end

ntcolstype(sch::Schema, schname::T) where {T<:AbstractVector{String}} =
    get!(sch.nttype_lookup, schname) do
        ntcolstype(sch, sch.name_lookup[schname])
    end
function ntcolstype(sch::Schema, idx::Int)
    child_indices = sch.graph[idx] # will error out in case of idx having no children
    names = [Symbol(x.name) for x in sch.schema[child_indices]]
    types = [(num_children(x) > 0) ? ntelemtype(sch, path_in_schema(sch, x)) : elemtype(sch, path_in_schema(sch, x)) for x in sch.schema[child_indices]]
    optionals = [isoptional(x) for x in sch.schema[child_indices]]
    types = [Vector{opt ? Union{t,Missing} : t} for (t, opt) in zip(types, optionals)]
    NamedTuple{(names...,),Tuple{types...}}
end

function ntcolstype(sch::Schema, schelem::SchemaElement)
    idx = findfirst(x -> x === schelem, sch.schema)
    return ntcolstype(sch, idx)
end

ntelemtype(sch::Schema, schname::T) where {T<:AbstractVector{String}} =
    get!(sch.nttype_lookup, schname) do
        ntelemtype(sch, sch.name_lookup[schname])
    end
function ntelemtype(sch::Schema, idx::Int)
    schelem = sch.schema[idx] # will error out in case of idx having no children
    child_indices = sch.graph[idx]
    names = [Symbol(x.name) for x in sch.schema[child_indices]]
    repeated = hasproperty(schelem, :repetition_type) && (schelem.repetition_type == FieldRepetitionType.REPEATED)
    types = [(num_children(x) > 0) ? ntelemtype(sch, path_in_schema(sch, x)) : elemtype(sch, path_in_schema(sch, x)) for x in sch.schema[child_indices]]
    optionals = [isoptional(x) for x in sch.schema[child_indices]]
    types = [opt ? Union{t,Missing} : t for (t, opt) in zip(types, optionals)]
    T = NamedTuple{(names...,),Tuple{types...}}
    repeated ? Vector{T} : T
end
function ntelemtype(sch::Schema, schelem::SchemaElement)
    idx = findfirst(x -> x === schelem, sch.schema)
    return ntelemtype(sch, idx)
end

bit_or_byte_length(sch::Schema, schname::Vector{String}) = bit_or_byte_length(elem(sch, schname))
bit_or_byte_length(schelem::SchemaElement) = hasproperty(schelem, :type_length) ? schelem.type_length : 0

num_children(schelem::SchemaElement) = hasproperty(schelem, :num_children) ? schelem.num_children : 0

function max_repetition_level(sch::Schema, schname::T) where {T<:AbstractVector{String}}
    lev = isrepeated(sch, schname) ? 1 : 0
    istoplevel(schname) ? lev : (lev + max_repetition_level(sch, parentname(schname)))
end

function max_definition_level(sch::Schema, schname::T) where {T<:AbstractVector{String}}
    lev = isrequired(sch, schname) ? 0 : 1
    istoplevel(schname) ? lev : (lev + max_definition_level(sch, parentname(schname)))
end

tables_schema(parfile) = tables_schema(schema(parfile))
function tables_schema(sch::Schema)
    cols = Parquet.ntcolstype(sch, sch.schema[1])
    colnames = fieldnames(cols)
    coltypes = eltype.(fieldtypes(cols))
    Tables.Schema(colnames, coltypes)
end

logical_decimal_unscaled_type(precision::Int32) = (precision < 5) ? UInt16 :
                                                  (precision < 10) ? UInt32 :
                                                  (precision < 19) ? UInt64 : UInt128

function map_logical_decimal(precision::Int32, scale::Int32; use_float::Bool=false)
    T = logical_decimal_unscaled_type(precision)
    if scale == 0
        # integral values
        return (signed(T), (bytes) -> logical_decimal_integer(bytes, T))
    elseif use_float
        # use Float64
        return (Float64, (bytes) -> logical_decimal_float64(bytes, T, scale))
    else
        # use Decimal
        return (Decimal, (bytes) -> logical_decimal_scaled(bytes, T, scale))
    end
end
