# A logical type map can be provided during schema construction.
# It contains mapping of a column to a logical type and the converter function to be applied.
# Columns can be indentified either by their actual type or column name (the full path in the schema)
const TLogicalTypeMap = Dict{Union{Int32,Vector{String}},Tuple{DataType,Function}}
const DEFAULT_LOGICAL_TYPE_MAP = TLogicalTypeMap(
    _Type.INT96 => (DateTime, logical_timestamp),
    _Type.BYTE_ARRAY => (String, logical_string)
)

# schema and helper methods
mutable struct Schema
    schema::Vector{SchemaElement}
    map_logical_types::TLogicalTypeMap
    name_lookup::Dict{Vector{String},SchemaElement}
    type_lookup::Dict{Vector{String},Union{DataType,Union}}
    nttype_lookup::Dict{Vector{String},Union{DataType,Union}}

    function Schema(elems::Vector{SchemaElement}, map_logical_types::TLogicalTypeMap=TLogicalTypeMap())
        name_lookup = Dict{Vector{String},SchemaElement}()
        name_stack = String[]
        nchildren_stack = Int[]

        for idx in 1:length(elems)
            sch = elems[idx]
            nested_name = [name_stack; sch.name]
            name_lookup[nested_name] = sch

            if (idx > 1) && (num_children(sch) > 0)
                push!(nchildren_stack, sch.num_children)
                push!(name_stack, sch.name)
            elseif !isempty(nchildren_stack)
                if nchildren_stack[end] == 1
                    pop!(nchildren_stack)
                    pop!(name_stack)
                else
                    nchildren_stack[end] -= 1
                end
            end
        end
        new(elems, map_logical_types, name_lookup, Dict{Vector{String},Union{DataType,Union}}(), Dict{Vector{String},Union{DataType,Union}}())
    end
end

leafname(schname::Vector{String}) = [schname[end]]

parentname(schname::Vector{String}) = istoplevel(schname) ? schname : schname[1:(end-1)]

istoplevel(schname::Vector) = !(length(schname) > 1)

elem(sch::Schema, schname::Vector{String}) = sch.name_lookup[schname]

isrepetitiontype(schelem::SchemaElement, repetition_type) = Thrift.isfilled(schelem, :repetition_type) && (schelem.repetition_type == repetition_type)

isrequired(sch::Schema, schname::Vector{String}) = isrequired(elem(sch, schname))
isrequired(schelem::SchemaElement) = isrepetitiontype(schelem, FieldRepetitionType.REQUIRED)

isoptional(sch::Schema, schname::Vector{String}) = isoptional(elem(sch, schname))
isoptional(schelem::SchemaElement) = isrepetitiontype(schelem, FieldRepetitionType.OPTIONAL)

isrepeated(sch::Schema, schname::Vector{String}) = isrepeated(elem(sch, schname))
isrepeated(schelem::SchemaElement) = isrepetitiontype(schelem, FieldRepetitionType.REPEATED)

function path_in_schema(sch::Schema, schelem::SchemaElement)
    for (n,v) in sch.name_lookup
        (v === schelem) && return n
    end
    error("schema element not found in schema")
end

function logical_convert(sch::Schema, schname::Vector{String}, val)
    elem = sch.name_lookup[schname]

    if schname in keys(sch.map_logical_types)
        logical_type, converter = sch.map_logical_types[schname]
        converter(val)::logical_type
    elseif isfilled(elem, :_type) && (elem._type in keys(sch.map_logical_types))
        logical_type, converter = sch.map_logical_types[elem._type]
        converter(val)::logical_type
    else
        val
    end
end

elemtype(sch::Schema, schname::Vector{String}) = get!(sch.type_lookup, schname) do
    elem = sch.name_lookup[schname]

    if schname in keys(sch.map_logical_types)
        logical_type, _converter = sch.map_logical_types[schname]
        logical_type
    elseif isfilled(elem, :_type) && (elem._type in keys(sch.map_logical_types))
        logical_type, _converter = sch.map_logical_types[elem._type]
        logical_type
    else
        elemtype(elem)
    end
end
function elemtype(schelem::SchemaElement)
    jtype = Nothing

    if isfilled(schelem, :_type)
        jtype = PLAIN_JTYPES[schelem._type+1]
    else
        jtype = Dict{Symbol,Any} # this is a nested type
    end

    if (isfilled(schelem, :_type) && (schelem._type == _Type.BYTE_ARRAY || schelem._type == _Type.FIXED_LEN_BYTE_ARRAY)) ||
       (isfilled(schelem, :repetition_type) && (schelem.repetition_type == FieldRepetitionType.REPEATED))  # array type
        jtype = Vector{jtype}
    end

    jtype
end

ntelemtype(sch::Schema, schname::Vector{String}) = get!(sch.nttype_lookup, schname) do
    ntelemtype(sch, sch.name_lookup[schname])
end
function ntelemtype(sch::Schema, schelem::SchemaElement)
    @assert num_children(schelem) > 0
    idx = findfirst(x->x===schelem, sch.schema)
    children_range = (idx+1):(idx+schelem.num_children)
    names = [Symbol(x.name) for x in sch.schema[children_range]]
    types = [(num_children(x) > 0) ? ntelemtype(sch, path_in_schema(sch, x)) : elemtype(sch, path_in_schema(sch, x)) for x in sch.schema[children_range]]
    optionals = [isoptional(x) for x in sch.schema[children_range]]
    types = [opt ? Union{t,Missing} : t for (t,opt) in zip(types, optionals)]
    NamedTuple{(names...,),Tuple{types...}}
end

bit_or_byte_length(sch::Schema, schname::Vector{String}) = bit_or_byte_length(elem(sch, schname))
bit_or_byte_length(schelem::SchemaElement) = Thrift.isfilled(schelem, :type_length) ? schelem.type_length : 0

num_children(schelem::SchemaElement) = Thrift.isfilled(schelem, :num_children) ? schelem.num_children : 0

function max_repetition_level(sch::Schema, schname::Vector{String})
    lev = isrepeated(sch, schname) ? 1 : 0
    istoplevel(schname) ? lev : (lev + max_repetition_level(sch, parentname(schname)))
end 

function max_definition_level(sch::Schema, schname::Vector{String})
    lev = isrequired(sch, schname) ? 0 : 1
    istoplevel(schname) ? lev : (lev + max_definition_level(sch, parentname(schname)))
end 
