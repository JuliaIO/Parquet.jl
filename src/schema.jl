
# schema and helper methods
mutable struct Schema
    schema::Vector{SchemaElement}
    name_lookup::Dict{AbstractString,SchemaElement}
    type_lookup::Dict{AbstractString,Union{DataType,Union}}
    nttype_lookup::Dict{AbstractString,Union{DataType,Union}}

    function Schema(elems::Vector{SchemaElement})
        name_lookup = Dict{AbstractString,SchemaElement}()
        name_stack = AbstractString[]
        nchildren_stack = Int[]

        for idx in 1:length(elems)
            sch = elems[idx]
            # TODO: may be better to have fully qualified names here to avoid chashes
            nested_name = (idx == 1) ? sch.name : join([name_stack; sch.name], '.')
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
        new(elems, name_lookup, Dict{AbstractString,Union{DataType,Union}}(), Dict{AbstractString,Union{DataType,Union}}())
    end
end

leafname(schname::AbstractString) = istoplevel(schname) ? schname : leafname(split(schname, '.'))
leafname(schname::Vector) = schname[end]

parentname(schname::Vector) = istoplevel(schname) ? schname : schname[1:(end-1)]
parentname(schname::AbstractString) = join(parentname(split(schname, '.')), '.')

istoplevel(schname::AbstractString) = !('.' in schname)
istoplevel(schname::Vector) = !(length(schname) > 1)

elem(sch::Schema, schname::AbstractString) = sch.name_lookup[schname]
elem(sch::Schema, schname::Vector) = elem(sch, join(schname, '.'))

isrepetitiontype(schelem::SchemaElement, repetition_type) = Thrift.isfilled(schelem, :repetition_type) && (schelem.repetition_type == repetition_type)

isrequired(sch::Schema, schname) = isrequired(elem(sch, schname))
isrequired(schelem::SchemaElement) = isrepetitiontype(schelem, FieldRepetitionType.REQUIRED)

isoptional(sch::Schema, schname) = isoptional(elem(sch, schname))
isoptional(schelem::SchemaElement) = isrepetitiontype(schelem, FieldRepetitionType.OPTIONAL)

isrepeated(sch::Schema, schname) = isrepeated(elem(sch, schname))
isrepeated(schelem::SchemaElement) = isrepetitiontype(schelem, FieldRepetitionType.REPEATED)

elemtype(sch::Schema, schname::AbstractString) = get!(sch.type_lookup, schname) do
    elemtype(sch.name_lookup[schname])
end
elemtype(sch::Schema, schname::Vector) = elemtype(sch, join(schname, '.'))
function elemtype(sch::SchemaElement)
    jtype = Nothing

    if isfilled(sch, :_type)
        jtype = PLAIN_JTYPES[sch._type+1]
    else
        jtype = Dict{Symbol,Any} # this is a nested type
    end

    if (isfilled(sch, :_type) && (sch._type == _Type.BYTE_ARRAY || sch._type == _Type.FIXED_LEN_BYTE_ARRAY)) ||
       (isfilled(sch, :repetition_type) && (sch.repetition_type == FieldRepetitionType.REPEATED))  # array type
        jtype = Vector{jtype}
    end

    jtype
end

ntelemtype(sch::Schema, schname::AbstractString) = get!(sch.nttype_lookup, schname) do
    ntelemtype(sch, sch.name_lookup[schname])
end
ntelemtype(sch::Schema, schname::Vector) = ntelemtype(sch, join(schname, '.'))
function ntelemtype(sch::Schema, schelem::SchemaElement)
    @assert num_children(schelem) > 0
    idx = findfirst(x->x===schelem, sch.schema)
    children_range = (idx+1):(idx+schelem.num_children)
    names = [Symbol(x.name) for x in sch.schema[children_range]]
    types = [(num_children(x) > 0) ? ntelemtype(sch, x) : elemtype(x) for x in sch.schema[children_range]]
    optionals = [isoptional(x) for x in sch.schema[children_range]]
    types = [opt ? Union{t,Missing} : t for (t,opt) in zip(types, optionals)]
    NamedTuple{(names...,),Tuple{types...}}
end

bit_or_byte_length(sch::Schema, schname) = bit_or_byte_length(elem(sch, schname))
bit_or_byte_length(schelem::SchemaElement) = Thrift.isfilled(schelem, :type_length) ? schelem.type_length : 0

num_children(schelem::SchemaElement) = Thrift.isfilled(schelem, :num_children) ? schelem.num_children : 0

function max_repetition_level(sch::Schema, schname)
    lev = isrepeated(sch, schname) ? 1 : 0
    istoplevel(schname) ? lev : (lev + max_repetition_level(sch, parentname(schname)))
end 

function max_definition_level(sch::Schema, schname)
    lev = isrequired(sch, schname) ? 0 : 1
    istoplevel(schname) ? lev : (lev + max_definition_level(sch, parentname(schname)))
end 
