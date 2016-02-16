
# schema and helper methods
type Schema
    schema::Vector{SchemaElement}
    name_lookup::Dict{AbstractString,SchemaElement}

    function Schema(elems::Vector{SchemaElement})
        name_lookup = Dict{AbstractString,SchemaElement}()
        for sch in elems
            name_lookup[sch.name] = sch
        end
        new(elems, name_lookup)
    end
end

leafname(schname::AbstractString) = ('.' in schname) ? leafname(split(schname, '.')) : schname
leafname(schname::Vector) = schname[end]
elem(sch::Schema, schname) = sch.name_lookup[leafname(schname)]
isrequired(sch::Schema, schname) = (elem(sch, schname).repetition_type == FieldRepetitionType.REQUIRED)

max_repetition_level(sch::Schema, schname::AbstractString) = max_repetition_level(sch, split(schname, '.'))
max_repetition_level(sch::Schema, schname) = sum([isrequired(sch, namepart) for namepart in schname])

max_definition_level(sch::Schema, schname::AbstractString) = max_definition_level(sch, split(schname, '.'))
max_definition_level(sch::Schema, schname) = sum([!isrequired(sch, namepart) for namepart in schname])

function schema_to_julia_types(sch::Schema, mod::Module=Main)

end
