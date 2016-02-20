
# schema and helper methods
type Schema
    schema::Vector{SchemaElement}
    name_lookup::Dict{AbstractString,SchemaElement}

    function Schema(elems::Vector{SchemaElement})
        name_lookup = Dict{AbstractString,SchemaElement}()
        for sch in elems
            # TODO: may be better to have fully qualified names here to avoid chashes
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

abstract SchemaConverter

# parquet schema to Protobuf schema converter
type ProtoConverter <: SchemaConverter
    to::IO
end

schema(conv::ProtoConverter, sch::Schema, schema_name::Symbol) = schema(conv, sch.schema, schema_name)
schema(conv::ProtoConverter, sch::Vector{SchemaElement}, schema_name::Symbol) = schema_to_proto_schema(conv.to, sch, schema_name)

function schema_to_proto_schema(io::IO, sch::Vector{SchemaElement}, schema_name::Symbol)
    nchildren = Int[]
    lev0 = IOBuffer()
    ios = IO[lev0]
    println(lev0, "message ", schema_name, " {")
    for schemaelem in sch
        _sch_to_proto(schemaelem, ios, nchildren)
    end
    lev0 = ios[end]
    println(lev0, "}")
    write(io, takebuf_string(lev0))
    nothing
end

function _sch_to_proto(sch::SchemaElement, ios::Vector{IO}, nchildren::Vector{Int}=Int[])
    lchildren = length(nchildren)

    if isfilled(sch, :_type)
        jtypestr = PLAIN_PROTOBUF_TYPES[sch._type+1]
    else
        # this is a composite type
        jtypestr = sch.name * "Type"
    end
    # we are not looking at converted types yet

    repntype = ""
    if isfilled(sch, :repetition_type)
        (sch.repetition_type == FieldRepetitionType.REQUIRED) && (repntype = "required")
        (sch.repetition_type == FieldRepetitionType.OPTIONAL) && (repntype = "optional")
        (sch.repetition_type == FieldRepetitionType.REPEATED) && (repntype = "repeated")
    end

    fldid = isfilled(sch, :field_id) ? (" = " * string(sch.field_id)) : ""

    if lchildren > 0
        println(ios[end], "    ", repntype, " ", jtypestr, " ", sch.name, fldid, ";")
    end

    if isfilled(sch, :num_children)
        if lchildren > 0
            lvlio = IOBuffer()
            println(lvlio, "struct ", jtypestr, " {")
            push!(ios, lvlio)
        end
        push!(nchildren, sch.num_children)
    elseif lchildren > 0
        nchildren[lchildren] -= 1
        if nchildren[lchildren] == 0
            pop!(nchildren)
            lvlio = pop!(ios)
            if !isempty(ios)
                println(lvlio, "}")
                prevlvlio = pop!(ios)
                println(lvlio, "")
                print(lvlio, takebuf_string(prevlvlio))
            end
            push!(ios, lvlio)
        end
    end
end


# parquet schema to Thrift schema converter
type ThriftConverter <: SchemaConverter
    to::IO
end

schema(conv::ThriftConverter, sch::Schema, schema_name::Symbol) = schema(conv, sch.schema, schema_name)
schema(conv::ThriftConverter, sch::Vector{SchemaElement}, schema_name::Symbol) = schema_to_thrift_schema(conv.to, sch, schema_name)

function schema_to_thrift_schema(io::IO, sch::Vector{SchemaElement}, schema_name::Symbol)
    nchildren = Int[]
    lev0 = IOBuffer()
    ios = IO[lev0]
    println(lev0, "struct ", schema_name, " {")
    for schemaelem in sch
        _sch_to_thrift(schemaelem, ios, nchildren)
    end
    lev0 = ios[end]
    println(lev0, "}")
    write(io, takebuf_string(lev0))
    nothing
end

function _sch_to_thrift(sch::SchemaElement, ios::Vector{IO}, nchildren::Vector{Int}=Int[])
    lchildren = length(nchildren)

    if isfilled(sch, :_type)
        jtypestr = PLAIN_THRIFT_TYPES[sch._type+1]
    else
        # this is a composite type
        jtypestr = sch.name * "Type"
    end
    # we are not looking at converted types yet

    if isfilled(sch, :repetition_type) && (sch.repetition_type == FieldRepetitionType.REPEATED)  # array type
        jtypestr = "list<" * jtypestr * ">"
    end

    repntype = ""
    if isfilled(sch, :repetition_type)
        (sch.repetition_type == FieldRepetitionType.REQUIRED) && (repntype = "required")
        (sch.repetition_type == FieldRepetitionType.OPTIONAL) && (repntype = "optional")
    end

    fldid = isfilled(sch, :field_id) ? (string(sch.field_id) * ":") : ""

    if lchildren > 0
        println(ios[end], "    ", fldid, " ", repntype, " ", jtypestr, " ", sch.name, (nchildren[lchildren] == 0) ? "." : "")
    end

    if isfilled(sch, :num_children)
        if lchildren > 0
            lvlio = IOBuffer()
            println(lvlio, "struct ", jtypestr, " {")
            push!(ios, lvlio)
        end
        push!(nchildren, sch.num_children)
    elseif lchildren > 0
        nchildren[lchildren] -= 1
        if nchildren[lchildren] == 0
            pop!(nchildren)
            lvlio = pop!(ios)
            if !isempty(ios)
                println(lvlio, "}")
                prevlvlio = pop!(ios)
                println(lvlio, "")
                print(lvlio, takebuf_string(prevlvlio))
            end
            push!(ios, lvlio)
        end
    end
end

# parquet schema to Julia types
type JuliaConverter <: SchemaConverter
    to::Union{Module,IO}
end

schema(conv::JuliaConverter, sch::Union{Schema, Vector{SchemaElement}}, schema_name::Symbol) = schema_to_julia_types(conv.to, sch, schema_name)

function schema_to_julia_types(mod::Module, sch::Schema, schema_name::Symbol)
    io = IOBuffer()
    schema_to_julia_types(io, sch, schema_name)
    typestr = "begin\n" * takebuf_string(io) * "\nend"
    parsedtypes = parse(typestr)
    eval(mod, parsedtypes)
end

schema_to_julia_types(io::IO, sch::Schema, schema_name::Symbol) = schema_to_julia_types(io, sch.schema, schema_name)

function schema_to_julia_types(io::IO, sch::Vector{SchemaElement}, schema_name::Symbol)
    nchildren = Int[]
    lev0 = IOBuffer()
    ios = IO[lev0]
    println(lev0, "type ", schema_name)
    println(lev0, "    ", schema_name, "() = new()")
    for schemaelem in sch
        _sch_to_julia(schemaelem, ios, nchildren)
    end
    lev0 = ios[end]
    println(lev0, "end")
    write(io, takebuf_string(lev0))
    nothing
end

function _sch_to_julia(sch::SchemaElement, ios::Vector{IO}, nchildren::Vector{Int}=Int[])
    lchildren = length(nchildren)

    if isfilled(sch, :_type)
        jtype = PLAIN_JTYPES[sch._type+1]
        jtypestr = string(jtype)
    else
        # this is a composite type
        jtypestr = sch.name * "Type"
    end
    # we are not looking at converted types yet

    if (isfilled(sch, :_type) && (sch._type == _Type.BYTE_ARRAY || sch._type == _Type.FIXED_LEN_BYTE_ARRAY)) || 
       (isfilled(sch, :repetition_type) && (sch.repetition_type == FieldRepetitionType.REPEATED))  # array type
        jtypestr = "Vector{" * jtypestr * "}"
    end

    if lchildren > 0
        println(ios[end], "    ", sch.name, "::", jtypestr)
    end

    if isfilled(sch, :num_children)
        if lchildren > 0
            lvlio = IOBuffer()
            println(lvlio, "type ", jtypestr)
            println(lvlio, "    ", jtypestr, "() = new()")
            push!(ios, lvlio)
        end
        push!(nchildren, sch.num_children)
    elseif lchildren > 0
        nchildren[lchildren] -= 1
        if nchildren[lchildren] == 0
            pop!(nchildren)
            lvlio = pop!(ios)
            if !isempty(ios)
                println(lvlio, "end")
                prevlvlio = pop!(ios)
                println(lvlio, "")
                print(lvlio, takebuf_string(prevlvlio))
            end
            push!(ios, lvlio)
        end
    end
end
