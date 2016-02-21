
# schema and helper methods
type Schema
    schema::Vector{SchemaElement}
    name_lookup::Dict{AbstractString,SchemaElement}

    function Schema(elems::Vector{SchemaElement})
        name_lookup = Dict{AbstractString,SchemaElement}()
        name_stack = AbstractString[]
        nchildren_stack = Int[]

        for idx in 1:length(elems)
            sch = elems[idx]
            # TODO: may be better to have fully qualified names here to avoid chashes
            nested_name = (idx == 1) ? sch.name : join([name_stack; sch.name], '.')
            name_lookup[nested_name] = sch

            if !isempty(nchildren_stack)
                @logmsg("$(sch.name) is a child. remaining $(nchildren_stack[end]-1) children")
                if nchildren_stack[end] == 1
                    pop!(nchildren_stack)
                    pop!(name_stack)
                else
                    nchildren_stack[end] -= 1
                end
            end

            if (idx > 1) && (Thrift.isfilled(sch, :num_children) && (sch.num_children > 0))
                @logmsg("$(sch.name) has $(sch.num_children) children")
                push!(nchildren_stack, sch.num_children)
                push!(name_stack, sch.name)
            end
        end
        new(elems, name_lookup)
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

function isrequired(sch::Schema, schname)
    schelem = elem(sch, schname)
    Thrift.isfilled(schelem, :repetition_type) && (schelem.repetition_type == FieldRepetitionType.REQUIRED)
end

function isrepeated(sch::Schema, schname)
    schelem = elem(sch, schname)
    Thrift.isfilled(schelem, :repetition_type) && (schelem.repetition_type == FieldRepetitionType.REPEATED)
end

function bit_or_byte_length(sch::Schema, schname)
    schelem = elem(sch, schname)
    Thrift.isfilled(schelem, :type_length) ? schelem.type_length : 0
end

function max_repetition_level(sch::Schema, schname)
    lev = isrepeated(sch, schname) ? 1 : 0
    istoplevel(schname) ? lev : (lev + max_repetition_level(sch, parentname(schname)))
end 

function max_definition_level(sch::Schema, schname)
    lev = isrequired(sch, schname) ? 0 : 1
    istoplevel(schname) ? lev : (lev + max_definition_level(sch, parentname(schname)))
end 

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
