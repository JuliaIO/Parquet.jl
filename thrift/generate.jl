# Pure-Julia generator for src/metadata/parquet.jl from the vendored Parquet Thrift IDL.
#
#   julia thrift/generate.jl          # regenerate src/metadata/parquet.jl
#   julia thrift/generate.jl --check  # exit 1 when the checked-in file is stale
#
# The generator uses no external Thrift compiler. Output is deterministic: it depends
# only on the IDL bytes and this file.
module ThriftGenerator

const FORMAT_VERSION = "2.13.0"
const FORMAT_COMMIT = "c47e2a66e88943fc46fde1b028a9432f14fdf5c0"
const IDL_PATH = joinpath(@__DIR__, "parquet.thrift")
const OUTPUT_PATH = normpath(joinpath(@__DIR__, "..", "src", "metadata", "parquet.jl"))

const BASE_TYPES = Dict(
    "bool" => "Bool", "byte" => "Int8", "i8" => "Int8", "i16" => "Int16", "i32" => "Int32",
    "i64" => "Int64", "double" => "Float64", "string" => "String", "binary" => "Vector{UInt8}")

const BASE_CODES = Dict(
    "byte" => "Thrift.BYTE", "i8" => "Thrift.BYTE", "i16" => "Thrift.I16", "i32" => "Thrift.I32",
    "i64" => "Thrift.I64", "double" => "Thrift.DOUBLE", "string" => "Thrift.BINARY",
    "binary" => "Thrift.BINARY")

const BASE_READERS = Dict(
    "byte" => "Thrift.readi8(r)", "i8" => "Thrift.readi8(r)", "i16" => "Thrift.readi16(r)",
    "i32" => "Thrift.readi32(r)", "i64" => "Thrift.readi64(r)", "double" => "Thrift.readdouble(r)",
    "string" => "Thrift.readstring(r)", "binary" => "Thrift.readbinary(r)")

const BASE_WRITERS = Dict(
    "byte" => "Thrift.writei8!", "i8" => "Thrift.writei8!", "i16" => "Thrift.writei16!",
    "i32" => "Thrift.writei32!", "i64" => "Thrift.writei64!", "double" => "Thrift.writedouble!",
    "string" => "Thrift.writestring!", "binary" => "Thrift.writebinary!")

# Identifiers that are mangled with a trailing underscore in generated Julia code.
const JULIA_KEYWORDS = Set(["abstract", "baremodule", "begin", "break", "catch", "const",
    "continue", "do", "else", "elseif", "end", "export", "false", "finally", "for", "function",
    "global", "if", "import", "in", "isa", "let", "local", "macro", "module", "mutable", "primitive",
    "quote", "return", "struct", "true", "try", "type", "using", "where", "while"])

# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

struct Token
    kind::Symbol
    text::String
    line::Int
end

mutable struct Scanner
    text::String
    pos::Int
    line::Int
end

function _peekchar(s::Scanner, ahead::Int=0)
    pos = s.pos
    for _ in 1:ahead
        pos > lastindex(s.text) && return '\0'
        pos = nextind(s.text, pos)
    end
    pos > lastindex(s.text) && return '\0'
    return s.text[pos]
end

function _advance!(s::Scanner)
    c = s.text[s.pos]
    c == '\n' && (s.line += 1)
    s.pos = nextind(s.text, s.pos)
    return c
end

function _skipline!(s::Scanner)
    while s.pos <= lastindex(s.text) && _peekchar(s) != '\n'
        _advance!(s)
    end
    return
end

function _skipblockcomment!(s::Scanner)
    _advance!(s)
    _advance!(s)
    while s.pos <= lastindex(s.text)
        _peekchar(s) == '*' && _peekchar(s, 1) == '/' && break
        _advance!(s)
    end
    s.pos <= lastindex(s.text) || error("unterminated block comment")
    _advance!(s)
    _advance!(s)
    return
end

function _isidentstart(c::Char)
    return c == '_' || ('a' <= c <= 'z') || ('A' <= c <= 'Z')
end

function _isidentchar(c::Char)
    return _isidentstart(c) || ('0' <= c <= '9') || c == '.'
end

function _scanwhile!(s::Scanner, predicate)
    start = s.pos
    while s.pos <= lastindex(s.text) && predicate(_peekchar(s))
        _advance!(s)
    end
    return s.text[start:prevind(s.text, s.pos)]
end

function _scanstring!(s::Scanner)
    quotechar = _advance!(s)
    start = s.pos
    while s.pos <= lastindex(s.text) && _peekchar(s) != quotechar
        _advance!(s)
    end
    s.pos <= lastindex(s.text) || error("unterminated string literal")
    text = s.text[start:prevind(s.text, s.pos)]
    _advance!(s)
    return text
end

function _isnumberchar(c::Char)
    return ('0' <= c <= '9') || c == '.' || c == 'x' || ('a' <= c <= 'f') || ('A' <= c <= 'F')
end

function _scannumber!(s::Scanner)
    start = s.pos
    _peekchar(s) == '-' && _advance!(s)
    _scanwhile!(s, _isnumberchar)
    return s.text[start:prevind(s.text, s.pos)]
end

function tokenize(text::String)
    s = Scanner(text, firstindex(text), 1)
    tokens = Token[]
    while s.pos <= lastindex(s.text)
        c = _peekchar(s)
        if isspace(c)
            _advance!(s)
        elseif (c == '/' && _peekchar(s, 1) == '/') || c == '#'
            _skipline!(s)
        elseif c == '/' && _peekchar(s, 1) == '*'
            _skipblockcomment!(s)
        elseif _isidentstart(c)
            push!(tokens, Token(:ident, _scanwhile!(s, _isidentchar), s.line))
        elseif ('0' <= c <= '9') || (c == '-' && '0' <= _peekchar(s, 1) <= '9')
            push!(tokens, Token(:number, _scannumber!(s), s.line))
        elseif c == '"' || c == '\''
            push!(tokens, Token(:string, _scanstring!(s), s.line))
        elseif c in "{}<>():;,=*"
            push!(tokens, Token(:punct, string(_advance!(s)), s.line))
        else
            error("unexpected character $(repr(c)) on line $(s.line)")
        end
    end
    return tokens
end

# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

struct TypeRef
    kind::Symbol
    name::String
    args::Vector{TypeRef}
end

struct FieldDef
    id::Int
    requiredness::Symbol
    type::TypeRef
    name::String
    default::Union{Nothing,Token}
end

struct EnumDef
    name::String
    values::Vector{Pair{String,Int32}}
end

struct StructDef
    name::String
    kind::Symbol
    fields::Vector{FieldDef}
end

struct TypedefDef
    name::String
    type::TypeRef
end

mutable struct Parser
    tokens::Vector{Token}
    pos::Int
end

function _peek(p::Parser)
    p.pos <= length(p.tokens) && return p.tokens[p.pos]
    return Token(:eof, "", 0)
end

function _next!(p::Parser)
    token = _peek(p)
    p.pos += 1
    return token
end

function _expect!(p::Parser, text::String)
    token = _next!(p)
    token.text == text || error("expected $(repr(text)) but found $(repr(token.text)) on line $(token.line)")
    return token
end

function _expectkind!(p::Parser, kind::Symbol)
    token = _next!(p)
    token.kind == kind || error("expected $kind but found $(repr(token.text)) on line $(token.line)")
    return token
end

function _accept!(p::Parser, text::String)
    _peek(p).text == text || return false
    p.pos += 1
    return true
end

function _skipannotations!(p::Parser)
    _accept!(p, "(") || return
    depth = 1
    while depth > 0
        token = _next!(p)
        token.kind == :eof && error("unterminated annotation")
        token.text == "(" && (depth += 1)
        token.text == ")" && (depth -= 1)
    end
    return
end

function _skipseparator!(p::Parser)
    _accept!(p, ",") || _accept!(p, ";")
    return
end

function parsetype!(p::Parser)
    token = _expectkind!(p, :ident)
    name = token.text
    if name == "list" || name == "set"
        _expect!(p, "<")
        element = parsetype!(p)
        _expect!(p, ">")
        _accept!(p, "cpp_type") && _expectkind!(p, :string)
        return TypeRef(Symbol(name), name, [element])
    elseif name == "map"
        _expect!(p, "<")
        key = parsetype!(p)
        _expect!(p, ",")
        value = parsetype!(p)
        _expect!(p, ">")
        _accept!(p, "cpp_type") && _expectkind!(p, :string)
        return TypeRef(:map, name, [key, value])
    end
    haskey(BASE_TYPES, name) && return TypeRef(:base, name, TypeRef[])
    return TypeRef(:named, name, TypeRef[])
end

function parsefield!(p::Parser)
    idtoken = _expectkind!(p, :number)
    id = parse(Int, idtoken.text)
    _expect!(p, ":")
    requiredness = :default
    _accept!(p, "required") && (requiredness = :required)
    _accept!(p, "optional") && (requiredness = :optional)
    type = parsetype!(p)
    _skipannotations!(p)
    name = _expectkind!(p, :ident).text
    default = _accept!(p, "=") ? _next!(p) : nothing
    _skipannotations!(p)
    _skipseparator!(p)
    return FieldDef(id, requiredness, type, name, default)
end

function parsestruct!(p::Parser, kind::Symbol)
    name = _expectkind!(p, :ident).text
    _expect!(p, "{")
    fields = FieldDef[]
    while !_accept!(p, "}")
        push!(fields, parsefield!(p))
    end
    _skipannotations!(p)
    return StructDef(name, kind, fields)
end

function parseenum!(p::Parser)
    name = _expectkind!(p, :ident).text
    _expect!(p, "{")
    values = Pair{String,Int32}[]
    nextvalue = Int32(0)
    while !_accept!(p, "}")
        entry = _expectkind!(p, :ident).text
        value = _accept!(p, "=") ? Int32(parse(Int, _expectkind!(p, :number).text)) : nextvalue
        _skipannotations!(p)
        _skipseparator!(p)
        push!(values, entry => value)
        nextvalue = value + Int32(1)
    end
    _skipannotations!(p)
    return EnumDef(name, values)
end

function parsedocument(tokens::Vector{Token})
    p = Parser(tokens, 1)
    definitions = Any[]
    while _peek(p).kind != :eof
        keyword = _next!(p)
        if keyword.text == "namespace"
            _next!(p)
            _next!(p)
        elseif keyword.text == "include" || keyword.text == "cpp_include"
            _expectkind!(p, :string)
        elseif keyword.text == "enum"
            push!(definitions, parseenum!(p))
        elseif keyword.text == "struct" || keyword.text == "union" || keyword.text == "exception"
            push!(definitions, parsestruct!(p, keyword.text == "union" ? :union : :struct))
        elseif keyword.text == "typedef"
            type = parsetype!(p)
            name = _expectkind!(p, :ident).text
            _skipseparator!(p)
            push!(definitions, TypedefDef(name, type))
        else
            error("unsupported Thrift definition $(repr(keyword.text)) on line $(keyword.line)")
        end
    end
    return definitions
end

# ---------------------------------------------------------------------------
# Emitter
# ---------------------------------------------------------------------------

struct Context
    kinds::Dict{String,Symbol}
    typedefs::Dict{String,TypeRef}
end

function resolve(ctx::Context, type::TypeRef)
    type.kind == :named || return type
    haskey(ctx.typedefs, type.name) && return resolve(ctx, ctx.typedefs[type.name])
    haskey(ctx.kinds, type.name) || error("unknown Thrift type $(type.name)")
    return type
end

function jltype(ctx::Context, type::TypeRef)
    type = resolve(ctx, type)
    type.kind == :base && return BASE_TYPES[type.name]
    type.kind == :list && return "Vector{$(jltype(ctx, type.args[1]))}"
    type.kind == :set && return "Vector{$(jltype(ctx, type.args[1]))}"
    type.kind == :map && return "Vector{Pair{$(jltype(ctx, type.args[1])), $(jltype(ctx, type.args[2]))}}"
    ctx.kinds[type.name] == :enum && return "$(type.name).T"
    return type.name
end

function wirecode(ctx::Context, type::TypeRef)
    type = resolve(ctx, type)
    type.kind == :base && return BASE_CODES[type.name]
    type.kind == :list && return "Thrift.LIST"
    type.kind == :set && return "Thrift.SET"
    type.kind == :map && return "Thrift.MAP"
    ctx.kinds[type.name] == :enum && return "Thrift.I32"
    return "Thrift.STRUCT"
end

function isbool(ctx::Context, type::TypeRef)
    type = resolve(ctx, type)
    return type.kind == :base && type.name == "bool"
end

function iscontainer(ctx::Context, type::TypeRef)
    kind = resolve(ctx, type).kind
    return kind == :list || kind == :set || kind == :map
end

function _checknestedset(ctx::Context, type::TypeRef)
    type = resolve(ctx, type)
    for arg in type.args
        inner = resolve(ctx, arg)
        inner.kind == :set && error("nested set types are not supported by the generator")
        _checknestedset(ctx, inner)
    end
    return
end

function readexpr(ctx::Context, type::TypeRef)
    type = resolve(ctx, type)
    type.kind == :base && return BASE_READERS[type.name]
    ctx.kinds[type.name] == :enum && return "$(type.name).T(Thrift.readi32(r))"
    return "Thrift.decode(r, $(type.name))"
end

function containerreadexpr(ctx::Context, type::TypeRef)
    type = resolve(ctx, type)
    type.kind == :map && return "Thrift.readmap(r, $(jltype(ctx, type.args[1])), $(jltype(ctx, type.args[2])))"
    return "Thrift.readlist(r, $(jltype(ctx, type.args[1])))"
end

function writestmt(ctx::Context, type::TypeRef, value::String)
    type = resolve(ctx, type)
    type.kind == :base && return "$(BASE_WRITERS[type.name])(w, $value)"
    type.kind == :map && return "Thrift.writemap!(w, $value)"
    (type.kind == :list || type.kind == :set) && return "Thrift.writelist!(w, $value)"
    ctx.kinds[type.name] == :enum && return "Thrift.writei32!(w, $value.value)"
    return "Thrift.encode!(w, $value)"
end

function defaultexpr(ctx::Context, field::FieldDef)
    token = field.default
    type = resolve(ctx, field.type)
    if type.kind == :base
        type.name == "bool" && return token.text
        type.name == "string" && return repr(token.text)
        type.name == "binary" && return "Vector{UInt8}(codeunits($(repr(token.text))))"
        return "$(BASE_TYPES[type.name])($(token.text))"
    end
    type.kind == :named && ctx.kinds[type.name] == :enum && return token.text
    error("unsupported default value for field $(field.name)")
end

function typetext(type::TypeRef)
    type.kind == :list && return "list<$(typetext(type.args[1]))>"
    type.kind == :set && return "set<$(typetext(type.args[1]))>"
    type.kind == :map && return "map<$(typetext(type.args[1])), $(typetext(type.args[2]))>"
    return type.name
end

function fieldcomment(field::FieldDef)
    text = "# $(field.id): "
    field.requiredness == :default || (text *= "$(field.requiredness) ")
    text *= "$(typetext(field.type)) $(field.name)"
    field.default === nothing || (text *= " = $(field.default.text)")
    return text
end

function isrequired(def::StructDef, field::FieldDef)
    return def.kind == :struct && field.requiredness == :required
end

function hasdefault(def::StructDef, field::FieldDef)
    return isrequired(def, field) && field.default !== nothing
end

function mangle(name::String)
    occursin('.', name) && error("Thrift identifier $(repr(name)) contains a dot")
    name in JULIA_KEYWORDS && return name * "_"
    return name
end

function fieldname(field::FieldDef)
    name = mangle(field.name)
    name == "unknown_fields" && error("Thrift field name $(repr(field.name)) is reserved")
    return name
end

function localname(field::FieldDef)
    return "f_$(fieldname(field))"
end

function fieldtype(ctx::Context, def::StructDef, field::FieldDef)
    _checknestedset(ctx, field.type)
    jt = jltype(ctx, field.type)
    isrequired(def, field) && return jt
    return "Union{Nothing, $jt}"
end

function emitenum(io::IO, def::EnumDef)
    println(io, "module ", mangle(def.name))
    println(io)
    println(io, "import ..Thrift")
    println(io)
    println(io, "struct T <: Thrift.ThriftEnum")
    println(io, "    value::Int32")
    println(io, "end")
    for (name, value) in def.values
        println(io)
        println(io, "const ", mangle(name), " = T(", value, ")")
    end
    names = join(("(Int32($value), :$(mangle(name)))" for (name, value) in def.values), ", ")
    println(io)
    println(io, "function Thrift.enumnames(::Core.Type{T})")
    println(io, "    return (", names, length(def.values) == 1 ? "," : "", ")")
    println(io, "end")
    println(io)
    println(io, "function Thrift.typecode(::Core.Type{T})")
    println(io, "    return Thrift.I32")
    println(io, "end")
    println(io)
    println(io, "function Thrift.readelement(r::Thrift.Reader, ::Core.Type{T})")
    println(io, "    return T(Thrift.readi32(r))")
    println(io, "end")
    println(io)
    println(io, "function Thrift.writeelement!(w::Thrift.Writer, x::T)")
    println(io, "    Thrift.writei32!(w, x.value)")
    println(io, "    return")
    println(io, "end")
    println(io)
    println(io, "end")
    return
end

function emitstructdef(io::IO, ctx::Context, def::StructDef)
    println(io, "# Thrift struct ", def.name)
    println(io, "Base.@kwdef struct ", def.name)
    for field in def.fields
        jt = fieldtype(ctx, def, field)
        if hasdefault(def, field)
            println(io, "    ", fieldname(field), "::", jt, " = ", defaultexpr(ctx, field), "  ", fieldcomment(field))
        elseif isrequired(def, field)
            println(io, "    ", fieldname(field), "::", jt, "  ", fieldcomment(field))
        else
            println(io, "    ", fieldname(field), "::", jt, " = nothing  ", fieldcomment(field))
        end
    end
    println(io, "    unknown_fields::Tuple{Vararg{Thrift.RawField}} = ()")
    println(io, "end")
    return
end

function knowncountexpr(def::StructDef)
    isempty(def.fields) && return "0"
    return join(("($(fieldname(field)) !== nothing)" for field in def.fields), " + ")
end

function emituniondef(io::IO, ctx::Context, def::StructDef)
    names = [fieldname(field) for field in def.fields]
    args = join(vcat(names, "unknown_fields"), ", ")
    println(io, "# Thrift union ", def.name)
    println(io, "struct ", def.name)
    for field in def.fields
        println(io, "    ", fieldname(field), "::", fieldtype(ctx, def, field), "  ", fieldcomment(field))
    end
    println(io, "    unknown_fields::Tuple{Vararg{Thrift.RawField}}")
    println(io, "    function ", def.name, "(", args, ")")
    println(io, "        Thrift.checkunionargs(:", def.name, ", ", knowncountexpr(def), ", unknown_fields)")
    println(io, "        return new(", args, ")")
    println(io, "    end")
    println(io, "end")
    println(io)
    kwargs = join(vcat(["$name=nothing" for name in names], "unknown_fields=()"), ", ")
    println(io, "function ", def.name, "(; ", kwargs, ")")
    println(io, "    return ", def.name, "(", args, ")")
    println(io, "end")
    return
end

function emitequality(io::IO, def::StructDef)
    names = [fieldname(field) for field in def.fields]
    push!(names, "unknown_fields")
    println(io)
    println(io, "function Base.:(==)(a::", def.name, ", b::", def.name, ")")
    println(io, "    return ", join(("a.$name == b.$name" for name in names), " && "))
    println(io, "end")
    println(io)
    println(io, "function Base.isequal(a::", def.name, ", b::", def.name, ")")
    println(io, "    return ", join(("isequal(a.$name, b.$name)" for name in names), " && "))
    println(io, "end")
    println(io)
    println(io, "function Base.hash(x::", def.name, ", h::UInt)")
    println(io, "    h = hash(:", def.name, ", h)")
    for name in names
        println(io, "    h = hash(x.", name, ", h)")
    end
    println(io, "    return h")
    println(io, "end")
    return
end

function emitelementhelpers(io::IO, def::StructDef)
    println(io)
    println(io, "function Thrift.typecode(::Core.Type{", def.name, "})")
    println(io, "    return Thrift.STRUCT")
    println(io, "end")
    println(io)
    println(io, "function Thrift.readelement(r::Thrift.Reader, ::Core.Type{", def.name, "})")
    println(io, "    return Thrift.decode(r, ", def.name, ")")
    println(io, "end")
    println(io)
    println(io, "function Thrift.writeelement!(w::Thrift.Writer, x::", def.name, ")")
    println(io, "    Thrift.encode!(w, x)")
    println(io, "    return")
    println(io, "end")
    return
end

function emitdecodebranch(io::IO, ctx::Context, field::FieldDef, keyword::String)
    target = localname(field)
    if isbool(ctx, field.type)
        println(io, "        ", keyword, " id == Int16(", field.id, ") && (ty == Thrift.BOOL_TRUE || ty == Thrift.BOOL_FALSE)")
        println(io, "            ", target, " = ty == Thrift.BOOL_TRUE")
    elseif iscontainer(ctx, field.type)
        value = "value_$(fieldname(field))"
        println(io, "        ", keyword, " id == Int16(", field.id, ") && ty == ", wirecode(ctx, field.type))
        println(io, "            ", value, " = ", containerreadexpr(ctx, field.type))
        println(io, "            if ", value, " === nothing")
        println(io, "                unknown = Thrift.pushunknown!(unknown, Thrift.readrawfield(r, id, ty))")
        println(io, "            else")
        println(io, "                ", target, " = ", value)
        println(io, "            end")
    else
        println(io, "        ", keyword, " id == Int16(", field.id, ") && ty == ", wirecode(ctx, field.type))
        println(io, "            ", target, " = ", readexpr(ctx, field.type))
    end
    return
end

function emitdecode(io::IO, ctx::Context, def::StructDef)
    println(io)
    println(io, "function Thrift.decode(r::Thrift.Reader, ::Core.Type{", def.name, "})")
    println(io, "    Thrift.enter!(r)")
    for field in def.fields
        init = hasdefault(def, field) ? defaultexpr(ctx, field) : "nothing"
        println(io, "    ", localname(field), " = ", init)
    end
    println(io, "    unknown = nothing")
    println(io, "    lastid = Int16(0)")
    println(io, "    while true")
    println(io, "        id, ty = Thrift.readfieldheader(r, lastid)")
    println(io, "        ty == Thrift.STOP && break")
    println(io, "        lastid = id")
    for (index, field) in enumerate(def.fields)
        emitdecodebranch(io, ctx, field, index == 1 ? "if" : "elseif")
    end
    isempty(def.fields) || println(io, "        else")
    indent = isempty(def.fields) ? "        " : "            "
    println(io, indent, "unknown = Thrift.pushunknown!(unknown, Thrift.readrawfield(r, id, ty))")
    isempty(def.fields) || println(io, "        end")
    println(io, "    end")
    println(io, "    Thrift.leave!(r)")
    println(io, "    unknown_fields = Thrift.finishunknown(unknown)")
    for field in def.fields
        isrequired(def, field) && !hasdefault(def, field) || continue
        println(io, "    ", localname(field), " === nothing && Thrift.missingfield(:", def.name, ", :", field.name, ")")
    end
    if def.kind == :union
        known = isempty(def.fields) ? "0" : join(("($(localname(field)) !== nothing)" for field in def.fields), " + ")
        println(io, "    Thrift.checkunion(:", def.name, ", ", known, ", unknown_fields)")
    end
    args = join((localname(field) for field in def.fields), ", ")
    isempty(def.fields) || (args *= ", ")
    println(io, "    return ", def.name, "(", args, "unknown_fields)")
    println(io, "end")
    return
end

function emitencodefield(io::IO, ctx::Context, def::StructDef, field::FieldDef)
    value = "x.$(fieldname(field))"
    indent = "    "
    if !isrequired(def, field)
        value = "value_$(fieldname(field))"
        println(io, "    ", value, " = x.", fieldname(field))
        println(io, "    if ", value, " !== nothing")
        indent = "        "
    end
    code = isbool(ctx, field.type) ? "$value ? Thrift.BOOL_TRUE : Thrift.BOOL_FALSE" : wirecode(ctx, field.type)
    println(io, indent, "lastid = Thrift.writefieldheader!(w, lastid, Int16(", field.id, "), ", code, ")")
    isbool(ctx, field.type) || println(io, indent, writestmt(ctx, field.type, value))
    println(io, indent, "(lastid, index) = Thrift.writeunknownafter!(w, unknown, index, lastid)")
    isrequired(def, field) || println(io, "    end")
    return
end

function emitencode(io::IO, ctx::Context, def::StructDef)
    println(io)
    println(io, "function Thrift.encode!(w::Thrift.Writer, x::", def.name, ")")
    println(io, "    unknown = x.unknown_fields")
    println(io, "    lastid = Int16(0)")
    println(io, "    index = 1")
    println(io, "    (lastid, index) = Thrift.writeunknownafter!(w, unknown, index, lastid)")
    for field in sort(def.fields; by=field -> field.id)
        emitencodefield(io, ctx, def, field)
    end
    println(io, "    Thrift.writeunknownrest!(w, unknown, index, lastid)")
    println(io, "    Thrift.writestop!(w)")
    println(io, "    return")
    println(io, "end")
    return
end

function emitstruct(io::IO, ctx::Context, def::StructDef)
    mangle(def.name) == def.name || error("Thrift struct name $(repr(def.name)) is a Julia keyword")
    ids = [field.id for field in def.fields]
    allunique(ids) || error("duplicate field ids in $(def.name)")
    allunique(fieldname(field) for field in def.fields) || error("duplicate field names in $(def.name)")
    def.kind == :union ? emituniondef(io, ctx, def) : emitstructdef(io, ctx, def)
    emitequality(io, def)
    emitelementhelpers(io, def)
    emitdecode(io, ctx, def)
    emitencode(io, ctx, def)
    return
end

function fnv1a64(bytes::AbstractVector{UInt8})
    h = 0xcbf29ce484222325
    for byte in bytes
        h = (h ⊻ UInt64(byte)) * 0x00000100000001b3
    end
    return h
end

function buildcontext(definitions::Vector{Any})
    ctx = Context(Dict{String,Symbol}(), Dict{String,TypeRef}())
    for def in definitions
        haskey(ctx.kinds, def.name) && error("duplicate definition $(def.name)")
        if def isa EnumDef
            ctx.kinds[def.name] = :enum
        elseif def isa StructDef
            ctx.kinds[def.name] = :struct
        else
            ctx.kinds[def.name] = :typedef
            ctx.typedefs[def.name] = def.type
        end
    end
    return ctx
end

"""
    generate(idl::String; version, commit) -> String

Generate the Julia source of the `Metadata` module from Thrift IDL text.
"""
function generate(idl::String; version::String=FORMAT_VERSION, commit::String=FORMAT_COMMIT)
    definitions = parsedocument(tokenize(idl))
    ctx = buildcontext(definitions)
    io = IOBuffer()
    println(io, "# Generated by thrift/generate.jl from thrift/parquet.thrift. Do not edit by hand.")
    println(io, "# Source: apache/parquet-format ", version, " (", commit, ")")
    println(io, "# IDL: ", sizeof(idl), " bytes, FNV-1a 64 0x", string(fnv1a64(codeunits(idl)); base=16, pad=16))
    println(io, "module Metadata")
    println(io)
    println(io, "import ..Thrift")
    for def in definitions
        def isa TypedefDef && continue
        println(io)
        def isa EnumDef ? emitenum(io, def) : emitstruct(io, ctx, def)
    end
    println(io)
    println(io, "end")
    return String(take!(io))
end

function main(args::Vector{String})
    output = generate(read(IDL_PATH, String))
    if "--check" in args
        existing = isfile(OUTPUT_PATH) ? read(OUTPUT_PATH, String) : ""
        existing == output && (println("src/metadata/parquet.jl is up to date"); return 0)
        println(stderr, "src/metadata/parquet.jl is stale; run julia thrift/generate.jl")
        return 1
    end
    mkpath(dirname(OUTPUT_PATH))
    write(OUTPUT_PATH, output)
    println("wrote ", OUTPUT_PATH)
    return 0
end

end

if abspath(PROGRAM_FILE) == @__FILE__
    exit(ThriftGenerator.main(ARGS))
end
