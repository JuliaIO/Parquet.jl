# Parquet.Writer: schema, row groups streamed to the sink, page indexes, and the footer.

struct _WriteOptions
    chunk::_ChunkOptions
    rowgroupsize::Union{Nothing,Int}
    dictionary::Bool
    encoding::Any
    bloomfilter::Any
    ntasks::Int
    nullable::Union{Nothing,Bool}
    metadata::Any
    colmetadata::Any
    limits::Limits
end

function _writerowgroupsize(value, rows::Int)
    value === nothing && return max(rows, 1)
    value isa Integer && !(value isa Bool) || throw(ArgumentError(
        "rowgroupsize must be a positive integer or nothing"))
    value > 0 || throw(ArgumentError(
        "rowgroupsize must be a positive integer or nothing"))
    value <= typemax(Int) || throw(ArgumentError(
        "rowgroupsize exceeds the Julia index range"))
    return Int(value)
end
function _writepagesize(value)
    value === nothing && return nothing
    value isa Integer && !(value isa Bool) || throw(ArgumentError(
        "pagesize must be a positive integer or nothing"))
    value > 0 || throw(ArgumentError(
        "pagesize must be a positive integer or nothing"))
    value <= typemax(Int64) || throw(ArgumentError(
        "pagesize exceeds Int64"))
    return Int64(value)
end

# pyarrow's and Polars' names for write keywords, for the unknown-keyword error.
const _WRITE_KEYWORD_ALIASES = Dict(:compression => :compress,
    :compression_level => :compresslevel, :row_group_size => :rowgroupsize,
    :use_dictionary => :dictionary, :data_page_size => :pagesize,
    :data_page_version => :pageversion, :write_statistics => :statistics,
    :write_page_index => :pageindex, :partition_cols => :partitionby,
    :partition_by => :partitionby)

@noinline function _unknownwritekeywords(names)
    listed = join((haskey(_WRITE_KEYWORD_ALIASES, name) ?
        "`$name` (use `$(_WRITE_KEYWORD_ALIASES[name])`)" : "`$name`" for name in names), ", ")
    throw(ArgumentError("unknown write keyword$(length(names) == 1 ? "" : "s") $listed. " *
        "The keywords are compress, compresslevel, dictionary, encoding, bloomfilter, " *
        "rowgroupsize, pagesize, pageversion, statistics, pageindex, checksum, ntasks, " *
        "metadata, colmetadata, and limits, plus partitionby and existing for " *
        "Parquet.write and nullable for Parquet.Writer"))
end

function _writeoptions(; compress=:snappy, compresslevel::Union{Nothing,Integer}=nothing,
        dictionary::Bool=true, encoding=nothing, bloomfilter=false, rowgroupsize=1_048_576,
        pagesize=1024 * 1024, pageversion=:v1, statistics::Bool=true, pageindex::Bool=true,
        checksum::Bool=false, ntasks::Integer=Threads.nthreads(), metadata=nothing,
        colmetadata=nothing, nullable::Union{Nothing,Bool}=nothing,
        limits::Limits=Limits(), codec=nothing,
        compressionlevel::Union{Nothing,Integer}=nothing, unknown...)
    # Behind `_barrier`: building the message joins strings through code that loading
    # CategoricalArrays invalidates.
    isempty(unknown) || _barrier(_unknownwritekeywords, collect(keys(unknown)))
    if codec !== nothing
        Base.depwarn("the `codec` keyword is deprecated; use `compress`", :write)
        compress = codec
    end
    if compressionlevel !== nothing
        Base.depwarn("the `compressionlevel` keyword is deprecated; use `compresslevel`",
            :write)
        compresslevel = compressionlevel
    end
    statlimit = _statisticlimit(limits)
    codecvalue = compress === nothing ? Metadata.CompressionCodec.UNCOMPRESSED :
        _writecodec(compress)
    level = compresslevel === nothing ? nothing : Int(compresslevel)
    # Validates the level now rather than at the first page.
    level === nothing || codecvalue == Metadata.CompressionCodec.UNCOMPRESSED ||
        _encoder(codecvalue, level)
    codecvalue == Metadata.CompressionCodec.UNCOMPRESSED && level !== nothing &&
        throw(ArgumentError("uncompressed output has no compression level"))
    version = _writepageversion(pageversion)
    target = _writepagesize(pagesize)
    rowgroupsize === nothing || _writerowgroupsize(rowgroupsize, 0)
    ntasks >= 1 || throw(ArgumentError("ntasks must be at least 1, got $ntasks"))
    encoder = codecvalue == Metadata.CompressionCodec.UNCOMPRESSED ? nothing :
        _encoder(codecvalue, level)
    chunk = _ChunkOptions(codecvalue, level, encoder, version === :v2,
        target === nothing ? typemax(Int) : Int(min(target, typemax(Int))),
        target === nothing ? typemax(Int) : _PAGE_ROW_LIMIT, checksum, statistics,
        pageindex, statlimit, limits)
    return _WriteOptions(chunk, rowgroupsize === nothing ? nothing : Int(rowgroupsize),
        dictionary, encoding, bloomfilter, Int(ntasks), nullable, metadata, colmetadata,
        limits)
end

## Schema

abstract type _FieldSpec end

# A scalar top-level column: one leaf.
struct _FlatField <: _FieldSpec
    name::String
    leaf::Int
end

# A nested top-level column; `fragment` is its flattened schema.
struct _NestedField <: _FieldSpec
    name::String
    leaves::UnitRange{Int}
    fragment::Vector{Metadata.SchemaElement}
end

struct _WriteSchema
    elements::Vector{Metadata.SchemaElement}
    schema::Schema
    semantic::_NestedSchemaPlan
    fields::Vector{_FieldSpec}
    leaves::Vector{_LeafSpec}
    stored::Bool
end

function _flatelement(name::String, values::AbstractVector, optional::Bool,
        limits::Limits)
    values isa LogicalColumn &&
        return _logicalcolumnwriteelement(name, values.spec, optional, limits)
    values isa FixedByteArrayVector && return _logicalwriteelement(name,
        Metadata.Type.FIXED_LEN_BYTE_ARRAY, optional; width=values.width)
    eltype(values) === Missing && return _logicalwriteelement(name, Metadata.Type.INT32,
        true; logical=Metadata.LogicalType(UNKNOWN=Metadata.NullType()))
    T = Base.nonmissingtype(eltype(values))
    map = _writemap(T)
    map === nothing || return _writeelement(map, name, optional)
    T <: Union{Decimal,DataDecimals.AbstractDecimal} &&
        return _decimalwriteelement(name, values, optional, limits)
    throw(_writeunsupported(T, name))
end

function _leafspecs(schema::Schema, semantic::_NestedSchemaPlan, options::_WriteOptions)
    plans = _writeleafplans(semantic)
    choices = _writeencodingchoices(plans, options.encoding, options.dictionary)
    blooms = _bloomleaves(plans, options.bloomfilter)
    leaves = _LeafSpec[]
    for (node, choice, bloom) in zip(schema.leaves, choices, blooms)
        element = node.element
        push!(leaves, _LeafSpec(element, copy(node.path), Int(node.max_repetition_level),
            Int(node.max_definition_level), _statisticorder(element), choice.encoding,
            choice.dictionary, bloom, _isutf8leaf(element)))
    end
    return leaves
end

function _schemafromfragments(fragments::Vector{Vector{Metadata.SchemaElement}},
        options::_WriteOptions)
    limits = options.limits
    elements = Metadata.SchemaElement[Metadata.SchemaElement(name="schema",
        num_children=Int32(length(fragments)))]
    for fragment in fragments
        append!(elements, fragment)
        _checklimit(:container_elements, length(elements), limits.max_container_elements)
    end
    schema = Schema(elements; limits=limits)
    semantic = _nestedplan(schema; limits=limits)
    return elements, schema, semantic
end

# The schema of the first table, inferred from its column types (and, for a few legacy
# types, values).
function _inferschema(columns::Vector{Pair{String,AbstractVector}}, options::_WriteOptions)
    limits = options.limits
    nullable = options.nullable === true
    fragments = Vector{Metadata.SchemaElement}[]
    shapes = Any[]
    for (name, values) in columns
        occursin('\0', name) && throw(ArgumentError(
            "Parquet top-level field names cannot contain NUL"))
        # Compiled for each column's type: `_barrier`.
        fragment, shape = _barrier(_columnfragment, name, values, nullable,
            limits)::Tuple{Vector{Metadata.SchemaElement},Any}
        push!(fragments, fragment)
        push!(shapes, shape)
    end
    elements, schema, semantic = _schemafromfragments(fragments, options)
    fields = _FieldSpec[]
    for (index, (name, _)) in enumerate(columns)
        range = _nestedleafrange(semantic.root.children[index])
        isempty(range) && throw(ArgumentError("zero-leaf fields cannot be written to Parquet"))
        push!(fields, shapes[index] === nothing ? _FlatField(name, Int(first(range))) :
            _NestedField(name, Int(first(range)):Int(last(range)), fragments[index]))
    end
    return _WriteSchema(elements, schema, semantic, fields,
        _leafspecs(schema, semantic, options), false), shapes
end

# The schema fragment of one input column, and its nested shape (`nothing` when the
# column is flat).
function _columnfragment(name::String, values::AbstractVector, nullable::Bool,
        limits::Limits)
    if _flatwritable(values)
        optional = nullable || Missing <: eltype(values)
        return Metadata.SchemaElement[_flatelement(name, values, optional, limits)], nothing
    end
    shape = _nestedwriteshape(name, eltype(values), values, limits; nullable=nullable)
    if _nestedwriteneedsscan(shape)
        for value in values
            _nestedwritescan!(shape, value)
        end
    end
    return _nestedwriteschema(shape, limits), shape
end

# A Parquet.Table rewrite keeps the table's stored schema exactly.
function _storedschema(table::Table, options::_WriteOptions)
    limits = options.limits
    elements = _storedelements(table)
    schema = Schema(elements; limits=limits)
    semantic = _nestedplan(schema; limits=limits)
    fragments = _writetopfields(elements)
    fields = _FieldSpec[]
    for (index, child) in enumerate(semantic.root.children)
        range = _nestedleafrange(child)
        isempty(range) && throw(ArgumentError(
            "schema-bearing writes do not support zero-leaf group " *
            repr(_plansource(child).element.name)))
        name = _plansource(child).element.name
        push!(fields, child isa _NestedLeafPlan ? _FlatField(name, Int(first(range))) :
            _NestedField(name, Int(first(range)):Int(last(range)), fragments[index]))
    end
    return _WriteSchema(elements, schema, semantic, fields,
        _leafspecs(schema, semantic, options), true)
end

# The schema a rewrite of `table` writes: its stored schema, except that INT96 leaves
# (deprecated, not written) become INT64 TIMESTAMPs, not adjusted to UTC as INT96 values
# are read, in the unit the table's INT96 columns were read in (`int96`).
function _storedelements(table::Table)
    elements = copy(_tablemetadata(table).schema)
    any(element -> element.type_ == Metadata.Type.INT96, elements) || return elements
    unit = Dates.Nanosecond
    for (child, column) in zip(_tableschema(table).root.children, _tablecolumns(table))
        child.element.type_ == Metadata.Type.INT96 || continue
        T = Base.nonmissingtype(eltype(column))
        T <: Durations.Timestamp && (unit = T.parameters[1])
        break
    end
    map = something(_timestampmap(unit, false))
    for (index, element) in enumerate(elements)
        element.type_ == Metadata.Type.INT96 || continue
        elements[index] = Metadata.SchemaElement(type_=map.physical,
            repetition_type=element.repetition_type, name=element.name,
            converted_type=map.converted, logicalType=map.logical,
            field_id=element.field_id)
    end
    return elements
end

## Binding a table's columns to the schema

abstract type _FieldBinding end

struct _FlatBinding{V<:AbstractVector,F} <: _FieldBinding
    column::V
    convert::F
    optional::Bool
end

struct _NestedBinding{V<:AbstractVector} <: _FieldBinding
    column::V
    node::_NestedWriteNode
end

# Converts a value by its leaf's stored annotation, for columns whose Julia type does not
# map to the leaf directly (schema-bearing rewrites, legacy types, widened partitions).
struct _SchemaConvert{K,P}
    kind::K
    element::Metadata.SchemaElement
    limits::Limits
end

function (convert::_SchemaConvert{K,P})(value) where {K,P}
    physical = convert.kind === nothing ? value :
        _physicalvalue(convert.kind, convert.element, value, convert.limits)
    return _schemaphysical(P, physical)
end

# Numbers widen into a leaf only exactly: Int64 into DOUBLE or Float64 into FLOAT would
# round silently.
function _schemaphysical(::Type{P}, value) where {P<:Union{Bool,Int32,Int64,Float32,Float64}}
    converted = convert(P, value)
    value isa Real && !isequal(converted, value) && throw(InexactError(:convert, P, value))
    return converted
end
_schemaphysical(::Type{_BytesBuffer}, value) = value

function _schemaphysicaltype(physical::Metadata.Type.T)
    physical == Metadata.Type.BOOLEAN && return Bool
    physical == Metadata.Type.INT32 && return Int32
    physical == Metadata.Type.INT64 && return Int64
    physical == Metadata.Type.FLOAT && return Float32
    physical == Metadata.Type.DOUBLE && return Float64
    return _BytesBuffer
end

function _flatconverter(leaf::_LeafSpec, values::AbstractVector, limits::Limits)
    T = Base.nonmissingtype(eltype(values))
    T === Union{} && return identity
    map = _nestedwriteleafmap(leaf.element, T)
    map === nothing || return map.convert
    kind = _logicalkind(leaf.element)
    return _SchemaConvert{typeof(kind),_schemaphysicaltype(leaf.element.type_)}(kind,
        leaf.element, limits)
end

function _bindflat(schema::_WriteSchema, field::_FlatField, values::AbstractVector,
        options::_WriteOptions)
    leaf = schema.leaves[field.leaf]
    optional = leaf.element.repetition_type == Metadata.FieldRepetitionType.OPTIONAL
    eltype(values) === Missing && !optional && _requiredmissing(field.name)
    return _FlatBinding(values, _flatconverter(leaf, values, options.limits), optional)
end

# What a leaf's Parquet type cannot say about values of type `T`, for the ARROW:schema:
# a zoned timestamp's time zone, the TimeUnit of a period stored as an INT64 count
# (seconds for Minute through Week), or the 128-bit integer type of FLBA(16) bytes.
_arrowhint(::Type) = nothing
_arrowhint(::Type{Durations.ZonedTimestamp{P,Z}}) where {P,Z} = String(Z)
_arrowhint(::Type{Dates.Nanosecond}) = Int16(3)
_arrowhint(::Type{Dates.Microsecond}) = Int16(2)
_arrowhint(::Type{Dates.Millisecond}) = Int16(1)
_arrowhint(::Type{<:Union{Dates.Second,Dates.Minute,Dates.Hour,Dates.Day,Dates.Week}}) =
    Int16(0)
_arrowhint(::Type{Int128}) = Int128
_arrowhint(::Type{UInt128}) = UInt128

function _writearrowhint!(hints::Dict{Vector{String},Any}, path::Vector{String}, T::Type)
    hint = _arrowhint(Base.nonmissingtype(T))
    hint === nothing || (hints[path] = hint)
    return hints
end

_writearrowhints!(hints, schema::_WriteSchema, field::_FlatField, binding::_FlatBinding) =
    _writearrowhint!(hints, schema.leaves[field.leaf].path, eltype(binding.column))

_writearrowhints!(hints, ::_WriteSchema, ::_FieldSpec, binding::_NestedBinding) =
    _writearrowhints!(hints, binding.node)

function _writearrowhints!(hints, node::_NestedWriteNode)
    plan = node.plan
    plan isa _NestedLeafPlan &&
        return _writearrowhint!(hints, plan.source.path, node.value_type)
    foreach(child -> _writearrowhints!(hints, child), node.children)
    return hints
end

_nestedbinding(values::AbstractVector, shape, plan) =
    _NestedBinding(values, _nestedwritebind(shape, plan))

# Binds one column of a later table: its declared type must give the field's schema.
function _bindcolumn(schema::_WriteSchema, index::Int, name::String,
        values::AbstractVector, shape, options::_WriteOptions)
    field = schema.fields[index]
    field.name == name || throw(ArgumentError("column $index is $(repr(name)) but the " *
        "file's column $index is $(repr(field.name))"))
    plan = schema.semantic.root.children[index]
    if schema.stored
        field isa _FlatField && return _bindflat(schema, field, values, options)
        return _NestedBinding(values, _provenancenode(plan))
    end
    if field isa _FlatField
        _flatwritable(values) || throw(ArgumentError("column $(repr(name)) holds " *
            "$(eltype(values)) values but the file's column holds scalars"))
        return _bindflat(schema, field, values, options)
    end
    if shape === nothing
        _flatwritable(values) && throw(ArgumentError("column $(repr(name)) holds " *
            "scalars but the file's column is nested"))
        shape = _nestedwriteshape(name, eltype(values), values, options.limits;
            nullable=options.nullable === true)
        _nestedwriteneedsscan(shape) && foreach(value -> _nestedwritescan!(shape, value),
            values)
        _nestedwriteschema(shape, options.limits) == field.fragment ||
            throw(ArgumentError("column $(repr(name)) has a different nested type than " *
                "the first table written"))
    end
    return _NestedBinding(values, _nestedwritebind(shape, plan))
end

## Encoding one field of one row group

function _encodefield(schema::_WriteSchema, field::_FlatField, binding::_FlatBinding,
        range::UnitRange{Int}, options::_WriteOptions)
    leaf = schema.leaves[field.leaf]
    offset = firstindex(binding.column) - 1
    if binding.column isa _PooledColumn
        chunk = _encodepooledchunk(leaf, binding.column, offset + first(range),
            offset + last(range), binding.convert, binding.optional, field.name,
            options.chunk)
        chunk === nothing || return _EncodedChunk[chunk]
    end
    segments = _segments(binding.column, offset + first(range), offset + last(range))
    segments === nothing || return _EncodedChunk[_encodesegments(leaf, segments,
        binding.convert, binding.optional, field.name, options.chunk)]
    rows = _FlatRows(binding.column, offset + first(range), offset + last(range),
        binding.convert, binding.optional, field.name)
    return _EncodedChunk[_encodechunk(leaf, rows, options.chunk)]
end

# The parts of a chunked vector that hold rows `first:last`, as (vector, first, last)
# triples, so the encoder reads each part directly instead of finding the part of every
# row (the SentinelArrays extension defines it for ChainedVector).
_segments(column, first::Int, last::Int) = nothing

function _encodesegments(leaf::_LeafSpec, segments::Vector, convert, optional::Bool,
        name::String, options::_ChunkOptions)
    chunk = _ChunkWriter(leaf, options, _encodescratch())
    # The chunk's buffers take the leaf's physical type, known at run time.
    return _barrier(_encodesegments!, chunk, segments, convert, optional,
        name)::_EncodedChunk
end

function _encodesegments!(chunk::_ChunkWriter, segments::Vector, convert, optional::Bool,
        name::String)
    base = 0
    for (vector, first, last) in segments
        _writerows!(chunk, _FlatRows(vector, first, last, convert, optional, name, base))
        base += last - first + 1
    end
    return _finishchunk(chunk)
end

Base.@nospecializeinfer function _encodefield(schema::_WriteSchema, field::_NestedField,
        binding::_NestedBinding, range::UnitRange{Int}, options::_WriteOptions)
    Base.@nospecialize binding
    offset = firstindex(binding.column) - 1
    return _encodenested(schema.semantic, binding.node, binding.column,
        (offset + first(range)):(offset + last(range)),
        _LeafSpec[schema.leaves[index] for index in field.leaves], options.chunk)
end

function _encodefielderror(field::_FieldSpec, err, range::UnitRange{Int}, offset::Int,
        indices::Union{Nothing,Vector{Int}})
    if err isa _InvalidUTF8
        row = first(range) + err.row - 1
        row = offset + (indices === nothing ? row : indices[row])
        leaf = join(err.leaf, ".")
        where = leaf == field.name ? "" : " (field $(repr(leaf)))"
        return ArgumentError("column $(repr(field.name))$where has a string at row $row " *
            "that is not valid UTF-8; STRING values must be UTF-8 text, so write binary " *
            "data as Vector{UInt8} values")
    end
    err isa Union{ArgumentError,InexactError,OverflowError} || return err
    message = err isa ArgumentError ? err.msg : sprint(showerror, err)
    occursin(repr(field.name), message) && return err
    return _writevalueerror(field.name, err)
end

## Ordered parallel work

# Runs `work(i)` for i in 1:count on up to `ntasks` tasks and hands the results to
# `consume(i, result)` in order on the calling task. A task starts job `i` only once job
# `i - 4ntasks` is consumed, so few results wait for `consume` while one slow job does
# not stall the other tasks (a window of `ntasks` leaves about half of them idle).
mutable struct _OrderedRun
    @atomic claimed::Int
    consumed::Int
    failure::Any
    results::Vector{Any}
    condition::Threads.Condition
end

function _runordered(work, consume, count::Int, ntasks::Int)
    if ntasks <= 1 || count <= 1
        for index in 1:count
            consume(index, work(index))
        end
        return
    end
    run = _OrderedRun(0, 0, nothing, Vector{Any}(nothing, count), Threads.Condition())
    window = 4 * ntasks
    function worker()
        while true
            index = @atomic run.claimed += 1
            index > count && return
            lock(run.condition) do
                while index > run.consumed + window && run.failure === nothing
                    wait(run.condition)
                end
            end
            run.failure === nothing || return
            result = try
                work(index)
            catch err
                lock(run.condition) do
                    run.failure === nothing && (run.failure = CapturedException(err,
                        catch_backtrace()))
                    notify(run.condition)
                end
                return
            end
            lock(run.condition) do
                run.results[index] = Some(result)
                notify(run.condition)
            end
        end
    end
    tasks = [errormonitor(Threads.@spawn worker()) for _ in 1:min(ntasks, count)]
    try
        for index in 1:count
            result = lock(run.condition) do
                while run.results[index] === nothing && run.failure === nothing
                    wait(run.condition)
                end
                value = run.results[index]
                run.results[index] = nothing
                value === nothing || (run.consumed = index; notify(run.condition))
                return value
            end
            result === nothing && break
            consume(index, something(result))
        end
    catch
        lock(run.condition) do
            run.failure === nothing && (run.failure = :stopped)
            notify(run.condition)
        end
        foreach(wait, tasks)
        rethrow()
    end
    foreach(wait, tasks)
    failure = run.failure
    failure isa CapturedException && throw(failure.ex)
    return
end

## The writer

struct _ChunkRecord
    meta::Metadata.ColumnMetaData
    columnindex::Union{Nothing,Metadata.ColumnIndex}
    offsetindex::Metadata.OffsetIndex
    # A bloom filter waiting to be written after its row group's column chunks.
    bloom::Vector{UInt8}
end

struct _GroupRecord
    chunks::Vector{_ChunkRecord}
    rows::Int64
    offset::Int64
    uncompressed::Int64
    compressed::Int64
end

"""
    Parquet.Writer(sink; kwargs...)
    Parquet.Writer(f, sink; kwargs...)

A Parquet file written incrementally to `sink`, a path or `IO`. Each
`Parquet.write(writer, table)` appends the table's rows as row groups; `close(writer)`
writes the footer. The first table fixes the schema; every later table must have the same
column names in the same order, and its values must convert to the first table's column
types exactly (an `Int32` column into an `Int64` one, for example). Every column is
nullable unless `nullable=false`, so later tables may hold `missing`. A column that is only
`missing` in the first table takes the scalar type of the first later table that gives it
one.

The function form passes the writer to `f` and closes it when `f` returns; when `f` throws,
the output is abandoned (a path sink is removed). Keyword arguments are those of
[`Parquet.write`](@ref), except `partitionby` and `existing`, plus `nullable`.

A path sink is written as [`Parquet.write`](@ref) writes one: to a temporary file that
replaces the file when the writer closes, so an existing file stays intact until then.
"""
mutable struct Writer{S<:IO}
    io::S
    # The sink path as given, and the file a temporary file replaces at close (symlinks
    # resolved); both `nothing` for an IO sink, `temporary` also for a device or pipe.
    path::Union{Nothing,String}
    target::Union{Nothing,String}
    temporary::Union{Nothing,String}
    options::_WriteOptions
    schema::Union{Nothing,_WriteSchema}
    groups::Vector{_GroupRecord}
    offset::Int64
    rows::Int64
    metadata::Union{Nothing,Vector{Metadata.KeyValue}}
    colmetadata::Dict{String,Vector{Metadata.KeyValue}}
    # Leaf path => time zone or duration unit for the ARROW:schema, from the first table.
    arrowhints::Dict{Vector{String},Any}
    closed::Bool
end

# The leading magic is written with the first column chunk (or by `close`), so a table
# rejected before any chunk is encoded leaves an IO sink untouched.
function _newwriter(io::IO, path, target, temporary, options::_WriteOptions)
    return Writer(io, path, target, temporary, options, nothing, _GroupRecord[], Int64(0),
        Int64(0), nothing, Dict{String,Vector{Metadata.KeyValue}}(),
        Dict{Vector{String},Any}(), false)
end

function Writer(io::IO; nullable::Bool=true, kwargs...)
    return _newwriter(io, nothing, nothing, nothing,
        _writeoptions(; nullable=nullable, kwargs...))
end

function Writer(path::AbstractString; nullable::Bool=true, kwargs...)
    return _openwriter(path, _writeoptions(; nullable=nullable, kwargs...))
end

# A file is written to a hidden temporary file beside the file it replaces (through
# symlinks), which keeps that file's mode and is renamed over it at close. An existing
# read-only file is refused as `open(path, "w")` would refuse it. Devices and pipes are
# written in place; a directory is never replaced.
function _openwriter(path::AbstractString, options::_WriteOptions)
    path = String(path)
    isdir(path) && throw(ArgumentError("cannot write a Parquet file to $(repr(path)): it " *
        "is a directory. To write a partitioned dataset into it, pass partitionby to " *
        "Parquet.write"))
    ispath(path) && !isfile(path) &&
        return _newwriter(open(path, "w"), path, nothing, nothing, options)
    existing = isfile(path)
    existing && close(open(path, "r+"))
    target = existing ? realpath(path) : islink(path) ?
        normpath(joinpath(dirname(abspath(path)), readlink(path))) : abspath(path)
    temporary = joinpath(dirname(target),
        "." * basename(target) * "." * string(rand(UInt64); base=36) * ".partial")
    io = try
        open(temporary, "w")
    catch err
        # A missing or unwritable directory: name the path the caller gave.
        err isa SystemError && throw(SystemError("opening file $(repr(path))", err.errnum))
        rethrow()
    end
    try
        existing && chmod(temporary, filemode(target) & 0o7777)
        return _newwriter(io, path, target, temporary, options)
    catch
        close(io)
        rm(temporary; force=true)
        rethrow()
    end
end

function Writer(f::Function, sink::Union{IO,AbstractString}; kwargs...)
    writer = Writer(sink; kwargs...)
    try
        f(writer)
    catch
        _abandon!(writer)
        rethrow()
    end
    close(writer)
    return sink
end

function _abandon!(writer::Writer)
    writer.closed && return
    writer.closed = true
    writer.path === nothing && return
    close(writer.io)
    writer.temporary === nothing || rm(writer.temporary; force=true)
    return
end

function Base.show(io::IO, writer::Writer)
    print(io, "Parquet.Writer(", writer.path === nothing ? "IO" : repr(writer.path), ", ",
        writer.rows, " rows in ", length(writer.groups), " row groups",
        writer.closed ? ", closed" : "", ")")
    return
end

"""
    Parquet.write(writer::Parquet.Writer, table) -> writer

Append the rows of `table` (every partition of a `Tables.partitions` source) to `writer`.
"""
function write(writer::Writer, table)
    writer.closed && throw(ArgumentError("the Parquet.Writer is closed"))
    offset = 0
    try
        for part in Tables.partitions(table)
            offset += _writetable!(writer, part, table, offset)
        end
    catch
        _abandon!(writer)
        rethrow()
    end
    return writer
end

# The columns of a Parquet.Table to rewrite, and its row count.
function _storedcolumns(table::Table)
    names = String[String(name) for name in Tables.columnnames(table)]
    vectors = AbstractVector[column for column in values(_tablecolumns(table))]
    rows = _tablerows(table)
    for (name, vector) in zip(names, vectors)
        length(vector) == rows || throw(ArgumentError("Parquet.Table column " *
            "$(repr(name)) has $(length(vector)) values for $rows rows"))
    end
    return Pair{String,AbstractVector}[name => vector
        for (name, vector) in zip(names, vectors)], rows
end

# Writes the rows of `table` and returns their count. Row r of `table` is row `offset + r`
# of the input (`offset + indices[r]` when `table` holds rows `indices` of a partition),
# which is the row an error names.
Base.@nospecializeinfer function _writetable!(writer::Writer, table, source, offset::Int=0,
        indices::Union{Nothing,Vector{Int}}=nothing)
    Base.@nospecialize table source
    options = writer.options
    stored = table isa Table
    # One assignment of `rows`: the encoding closures below capture it unboxed.
    columns, rows = (stored ? _storedcolumns(table) : _writeinputcolumns(table,
        options.limits))::Tuple{Vector{Pair{String,AbstractVector}},Int}
    shapes = nothing
    isfirst = writer.schema === nothing
    if isfirst
        if stored
            writer.schema = _storedschema(table, options)
        else
            writer.schema, shapes = _inferschema(columns, options)
        end
        _resolvemetadata!(writer, source, table, columns)
    elseif writer.schema.stored || stored
        writer.schema.stored && stored &&
            _storedelements(table) == writer.schema.elements ||
            throw(ArgumentError("a Parquet.Writer that starts with a Parquet.Table " *
                "accepts only Parquet.Table inputs with the same stored schema, and the " *
                "reverse; write Tables.columntable(table) instead"))
    end
    length(columns) == length(writer.schema.fields) ||
        _barrier(_bindmismatch, writer.schema, columns)
    retyped = isfirst || writer.schema.stored ? Int[] : _retypenullcolumns!(writer, columns)
    # Read after retyping, which replaces it.
    schema = writer.schema::_WriteSchema
    # Each column binds and encodes behind `_barrier`, compiled for its own type.
    bindings = _FieldBinding[]
    for (index, (name, values)) in enumerate(columns)
        if shapes !== nothing && shapes[index] !== nothing
            push!(bindings, _barrier(_nestedbinding, values, shapes[index],
                schema.semantic.root.children[index])::_FieldBinding)
        else
            push!(bindings, _barrier(_bindcolumn, schema, index, name, values, nothing,
                options)::_FieldBinding)
        end
    end
    if isfirst
        stored && merge!(writer.arrowhints, _arrowhints(_tablemetadata(table),
            _tableschema(table), options.limits))
        for (field, binding) in zip(schema.fields, bindings)
            _barrier(_writearrowhints!, writer.arrowhints, schema, field, binding)
        end
    end
    for index in retyped
        _barrier(_writearrowhints!, writer.arrowhints, schema, schema.fields[index],
            bindings[index])
    end
    rows == 0 && return rows
    size = something(options.rowgroupsize, rows)
    groups = cld(rows, size)
    fieldcount = length(schema.fields)
    pending = _ChunkRecord[]
    groupstart = Ref(Int64(0))
    jobs = _encodejobs(schema, rows, size)
    work = function (job)
        group, fields = jobs[job]
        first = (group - 1) * size + 1
        range = first:min(rows, group * size)
        return map(fields) do fieldindex
            field = schema.fields[fieldindex]
            try
                return _barrier(_encodefield, schema, field, bindings[fieldindex], range,
                    options)::Vector{_EncodedChunk}
            catch err
                # Error paths stay out of the encoding's inference.
                throw(_barrier(_encodefielderror, field, err, range, offset, indices))
            end
        end
    end
    consume = function (job, results::Vector{Vector{_EncodedChunk}})
        group, fields = jobs[job]
        for (fieldindex, chunks) in zip(fields, results)
            _writemagic!(writer)
            fieldindex == 1 && (groupstart[] = writer.offset)
            field = schema.fields[fieldindex]
            leaves = field isa _FlatField ? (field.leaf:field.leaf) : field.leaves
            for (leafindex, chunk) in zip(leaves, chunks)
                push!(pending, _writechunk!(writer, schema.leaves[leafindex], chunk,
                    field.name))
            end
            fieldindex == fieldcount || continue
            _writeblooms!(writer, pending)
            grouprows = min(rows, group * size) - (group - 1) * size
            uncompressed = sum(record.meta.total_uncompressed_size for record in pending)
            compressed = sum(record.meta.total_compressed_size for record in pending)
            push!(writer.groups, _GroupRecord(copy(pending), Int64(grouprows),
                groupstart[], uncompressed, compressed))
            writer.rows += grouprows
            empty!(pending)
        end
        return
    end
    _runordered(work, consume, length(jobs), options.ntasks)
    return rows
end

# A column that held only missing so far was written as a null (UNKNOWN) column. The first
# later table that gives it a scalar type sets its type: the earlier row groups hold only
# nulls, whose pages are the same for every physical type, so only their chunk metadata
# changes. Returns the indexes of the retyped columns.
function _retypenullcolumns!(writer::Writer, columns)
    schema = writer.schema
    options = writer.options
    retyped = Int[]
    elements = Metadata.SchemaElement[first(schema.elements)]
    for (index, (field, (name, values))) in enumerate(zip(schema.fields, columns))
        if field isa _NestedField
            append!(elements, field.fragment)
            continue
        end
        element = schema.leaves[field.leaf].element
        logical = element.logicalType
        if logical !== nothing && logical.UNKNOWN !== nothing
            # The column's type is known at run time.
            typed = _barrier(_nullcolumnelement, name, values,
                options.limits)::Union{Nothing,Metadata.SchemaElement}
            typed === nothing || (element = typed; push!(retyped, index))
        end
        push!(elements, element)
    end
    isempty(retyped) && return retyped
    tree = Schema(elements; limits=options.limits)
    semantic = _nestedplan(tree; limits=options.limits)
    leaves = _leafspecs(tree, semantic, options)
    for group in writer.groups, (index, record) in enumerate(group.chunks)
        type = leaves[index].element.type_
        meta = record.meta
        meta.type_ == type && continue
        group.chunks[index] = _ChunkRecord(_withfields(meta; type_=type),
            record.columnindex, record.offsetindex, record.bloom)
    end
    writer.schema = _WriteSchema(elements, tree, semantic, schema.fields, leaves, false)
    return retyped
end

# The element of a null column that a later table's `values` give a scalar type, or
# `nothing` while they are only missing.
function _nullcolumnelement(name::String, values::AbstractVector, limits::Limits)
    eltype(values) === Missing && return nothing
    _flatwritable(values) || throw(ArgumentError("column $(repr(name)) was only " *
        "missing in the first table, so it was written as a null column, which " *
        "cannot become nested; give it its type in the first table, for example " *
        "Union{Missing,Vector{Int}}[missing, ...]"))
    return _flatelement(name, values, true, limits)
end

# A job encodes a run of consecutive fields of one row group: a large field alone, small
# ones together until they hold _JOB_VALUES leaf values, so a table of many short columns
# does not pay a task handoff per column.
const _JOB_VALUES = 1 << 20

function _encodejobs(schema::_WriteSchema, rows::Int, size::Int)
    jobs = Tuple{Int,UnitRange{Int}}[]
    fieldcount = length(schema.fields)
    for group in 1:cld(rows, size)
        grouprows = min(rows, group * size) - (group - 1) * size
        start = 1
        values = 0
        for fieldindex in 1:fieldcount
            field = schema.fields[fieldindex]
            values += grouprows * (field isa _FlatField ? 1 : length(field.leaves))
            if values >= _JOB_VALUES || fieldindex == fieldcount
                push!(jobs, (group, start:fieldindex))
                start = fieldindex + 1
                values = 0
            end
        end
    end
    return jobs
end

@noinline function _bindmismatch(schema::_WriteSchema, columns)
    names = [field.name for field in schema.fields]
    given = [first(column) for column in columns]
    throw(ArgumentError("the table has columns $(given) but the file's columns are " *
        "$(names)"))
end

# Bloom filters follow their row group's column chunks (parquet-format BloomFilter.md
# allows them between row groups), so the writer holds one row group's filters at most.
function _writeblooms!(writer::Writer, records::Vector{_ChunkRecord})
    for (index, record) in enumerate(records)
        isempty(record.bloom) && continue
        offset = writer.offset
        Base.write(writer.io, record.bloom)
        writer.offset += length(record.bloom)
        records[index] = _ChunkRecord(_withbloom(record.meta, offset,
            Int32(length(record.bloom))), record.columnindex, record.offsetindex, UInt8[])
    end
    return records
end

_withbloom(meta::Metadata.ColumnMetaData, offset::Int64, length::Int32) =
    Metadata.ColumnMetaData((name === :bloom_filter_offset ? offset :
        name === :bloom_filter_length ? length : getfield(meta, name)
        for name in fieldnames(Metadata.ColumnMetaData))...)

function _writemagic!(writer::Writer)
    iszero(writer.offset) || return
    Base.write(writer.io, PARQUET_MAGIC)
    writer.offset = length(PARQUET_MAGIC)
    return
end

function _writechunk!(writer::Writer, leaf::_LeafSpec, chunk::_EncodedChunk,
        name::String)
    _writemagic!(writer)
    start = writer.offset
    io = writer.io
    isempty(chunk.dictionary) || Base.write(io, chunk.dictionary)
    Base.write(io, chunk.data)
    dataoffset = start + length(chunk.dictionary)
    writer.offset = dataoffset + length(chunk.data)
    meta = Metadata.ColumnMetaData(
        type_=leaf.element.type_,
        encodings=chunk.encodings,
        path_in_schema=leaf.path,
        codec=writer.options.chunk.codec,
        num_values=chunk.values,
        total_uncompressed_size=chunk.uncompressed,
        total_compressed_size=Int64(length(chunk.dictionary) + length(chunk.data)),
        key_value_metadata=get(writer.colmetadata, name, nothing),
        data_page_offset=dataoffset,
        dictionary_page_offset=isempty(chunk.dictionary) ? nothing : start,
        statistics=chunk.statistics,
        encoding_stats=chunk.encodingstats,
    )
    locations = Metadata.PageLocation[Metadata.PageLocation(offset=dataoffset + page.offset,
        compressed_page_size=page.size, first_row_index=page.firstrow)
        for page in chunk.pages]
    return _ChunkRecord(meta, chunk.columnindex,
        Metadata.OffsetIndex(page_locations=locations), chunk.bloom)
end

"""
    close(writer::Parquet.Writer)

Write the page indexes and footer. A writer to a path then renames its temporary file
over the file it replaces; a writer to an `IO` leaves the `IO` open.
"""
function Base.close(writer::Writer)
    writer.closed && return
    try
        _finishfile!(writer)
    catch
        _abandon!(writer)
        rethrow()
    end
    writer.closed = true
    if writer.path === nothing
        flush(writer.io)
        return
    end
    close(writer.io)
    temporary = writer.temporary
    temporary === nothing && return
    # A rename replaces the file in one step, and fails rather than replace a directory.
    err = ccall(:jl_fs_rename, Int32, (Cstring, Cstring), temporary, writer.target)
    err < 0 || return
    rm(temporary; force=true)
    hint = Sys.iswindows() ? " (close programs that have it open, then retry)" : ""
    throw(Base.IOError("Parquet could not replace $(repr(writer.path)); it is " *
        "unchanged$hint", err))
end

function _finishfile!(writer::Writer)
    schema = writer.schema
    schema === nothing && throw(ArgumentError(
        "a Parquet.Writer was closed before any table was written, so the file has no " *
        "schema"))
    options = writer.options
    limits = options.limits
    _writemagic!(writer)
    pageindex = options.chunk.pageindex
    columnindexes = Dict{Tuple{Int,Int},Tuple{Int64,Int32}}()
    offsetindexes = Dict{Tuple{Int,Int},Tuple{Int64,Int32}}()
    if pageindex
        for (group, record) in enumerate(writer.groups), (column, chunk) in enumerate(record.chunks)
            chunk.columnindex === nothing && continue
            columnindexes[(group, column)] = _writeindex!(writer, chunk.columnindex, limits)
        end
        for (group, record) in enumerate(writer.groups), (column, chunk) in enumerate(record.chunks)
            offsetindexes[(group, column)] = _writeindex!(writer, chunk.offsetindex, limits)
        end
    end
    ordinals = length(writer.groups) <= Int(typemax(Int16)) + 1
    rowgroups = Metadata.RowGroup[]
    for (group, record) in enumerate(writer.groups)
        columns = Metadata.ColumnChunk[]
        for (column, chunk) in enumerate(record.chunks)
            columnindex = get(columnindexes, (group, column), nothing)
            offsetindex = get(offsetindexes, (group, column), nothing)
            push!(columns, Metadata.ColumnChunk(file_offset=Int64(0), meta_data=chunk.meta,
                offset_index_offset=offsetindex === nothing ? nothing : offsetindex[1],
                offset_index_length=offsetindex === nothing ? nothing : offsetindex[2],
                column_index_offset=columnindex === nothing ? nothing : columnindex[1],
                column_index_length=columnindex === nothing ? nothing : columnindex[2]))
        end
        push!(rowgroups, Metadata.RowGroup(columns=columns,
            total_byte_size=record.uncompressed, num_rows=record.rows,
            file_offset=record.offset, total_compressed_size=record.compressed,
            ordinal=ordinals ? Int16(group - 1) : nothing))
    end
    metadata = Metadata.FileMetaData(
        version=Int32(1),
        schema=schema.elements,
        num_rows=writer.rows,
        row_groups=rowgroups,
        key_value_metadata=_withschema(writer, schema),
        created_by=_createdby(),
        column_orders=options.chunk.statistics ? _writecolumnorders(schema.schema) : nothing,
    )
    footer = _writeencodefooter(metadata, limits)
    io = writer.io
    Base.write(io, footer)
    Base.write(io, htol(UInt32(length(footer))))
    Base.write(io, PARQUET_MAGIC)
    writer.offset += length(footer) + 4 + length(PARQUET_MAGIC)
    return
end

# The file's key-value metadata ends with an ARROW:schema generated from the written
# schema, so Arrow readers recover time zones and durations the Parquet types cannot hold.
# It repeats the file and column metadata, which pyarrow reads from there. A stored schema
# with an annotation this package cannot read gets none.
function _withschema(writer::Writer, schema::_WriteSchema)
    metadata = writer.metadata
    arrowpairs(entries) = Pair{String,String}[entry.key => something(entry.value, "")
        for entry in something(entries, Metadata.KeyValue[])]
    arrow = try
        _arrowschema(schema.schema; hints=writer.arrowhints, metadata=arrowpairs(metadata),
            fieldmetadata=Dict(name => arrowpairs(entries)
                for (name, entries) in writer.colmetadata))
    catch err
        err isa Union{FormatError,UnsupportedFeatureError} || rethrow()
        nothing
    end
    arrow === nothing && return metadata
    entries = metadata === nothing ? Metadata.KeyValue[] : copy(metadata)
    push!(entries, Metadata.KeyValue(key="ARROW:schema", value=arrow))
    return entries
end

function _writeindex!(writer::Writer, index, limits::Limits)
    bytes = Thrift.encode(index)
    _checklimit(:page_index_bytes, length(bytes), limits.max_page_index_bytes)
    length(bytes) <= typemax(Int32) || throw(LimitError(:page_index_bytes,
        Int64(length(bytes)), Int64(typemax(Int32))))
    offset = writer.offset
    Base.write(writer.io, bytes)
    writer.offset += length(bytes)
    return offset, Int32(length(bytes))
end

## Key-value metadata

function _keyvalues(entries, what::String)
    entries === nothing && return nothing
    output = Metadata.KeyValue[]
    for (key, value) in (entries isa NamedTuple ? pairs(entries) : entries)
        name = String(string(key))::String
        name == "ARROW:schema" && throw(ArgumentError(
            "the ARROW:schema $what key is reserved for the writer"))
        push!(output, Metadata.KeyValue(key=name,
            value=value === nothing ? nothing : String(string(value))::String))
    end
    return isempty(output) ? nothing : output
end

# Compiled once for every input type; the DataAPI methods of the input's type are
# called behind `_barrier`.
Base.@nospecializeinfer function _inputmetadata(table)
    Base.@nospecialize table
    table isa Table && return nothing
    _barrier(DataAPI.metadatasupport, typeof(table)).read || return nothing
    entries = Pair{String,String}[]
    for key in _barrier(DataAPI.metadatakeys, table)
        name = String(string(key))::String
        name in _SOURCE_SCHEMA_KEYS && continue
        push!(entries, name => string(_barrier(DataAPI.metadata, table, key))::String)
    end
    return entries
end

Base.@nospecializeinfer function _inputcolmetadata(table, names::Vector{String})
    Base.@nospecialize table
    _barrier(DataAPI.colmetadatasupport, typeof(table)).read || return nothing
    output = Dict{String,Vector{Pair{String,String}}}()
    for name in names
        column = Symbol(name)
        entries = Pair{String,String}[String(string(key))::String =>
            string(_barrier(DataAPI.colmetadata, table, column, key))::String
            for key in _barrier(DataAPI.colmetadatakeys, table,
                column)]::Vector{Pair{String,String}}
        isempty(entries) || (output[name] = entries)
    end
    return output
end

Base.@nospecializeinfer function _resolvemetadata!(writer::Writer, source, table, columns)
    Base.@nospecialize source table
    options = writer.options
    limits = options.limits
    if options.metadata === nothing
        if table isa Table
            writer.metadata = _writekeyvaluemetadata(table, limits)
        else
            # The input's metadata methods are found for its type at run time.
            entries = _barrier(_inputmetadata, source)
            entries === nothing && source !== table &&
                (entries = _barrier(_inputmetadata, table))
            writer.metadata = _keyvalues(entries, "metadata")
        end
    else
        writer.metadata = _keyvalues(options.metadata, "metadata")
    end
    names = String[first(column) for column in columns]
    colmetadata = options.colmetadata
    if colmetadata === nothing
        colmetadata = _barrier(_inputcolmetadata, source, names)
        colmetadata === nothing && source !== table &&
            (colmetadata = _barrier(_inputcolmetadata, table, names))
    end
    if colmetadata !== nothing
        for (key, entries) in (colmetadata isa NamedTuple ? pairs(colmetadata) : colmetadata)
            name = String(string(key))::String
            name in names || throw(ArgumentError(
                "colmetadata names $(repr(name)), which is not a column"))
            values = _keyvalues(entries, "colmetadata")
            values === nothing || (writer.colmetadata[name] = values)
        end
    end
    return
end

## One-shot writes

"""
    Parquet.write(sink, table; kwargs...) -> sink
    Parquet.write(sink; kwargs...) -> table -> Parquet.write(sink, table; kwargs...)

Write a Tables.jl source to `sink`, a path or `IO`, and return `sink`. Row groups are
encoded and written one at a time, so memory stays near the size of one row group. Each
partition of a `Tables.partitions` source is written as one or more row groups, with
every column nullable and later partitions converted to the first partition's types; a
single table keeps the nullability of its element types. The one-argument form returns a
function, so a table can be piped: `CSV.File(path) |> Parquet.write("out.parquet")`.
A path sink is written to a temporary file beside the file it replaces and renamed over it
at the end, so a failed write leaves an existing file intact. An existing file keeps its
permissions, a symlink is written through, a read-only file is refused, and a directory is
an error (pass `partitionby` to write a dataset into it).

Keywords and defaults (pyarrow's):

- `compress=:snappy`: `:snappy`, `:zstd`, `:gzip`, `:lz4` (LZ4_RAW), `:brotli`, or
  `nothing`. `compresslevel` sets the codec's level.
- `dictionary=true`: dictionary encoding for each column chunk while its dictionary stays
  below 1 MiB and it makes the chunk's first page smaller; otherwise PLAIN. A pooled
  column (`DataAPI.refpool`, as in PooledArray and CategoricalArray) writes the pool
  entries its rows use as the dictionary.
- `encoding=nothing`: an encoding Symbol or string for every column, or a `Pair`,
  `NamedTuple`, or dictionary from column names (or leaf paths or ordinals) to encodings;
  `:dictionary` requests dictionary encoding for a column.
- `rowgroupsize=1_048_576` rows (`nothing`: one row group per partition) and
  `pagesize=1_048_576` bytes; a data page also holds at most 20,000 rows
  (`pagesize=nothing`: one page per column chunk).
- `pageversion=:v1` or `:v2`; `statistics=true`; `pageindex=true` (column and offset
  indexes); `checksum=false` (page CRC32).
- `bloomfilter=false`: `true` for every column, or column names (or leaf path tuples or
  ordinals) whose column chunks get split-block bloom filters. Each filter is sized from
  its chunk's distinct values for a 1% false-positive rate. BOOLEAN columns get none.
- `metadata`, `colmetadata`: file and column key-value metadata, by default the table's
  DataAPI metadata. Column metadata is stored with each of the column's chunks. The
  writer adds an `ARROW:schema` entry generated from the written schema (time zones of
  zoned timestamps, units of `Dates` periods); that key is reserved.
- `ntasks=Threads.nthreads()`: tasks encoding column chunks in parallel; the output does
  not depend on it.
- `limits=Parquet.Limits()`.
- `partitionby`: column names; `sink` is then a directory that gets a Hive-style dataset
  (`sink/year=2024/month=1/part-0.parquet`), with one file per distinct key holding that
  key's rows without the key columns. Keys are integers, floats, Bools, `Date`s,
  timestamps, or strings, written and percent-encoded as pyarrow does; `missing` is
  `__HIVE_DEFAULT_PARTITION__`, and an empty string is an empty value (`key=`) that
  reads back as `""`. A non-empty `sink` is an error unless `existing=:delete_matching`,
  which first deletes the partitions the write produces and keeps the others. Each input
  partition adds row groups to its keys' files; at most 128 files are open at once, so a
  key may continue in `part-1.parquet` and so on. Files appear complete or not at all,
  but a failed write can leave some written.

Writing a `Parquet.Table` keeps its stored schema and file key-value metadata, except the
entries that describe the source's schema (`ARROW:schema`, which is generated again,
`pandas`, and Spark's row metadata).
"""
function write(sink::Union{IO,AbstractString}, table; partitionby=nothing,
        existing=nothing, kwargs...)
    if partitionby !== nothing
        sink isa AbstractString || throw(ArgumentError(
            "partitionby writes a directory, so the sink must be a path"))
        _writepartitioned(String(sink), table, partitionby, something(existing, :error),
            values(kwargs))
        return sink
    end
    existing === nothing || throw(ArgumentError(
        "existing applies only to partitioned writes (partitionby)"))
    nullable = Tables.partitions(table) isa Tuple{Any} ? nothing : true
    options = _writeoptions(; nullable=nullable, kwargs...)
    writer = sink isa IO ? _newwriter(sink, nothing, nothing, nothing, options) :
        _openwriter(sink, options)
    write(writer, table)
    if writer.schema === nothing
        _abandon!(writer)
        throw(ArgumentError("Parquet.write needs a table with at least one partition"))
    end
    close(writer)
    return sink
end

function write(sink::Union{IO,AbstractString}; kwargs...)
    return table -> write(sink, table; kwargs...)
end

# Bytes of a file written with the defaults the package tests were written against.
function _encodefile(table; checksum::Bool=true, dictionary::Bool=false,
        codec=:uncompressed, compressionlevel=nothing, kwargs...)
    io = IOBuffer()
    write(io, table; checksum=checksum, dictionary=dictionary, compress=codec,
        compresslevel=compressionlevel, kwargs...)
    return take!(io)
end
