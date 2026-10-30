# Hive-partitioned output: Parquet.write(dir, table; partitionby=[:a, :b]) writes the rows of
# each distinct key under dir/a=<value>/b=<value>/part-<i>.parquet, without the key columns.

# A partitioned write keeps at most this many files open; past it the oldest file is
# finished and its partition's later rows go to a new file (part-1, part-2, ...).
const _MAX_OPEN_PARTITIONS = 128

const _HIVE_NULL = "__HIVE_DEFAULT_PARTITION__"

# A key value as pyarrow writes it: percent-encoded UTF-8 except A-Z a-z 0-9 - . _ ~.
function _hiveescape(text::AbstractString)
    output = IOBuffer()
    for byte in codeunits(text)
        if UInt8('A') <= byte <= UInt8('Z') || UInt8('a') <= byte <= UInt8('z') ||
                UInt8('0') <= byte <= UInt8('9') || byte in (UInt8('-'), UInt8('.'),
                UInt8('_'), UInt8('~'))
            Base.write(output, byte)
        else
            print(output, '%', uppercase(string(byte; base=16, pad=2)))
        end
    end
    return String(take!(output))
end

_hivevalue(::Missing, ::String) = _HIVE_NULL
_hivevalue(value::Bool, ::String) = value ? "true" : "false"
_hivevalue(value::Union{Integer,AbstractFloat}, ::String) = _hiveescape(string(value))
_hivevalue(value::Dates.Date, ::String) = _hiveescape(string(value))
_hivevalue(value::Union{AbstractString,Symbol,AbstractChar}, ::String) =
    _hiveescape(string(value))
# Timestamps as pyarrow writes them: "2024-01-01 12:30:00.000" (digits by unit), with a
# trailing "Z" when adjusted to UTC. They read back as strings, as with pyarrow.
_hivevalue(value::Dates.DateTime, name::String) =
    _hivevalue(Durations.Timestamp{Dates.Millisecond}(value), name)
function _hivevalue(value::Durations.Timestamp{P}, ::String) where {P}
    persecond = Dates.tons(Dates.Second(1)) ÷ Dates.tons(P(1))
    seconds, fraction = fldmod(Dates.value(value), persecond)
    text = Dates.format(Dates.DateTime(1970) + Dates.Second(seconds), "yyyy-mm-dd HH:MM:SS")
    digits = ndigits(persecond) - 1
    return _hiveescape(digits == 0 ? text : text * "." * lpad(fraction, digits, '0'))
end
_hivevalue(value::Durations.ZonedTimestamp{P,:UTC}, name::String) where {P} =
    _hivevalue(value.utc, name) * "Z"
_hivevalue(value, name::String) = throw(ArgumentError("partition column $(repr(name)) " *
    "holds $(typeof(value)) values; partition by integer, float, Bool, Date, timestamp, " *
    "or string columns (convert others first, for example with string.(column))"))

function _hivekey(name::String)
    isempty(name) && throw(ArgumentError("a partition column must have a nonempty name"))
    occursin('\0', name) && throw(ArgumentError("Parquet top-level field names cannot contain NUL"))
    key = _hiveescape(name)
    # Dataset readers skip directory names that start with either character.
    first(key) in ('.', '_') &&
        (key = "%" * uppercase(string(UInt8(first(key)); base=16, pad=2)) * key[2:end])
    return key
end

function _hivecomponent(name::String, value)
    text = _hivevalue(DataAPI.unwrap(value), name)
    value !== missing && text == _HIVE_NULL && throw(ArgumentError(
        "partition column $(repr(name)) holds the reserved Hive null value " *
        repr(_HIVE_NULL) * "; replace that value before partitioning"))
    return _hivekey(name) * "=" * text
end

# Rows of each distinct key, in order of first appearance.
function _partitiongroups(keys::Tuple, rows::Int)
    K = Tuple{map(eltype, keys)...}
    groups = Dict{K,Vector{Int}}()
    order = K[]
    for row in 1:rows
        key = map(column -> column[firstindex(column) + row - 1], keys)
        indices = get!(groups, key) do
            push!(order, key)
            Int[]
        end
        push!(indices, row)
    end
    return Pair{K,Vector{Int}}[key => groups[key] for key in order]
end

mutable struct _PartitionedWrite
    root::String
    options::_WriteOptions
    existing::Symbol
    writers::Dict{String,Writer}
    opened::Vector{String}
    files::Dict{String,Int}
    # The first file's writer: every later file takes its schema and metadata, so all
    # files of the dataset have the same columns and types.
    template::Union{Nothing,Writer}
end

function _partitionwriter!(state::_PartitionedWrite, directory::String)
    writer = get(state.writers, directory, nothing)
    writer === nothing || return writer
    if length(state.opened) >= _MAX_OPEN_PARTITIONS
        oldest = popfirst!(state.opened)
        close(pop!(state.writers, oldest))
    end
    full = joinpath(state.root, directory)
    path = state.root
    for component in split(directory, '/')
        path = joinpath(path, component)
        islink(path) && throw(ArgumentError("partition output $(repr(path)) is a " *
            "symbolic link; remove it before writing into this dataset"))
    end
    count = get(state.files, directory, 0)
    # `delete_matching` clears a partition the first time this write touches it.
    count == 0 && state.existing === :delete_matching && rm(full; force=true, recursive=true)
    mkpath(full)
    writer = _openwriter(joinpath(full, "part-$count.parquet"), state.options)
    template = state.template
    if template !== nothing
        writer.schema = template.schema
        writer.metadata = template.metadata
        merge!(writer.colmetadata, template.colmetadata)
        merge!(writer.arrowhints, template.arrowhints)
    end
    state.files[directory] = count + 1
    state.writers[directory] = writer
    push!(state.opened, directory)
    return writer
end

Base.@nospecializeinfer function _writepartitioned(root::String, table, partitionby,
        existing::Symbol, kwargs)
    Base.@nospecialize table kwargs
    keys = String[String(name) for name in (partitionby isa Union{Symbol,AbstractString} ?
        (partitionby,) : partitionby)]
    isempty(keys) && throw(ArgumentError("partitionby names no columns"))
    allunique(keys) || throw(ArgumentError("partitionby names a column twice"))
    foreach(_hivekey, keys)
    existing in (:error, :delete_matching) || throw(ArgumentError(
        "existing must be :error or :delete_matching, got :$existing"))
    isfile(root) && throw(ArgumentError("partitioned output $(repr(root)) is a file"))
    existing === :error && isdir(root) && !isempty(readdir(root)) && throw(ArgumentError(
        "partitioned output directory $(repr(root)) is not empty; remove it first, or " *
        "pass existing=:delete_matching to replace only the partitions this write produces"))
    metadata = get(kwargs, :metadata, nothing)
    metadata === nothing && (metadata = _barrier(_inputmetadata, table))
    colmetadata = get(kwargs, :colmetadata, nothing)
    # Only a table with column metadata is asked for its columns (a stream is not read).
    colmetadata === nothing && _barrier(DataAPI.colmetadatasupport, typeof(table)).read &&
        (colmetadata = _barrier(_inputcolmetadata, table, String[String(name)::String
            for name in Tables.columnnames(Tables.columns(table))]))
    colmetadata === nothing || (colmetadata = Pair{String,Any}[
        String(string(name))::String => entries
        for (name, entries) in pairs(colmetadata) if !(String(string(name)) in keys)])
    nullable = Tables.partitions(table) isa Tuple{Any} ? nothing : true
    # The keywords are untyped here; the options are built behind `_barrier`.
    options = _barrier(_writeoptions; nullable=nullable, metadata=metadata,
        colmetadata=colmetadata,
        Base.structdiff(kwargs, NamedTuple{(:metadata, :colmetadata)})...)::_WriteOptions
    state = _PartitionedWrite(root, options, existing, Dict{String,Writer}(), String[],
        Dict{String,Int}(), nothing)
    try
        offset = 0
        for part in Tables.partitions(table)
            offset += _writepartition!(state, part, keys, offset)
        end
        while !isempty(state.opened)
            close(pop!(state.writers, popfirst!(state.opened)))
        end
    catch
        foreach(_abandon!, values(state.writers))
        rethrow()
    end
    return root
end

# Writes the rows of `part`, rows `offset + 1` on of the input, and returns their count.
Base.@nospecializeinfer function _writepartition!(state::_PartitionedWrite, part,
        keys::Vector{String}, offset::Int)
    Base.@nospecialize part
    columns, rows = _writecolumnvectors(Tables.columns(part), state.options.limits)
    names = first.(columns)
    for key in keys
        key in names || throw(ArgumentError("partitionby names $(repr(key)), which is " *
            "not a column"))
    end
    data = Symbol[Symbol(name) for name in names if !(name in keys)]
    isempty(data) && throw(ArgumentError(
        "partitionby uses every column, so the files would have no columns"))
    keycolumns = Tuple(Any[last(columns[findfirst(==(key), names)]) for key in keys])
    datacolumns = AbstractVector[_barrier(_partitioncolumn, String(name),
        last(columns[findfirst(==(String(name)), names)])) for name in data]
    # The key columns' types are known at run time.
    groups = _barrier(_partitiongroups, keycolumns, rows)
    _barrier(_writegroups!, state, groups, keys, data, datacolumns, offset)
    return rows
end

# Writes each group of rows (key => indices) to its partition's file.
function _writegroups!(state::_PartitionedWrite, groups::Vector{<:Pair},
        keys::Vector{String}, data::Vector{Symbol}, datacolumns::Vector{AbstractVector},
        base::Int)
    for (key, indices) in groups
        directory = join((_hivecomponent(name, value)
            for (name, value) in zip(keys, key)), '/')
        group = NamedTuple{Tuple(data)}(Tuple(Any[_barrier(view, column,
            firstindex(column) == 1 ? indices : indices .+ (firstindex(column) - 1))
            for column in datacolumns]))
        writer = _partitionwriter!(state, directory)
        _writetable!(writer, group, group, base, indices)
        state.template === nothing && (state.template = writer)
    end
    return
end

# Abstractly typed columns narrow once for the whole input, not per file.
function _partitioncolumn(name::String, values)
    values isa AbstractVector || throw(ArgumentError("Parquet columns must be vectors"))
    DataAPI.refpool(values) === nothing && !_writedeclared(eltype(values)) &&
        (values = _barrier(_narrowwritecolumn, name, values))
    return _barrier(_writeextensioncolumn, name, values)::AbstractVector
end
