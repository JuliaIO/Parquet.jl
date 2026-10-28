# Multi-file datasets: a directory, glob pattern, or list of files read as one table, with
# Hive-style partition columns from `key=value` directories.

# One file of a dataset: what reading it needs (its input, cached footer, and partition
# values) and its columns as a read returns them.
struct _DatasetFile
    input::Any
    path::Union{Nothing,String}
    footer::Footer{Vector{UInt8}}
    metadata::Metadata.FileMetaData
    columns::Vector{Symbol}
    types::Vector{Type}
    elements::Vector{Vector{Metadata.SchemaElement}}
    colmetadata::Vector{Vector{Pair{String,String}}}
    partitions::Vector{Any}
end

"""
    Parquet.Dataset(source; partitioning=:hive, select=nothing, drop=nothing,
                    filter=nothing, limit=nothing, scan=nothing,
                    ntasks=Threads.nthreads(), stringtype=DataString,
                    int96=Dates.Nanosecond, checksum=false, limits=Parquet.Limits())

Parquet files read as one table. `source` is a directory (every file below it), a glob
pattern such as `"data/*.parquet"` or `"lake/**/*.parquet"` (`*` and `?` match within a
path segment, `**` across segments), or a vector of paths or sources. Files and
directories whose names start with `.` or `_` (`_SUCCESS`, `.crc` files, `_metadata`)
are skipped in directories and patterns. The keywords are those of
[`Parquet.Table`](@ref).

- A directory named `key=value` below the dataset's root gives its files a column `key`,
  typed `Int64`, `Float64`, `Dates.Date`, or `Bool`, the first that all of its values
  parse as, or else a string (`DataStrings.DataString`); it follows the files' columns
  unless the files have a column of that name, whose values it replaces (as pyarrow,
  Polars, and DuckDB do). `__HIVE_DEFAULT_PARTITION__` reads as `missing`, and an empty
  value as the string `""`, as in pyarrow, Polars, and DuckDB.
  `partitioning=nothing` reads the files' columns only.
- Columns match by name across files. A column some files lack is `missing` in their
  rows. A column whose files hold narrower integers, a narrower float, or a coarser
  timestamp unit reads as the wider type; other differences in element type are an
  `ArgumentError`.
- A filter skips the files whose partition values rule it out, and row groups and pages
  within files as `Parquet.Table` does.
- Iterating the dataset yields one `Parquet.Table` per row group of each file, so it is
  its own `Tables.partitions`; `length(ds)`, `ds[i]`, `Tables.schema(ds)`,
  `DataAPI.nrow(ds)`, and `DataAPI.metadata(ds)` (the first file's key-value metadata)
  work as for [`Parquet.File`](@ref). `Parquet.Table(ds)` reads it all;
  `Parquet.Table(source)` and `Parquet.read(source, sink)` read a dataset `source`
  directly.

The footers are read when the dataset is created, on up to `ntasks` tasks; reads spread
over files and their column chunks on up to `ntasks` tasks.

    Parquet.Dataset(store, prefix; credentials=nothing, keywords...)

With CloudStore loaded, the objects whose keys start with `prefix` in a CloudStore bucket
or container, with partition directories below `prefix` (end it with `/`).
"""
struct Dataset
    files::Vector{_DatasetFile}
    names::Vector{Symbol}
    types::Vector{Type}
    elements::Vector{Vector{Metadata.SchemaElement}}
    colmetadata::Vector{Vector{Pair{String,String}}}
    partition::Vector{Int}
    bound::Union{Nothing,Tables.BoundScan}
    ntasks::Int
    stringtype::Type
    int96::Type
    checksum::Bool
    limits::Limits
end

function Dataset(input; kw...)
    inputs, paths, base = _datasetinputs(input)
    isempty(inputs) && throw(ArgumentError("no Parquet files found in $(repr(input))"))
    return _dataset(inputs, paths, base; kw...)
end

# A dataset of `inputs`, whose `paths` (`nothing` for sources) name their partition
# directories below `base`.
function _dataset(inputs::Vector{Any}, paths::Vector{Union{Nothing,String}}, base;
        partitioning=:hive, select=nothing, drop=nothing, filter=nothing, limit=nothing,
        scan::Union{Nothing,Tables.Scan}=nothing, ntasks::Integer=Threads.nthreads(),
        stringtype::Type=DataStrings.DataString, int96::Type=Dates.Nanosecond,
        checksum::Bool=false, limits::Limits=Limits())
    ntasks >= 1 || throw(ArgumentError("ntasks must be at least 1, got $ntasks"))
    partitioning in (:hive, nothing) || throw(ArgumentError(
        "partitioning must be :hive or nothing, got $(repr(partitioning))"))
    request = _scanrequest(select, drop, filter, limit, scan)
    _ReadOptions(; int96=int96, stringtype=stringtype)
    keys, values = partitioning === nothing ?
        (String[], [Union{Nothing,String}[] for _ in inputs]) : _hivepartitions(paths, base)
    partitions, partitiontypes = _hivevalues(keys, values, stringtype)
    files = collect(_DatasetFile, _readmap(eachindex(inputs),
            min(Int(ntasks), length(inputs))) do index, _
        return _barrier(_datasetfile, inputs[index], paths[index], partitions[index],
            stringtype, int96, limits)::_DatasetFile
    end)
    names, types, elements = _unifycolumns(files, Symbol.(keys))
    colmetadata = map(names) do name
        for file in files
            position = findfirst(==(name), file.columns)
            position === nothing || return file.colmetadata[position]
        end
        return Pair{String,String}[]
    end
    partition = zeros(Int, length(names))
    for (position, (key, T)) in enumerate(zip(keys, partitiontypes))
        name = Symbol(key)
        index = findfirst(==(name), names)
        if index === nothing
            push!(names, name)
            push!(types, T)
            push!(elements, [_partitionelement(key, T)])
            push!(colmetadata, Pair{String,String}[])
            push!(partition, position)
        else
            types[index] = T
            elements[index] = [_partitionelement(key, T)]
            colmetadata[index] = Pair{String,String}[]
            partition[index] = position
        end
    end
    bound = request === nothing ? nothing :
        _bindscan(_barrier(Tables.resolve, _scanfor(request, names), names)::Tables.BoundScan,
            Dict{Symbol,Type}(zip(names, types)))
    bound === nothing || any(column -> column.type !== nothing, bound.columns) &&
        throw(ArgumentError("Parquet does not apply scan type overrides; convert the " *
            "columns after reading"))
    return Dataset(files, names, types, elements, colmetadata, partition, bound,
        Int(ntasks), stringtype, int96, checksum, limits)
end

## Finding the files

_hiddenname(name::AbstractString) = startswith(name, '.') || startswith(name, '_')
_isglob(path::AbstractString) = any(in(('*', '?', '[')), path)

# The inputs a dataset source names, their paths (`nothing` for sources), and the
# directory partition keys are read below.
function _datasetinputs(input)
    if input isa AbstractString
        path = String(input)
        isfile(path) && return Any[path], Union{Nothing,String}[path], dirname(path)
        isdir(path) && return _directoryfiles(path)
        _isurl(path) && throw(ArgumentError(_urlmessage(path)))
        _isglob(path) && return _globfiles(path)
        throw(ArgumentError("$(repr(path)) is not a file, a directory, or a glob pattern"))
    elseif input isa AbstractVector && !(input isa AbstractVector{UInt8})
        paths = Union{Nothing,String}[_sourcepath(item) for item in input]
        found = String[path for path in paths if path !== nothing]
        return collect(Any, input), paths, _commondirectory(found)
    end
    throw(ArgumentError("a dataset source is a directory, a glob pattern, or a vector of " *
        "paths or sources, got $(typeof(input))"))
end

# The path a dataset input names, for its partition directories, or `nothing`.
_sourcepath(path::AbstractString) = String(path)
_sourcepath(input) = nothing

function _directoryfiles(root::String)
    files = String[]
    _walkfiles!(files, root)
    sort!(files; by=_pathorder)
    return collect(Any, files), collect(Union{Nothing,String}, files), root
end

function _walkfiles!(files::Vector{String}, directory::String)
    for name in readdir(directory)
        _hiddenname(name) && continue
        path = joinpath(directory, name)
        if isdir(path)
            _walkfiles!(files, path)
        elseif isfile(path)
            push!(files, path)
        end
    end
    return files
end

function _globfiles(pattern::String)
    Sys.iswindows() && (pattern = replace(pattern, '\\' => '/'))
    parts = split(pattern, '/'; keepempty=true)
    literal = something(findfirst(_isglob, parts), length(parts) + 1) - 1
    prefix = join(parts[1:literal], '/')
    root = literal == 0 ? "." : isempty(prefix) ? "/" : prefix
    regex = _globregex(join(parts[(literal + 1):end], '/'))
    files = String[]
    isdir(root) && _walkfiles!(files, root)
    matched = String[path for path in files if occursin(regex, _relativepath(path, root))]
    sort!(matched; by=_pathorder)
    return collect(Any, matched), collect(Union{Nothing,String}, matched), root
end

_relativepath(path::String, root::String) = replace(relpath(path, root), '\\' => '/')

# Files read in the order of their '/'-separated paths on every platform (on Windows, `\`
# sorts after letters, `/` before them).
_pathorder(path::String) = Sys.iswindows() ? replace(path, '\\' => '/') : path

# A glob pattern as a regex over '/'-separated relative paths.
function _globregex(pattern::AbstractString)
    io = IOBuffer()
    print(io, '^')
    chars = collect(pattern)
    index = 1
    while index <= length(chars)
        char = chars[index]
        if char == '*' && index < length(chars) && chars[index + 1] == '*'
            if index + 2 <= length(chars) && chars[index + 2] == '/'
                print(io, "(?:[^/]*/)*")
                index += 3
            else
                print(io, ".*")
                index += 2
            end
            continue
        elseif char == '*'
            print(io, "[^/]*")
        elseif char == '?'
            print(io, "[^/]")
        elseif char == '[' && (close = findnext(==(']'), chars, index + 2)) !== nothing
            class = chars[(index + 1):(close - 1)]
            print(io, '[')
            negated = first(class) == '!'
            negated && print(io, '^')
            for member in (negated ? class[2:end] : class)
                member in ('\\', '^', '[', ']') && print(io, '\\')
                print(io, member)
            end
            print(io, ']')
            index = close + 1
            continue
        else
            print(io, _regexescape(char))
        end
        index += 1
    end
    print(io, '$')
    return Regex(String(take!(io)))
end

_regexescape(char::Char) = char in raw"\.^$|?*+()[]{}/" ? string('\\', char) : string(char)

function _commondirectory(paths::Vector{String})
    isempty(paths) && return nothing
    common = splitpath(dirname(abspath(first(paths))))
    for path in paths
        parts = splitpath(dirname(abspath(path)))
        count = 0
        while count < min(length(common), length(parts)) && common[count + 1] == parts[count + 1]
            count += 1
        end
        resize!(common, count)
    end
    return isempty(common) ? nothing : joinpath(common...)
end

## Hive partitions

const _HIVE_DEFAULT = "__HIVE_DEFAULT_PARTITION__"

# The partition keys of the files' `key=value` directories below `base`, in order of
# first appearance, and each file's raw value per key (`nothing` for none or a default).
function _hivepartitions(paths::Vector{Union{Nothing,String}}, base)
    keys = String[]
    found = [Dict{String,Union{Nothing,String}}() for _ in paths]
    for (index, path) in enumerate(paths)
        (path === nothing || base === nothing) && continue
        for segment in _relativedirectories(path, base)
            equals = findfirst('=', segment)
            (equals === nothing || equals == 1) && continue
            key = segment[1:(equals - 1)]
            value = _unescapeuri(segment[(equals + 1):end])
            key in keys || push!(keys, key)
            found[index][key] = value == _HIVE_DEFAULT ? nothing : value
        end
    end
    return keys, [Union{Nothing,String}[get(entries, key, nothing) for key in keys]
        for entries in found]
end

# The directory names between `base` and the file `path` (a path or an object key).
function _relativedirectories(path::String, base::String)
    rest = if startswith(path, base) && (endswith(base, '/') ||
            get(path, nextind(path, lastindex(base)), '/') in ('/', '\\'))
        path[(ncodeunits(base) + 1):end]
    else
        relpath(abspath(path), abspath(base))
    end
    segments = split(rest, Sys.iswindows() ? ('/', '\\') : ('/',); keepempty=false)
    return isempty(segments) ? SubString{String}[] : segments[1:(end - 1)]
end

function _unescapeuri(value::AbstractString)
    occursin('%', value) || return String(value)
    bytes = UInt8[]
    units = codeunits(value)
    index = 1
    while index <= length(units)
        if units[index] == UInt8('%') && index + 2 <= length(units) &&
                (byte = tryparse(UInt8, String(units[(index + 1):(index + 2)]); base=16)) !== nothing
            push!(bytes, byte)
            index += 3
        else
            push!(bytes, units[index])
            index += 1
        end
    end
    return String(bytes)
end

# The typed value of every file per key, and each key's element type: `Int64`,
# `Float64`, `Date`, `Bool`, or `stringtype` (read like a STRING column), whichever all of
# its values parse as, with `Missing` when a file has none.
function _hivevalues(keys::Vector{String}, values::Vector{Vector{Union{Nothing,String}}},
        stringtype::Type=DataStrings.DataString)
    typed = [Any[] for _ in values]
    types = Type[]
    for position in eachindex(keys)
        raw = Union{Nothing,String}[entries[position] for entries in values]
        present = String[value for value in raw if value !== nothing]
        T = isempty(present) ? Missing :
            all(_hiveint, present) ? Int64 :
            all(_hivefloat, present) ? Float64 :
            all(_hivedate, present) ? Dates.Date :
            all(value -> value in ("true", "false"), present) ? Bool : stringtype
        for (index, value) in enumerate(raw)
            push!(typed[index], value === nothing ? missing : _hiveparse(T, value))
        end
        push!(types, any(isnothing, raw) && T !== Missing ? Union{Missing,T} : T)
    end
    return typed, types
end

_hiveint(value::String) = occursin(r"^[+-]?[0-9]+$", value) && tryparse(Int64, value) !== nothing
_hivefloat(value::String) = occursin(r"^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$",
    value) && tryparse(Float64, value) !== nothing
_hivedate(value::String) = occursin(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$", value) &&
    tryparse(Dates.Date, value) !== nothing

_hiveparse(::Type{T}, value::String) where {T<:Union{Int64,Float64,Dates.Date}} = parse(T, value)
_hiveparse(::Type{Bool}, value::String) = value == "true"
_hiveparse(::Type{<:Union{String,DataStrings.DataString}}, value::String) = value

# The stored schema element of a partition column, so a read dataset writes back with it.
function _partitionelement(key::String, T::Type)
    S = Base.nonmissingtype(T)
    repetition = T === S && S !== Missing ? Metadata.FieldRepetitionType.REQUIRED :
        Metadata.FieldRepetitionType.OPTIONAL
    S === Int64 && return Metadata.SchemaElement(name=key, type_=Metadata.Type.INT64,
        repetition_type=repetition)
    S === Float64 && return Metadata.SchemaElement(name=key, type_=Metadata.Type.DOUBLE,
        repetition_type=repetition)
    S === Bool && return Metadata.SchemaElement(name=key, type_=Metadata.Type.BOOLEAN,
        repetition_type=repetition)
    S === Dates.Date && return Metadata.SchemaElement(name=key, type_=Metadata.Type.INT32,
        repetition_type=repetition, converted_type=Metadata.ConvertedType.DATE,
        logicalType=Metadata.LogicalType(DATE=Metadata.DateType()))
    S in (DataStrings.DataString, String) && return Metadata.SchemaElement(name=key,
        type_=Metadata.Type.BYTE_ARRAY,
        repetition_type=repetition, converted_type=Metadata.ConvertedType.UTF8,
        logicalType=Metadata.LogicalType(STRING=Metadata.StringType()))
    return Metadata.SchemaElement(name=key, type_=Metadata.Type.INT32,
        repetition_type=repetition, logicalType=Metadata.LogicalType(UNKNOWN=Metadata.NullType()))
end

## Reading footers and unifying columns

function _datasetfile(input, path, partitions::Vector{Any}, stringtype::Type, int96::Type,
        limits::Limits)
    file = try
        File(input; ntasks=1, stringtype=stringtype, int96=int96, limits=limits)
    catch err
        _rethrowfor(err, path)
    end
    try
        schema = Tables.schema(file)
        metadata = file.metadata
        elements = metadata.schema
        subtrees = Vector{Metadata.SchemaElement}[]
        index = 2
        for _ in 1:something(elements[1].num_children, Int32(0))
            last = _subtreeend(elements, index)
            push!(subtrees, elements[index:last])
            index = last + 1
        end
        footer = file.footer
        return _DatasetFile(input, path, footer, metadata,
            collect(Symbol, schema.names), collect(Type, schema.types), subtrees,
            file.plan.colmetadata, partitions)
    catch err
        _rethrowfor(err, path)
    finally
        close(file)
    end
end

# Errors reading a dataset file name the file.
function _rethrowfor(err, path)
    path === nothing && rethrow()
    err isa FormatError && throw(FormatError("$(path): $(err.message)"))
    err isa UnsupportedFeatureError && throw(UnsupportedFeatureError("$(path): $(err.message)"))
    rethrow()
end

_describe(file::_DatasetFile, index::Int) =
    file.path === nothing ? "source $index" : repr(file.path)

# Columns of all files by name, in order of first appearance: the element type (with
# `Missing` added when a file lacks the column or has it optional) and the stored schema
# elements of its first file. Partition keys among them keep only their position.
function _unifycolumns(files::Vector{_DatasetFile}, keys::Vector{Symbol})
    names = Symbol[]
    types = Type[]
    elements = Vector{Metadata.SchemaElement}[]
    first = Int[]
    position = Dict{Symbol,Int}()
    for (index, file) in enumerate(files)
        for (name, T, subtree) in zip(file.columns, file.types, file.elements)
            known = get(position, name, 0)
            if known == 0
                push!(names, name)
                push!(types, T)
                push!(elements, subtree)
                push!(first, index)
                position[name] = length(names)
                continue
            end
            U = types[known]
            (U === T || name in keys) && continue
            S, R = Base.nonmissingtype(U), Base.nonmissingtype(T)
            if S === R || T === Missing
                types[known] = Union{Missing,U}
            elseif U === Missing
                types[known] = Union{Missing,T}
                elements[known] = subtree
            elseif (W = _widentype(S, R)) !== nothing
                types[known] = Missing <: U || Missing <: T ? Union{Missing,W} : W
                W === R && (elements[known] = subtree)
                W === S || W === R ||
                    (elements[known] = [_partitionelement(String(name), W)])
            else
                throw(ArgumentError("column $(repr(String(name))) is $U in " *
                    "$(_describe(files[first[known]], first[known])) but $T in " *
                    "$(_describe(file, index)); the files' columns must have one type, " *
                    "or types that widen without loss (integers, floats, or timestamp " *
                    "units)"))
            end
        end
    end
    for (index, name) in enumerate(names)
        all(file -> name in file.columns, files) || (types[index] = Union{Missing,types[index]})
    end
    return names, types, elements
end

# The type that holds every value of the read types `S` and `T` exactly, when one
# widens the other without loss: integers (to the wider, or to Int64 for mixed
# signedness), floats, and timestamps of one zone (to the finer unit). `nothing` for any
# other pair.
function _widentype(S::Type, T::Type)
    if S <: Union{Signed,Unsigned} && T <: Union{Signed,Unsigned}
        for W in (S, T, Int64)
            typemin(W) <= typemin(S) && typemin(W) <= typemin(T) &&
                typemax(S) <= typemax(W) && typemax(T) <= typemax(W) && return W
        end
    elseif S <: AbstractFloat && T <: AbstractFloat
        return promote_type(S, T)
    elseif S <: Durations.Timestamp && T <: Durations.Timestamp
        return _finerunit(S, T)
    elseif S <: Durations.ZonedTimestamp && T <: Durations.ZonedTimestamp &&
            S.parameters[2] === T.parameters[2]
        return _finerunit(S, T)
    end
    return nothing
end

_finerunit(S::Type, T::Type) = Dates.toms(oneunit(S.parameters[1])) <=
    Dates.toms(oneunit(T.parameters[1])) ? S : T

## Reading

# The output columns of a read: (dataset column index, output name).
function _datasetoutputs(ds::Dataset)
    ds.bound === nothing && return [(index, name) for (index, name) in enumerate(ds.names)]
    return [(column.index, column.name) for column in ds.bound.columns]
end

# The file's filter: its partition columns and the dataset columns it lacks replaced by
# what they evaluate to. `false` when no row of the file can match, `nothing` without a
# filter.
function _filefilter(ds::Dataset, file::_DatasetFile)
    bound = ds.bound
    (bound === nothing || bound.filter === nothing) && return nothing
    folded = _foldfilter(bound.filter, false, ds, file)
    folded isa Tables.AlwaysFalse && return false
    folded isa Tables.AlwaysTrue && return nothing
    return folded
end

# `expr` (negated when `negate`) with its NOTs pushed down to the leaves (De Morgan, which
# holds in three-valued logic) and its leaves on partition columns, on columns the file
# lacks, and on unknown columns folded into constants. Below no NOT a `missing` leaf
# keeps the same rows `true` as a `false` one, so both fold to `AlwaysFalse`.
function _foldfilter(expr, negate::Bool, ds::Dataset, file::_DatasetFile)
    if expr isa Tables.AndExpr || expr isa Tables.OrExpr
        all_ = (expr isa Tables.AndExpr) != negate
        args = Tables.ScanExpr[]
        for arg in expr.args
            folded = _foldfilter(arg, negate, ds, file)
            if folded isa Tables.AlwaysTrue
                all_ || return folded
            elseif folded isa Tables.AlwaysFalse
                all_ && return folded
            else
                push!(args, folded)
            end
        end
        isempty(args) && return all_ ? Tables.AlwaysTrue() : Tables.AlwaysFalse()
        length(args) == 1 && return only(args)
        return all_ ? Tables.AndExpr(args) : Tables.OrExpr(args)
    elseif expr isa Tables.NotExpr
        return _foldfilter(expr.arg, !negate, ds, file)
    elseif expr isa Tables.AlwaysTrue || expr isa Tables.AlwaysFalse
        return (expr isa Tables.AlwaysTrue) != negate ? Tables.AlwaysTrue() :
            Tables.AlwaysFalse()
    end
    ref = expr.lhs.ref
    index = ref isa Symbol ? something(findfirst(==(ref), ds.names), 0) : 0
    key = index == 0 ? 0 : ds.partition[index]
    key == 0 && index > 0 && ref in file.columns &&
        return negate ? Tables.NotExpr(expr) : expr
    # A partition value, or `missing` for a column the file lacks.
    value = key > 0 ? file.partitions[key] : missing
    t, f = _evalmask(expr, NamedTuple{(ref isa Symbol ? ref : :_,)}(([value],)), 1)
    return (negate ? f[1] : t[1]) ? Tables.AlwaysTrue() : Tables.AlwaysFalse()
end

# The scan a file is read with: the dataset's output columns it has, its folded filter,
# and a row window.
function _filescan(ds::Dataset, file::_DatasetFile, filter, offset::Int, limit)
    needed = Symbol[]
    for (index, _) in _datasetoutputs(ds)
        name = ds.names[index]
        ds.partition[index] == 0 && name in file.columns && !(name in needed) &&
            push!(needed, name)
    end
    return Tables.Scan(; select=needed, filter=filter, limit=limit, offset=offset)
end

function _openpart(ds::Dataset, file::_DatasetFile, scan::Tables.Scan, ntasks::Int)
    try
        return _barrier(_openfile, file.input, (file.footer, file.metadata); scan=scan,
            ntasks=ntasks, stringtype=ds.stringtype, int96=ds.int96, checksum=ds.checksum,
            limits=ds.limits)::File
    catch err
        _rethrowfor(err, file.path)
    end
end

# `f(handle)` for `file` opened for `scan`, which is closed after. The source type of
# a dataset's file (a path, bytes, or a source) is known only at run time, so `f` runs
# behind `_barrier`.
function _withpart(f, ds::Dataset, file::_DatasetFile, scan::Tables.Scan, ntasks::Int)
    handle = _openpart(ds, file, scan, ntasks)
    try
        return _barrier(f, handle)
    finally
        close(handle)
    end
end

# A file's table as a table of the dataset: its columns converted to the dataset's
# element types, with its partition values and `missing` for the columns it lacks, and
# a stored schema that covers them.
function _datasettable(ds::Dataset, file::_DatasetFile, table::Table)
    rows = _tablerows(table)
    outputs = _datasetoutputs(ds)
    names = Symbol[]
    columns = AbstractVector[]
    elements = Metadata.SchemaElement[]
    for (index, name) in outputs
        T = ds.types[index]
        column = if ds.partition[index] > 0
            _constantcolumn(T, file.partitions[ds.partition[index]], rows)
        elseif ds.names[index] in file.columns
            _convertcolumn(T, Tables.getcolumn(table, ds.names[index]))
        else
            _constantcolumn(T, missing, rows)
        end
        push!(names, name)
        push!(columns, column)
        subtree = copy(ds.elements[index])
        repetition = Missing <: T ? Metadata.FieldRepetitionType.OPTIONAL :
            first(subtree).repetition_type
        subtree[1] = _withfields(first(subtree); name=String(name),
            repetition_type=repetition)
        append!(elements, subtree)
    end
    metadata = _tablemetadata(table)
    root = _withfields(first(metadata.schema); num_children=Int32(length(outputs)))
    metadata = _withfields(metadata; schema=vcat([root], elements),
        row_groups=Metadata.RowGroup[], num_rows=Int64(rows), column_orders=nothing)
    return Table(metadata, Schema(metadata; limits=ds.limits),
        NamedTuple{Tuple(names)}(Tuple(columns)), rows,
        [ds.colmetadata[index] for (index, _) in outputs])
end

# A column of `rows` copies of `value`, of element type `T`.
function _constantcolumn(T::Type, value, rows::Int)
    S = Base.nonmissingtype(T)
    if S === DataStrings.DataString || S === DataStrings.DataBytes
        payload, buffers = if ismissing(value)
            DataStrings.PAYLOAD_MISSING, Vector{UInt8}[]
        else
            bytes = Vector{UInt8}(codeunits(String(value)))
            length(bytes) <= DataStrings.INLINE_MAX ?
                (DataStrings.inline_payload(bytes, 1, length(bytes)), Vector{UInt8}[]) :
                (DataStrings.view_payload(bytes, 1, length(bytes), 0, 0), [bytes])
        end
        V = S === DataStrings.DataString ? DataStrings.StringVector{T} :
            DataStrings.BytesVector{T}
        return V(fill(payload, rows), buffers, Val(:trusted))
    end
    T === Missing && return fill(missing, rows)
    return fill!(Vector{T}(undef, rows), ismissing(value) ? missing : convert(S, value))
end

# `column` with element type `T` (a file's column where other files make it optional).
function _convertcolumn(T::Type, column::AbstractVector)
    eltype(column) === T && return column
    column isa DataStrings.StringVector &&
        return DataStrings.StringVector{T}(column.payloads, column.buffers, Val(:trusted))
    column isa DataStrings.BytesVector &&
        return DataStrings.BytesVector{T}(column.payloads, column.buffers, Val(:trusted))
    return Vector{T}(column)
end

# A file's rows for the part `(offset, limit)` of its folded filter's matches.
function _readpart(ds::Dataset, file::_DatasetFile, filter, offset::Int, limit, ntasks::Int)
    scan = _filescan(ds, file, filter, offset, limit)
    table = _withpart(ds, file, scan, ntasks) do handle
        try
            return _readtable(handle, eachindex(handle.plan.metadata.row_groups),
                _ByteBudget(ds.limits))
        catch err
            _rethrowfor(err, file.path)
        end
    end
    return _datasettable(ds, file, table::Table)
end

# The whole dataset as one table.
function _readdataset(ds::Dataset)
    bound = ds.bound
    offset, limit = bound === nothing ? (0, nothing) : (bound.offset, bound.limit)
    filters = [_filefilter(ds, file) for file in ds.files]
    filtered = bound !== nothing && bound.filter !== nothing
    jobs = Tuple{Int,Int,Union{Nothing,Int}}[]
    tables = Table[]
    if !filtered
        # Row counts are in the footers, so the window picks files and rows up front.
        skip, keep = offset, limit
        for (index, file) in enumerate(ds.files)
            keep === nothing || keep > 0 || break
            rows = Int(file.metadata.num_rows)
            drop = min(skip, rows)
            take = keep === nothing ? rows - drop : min(keep, rows - drop)
            skip -= drop
            keep === nothing || (keep -= take)
            take > 0 && push!(jobs, (index, drop, take == rows - drop ? nothing : take))
        end
    elseif iszero(offset) && limit === nothing
        append!(jobs, (index, 0, nothing) for (index, filter) in enumerate(filters)
            if filter !== false)
    else
        # Matches are counted while reading, so files are read in order.
        skip, keep = offset, limit
        for (index, file) in enumerate(ds.files)
            keep == 0 && break
            filters[index] === false && continue
            table = _readpart(ds, file, filters[index], 0,
                keep === nothing ? nothing : Base.checked_add(skip, keep), ds.ntasks)
            rows = _tablerows(table)
            drop = min(skip, rows)
            take = keep === nothing ? rows - drop : min(keep, rows - drop)
            skip -= drop
            keep === nothing || (keep -= take)
            push!(tables, _slicetable(table, drop, take))
        end
    end
    if !isempty(jobs)
        outer = min(ds.ntasks, length(jobs))
        inner = max(1, ds.ntasks ÷ outer)
        append!(tables, _readmap(jobs, outer) do (index, drop, take), _
            return _readpart(ds, ds.files[index], filters[index], drop, take, inner)
        end)
    end
    isempty(tables) && return _emptydatasettable(ds)
    return _vcattables(ds, tables)
end

# A table of the dataset's columns with no rows.
function _emptydatasettable(ds::Dataset)
    file = first(ds.files)
    table = _withpart(ds, file, _filescan(ds, file, Tables.AlwaysFalse(), 0, nothing),
        1) do handle
        return _readtable(handle, 1:0, _ByteBudget(ds.limits))
    end
    return _datasettable(ds, file, table::Table)
end

# One table of the rows of `tables` (one per file, in order, with the same columns).
function _vcattables(ds::Dataset, tables::Vector{Table})
    length(tables) == 1 && return only(tables)
    columns = _tablecolumns(first(tables))
    merged = NamedTuple{keys(columns)}(Tuple(_vcatcolumns(AbstractVector[
        _tablecolumns(table)[position] for table in tables])
        for position in eachindex(keys(columns))))
    rows = sum(_tablerows, tables)
    metadata = _withfields(_tablemetadata(first(tables)); num_rows=Int64(rows))
    return Table(metadata, _tableschema(first(tables)), merged, rows,
        _tablecolmetadata(first(tables)))
end

function _vcatcolumns(pieces::Vector{AbstractVector})
    if all(piece -> piece isa DataStrings.StringVector, pieces) ||
            all(piece -> piece isa DataStrings.BytesVector, pieces)
        payloads = DataStrings.StringPayload[]
        buffers = Vector{UInt8}[]
        starts = Int[]
        chunks = Vector{Vector{UInt8}}[]
        for piece in pieces
            push!(starts, length(payloads))
            append!(payloads, piece.payloads)
            push!(chunks, piece.buffers)
        end
        column = _rebasepayloads!(payloads, chunks, starts)
        T = eltype(first(pieces))
        V = first(pieces) isa DataStrings.StringVector ? DataStrings.StringVector{T} :
            DataStrings.BytesVector{T}
        return V(payloads, column, Val(:trusted))
    end
    return reduce(vcat, pieces)
end

# Rebases the buffer indices of payloads whose pieces start at `starts` onto the
# concatenation of the pieces' buffers, which it returns.
function _rebasepayloads!(payloads::Vector{DataStrings.StringPayload},
        chunks::Vector{Vector{Vector{UInt8}}}, starts::Vector{Int})
    buffers = Vector{UInt8}[]
    for (piece, chunk) in enumerate(chunks)
        base = length(buffers)
        if base > 0
            last = piece == length(starts) ? length(payloads) : starts[piece + 1]
            for index in (starts[piece] + 1):last
                payload = payloads[index]
                DataStrings.payloadlength(payload) > DataStrings.INLINE_MAX || continue
                buffer = UInt64(DataStrings.payloadbufidx(payload) + base)
                payloads[index] = DataStrings.StringPayload(payload.a,
                    (payload.b & 0xffffffff00000000) | buffer)
            end
        end
        append!(buffers, chunk)
    end
    return buffers
end

Table(ds::Dataset) = _readdataset(ds)

## Row groups: iteration, length, and indexing

# Each row group the dataset yields: (file, row group, offset, limit), or `nothing` when
# a filter with a row window leaves them unknown before reading.
function _datasetgroups(ds::Dataset)
    bound = ds.bound
    offset, limit = bound === nothing ? (0, nothing) : (bound.offset, bound.limit)
    filtered = bound !== nothing && bound.filter !== nothing
    filtered && (offset > 0 || limit !== nothing) && return nothing
    groups = Tuple{Int,Int,Int,Union{Nothing,Int}}[]
    skip, keep = offset, limit
    for (index, file) in enumerate(ds.files)
        if filtered
            append!(groups, (index, group, 0, nothing) for group in _filegroupsof(ds, index))
            continue
        end
        for (group, metadata) in enumerate(file.metadata.row_groups)
            keep === nothing || keep > 0 || break
            rows = Int(metadata.num_rows)
            drop = min(skip, rows)
            take = keep === nothing ? rows - drop : min(keep, rows - drop)
            skip -= drop
            keep === nothing || (keep -= take)
            take > 0 && push!(groups, (index, group, drop, take == rows - drop ? nothing :
                take))
        end
    end
    return groups
end

# The row groups of file `index` its folded filter does not rule out by statistics.
function _filegroupsof(ds::Dataset, index::Int)
    file = ds.files[index]
    filter = _filefilter(ds, file)
    filter === false && return Int[]
    return _withpart(ds, file, _filescan(ds, file, filter, 0, nothing), 1) do handle
        return Int[group for (group, _, _) in _filegroups(handle)]
    end::Vector{Int}
end

function _datasetgroupsknown(ds::Dataset)
    groups = _datasetgroups(ds)
    groups === nothing && throw(ArgumentError("the row groups of a Parquet.Dataset with " *
        "both a filter and a limit or offset are known only while reading; iterate it"))
    return groups
end

function _readdatasetgroup(ds::Dataset, (index, group, offset, limit))
    file = ds.files[index]
    filter = _filefilter(ds, file)
    table = _withpart(ds, file, _filescan(ds, file,
            filter === false ? Tables.AlwaysFalse() : filter, 0, nothing),
            ds.ntasks) do handle
        try
            return _readtable(handle, group:group, _ByteBudget(ds.limits);
                window=(offset, limit))
        catch err
            _rethrowfor(err, file.path)
        end
    end
    return _datasettable(ds, file, table::Table)
end

Base.length(ds::Dataset) = length(_datasetgroupsknown(ds))
Base.IteratorSize(::Type{Dataset}) = Base.SizeUnknown()
Base.eltype(::Type{Dataset}) = Table

function Base.getindex(ds::Dataset, index::Integer)
    groups = _datasetgroupsknown(ds)
    checkbounds(groups, index)
    return _readdatasetgroup(ds, groups[index])
end

function Base.iterate(ds::Dataset)
    groups = _datasetgroups(ds)
    groups === nothing || return iterate(ds, (groups, 1))
    return iterate(ds, (1, nothing, 1, ds.bound.offset, ds.bound.limit))
end

function Base.iterate(ds::Dataset, (groups, position)::Tuple{Vector,Int})
    position > length(groups) && return nothing
    return _readdatasetgroup(ds, groups[position]), (groups, position + 1)
end

# With a filter and a row window: each file's filtered row groups, in order, skipping
# and keeping rows until the window is filled.
function Base.iterate(ds::Dataset, (index, groups, position, skip, keep)::Tuple{Int,Any,Int,Int,Any})
    while index <= length(ds.files)
        keep == 0 && return nothing
        if groups === nothing
            groups, position = _filegroupsof(ds, index), 1
        end
        if position > length(groups)
            index, groups = index + 1, nothing
            continue
        end
        table = _readdatasetgroup(ds, (index, groups[position], 0, nothing))
        position += 1
        rows = _tablerows(table)
        drop = min(skip, rows)
        take = keep === nothing ? rows - drop : min(keep, rows - drop)
        skip -= drop
        keep === nothing || (keep -= take)
        take > 0 && return _slicetable(table, drop, take), (index, groups, position, skip, keep)
    end
    return nothing
end

Tables.partitions(ds::Dataset) = ds
Tables.istable(::Type{Dataset}) = true
Tables.columnaccess(::Type{Dataset}) = true
Tables.columns(ds::Dataset) = Tables.columns(_readdataset(ds))

function Tables.schema(ds::Dataset)
    outputs = _datasetoutputs(ds)
    return Tables.Schema(Tuple(name for (_, name) in outputs),
        Tuple(ds.types[index] for (index, _) in outputs))
end

Tables.columnnames(ds::Dataset) = Tables.schema(ds).names

function DataAPI.nrow(ds::Dataset)
    bound = ds.bound
    bound === nothing || bound.filter === nothing || throw(ArgumentError(
        "the row count of a filtered Parquet.Dataset is known only after reading it"))
    return sum(group -> something(group[4],
        Int(ds.files[group[1]].metadata.row_groups[group[2]].num_rows) - group[3]),
        _datasetgroupsknown(ds); init=0)
end

DataAPI.metadatasupport(::Type{Dataset}) = (read=true, write=false)
DataAPI.colmetadatasupport(::Type{Dataset}) = (read=true, write=false)

function _colmetadataentries(ds::Dataset)
    outputs = _datasetoutputs(ds)
    return Symbol[name for (_, name) in outputs], [ds.colmetadata[index] for (index, _) in outputs]
end

# The first file's key-value metadata, without its ARROW:schema (as for a `File`).
function _keyvaluemetadata(ds::Dataset)
    entries = something(first(ds.files).metadata.key_value_metadata, Metadata.KeyValue[])
    return filter(entry -> entry.key != "ARROW:schema", entries)
end

DataAPI.metadatakeys(ds::Dataset) = unique!(String[entry.key for entry in _keyvaluemetadata(ds)])

function DataAPI.metadata(ds::Dataset, key::AbstractString; style::Bool=false)
    for entry in Iterators.reverse(_keyvaluemetadata(ds))
        entry.key == key && return style ? (entry.value, :default) : entry.value
    end
    throw(KeyError(key))
end

function DataAPI.metadata(ds::Dataset, key::AbstractString, default; style::Bool=false)
    for entry in Iterators.reverse(_keyvaluemetadata(ds))
        entry.key == key && return style ? (entry.value, :default) : entry.value
    end
    return style ? (default, :default) : default
end

function _columnentries(x, column::Union{Symbol,Int})
    names, entries = _colmetadataentries(x)
    index = column isa Int ? column : findfirst(==(column), names)
    (index === nothing || !(1 <= index <= length(names))) && throw(ArgumentError(
        "$(column isa Symbol ? ":" : "")$column is not a column of this " *
        "$(nameof(typeof(x)))"))
    return entries[index]
end

function DataAPI.colmetadatakeys(x::Union{File,Table,Dataset}, column::Union{Symbol,Int})
    return unique!(String[first(entry) for entry in _columnentries(x, column)])
end

function DataAPI.colmetadatakeys(x::Union{File,Table,Dataset})
    names, entries = _colmetadataentries(x)
    return [name => unique!(String[first(entry) for entry in values])
        for (name, values) in zip(names, entries) if !isempty(values)]
end

function DataAPI.colmetadata(x::Union{File,Table,Dataset}, column::Union{Symbol,Int},
        key::AbstractString; style::Bool=false)
    for (name, value) in Iterators.reverse(_columnentries(x, column))
        name == key && return style ? (value, :default) : value
    end
    throw(KeyError(key))
end

function DataAPI.colmetadata(x::Union{File,Table,Dataset}, column::Union{Symbol,Int},
        key::AbstractString, default; style::Bool=false)
    for (name, value) in Iterators.reverse(_columnentries(x, column))
        name == key && return style ? (value, :default) : value
    end
    return style ? (default, :default) : default
end

function Base.show(io::IO, ds::Dataset)
    print(io, "Parquet.Dataset(", length(ds.files), " files, ",
        sum(file -> file.metadata.num_rows, ds.files; init=Int64(0)), " rows, ",
        length(_datasetoutputs(ds)), " columns)")
    return
end

Base.close(::Dataset) = nothing
