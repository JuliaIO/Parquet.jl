const DATASET_METADATA_FILES = ("_common_metadata", "_metadata")

"""
    Parquet.Dataset(path; kwargs...)

Returns the table contained in the parquet dataset in an Tables.jl compatible format.
A dataset comprises of multiple parquet files and optionally some metadata files.

These options if provided are passed along while reading each parquet file in the dataset:
- `filter`: Filter function that takes the path to partitioned file and returns boolean to indicate whether to include the partition while loading. All partitions are loaded by default.
- `batchsize`: Maximum number of rows to read in each batch (default: row count of first row group). Applied to each file in the partition.
- `use_threads`: Whether to use threads while reading the file; applicable only for Julia v1.3 and later and switched on by default if julia processes is started with multiple threads.
- `column_generator`: Function to generate a partitioned column when not found in the partitioned table. Parameters provided to the function: table, column index, length of column to generate. Default implementation determines column values from the table path.

One can easily convert the returned object to any Tables.jl compatible table e.g. DataFrames.DataFrame via

```
using DataFrames
df = DataFrame(read_parquet(path))
```
"""
struct Dataset <: Tables.AbstractColumns
    path::String
    filter::Function
    ncols::Int
    kwargs::NamedTuple{(:batchsize, :use_threads, :column_generator), Tuple{Union{Nothing,Signed}, Bool, Function}}
    schema::Tables.Schema
    lookup::Dict{Symbol, Int} # map column name => index
    columns::Vector{AbstractVector}
    tables::Vector{Table}

    function Dataset(path;
            filter::Function=(path)->true,
            batchsize::Union{Nothing,Signed}=nothing,
            column_generator::Function=column_generator,
            use_threads::Bool=(nthreads() > 1))

        isdir(path) || error("Invalid Dataset path. Not a directory - $path")
        sch = dataset_schema(string(path))
        ncols = length(sch.names)
        lookup = Dict{Symbol, Int}(nm => i for (i, nm) in enumerate(sch.names))
        kwargs = (batchsize=batchsize, use_threads=use_threads, column_generator=dataset_column_generator)
        new(path, filter, ncols, kwargs, sch, lookup, AbstractVector[], Table[])
    end
end

const PARTITION_DATE_FORMATS = [dateformat"Y-m-d", dateformat"Y-m-d HH:MI:SS", dateformat"Y-m-dTHH:MI:SS"]
const PARTITION_DATETIME_FORMATS = [dateformat"Y-m-d HH:MI:SS", dateformat"Y-m-dTHH:MI:SS", dateformat"Y-m-d"]
function parse_date(missingstrval)
    for format in PARTITION_DATE_FORMATS
        try
            return Date(missingstrval, format)
        catch ex
            (format == last(PARTITION_DATE_FORMATS)) && rethrow()
        end
    end
end
function parse_datetime(missingstrval)
    for format in PARTITION_DATETIME_FORMATS
        try
            return DateTime(missingstrval, format)
        catch ex
            (format == last(PARTITION_DATETIME_FORMATS)) && rethrow()
        end
    end
end

function dataset_column_generator(table::Table, colidx::Int, len::Int)
    table_path = getfield(table, :path)
    schema = getfield(table, :schema)
    colname = schema.names[colidx]
    coltype = schema.types[colidx]

    pattern = Regex("\\S+[\\/\\\\]?$(colname)=([a-zA-Z0-9 :\\-\\.]*)[\\/\\\\]?\\S+")
    matches = match(pattern, table_path)
    if (matches !== nothing) && (length(matches.captures) == 1)
        missingstrval = matches.captures[1]
        nm_coltype = nonmissingtype(coltype)
        if nm_coltype <: Real
            missingval = parse(nm_coltype, lowercase(missingstrval))
        elseif nm_coltype <: Date
            missingval = parse_date(missingstrval)
        elseif nm_coltype <: DateTime
            missingval = parse_datetime(missingstrval)
        elseif nm_coltype <: String
            missingval = string(missingstrval)
        else
            error("Unhandled dataset partitioned column type $nm_coltype for column $colname of table $table_path")
        end
    else
        missingval = nonmissingtype(coltype) === coltype ? undef : missing
    end

    if missingval === undef || missingval === missing
        Array{coltype}(missingval, len)
    else
        fill!(Array{coltype}(undef, len), missingval)
    end
end

function close(dataset::Dataset)
    empty!(getfield(dataset, :columns))
    for table in getfield(dataset, :tables)
        close(table)
    end
    empty!(getfield(dataset, :tables))
    nothing
end

function dataset_schema(path::String)
    schema = nothing

    # look for _common_metadata or _metadata file
    for name in DATASET_METADATA_FILES
        meta_file = joinpath(path, name)
        if isfile(meta_file)
            schema = tables_schema(Parquet.File(meta_file))
            break
        end
    end

    if schema === nothing
        # else extract schema from any file in the dataset
        for (root, dirs, files) in walkdir(path)
            for file in files
                full_filename = joinpath(root, file)
                if is_par_file(full_filename)
                    schema = tables_schema(Parquet.File(full_filename))
                    break
                end
            end
            (schema === nothing) || break
        end
    end

    schema
end

"""
Iterator to iterate over partitions of a parquet dataset, returned by the `Tables.partitions(dataset)` method.
Each partition is a Parquet.Table.
"""
struct DatasetPartitions
    dataset::Dataset
    ncols::Int
    filter::Function

    function DatasetPartitions(dataset::Dataset, filter::Function)
        new(dataset, getfield(dataset, :ncols), filter)
    end
end

function iterated_partition(partitions::DatasetPartitions, cursor)
    partition = nothing
    walker, root, files, fileidx, step = cursor
    schema = getfield(partitions.dataset, :schema)

    while partition === nothing
        if 0 < fileidx <= length(files)  # we are iterating on files in a directory
            file = files[fileidx]
            fileidx += 1
            if !(file in DATASET_METADATA_FILES)
                full_filename = joinpath(root, file)
                if partitions.filter(full_filename) && is_par_file(full_filename)
                    partition = Table(full_filename, schema; getfield(partitions.dataset, :kwargs)...)
                end
            end
        else # walk further into directory tree
            itervals = (fileidx == 0) ? iterate(walker) : iterate(walker, step)
            (itervals === nothing) && (return nothing)  # end of directory tree

            dirinfo, step = itervals
            root, dirs, files = dirinfo
            fileidx = 1
        end
    end
    partition, (walker, root, files, fileidx, step)
end
Base.iterate(partitions::DatasetPartitions, cursor) = iterated_partition(partitions, cursor)
Base.iterate(partitions::DatasetPartitions) = iterated_partition(partitions, (walkdir(getfield(partitions.dataset, :path)), "", [], 0, nothing))

loaded(dataset::Dataset) = !isempty(getfield(dataset, :columns))
function load(dataset::Dataset)
    tables = getfield(dataset, :tables)
    columns = getfield(dataset, :columns)
    ncols = getfield(dataset, :ncols)
    empty!(tables)
    empty!(columns)
    for table in Tables.partitions(dataset)
        push!(tables, table)
        if isempty(columns)
            for colidx in 1:ncols
                push!(columns, ChainedVector([Tables.getcolumn(table, colidx)]))
            end
        else
            for colidx in 1:ncols
                append!(columns[colidx], Tables.getcolumn(table, colidx))
            end
        end
    end
    nothing
end

Tables.istable(::Dataset) = true
Tables.columnaccess(::Dataset) = true
Tables.schema(d::Dataset) = getfield(d, :schema)
Tables.columnnames(d::Dataset) = getfield(d, :schema).names
Tables.columns(d::Dataset) = Tables.CopiedColumns(d)
Tables.getcolumn(d::Dataset, nm::Symbol) = Tables.getcolumn(d, getfield(d, :lookup)[nm])
function Tables.getcolumn(d::Dataset, i::Int)
    loaded(d) || load(d)
    getfield(d, :columns)[i]
end
Tables.partitions(d::Dataset) = DatasetPartitions(d, getfield(d, :filter))