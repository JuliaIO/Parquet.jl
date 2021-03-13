"""
    read_parquet(path; kwargs...)

Returns the table contained in the parquet file or dataset (partitioned parquet files in a folder)
in a Tables.jl compatible format.

Options:
- `rows`: The row range to iterate through, all rows by default. Applicable only when reading a single file.
- `filter`: Filter function to apply while loading only a subset of partitions from a dataset.
- `batchsize`: Maximum number of rows to read in each batch (default: row count of first row group). Applied only when reading a single file, and to each file when reading a dataset.
- `use_threads`: Whether to use threads while reading the file; applicable only for Julia v1.3 and later and switched on by default if julia processes is started with multiple threads.
- `column_generator`: Function to generate a partitioned column when not found in the partitioned table. Parameters provided to the function: table, column index, length of column to generate. Default implementation determines column values from the table path.

One can easily convert the returned object to any Tables.jl compatible table e.g. DataFrames.DataFrame via

```
using DataFrames
df = DataFrame(read_parquet(path))
```
"""
function read_parquet(path; kwargs...)
    if isdir(path)
        Parquet.Dataset(path; kwargs...)
    elseif isfile(path)
        Parquet.Table(path; kwargs...)
    else
        error("Invalid path to parquet file or dataset - $path")
    end
end

"""
    Parquet.Table(path; kwargs...)

Returns the table contained in the parquet file in a Tables.jl compatible format.

Options:
- `rows`: The row range to iterate through, all rows by default.
- `batchsize`: Maximum number of rows to read in each batch (default: row count of first row group).
- `use_threads`: Whether to use threads while reading the file; applicable only for Julia v1.3 and later and switched on by default if julia processes is started with multiple threads.

One can easily convert the returned object to any Tables.jl compatible table e.g. DataFrames.DataFrame via

```
using DataFrames
df = DataFrame(read_parquet(path))
```
"""
struct Table <: Tables.AbstractColumns
    path::String
    ncols::Int
    rows::Union{Nothing,UnitRange}
    batchsize::Union{Nothing,Signed}
    use_threads::Bool
    parfile::Parquet.File
    schema::Tables.Schema
    lookup::Dict{Symbol, Int} # map column name => index
    columns::Vector{AbstractVector}
    column_generator::Function

    Table(path, sch::Tables.Schema; kwargs...) = Table(path, Parquet.File(path), sch; kwargs...)
    function Table(path, parfile::Parquet.File=Parquet.File(path), sch::Tables.Schema=tables_schema(parfile);
            rows::Union{Nothing,UnitRange}=nothing,
            batchsize::Union{Nothing,Signed}=nothing,
            column_generator::Function=column_generator,
            use_threads::Bool=(nthreads() > 1))
        ncols = length(sch.names)
        lookup = Dict{Symbol, Int}(nm => i for (i, nm) in enumerate(sch.names))
        new(path, ncols, rows, batchsize, use_threads, parfile, sch, lookup, AbstractVector[], column_generator)
    end
end

function close(table::Table)
    empty!(getfield(table, :columns))
    close(getfield(table, :parfile))
end

function column_generator(table::Table, colidx::Int, len::Int)
    schema = getfield(table, :schema)
    coltype = schema.types[colidx]
    missingval = nonmissingtype(coltype) === coltype ? undef : missing
    Array{coltype}(missingval, len)
end

"""
Represents one partition of the parquet file.
Typically a row group, but could be any other unit as mentioned while opening the table.
"""
struct TablePartition <: Tables.AbstractColumns
    table::Table
    columns::Vector{AbstractVector}
end

"""
Iterator to iterate over partitions of a parquet file, returned by the `Tables.partitions(table)` method.
Each partition is typically a row group, but could be any other unit as mentioned while opening the table.
"""
struct TablePartitions
    table::Table
    ncols::Int
    schema::Tables.Schema
    cursor::BatchedColumnsCursor

    function TablePartitions(table::Table)
        new(table, getfield(table, :ncols), getfield(table, :schema), cursor(table))
    end
end
length(tp::TablePartitions) = length(tp.cursor)
function iterated_partition(partitions::TablePartitions, iterresult)
    (iterresult === nothing) && (return nothing)
    chunk, batchid = iterresult
    columns = AbstractVector[]
    for colidx in 1:partitions.ncols
        colname = partitions.schema.names[colidx]
        if hasproperty(chunk, colname)
            push!(columns, getproperty(chunk, colname))
        else
            generator = getfield(partitions.table, :column_generator)
            push!(columns, generator(partitions.table, colidx, length(first(chunk))))
        end
    end
    TablePartition(partitions.table, columns), batchid
end
Base.iterate(partitions::TablePartitions, batchid) = iterated_partition(partitions, iterate(partitions.cursor, batchid))
Base.iterate(partitions::TablePartitions) = iterated_partition(partitions, iterate(partitions.cursor))

function cursor(table::Table)
    kwargs = Dict{Symbol,Any}(:use_threads => getfield(table, :use_threads), :reusebuffer => false)
    (getfield(table, :rows) === nothing) || (kwargs[:rows] = getfield(table, :rows))
    (getfield(table, :batchsize) === nothing) || (kwargs[:batchsize] = getfield(table, :batchsize))
    BatchedColumnsCursor(getfield(table, :parfile); kwargs...)
end

loaded(table::Table) = !isempty(getfield(table, :columns))
load(table::Table) = load(table, cursor(table))
function load(table::Table, colcursor::BatchedColumnsCursor)
    chunks = [chunk for chunk in colcursor]
    ncols = getfield(table, :ncols)
    columns = getfield(table, :columns)
    schema = getfield(table, :schema)
    generator = getfield(table, :column_generator)

    empty!(columns)
    nchunks = length(chunks)
    if nchunks == 1
        chunk = chunks[1]
        for colidx in 1:ncols
            colname = schema.names[colidx]
            if hasproperty(chunk, colname)
                push!(columns, getproperty(chunk, colname))
            else
                push!(columns, generator(table, colidx, length(first(chunk))))
            end
        end
    elseif nchunks > 1
        for colidx in 1:ncols
            colname = schema.names[colidx]
            coltype = schema.types[colidx]
            vecs = Vector{coltype}[]
            for chunkidx in 1:nchunks
                chunk = chunks[chunkidx]
                if hasproperty(chunk, colname)
                    push!(vecs, getproperty(chunk, colname))
                else
                    push!(vecs, generator(table, colidx, length(first(chunk))))
                end
            end
            push!(columns, ChainedVector(vecs))
        end
    else
        schema = getfield(table, :schema)
        coltypes = schema.types
        for colidx in 1:ncols
            push!(columns, (coltypes[colidx])[])
        end
    end
    nothing
end

Tables.istable(::Table) = true
Tables.columnaccess(::Table) = true
Tables.schema(t::Table) = getfield(t, :schema)
Tables.columnnames(t::Table) = getfield(t, :schema).names
Tables.columns(t::Table) = Tables.CopiedColumns(t)
Tables.getcolumn(t::Table, nm::Symbol) = Tables.getcolumn(t, getfield(t, :lookup)[nm])
function Tables.getcolumn(t::Table, i::Int)
    loaded(t) || load(t)
    getfield(t, :columns)[i]
end
Tables.partitions(t::Table) = TablePartitions(t)

Tables.istable(::TablePartition) = true
Tables.columnaccess(::TablePartition) = true
Tables.schema(tp::TablePartition) = Tables.schema(getfield(tp, :table))
Tables.columnnames(tp::TablePartition) = Tables.columnnames(getfield(tp, :table))
Tables.columns(tp::TablePartition) = Tables.CopiedColumns(tp)
Tables.getcolumn(tp::TablePartition, nm::Symbol) = Tables.getcolumn(tp, getfield(getfield(tp, :table), :lookup)[nm])
Tables.getcolumn(tp::TablePartition, i::Int) = getfield(tp, :columns)[i]
