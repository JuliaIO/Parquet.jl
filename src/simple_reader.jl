"""
    read_parquet(path; kwargs...)
    Parquet.Table(path; kwargs...)

Returns the table contained in the parquet file in an Tables.jl compatible format.

Options:
- `rows`: the row range to iterate through, all rows by default.
- `batchsize`: maximum number of rows to read in each batch (default: row count of first row group).
- `use_threads`: whether to use threads while reading the file; applicable only for Julia v1.3 and later and switched on by default if julia processes is started with multiple threads.

One can easily convert the returned object to any Tables.jl compatible table e.g.
DataFrames.DataFrame via

```
using DataFrames
df = DataFrame(read_parquet(path; copycols=false))
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

    function Table(path;
            rows::Union{Nothing,UnitRange}=nothing,
            batchsize::Union{Nothing,Signed}=nothing,
            use_threads::Bool=(nthreads() > 1))
        parfile = Parquet.File(path)
        sch = tables_schema(parfile)
        ncols = length(sch.names)
        lookup = Dict{Symbol, Int}(nm => i for (i, nm) in enumerate(sch.names))
        new(path, ncols, rows, batchsize, use_threads, parfile, sch, lookup, AbstractVector[])
    end
end

function close(table::Table)
    empty!(getfield(table, :columns))
    close(getfield(table, :parfile))
end

const read_parquet = Table

struct TablePartition <: Tables.AbstractColumns
    table::Table
    columns::Vector{AbstractVector}
end

struct TablePartitions
    table::Table
    ncols::Int
    cursor::BatchedColumnsCursor

    function TablePartitions(table::Table)
        new(table, getfield(table, :ncols), cursor(table))
    end
end
length(tp::TablePartitions) = length(tp.cursor)
function iterated_partition(partitions::TablePartitions, iterresult)
    (iterresult === nothing) && (return nothing)
    chunk, batchid = iterresult
    TablePartition(partitions.table, AbstractVector[chunk[colidx] for colidx in 1:partitions.ncols]), batchid
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
function load(table::Table, chunks::BatchedColumnsCursor)
    chunks = [chunk for chunk in chunks]
    ncols = getfield(table, :ncols)
    columns = getfield(table, :columns)

    empty!(columns)
    nchunks = length(chunks)
    if nchunks == 1
        for colidx in 1:ncols
            push!(columns, chunks[1][colidx])
        end
    elseif nchunks > 1
        for colidx in 1:ncols
            push!(columns, ChainedVector([chunks[chunkidx][colidx] for chunkidx = 1:nchunks]))
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
