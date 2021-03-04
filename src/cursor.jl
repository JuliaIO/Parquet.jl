##
# layer 3 access
# read data as records which are named tuple representations of the schema

##
# Column cursor iterates through all values of the column, including null values.
# Each iteration returns the value (as a Union{T,Nothing}), definition level, and repetition level for each value.
# Row can be deduced from repetition level.
mutable struct ColCursor{T}
    par::Parquet.File
    colname::Vector{String}                                 # column name (full path in schema)
    colnamesym::Vector{Symbol}                              # column name converted to symbols
    required_at::Vector{Bool}                               # at what level in the schema the column is required
    repeated_at::Vector{Bool}                               # at what level in the schema the column is repeated
    logical_converter_fn::Function                          # column converter function as per schema or options (identity if none)
    maxdefn::Int32                                          # maximum definition level of the column as per schema

    row::Int                                                # current row
    rows::UnitRange{Int}                                    # row range to limit to
    rowgroups::Vector{RowGroup}                             # list of row groups
    row_positions::Vector{Int64}                            # starting positions if each rowgroup
    rgidx::Int                                              # current rowgroup
    colchunks::Union{Vector{ColumnChunk},Nothing}           # list of column chunk in the current rowgroup (metadata only)
    ccidx::Int                                              # index of column chunk that contains the current position

    pageiter::Union{Nothing,ColumnChunkPageValues{T}}       # iterator for the current page
    pagerange::Union{UnitRange{Int64},Nothing}              # range of positions (rows) that the current page contains
    pageiternext::Int64
    pagevals::OutputState{T}
    page_defn::OutputState{Int32}
    page_repn::OutputState{Int32}

    valpos::Int64                                           # current position within values of the current page
    levelpos::Int64                                         # current position within levels of the current page
    levelend::Int64

    function ColCursor{T}(par::Parquet.File, row_positions::Vector{Int64}, colname::Vector{String}, rows::UnitRange) where T
        colnamesym = [Symbol(name) for name in colname]
        sch = schema(par)

        len_colname_parts = length(colname)
        required_at = Array{Bool}(undef, len_colname_parts)
        repeated_at = Array{Bool}(undef, len_colname_parts)
        for idx in 1:len_colname_parts
            partname = colname[1:idx]
            required_at[idx] = isrequired(sch, partname)         # determine whether field is optional and repeated
            repeated_at[idx] = isrepeated(sch, partname)
        end

        logical_converter_fn = logical_converter(sch, colname)
        maxdefn = max_definition_level(sch, colname)
        new{T}(par, colname, colnamesym, required_at, repeated_at, logical_converter_fn, maxdefn, 0, rows, rowgroups(par), row_positions, 0, nothing, 0, nothing, nothing, 0)
    end
end

function ColCursor(par::Parquet.File, colname::Vector{String}; rows::UnitRange=1:nrows(par), row::Signed=first(rows))
    row_positions = rowgroup_row_positions(par)
    @assert last(rows) <= nrows(par)
    @assert first(rows) >= 1
    T = elemtype(schema(par), colname)
    cursor = ColCursor{T}(par, row_positions, colname, rows)
    setrow(cursor, Int64(row))
    cursor
end

eltype(::Type{ColCursor{T}}) where {T} = NamedTuple{(:value, :defn_level, :repn_level),Tuple{Union{Nothing,T},Int64,Int64}}
length(cursor::ColCursor) = length(cursor.rows)

function setrow(cursor::ColCursor{T}, row::Int64) where {T}
    # check if cursor is done
    if ((cursor.row > 0) && !(cursor.row in cursor.rows)) || isempty(cursor.rowgroups)
        cursor.colchunks = nothing
        cursor.ccidx = 0
        cursor.valpos = cursor.levelpos = 0
        cursor.levelend = -1
        return
    end

    # check if we are already on that row
    (cursor.row == row) && return

    par = cursor.par
    prevrow = cursor.row
    cursor.row = row

    # advance row group if needed
    if (cursor.rgidx == 0) || (cursor.row_positions[cursor.rgidx+1] <= row)
        rgidx = findfirst(x->x>row, cursor.row_positions)
        if rgidx === nothing
            cursor.rgidx = length(cursor.rowgroups) + 1
            return
        else
            cursor.rgidx = rgidx - 1
            rg = cursor.rowgroups[cursor.rgidx]
            cursor.colchunks = columns(par, rg, cursor.colname)
            cursor.pagerange = nothing
            cursor.pageiternext = 0
            cursor.pageiter = nothing
            cursor.ccidx = 0
        end
    end

    # advance column chunk and page within column  chunk if needed
    while (cursor.pagerange === nothing) || !(row in cursor.pagerange)
        cursor.valpos = cursor.levelpos = 0
        cursor.levelend = -1
        startrow = (cursor.pagerange === nothing) ? cursor.row_positions[cursor.rgidx] : (last(cursor.pagerange) + 1)
        if cursor.pageiter === nothing
            # need to start a page cursor for a new column chunk
            cursor.ccidx += 1
            cursor.pageiter = ColumnChunkPageValues(par, cursor.colchunks[cursor.ccidx], T, cursor.logical_converter_fn)
            cursor.pageiternext = 0
        end
        page_iter_result = (cursor.pageiternext > 0) ? iterate(cursor.pageiter, cursor.pageiternext) : iterate(cursor.pageiter)
        if page_iter_result === nothing
            # need to start on a new column chunk
            cursor.pageiter = nothing
        else
            resultdata,cursor.pageiternext = page_iter_result
            cursor.pagevals = resultdata.value
            cursor.page_repn = resultdata.repn_level
            cursor.page_defn = resultdata.defn_level
            nrowspage = 0
            if cursor.pageiter.has_repn_levels
                for i in 1:cursor.page_repn.offset
                    (cursor.page_repn.data[i] !== Int32(0)) && (nrowspage += 1)
                end
            else
                if cursor.pageiter.has_defn_levels
                    nrowspage = cursor.page_defn.offset
                else
                    nrowspage = cursor.pagevals.offset # number of values is number of rows
                end
            end
            if nrowspage > 0
                cursor.pagerange = startrow:(startrow+nrowspage-1)
            end
        end
    end

    # advance value and level positions within the page
    if cursor.pageiter.has_defn_levels
        if cursor.levelpos == 0
            for idx in 1:(row - first(cursor.pagerange) + 1)
                cursor.levelpos += 1
                (cursor.page_defn.data[cursor.levelpos] === cursor.maxdefn) && (cursor.valpos += 1)
            end
        elseif cursor.page_defn.data[cursor.levelpos] === cursor.maxdefn
            cursor.valpos += 1
        end
    else
        # all entries are required, so there must be a corresponding value
        cursor.valpos = cursor.levelpos = row - first(cursor.pagerange) + 1
    end

    if cursor.pageiter.has_repn_levels
        # multiple entries may constitute one row
        if cursor.levelend == -1
            cursor.levelend = Int64(something(findnext(x->x===Int32(0), cursor.page_repn.data, cursor.levelpos+1), cursor.page_repn.offset+1) - 1)
        end
    else
        # no repetitions, so each entry corresponds to one full row
        cursor.levelend = cursor.levelpos
    end
    nothing
end

function _start(cursor::ColCursor)
    setrow(cursor, Int64(first(cursor.rows)))
    cursor.row, cursor.levelpos
end
function _done(cursor::ColCursor, rowandlevel::Tuple{Int64,Int64})
    row, levelpos = rowandlevel
    (levelpos > cursor.levelend) || !(row in cursor.rows)
end
function _next(cursor::ColCursor{T}, rowandlevel::Tuple{Int64,Int64}) where {T}
    # find values for current row and level in row
    row, levelpos = rowandlevel
    (levelpos == cursor.levelpos) || throw(InvalidStateException("Invalid column cursor state", :levelpos))

    maxdefn = cursor.maxdefn
    defn_level = cursor.pageiter.has_defn_levels ? cursor.page_defn.data[cursor.levelpos] : maxdefn
    repn_level = cursor.pageiter.has_repn_levels ? cursor.page_repn.data[cursor.levelpos] : Int32(0)
    if defn_level == maxdefn
        val = (cursor.pagevals.data[cursor.valpos])::T
    else
        val = nothing
    end

    # advance row
    cursor.levelpos += 1
    if cursor.levelpos > cursor.levelend
        row += 1
        cursor.levelend = -1
        setrow(cursor, Int64(row))
    end

    NamedTuple{(:value, :defn_level, :repn_level),Tuple{Union{Nothing,T},Int64,Int64}}((val, defn_level, repn_level)), (row, cursor.levelpos)
end

function Base.iterate(cursor::ColCursor{T}, state) where {T}
    _done(cursor, state) && return nothing
    return _next(cursor, state)
end

function Base.iterate(cursor::ColCursor)
    r = iterate(cursor, _start(cursor))
    return r
end

##

mutable struct BatchedColumnsCursor{T}
    par::Parquet.File
    colnames::Vector{Vector{String}}
    colcursors::Vector{ColCursor}
    colstates::Vector{Tuple{Int64,Int64}}
    colbuffs::Vector{Union{Nothing,Vector}}
    batchid::Int
    rows::UnitRange{Int64}
    row::Int64
    batchsize::Int64
    nbatches::Int
    reusebuffer::Bool
    use_threads::Bool
end

"""
Create cursor to iterate over batches of column values. Each iteration returns a named tuple of column names with batch of column values. Files with nested schemas can not be read with this cursor.

```julia
BatchedColumnsCursor(par::Parquet.File; kwargs...)
```

Cursor options:
- `rows`: the row range to iterate through, all rows by default.
- `batchsize`: maximum number of rows to read in each batch (default: row count of first row group).
- `reusebuffer`: boolean to indicate whether to reuse the buffers with every iteration; if each iteration processes the batch and does not need to refer to the same data buffer again, then setting this to `true` reduces GC pressure and can help significantly while processing large files.
- `use_threads`: whether to use threads while reading the file; applicable only for Julia v1.3 and later and switched on by default if julia processes is started with multiple threads.
"""
function BatchedColumnsCursor(par::Parquet.File;
        rows::UnitRange=1:nrows(par),
        batchsize::Signed=length(rows) > 0 ? min(length(rows), first(rowgroups(par)).num_rows) : 0,
        reusebuffer::Bool=false,
        use_threads::Bool=(nthreads() > 1))

    sch = schema(par)

    # supports only non nested columns as of now
    if !all(num_children(schemaelem) == 0 for schemaelem in sch.schema[2:end])
        error("nested schemas are not supported with BatchedColumnsCursor yet")
    end

    colcursors = [ColCursor(par, colname; rows=rows) for colname in colnames(par)]
    rectype = ntcolstype(sch, sch.schema[1])
    nbatches = batchsize > 0 ? ceil(Int, length(rows)/batchsize) : 0
    colbuffs = Union{Nothing,Vector}[nothing for idx in 1:length(colcursors)]

    BatchedColumnsCursor{rectype}(par, colnames(par), colcursors, Array{Tuple{Int64,Int64}}(undef, length(colcursors)), colbuffs, 1, rows, first(rows), batchsize, nbatches, reusebuffer, (VERSION < v"1.3") ? false : use_threads)
end

eltype(::Type{BatchedColumnsCursor{T}}) where {T} = T
length(cursor::BatchedColumnsCursor) = cursor.nbatches

function colcursor_advance(colcursor::ColCursor, rows_by::Int64, vals_by::Int64=rows_by)
    if (colcursor.row + rows_by) > last(colcursor.pagerange)
        setrow(colcursor, Int64(colcursor.row+rows_by))
    else
        colcursor.valpos += vals_by
        colcursor.row += rows_by
        colcursor.levelpos += rows_by
    end
    nothing
end

function colcursor_values(colcursor::ColCursor{T}, batchsize::Int64, ::Type{Vector{Union{Missing,T}}}, cache) where {T}
    row = colcursor.row
    batchsize = min(batchsize, last(colcursor.rows)-row+1)
    vals = (cache === nothing) ? Array{Union{Missing,T}}(undef, batchsize) : resize!(cache::Vector{Union{Missing,T}}, batchsize)

    fillpos = 1
    while fillpos <= batchsize
        pagevals = colcursor.pagevals
        defn_levels = colcursor.page_defn
        val_idx = (colcursor.valpos == 0) ? 0 : (colcursor.valpos-1)
        nvals_from_page = min(batchsize - fillpos + 1, defn_levels.offset - colcursor.levelpos + 1)
        @inbounds for idx in 1:nvals_from_page
            if defn_levels.data[colcursor.levelpos+idx-1] === Int32(1)
                vals[fillpos+idx-1] = pagevals.data[val_idx+=1]
            else
                vals[fillpos+idx-1] = missing
            end
        end
        fillpos += nvals_from_page
        valposincr = (colcursor.valpos == 0) ? val_idx : (val_idx - colcursor.valpos + 1)
        colcursor_advance(colcursor, nvals_from_page, Int64(valposincr))
    end
    vals
end

function colcursor_values(colcursor::ColCursor{T}, batchsize::Int64, ::Type{Vector{T}}, cache) where {T}
    row = colcursor.row
    batchsize = min(batchsize, last(colcursor.rows)-row+1)
    vals = (cache === nothing) ? Array{T}(undef, batchsize) : resize!(cache::Vector{T}, batchsize)

    fillpos = 1
    while fillpos <= batchsize
        pagevals = colcursor.pagevals
        nvals_from_page = min(batchsize - fillpos + 1, pagevals.offset - colcursor.valpos + 1)
        @inbounds for idx in 1:nvals_from_page
            vals[fillpos+idx-1] = pagevals.data[pagevals.offset+idx]
        end
        fillpos += nvals_from_page
        colcursor_advance(colcursor, nvals_from_page)
    end
    vals
end

function Base.iterate(cursor::BatchedColumnsCursor{T}, batchid) where {T}
    (batchid > length(cursor)) && (return nothing)

    colcursors = cursor.colcursors
    coltypes = T.types
    if cursor.use_threads
        L = length(colcursors)
        colvals = Array{Any}(undef, L)
        @threads for idx in 1:L
            colbuff = cursor.reusebuffer ? cursor.colbuffs[idx] : nothing
            colvals[idx] = colcursor_values(colcursors[idx], cursor.batchsize, coltypes[idx], colbuff)
        end
        cursor.reusebuffer && (cursor.colbuffs = colvals)
    else
        if cursor.reusebuffer
            colvals = cursor.colbuffs = [colcursor_values(colcursor,cursor.batchsize,coltype,colbuff) for (colcursor,coltype,colbuff) in zip(colcursors,coltypes,cursor.colbuffs)]
        else
            colvals = [colcursor_values(colcursor,cursor.batchsize,coltype,nothing) for (colcursor,coltype) in zip(colcursors,coltypes)]
        end
    end

    cursor.row += cursor.batchsize
    cursor.batchid += 1
    T(colvals), cursor.batchid
end

function Base.iterate(cursor::BatchedColumnsCursor{T}) where {T}
    cursor.row = first(cursor.rows)
    for colcursor in cursor.colcursors
        setrow(colcursor, Int64(cursor.row))
    end
    iterate(cursor, cursor.batchid)
end


##

mutable struct RecordCursor{T}
    par::Parquet.File
    colnames::Vector{Vector{String}}
    colcursors::Vector{ColCursor}
    colstates::Vector{Tuple{Int64,Int64}}
    rows::UnitRange{Int64}                      # rows to scan over
    row::Int64                                  # current row
end

"""
Create cursor to iterate over records. In parallel mode, multiple remote cursors can be created and iterated on in parallel.

```julia
RecordCursor(par::Parquet.File; kwargs...)
```

Cursor options:
- `rows`: the row range to iterate through, all rows by default.
- `colnames`: the column names to retrieve; all by default
"""
function RecordCursor(par::Parquet.File; rows::UnitRange=1:nrows(par), colnames::Vector{Vector{String}}=colnames(par))
    colcursors = [ColCursor(par, colname; rows=rows, row=first(rows)) for colname in colnames]
    sch = schema(par)
    rectype = ntelemtype(sch, sch.schema[1])
    RecordCursor{rectype}(par, colnames, colcursors, Array{Tuple{Int64,Int64}}(undef, length(colcursors)), rows, first(rows))
end

eltype(::Type{RecordCursor{T}}) where {T} = T
length(cursor::RecordCursor) = length(cursor.rows)

function Base.iterate(cursor::RecordCursor{T}, row) where {T}
    (row > last(cursor.rows)) && (return nothing)

    states = cursor.colstates
    cursors = cursor.colcursors

    colvals = Dict{Symbol,Any}()
    col_repeat_state = Dict{Tuple{Int,Int},Int}()
    for colid in 1:length(states)  # for each column
        colcursor = cursors[colid]
        colstate = states[colid]
        states[colid] = update_record(cursor.par, colvals, colid, colcursor, colstate, col_repeat_state)
    end
    cursor.row += 1
    _nt(colvals, T), cursor.row
end

function Base.iterate(cursor::RecordCursor{T}) where {T}
    cursor.row = first(cursor.rows)
    cursor.colstates = [_start(colcursor) for colcursor in cursor.colcursors]
    iterate(cursor, cursor.row)
end

function _val_or_missing(dict::Dict{Symbol,Any}, k::Symbol, ::Type{T}) where {T}
    v = get(dict, k, missing)
    if isa(v, Vector)
        elt = eltype(v)
        if Dict{Symbol,Any} <: elt
            nonmissing_elt = nonmissingtype(eltype(T))
            v = [el === missing ? el : _nt(el,nonmissing_elt) for el in v]
        end
    end
    (isa(v, Dict{Symbol,Any}) ? _nt(v, nonmissingtype(T)) : v)::T
end

@generated function _nt(dict::Dict{Symbol,Any}, ::Type{T}) where {T}
    names = fieldnames(T)
    strnames = ["$n" for n in names]
    quote
        return T(($([:(_val_or_missing(dict,Symbol($(strnames[i])),$(fieldtype(T,i)))) for i in 1:length(names)]...),))
    end
end

default_init(::Type{Vector{T}}) where {T} = Vector{T}()
default_init(::Type{Dict{Symbol,Any}}) = Dict{Symbol,Any}()
default_init(::Type{T}) where {T} = ccall(:jl_new_struct_uninit, Any, (Any,), T)::T

function update_record(par::Parquet.File, row::Dict{Symbol,Any}, colid::Int, colcursor::ColCursor, colcursor_state::Tuple{Int64,Int64}, col_repeat_state::Dict{Tuple{Int,Int},Int})
    colpos = colcursor.row
    # iterate all repeated values from the column cursor (until it advances to the next row)
    while !_done(colcursor, colcursor_state)
        colval, colcursor_state = _next(colcursor, colcursor_state)                                                        # for each value, defn level, repn level in column
        update_record(par, row, colid, colcursor, colval.value, colval.defn_level, colval.repn_level, col_repeat_state)    # update record
        (colcursor.row > colpos) && break
    end
    colcursor_state # return new colcursor state
end

function update_record(par::Parquet.File, row::Dict{Symbol,Any}, colid::Int, colcursor::ColCursor, val, defn_level::Int64, repn_level::Int64, col_repeat_state::Dict{Tuple{Int,Int},Int})
    nameparts = colcursor.colname
    symnameparts = colcursor.colnamesym
    required_at = colcursor.required_at
    repeated_at = colcursor.repeated_at

    lparts = length(nameparts)
    sch = par.schema
    F = row  # the current field corresponding to the level in nameparts
    Fdefn = 0
    Frepn = 0

    # for each name part of colname (a field)
    for idx in 1:lparts
        colname = view(nameparts, 1:idx)
        #@debug("updating part $colname of $nameparts isnull:$(val === nothing), def:$(defn_level), rep:$(repn_level)")
        leaf = nameparts[idx]
        symleaf = symnameparts[idx]

        required = required_at[idx]                 # determine whether field is optional and repeated
        repeated = repeated_at[idx]
        required || (Fdefn += 1)                    # if field is optional, increment defn level
        repeated && (Frepn += 1)                    # if field can repeat, increment repn level

        defined = ((val === nothing) || (idx < lparts)) ? haskey(F, symleaf) : false
        mustdefine = defn_level >= Fdefn
        mustrepeat = repeated && (repn_level <= Frepn)
        repidx = 0
        if mustrepeat
            repkey = (colid, idx)
            repidx = get(col_repeat_state, repkey, 0)
            repidx += 1
            col_repeat_state[repkey] = repidx
        end
        nreps = (defined && isa(F[symleaf], Vector)) ? length(F[symleaf]) : 0

        #@debug("repeat:$mustrepeat, nreps:$nreps, repidx:$repidx, defined:$defined, mustdefine:$mustdefine")
        if mustrepeat && (nreps < repidx)
            if !defined && mustdefine
                Vtyp = elemtype(sch, colname)
                Vrep = F[symleaf] = default_init(Vtyp)
            else
                Vrep = F[symleaf]
            end
            if length(Vrep) < repidx
                resize!(Vrep, repidx)
                if !isbits(eltype(Vrep))
                    Vrep[repidx] = default_init(eltype(Vrep))
                end
            end
            F = Vrep[repidx]
        elseif !defined && mustdefine
            if idx == length(nameparts)
                V = val
            else
                Vtyp = elemtype(sch, colname)
                V = default_init(Vtyp)
            end
            F[symleaf] = V
            F = V
        else
            F = get(F, symleaf, nothing)
        end
    end
    nothing
end
