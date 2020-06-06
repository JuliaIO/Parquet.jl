##
# layer 3 access
# read data as records which are named tuple representations of the schema

##
# Row cursor iterates through row numbers of a column 
mutable struct RowCursor
    par::ParFile

    rows::UnitRange{Int64}                      # rows to scan over
    row::Int64                                  # current row
    
    rowgroups::Vector{RowGroup}                 # row groups in range
    rg::Union{Int,Nothing}                      # current row group
    rgrange::Union{UnitRange{Int64},Nothing}    # current rowrange

    function RowCursor(par::ParFile, rows::UnitRange{Int64}, col::Vector{String}, row::Int64=first(rows))
        rgs = rowgroups(par, col, rows)
        cursor = new(par, rows, row, rgs, nothing, nothing)
        setrow(cursor, row)
        cursor
    end
end

function setrow(cursor::RowCursor, row::Int64)
    cursor.row = row
    (cursor.rgrange !== nothing) && (cursor.row in cursor.rgrange) && return
    startrow = Int64(1)
    rowgroups = cursor.rowgroups
    for rowgroup_idx in 1:length(rowgroups)
        rowgroup_row_range = startrow:(startrow + rowgroups[rowgroup_idx].num_rows)
        if row in rowgroup_row_range
            cursor.row = row
            cursor.rg = rowgroup_idx
            cursor.rgrange = rowgroup_row_range
            return
        else
            startrow = last(rowgroup_row_range) + 1
        end
    end
    throw(BoundsError(cursor.par.path, row))
end

rowgroup_offset(cursor::RowCursor) = cursor.row - first(cursor.rgrange)

function _start(cursor::RowCursor)
    row = first(cursor.rows)
    (cursor.row === row) || setrow(cursor, row)
    row
end
_done(cursor::RowCursor, row::Int64) = (row > last(cursor.rows))
function _next(cursor::RowCursor, row::Int64)
    setrow(cursor, row)
    row, (row+1)
end

function Base.iterate(cursor::RowCursor, state)
    _done(cursor, state) && return nothing
    return _next(cursor, state)
end

function Base.iterate(cursor::RowCursor)
    r = iterate(cursor, _start(cursor))
    return r
end

##
# Column cursor iterates through all values of the column, including null values.
# Each iteration returns the value (as a Union{T,Nothing}), definition level, and repetition level for each value.
# Row can be deduced from repetition level.
mutable struct ColCursor{T}
    row::RowCursor
    colname::Vector{String}
    required_at::Vector{Bool}
    repeated_at::Vector{Bool}
    logical_converter_fn::Function
    maxdefn::Int32

    colchunks::Union{Vector{ColumnChunk},Nothing}
    cc::Union{Int,Nothing}
    ccrange::Union{UnitRange{Int64},Nothing}

    vals::Vector{T}
    valpos::Int64

    defn_levels::Vector{Int32}
    repn_levels::Vector{Int32}
    levelpos::Int64
    levelrange::UnitRange{Int}

    function ColCursor{T}(row::RowCursor, colname::Vector{String}) where T
        sch = schema(row.par)

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
        new{T}(row, colname, required_at, repeated_at, logical_converter_fn, maxdefn, nothing, nothing, nothing)
    end
end

function ColCursor(par::ParFile, colname::Vector{String}; rows::UnitRange=1:nrows(par), row::Signed=first(rows))
    rowcursor = RowCursor(par, UnitRange{Int64}(rows), colname, Int64(row))

    rowgroup = rowcursor.rowgroups[rowcursor.rg] 
    colchunks = columns(par, rowgroup, colname)
    parquet_coltype = coltype(colchunks[1])
    T = PLAIN_JTYPES[parquet_coltype+1]
    if (parquet_coltype == _Type.BYTE_ARRAY) || (parquet_coltype == _Type.FIXED_LEN_BYTE_ARRAY)
        T = Vector{T}
    end

    cursor = ColCursor{T}(rowcursor, colname)
    setrow(cursor, Int64(row))
    cursor
end

eltype(cursor::ColCursor{T}) where {T} = NamedTuple{(:value, :defn_level, :repn_level),Tuple{Union{Nothing,T},Int64,Int64}}
length(cursor::ColCursor) = length(cursor.row.rows)

function setrow(cursor::ColCursor{T}, row::Int64) where {T}
    par = cursor.row.par
    rg = cursor.row.rowgroups[cursor.row.rg]
    ccincr = (row - cursor.row.row) == 1 # whether this is just an increment within the column chunk
    setrow(cursor.row, row) # set the row cursor
    if cursor.colchunks === nothing
        cursor.colchunks = columns(par, rg, cursor.colname)
    end

    # check if cursor is done
    if _done(cursor.row, row)
        cursor.cc = length(cursor.colchunks) + 1
        #cursor.ccrange = row:(row-1)
        #cursor.vals = Vector{T}()
        #cursor.repn_levels = cursor.defn_levels = Int32[]
        cursor.valpos = cursor.levelpos = 0
        cursor.levelrange = 0:-1
        return
    end

    # find the column chunk with the row
    if (cursor.ccrange === nothing) || !(row in cursor.ccrange)
        offset = rowgroup_offset(cursor.row) # the offset of row from beginning of current rowgroup
        colchunks = (cursor.colchunks)::Vector{ColumnChunk}

        startrow = row - offset
        for cc in 1:length(colchunks)
            vals, defn_levels, repn_levels = values(par, colchunks[cc], T)
            if isempty(repn_levels)
                nrowscc = length(vals) # number of values is number of rows
            else
                nrowscc = 0
                for i in 1:length(repn_levels)
                    (repn_levels[i] !== Int32(0)) && (nrowscc += 1)
                end
            end
            ccrange = startrow:(startrow + nrowscc)

            if row in ccrange
                cursor.cc = cc
                cursor.ccrange = ccrange
                cursor.vals = vals
                cursor.defn_levels = defn_levels
                cursor.repn_levels = repn_levels
                cursor.valpos = cursor.levelpos = 0
                ccincr = false
                break
            else
                startrow = last(ccrange) + 1
            end
        end
    end

    if cursor.ccrange === nothing
        # we did not find the row in this column
        cursor.valpos = cursor.levelpos = 0
        cursor.levelrange = 0:-1
    else
        # find the starting positions for values and levels
        ccrange = (cursor.ccrange)::UnitRange{Int64}
        defn_levels = cursor.defn_levels
        repn_levels = cursor.repn_levels
        levelpos = valpos = Int64(0)

        # compute the level and value pos for row
        if isempty(repn_levels)
            # no repetitions, so each entry corresponds to one full row
            levelpos = row - first(ccrange) + 1
            levelrange = levelpos:levelpos
        else
            # multiple entries may constitute one row
            idx = first(ccrange)
            levelpos = Int64(findfirst(x->x===Int32(0), repn_levels)) # NOTE: can start from cursor.levelpos to optimize, but that will prevent using setrow to go backwards
            while idx < row
                levelpos = Int64(findnext(x->x===Int32(0), repn_levels, levelpos+1))
                idx += 1
            end
            levelend = Int64(max(findnext(x->x===Int32(0), repn_levels, levelpos+1)-1, length(repn_levels)))
            levelrange = levelpos:levelend
        end

        # compute the val pos for row
        if isempty(defn_levels)
            # all entries are required, so there must be a corresponding value
            valpos = levelpos
        else
            maxdefn = cursor.maxdefn
            if ccincr
                valpos = cursor.valpos
            else
                valpos = 1
                for idx in 1:(levelpos-1)
                    (defn_levels[idx] === maxdefn) && (valpos += 1)
                end
            end
        end

        cursor.levelpos = levelpos
        cursor.levelrange = levelrange
        cursor.valpos = valpos
    end
    nothing
end

function _start(cursor::ColCursor)
    row = _start(cursor.row)
    setrow(cursor, row)
    row, cursor.levelpos
end
function _done(cursor::ColCursor, rowandlevel::Tuple{Int64,Int64})
    row, levelpos = rowandlevel
    (levelpos > last(cursor.levelrange)) || _done(cursor.row, row)
end
function _next(cursor::ColCursor{T}, rowandlevel::Tuple{Int64,Int64}) where {T}
    # find values for current row and level in row
    row, levelpos = rowandlevel
    (levelpos == cursor.levelpos) || throw(InvalidStateException("Invalid column cursor state", :levelpos))

    maxdefn = cursor.maxdefn
    defn_level = isempty(cursor.defn_levels) ? maxdefn : cursor.defn_levels[cursor.valpos]
    repn_level = isempty(cursor.repn_levels) ? 0 : cursor.repn_levels[cursor.valpos]
    cursor.levelpos += 1
    if defn_level == maxdefn
        val = (cursor.vals[cursor.valpos])::T
        cursor.valpos += 1
    else
        val = nothing
    end

    # advance row
    if cursor.levelpos > last(cursor.levelrange)
        row += 1
        setrow(cursor, row)
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
    par::ParFile
    colnames::Vector{Vector{String}}
    colcursors::Vector{ColCursor}
    colstates::Vector{Tuple{Int64,Int64}}
    rowgroupid::Int
    row::Int64
end

function BatchedColumnsCursor(par::ParFile)
    sch = schema(par)

    # supports only non nested columns as of now
    if !all(num_children(schemaelem) == 0 for schemaelem in sch.schema[2:end])
        error("nested schemas are not supported with BatchedColumnsCursor yet")
    end

    colcursors = [ColCursor(par, colname) for colname in colnames(par)]
    rectype = ntcolstype(sch, sch.schema[1])
    BatchedColumnsCursor{rectype}(par, colnames(par), colcursors, Array{Tuple{Int64,Int64}}(undef, length(colcursors)), 1, 1)
end

eltype(cursor::BatchedColumnsCursor{T}) where {T} = T
length(cursor::BatchedColumnsCursor) = length(rowgroups(cursor.par))

function colcursor_values(colcursor::ColCursor{T}, ::Type{Vector{Union{Missing,V}}}) where {T,V}
    defn_levels = colcursor.defn_levels
    vals = colcursor.vals
    val_idx = 0
    logical_converter_fn = colcursor.logical_converter_fn
    Union{Missing,V}[defn_levels[idx] === Int32(1) ? logical_converter_fn(vals[val_idx+=1]) : missing for idx in 1:length(defn_levels)]
end

function colcursor_values(colcursor::ColCursor{T}, ::Type{Vector{Union{Missing,T}}}) where {T}
    defn_levels = colcursor.defn_levels
    vals = colcursor.vals
    val_idx = 0
    Union{Missing,T}[defn_levels[idx] === Int32(1) ? vals[val_idx+=1] : missing for idx in 1:length(defn_levels)]
end

function colcursor_values(colcursor::ColCursor{T}, ::Type{Vector{V}}) where {T,V}
    defn_levels = colcursor.defn_levels
    vals = colcursor.vals
    logical_converter_fn = colcursor.logical_converter_fn
    V[logical_converter_fn(v) for v in vals]
end

colcursor_values(colcursor::ColCursor{T}, ::Type{Vector{T}}) where {T} = colcursor.vals

function Base.iterate(cursor::BatchedColumnsCursor{T}, rowgroupid) where {T}
    (rowgroupid > length(cursor)) && (return nothing)

    colcursors = cursor.colcursors
    for colcursor in colcursors
        setrow(colcursor, cursor.row)
    end
    coltypes = T.types
    colvals = [colcursor_values(colcursor,coltype) for (colcursor,coltype) in zip(colcursors,coltypes)]

    cursor.row += (rowgroups(cursor.par)[cursor.rowgroupid]).num_rows
    cursor.rowgroupid += 1
    T(colvals), cursor.rowgroupid
end

function Base.iterate(cursor::BatchedColumnsCursor{T}) where {T}
    cursor.row = 1
    cursor.colstates = [_start(colcursor) for colcursor in cursor.colcursors]
    iterate(cursor, cursor.rowgroupid)
end


##

mutable struct RecordCursor{T}
    par::ParFile
    colnames::Vector{Vector{String}}
    colcursors::Vector{ColCursor}
    colstates::Vector{Tuple{Int64,Int64}}
    rows::UnitRange{Int64}                      # rows to scan over
    row::Int64                                  # current row
end

function RecordCursor(par::ParFile; rows::UnitRange=1:nrows(par), colnames::Vector{Vector{String}}=colnames(par), row::Signed=first(rows))
    colcursors = [ColCursor(par, colname; rows=rows, row=row) for colname in colnames]
    sch = schema(par)
    rectype = ntelemtype(sch, sch.schema[1])
    RecordCursor{rectype}(par, colnames, colcursors, Array{Tuple{Int64,Int64}}(undef, length(colcursors)), rows, row)
end

eltype(cursor::RecordCursor{T}) where {T} = T
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

function update_record(par::ParFile, row::Dict{Symbol,Any}, colid::Int, colcursor::ColCursor, colcursor_state::Tuple{Int64,Int64}, col_repeat_state::Dict{Tuple{Int,Int},Int})
    if !_done(colcursor, colcursor_state)
        colval, colcursor_state = _next(colcursor, colcursor_state)                                                         # for each value, defn level, repn level in column
        update_record(par, row, colid, colcursor, colval.value, colval.defn_level, colval.repn_level, col_repeat_state)    # update record
    end
    colcursor_state # return new colcursor state
end

function update_record(par::ParFile, row::Dict{Symbol,Any}, colid::Int, colcursor::ColCursor, val, defn_level::Int64, repn_level::Int64, col_repeat_state::Dict{Tuple{Int,Int},Int})
    nameparts = colcursor.colname
    required_at = colcursor.required_at
    repeated_at = colcursor.repeated_at
    logical_converter_fn = colcursor.logical_converter_fn

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
        symleaf = Symbol(leaf)

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
                V = logical_converter_fn(val)
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
