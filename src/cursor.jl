##
# layer 3 access
# read data as records which are named tuple representations of the schema

##
# Row cursor iterates through row numbers of a column 
mutable struct RowCursor
    par::ParFile

    rows::UnitRange{Int}            # rows to scan over
    row::Int                        # current row
    
    rowgroups::Vector{RowGroup}     # row groups in range
    rg::Union{Int,Nothing}          # current row group
    rgrange::Union{UnitRange{Int},Nothing} # current rowrange

    function RowCursor(par::ParFile, rows::UnitRange{Int64}, col::Vector{String}, row::Signed=first(rows))
        rgs = rowgroups(par, col, rows)
        cursor = new(par, rows, row, rgs, nothing, nothing)
        setrow(cursor, row)
        cursor
    end
end

function setrow(cursor::RowCursor, row::Signed)
    cursor.row = row
    cursor.rgrange!==nothing && (row in cursor.rgrange) && return
    startrow = 1
    rgs = cursor.rowgroups
    for rg in 1:length(rgs)
        rgrange = startrow:(startrow + rgs[rg].num_rows)
        if row in rgrange
            cursor.row = row
            cursor.rg = rg
            cursor.rgrange = rgrange
            return
        end
        startrow = last(rgrange) + 1
    end
    throw(BoundsError(par.path, row))
end

rowgroup_offset(cursor::RowCursor) = cursor.row - first(cursor.rgrange)

function _start(cursor::RowCursor)
    row = first(cursor.rows)
    setrow(cursor, row)
    row
end
_done(cursor::RowCursor, row::Signed) = (row > last(cursor.rows))
function _next(cursor::RowCursor, row::Signed)
    setrow(cursor, row)
    row, (row+1)
end

function Base.iterate(cursor::RowCursor, state)
    _done(cursor, state) && return nothing
    return _next(cursor, state)
end

function Base.iterate(cursor::RowCursor)
    r = iterate(x, _start(x))
    return r
end

##
# Column cursor iterates through all values of the column, including null values.
# Each iteration returns the value (as a Union{T,Nothing}), definition level, and repetition level for each value.
# Row can be deduced from repetition level.
mutable struct ColCursor{T}
    row::RowCursor
    colname::Vector{String}
    maxdefn::Int

    colchunks::Union{Vector{ColumnChunk},Nothing}
    cc::Union{Int,Nothing}
    ccrange::Union{UnitRange{Int},Nothing}

    vals::Vector{T}
    valpos::Int
    #valrange::UnitRange{Int}

    defn_levels::Vector{Int}
    repn_levels::Vector{Int}
    levelpos::Int
    levelrange::UnitRange{Int}

    function ColCursor{T}(row::RowCursor, colname::Vector{String}) where T
        maxdefn = max_definition_level(schema(row.par), colname)
        new{T}(row, colname, maxdefn, nothing, nothing, nothing)
    end
end

function ColCursor(par::ParFile, rows::UnitRange{Int64}, colname::Vector{String}, row::Signed=first(rows))
    rowcursor = RowCursor(par, rows, colname, row)

    rg = rowcursor.rowgroups[rowcursor.rg] 
    colchunks = columns(par, rg, colname)
    ctype = coltype(colchunks[1])
    T = PLAIN_JTYPES[ctype+1]
    if (ctype == _Type.BYTE_ARRAY) || (ctype == _Type.FIXED_LEN_BYTE_ARRAY)
        T = Vector{T}
    end

    cursor = ColCursor{T}(rowcursor, colname)
    setrow(cursor, row)
    cursor
end

function setrow(cursor::ColCursor{T}, row::Signed) where {T}
    par = cursor.row.par
    rg = cursor.row.rowgroups[cursor.row.rg]
    ccincr = (row - cursor.row.row) == 1 # whether this is just an increment within the column chunk
    setrow(cursor.row, row) # set the row cursor
    cursor.colchunks!==nothing || (cursor.colchunks = columns(par, rg, cursor.colname))

    # check if cursor is done
    if _done(cursor.row, row)
        cursor.cc = length(cursor.colchunks) + 1
        cursor.ccrange = row:(row-1)
        cursor.vals = Array{T}(undef, 0)
        cursor.repn_levels = cursor.defn_levels = Int[]
        cursor.valpos = cursor.levelpos = 0
        cursor.levelrange = 0:-1 #cursor.valrange = 0:-1
        return
    end

    # find the column chunk with the row
    if cursor.ccrange===nothing || !(row in cursor.ccrange)
        offset = rowgroup_offset(cursor.row) # the offset of row from beginning of current rowgroup
        colchunks = cursor.colchunks

        startrow = row - offset
        for cc in 1:length(colchunks)
            vals, defn_levels, repn_levels = values(par, colchunks[cc])
            if isempty(repn_levels)
                nrowscc = length(vals) # number of values is number of rows
            else
                nrowscc = length(repn_levels) - length(find(repn_levels))   # number of values where repeation level is 0
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

    # find the starting positions for values and levels
    ccrange = cursor.ccrange
    defn_levels = cursor.defn_levels
    repn_levels = cursor.repn_levels

    # compute the level and value pos for row
    if isempty(repn_levels)
        # no repetitions, so each entry corresponds to one full row
        levelpos = row - first(ccrange) + 1
        levelrange = levelpos:levelpos
    else
        # multiple entries may constitute one row
        idx = first(ccrange)
        levelpos = findfirst(repn_levels, 0) # NOTE: can start from cursor.levelpos to optimize, but that will prevent using setrow to go backwards
        while idx < row
            levelpos = findnext(repn_levels, 0, levelpos+1)
            idx += 1
        end
        levelend = max(findnext(repn_levels, 0, levelpos+1)-1, length(repn_levels))
        levelrange = levelpos:levelend
    end

    # compute the val pos for row
    if isempty(defn_levels)
        # all entries are required, so there must be a corresponding value
        valpos = levelpos
        #valrange = levelrange
    else
        maxdefn = cursor.maxdefn
        if ccincr
            valpos = cursor.valpos
        else
            valpos = sum(view(defn_levels, 1:(levelpos-1)) .== maxdefn) + 1
        end
        #nvals = sum(sub(defn_levels, levelrange) .== maxdefn)
        #valrange = valpos:(valpos+nvals-1)
    end

    cursor.levelpos = levelpos
    cursor.levelrange = levelrange
    cursor.valpos = valpos
    #cursor.valrange = valrange
    nothing
end

function _start(cursor::ColCursor)
    row = _start(cursor.row)
    setrow(cursor, row)
    row, cursor.levelpos
end
function _done(cursor::ColCursor, rowandlevel::Tuple{Int,Int})
    row, levelpos = rowandlevel
    (levelpos > last(cursor.levelrange)) && _done(cursor.row, row)
end
function _next(cursor::ColCursor{T}, rowandlevel::Tuple{Int,Int}) where {T}
    # find values for current row and level in row
    row, levelpos = rowandlevel
    (levelpos == cursor.levelpos) || throw(InvalidStateException("Invalid column cursor state", :levelpos))

    maxdefn = cursor.maxdefn
    defn_level = isempty(cursor.defn_levels) ? maxdefn : cursor.defn_levels[levelpos]
    repn_level = isempty(cursor.repn_levels) ? 0 : cursor.repn_levels[levelpos]
    cursor.levelpos += 1
    if defn_level == maxdefn
        val = (cursor.vals[cursor.valpos])::Union{Nothing,T}
        cursor.valpos += 1
    else
        val = (nothing)::Union{Nothing,T}
    end

    # advance row
    if cursor.levelpos > last(cursor.levelrange)
        row += 1
        setrow(cursor, row)
    end

    (val, defn_level, repn_level), (row, cursor.levelpos)
end

function Base.iterate(cursor::ColCursor, state)
    _done(cursor, state) && return nothing
    return _next(cursor, state)
end

function Base.iterate(cursor::ColCursor)
    r = iterate(x, _start(x))
    return r
end

##

mutable struct RecordCursor
    par::ParFile
    colnames::Vector{Vector{String}}
    colcursors::Vector{ColCursor}
    colstates::Vector{Tuple{Int,Int}}
    rectype::DataType

    function RecordCursor(par::ParFile; rows::UnitRange{Int64}=1:nrows(par), colnames::Vector{Vector{String}}=colnames(par), row::Signed=first(rows))
        colcursors = [ColCursor(par, rows, colname, row) for colname in colnames]
        sch = schema(par)
        rectype = ntelemtype(sch, sch.schema[1])
        new(par, colnames, colcursors, Array{Tuple{Int,Int}}(undef, length(colcursors)), rectype)
    end
end

function state(cursor::RecordCursor)
    col1state = cursor.colstates[1]
    col1state[1] # return row as state
end

function _start(cursor::RecordCursor)
    cursor.colstates = [_start(colcursor) for colcursor in cursor.colcursors]
    state(cursor)
end
_done(cursor::RecordCursor, row::Signed) = _done(cursor.colcursors[1].row, row)

function _next(cursor::RecordCursor, row::Signed)
    states = cursor.colstates
    cursors = cursor.colcursors

    row = Dict{Symbol,Any}()
    col_repeat_state = Dict{AbstractString,Int}()
    for colid in 1:length(states)                                                               # for each column
        colcursor = cursors[colid]
        colval, colstate = _next(colcursor, states[colid])                                      # for each value, defn level, repn level in column
        val, def, rep = colval
        update_record(cursor.par, row, colcursor.colname, val, def, rep, col_repeat_state)      # update record
        states[colid] = colstate                                                                # set last state to states
    end
    _nt(row, cursor.rectype), state(cursor)
end

function Base.iterate(cursor::RecordCursor, state)
    _done(cursor, state) && return nothing
    return _next(cursor, state)
end

function Base.iterate(cursor::RecordCursor)
    r = iterate(cursor, _start(cursor))
    return r
end

function _nt(dict::Dict{Symbol,Any}, rectype::DataType)
    _val_or_missing = (idx,k) -> begin
        v = get(dict, k, missing)
        isa(v, Dict{Symbol,Any}) ? _nt(v, rectype.types[idx]) : v
    end
    values = [_val_or_missing(idx,k) for (idx,k) in enumerate(rectype.names)]
    rectype((values...,))
end

default_init(::Type{Vector{T}}) where {T} = Vector{T}()
default_init(::Type{Dict{Symbol,Any}}) = Dict{Symbol,Any}()
default_init(::Type{T}) where {T} = ccall(:jl_new_struct_uninit, Any, (Any,), T)::T

function update_record(par::ParFile, row::Dict{Symbol,Any}, nameparts::Vector{String}, val, defn_level::Signed, repn_level::Signed, col_repeat_state::Dict{AbstractString,Int})
    lparts = length(nameparts)
    sch = par.schema
    F = row  # the current field corresponding to the level in nameparts
    Fdefn = 0
    Frepn = 0

    # for each name part of colname (a field)
    for idx in 1:lparts
        colname = nameparts[1:idx]
        #@debug("updating part $colname of $nameparts isnull:$(val === nothing), def:$(defn_level), rep:$(repn_level)")
        leaf = nameparts[idx]
        symleaf = Symbol(leaf)

        required = isrequired(sch, colname)         # determine whether field is optional and repeated
        repeated = isrepeated(sch, colname)
        required || (Fdefn += 1)                    # if field is optional, increment defn level
        repeated && (Frepn += 1)                    # if field can repeat, increment repn level

        defined = ((val === nothing) || (idx < lparts)) ? haskey(F, symleaf) : false
        mustdefine = defn_level >= Fdefn
        mustrepeat = repeated && (repn_level == Frepn)
        repkey = join(nameparts, '.') * ":" * join(colname, '.')
        repidx = get(col_repeat_state, repkey, 0)
        if mustrepeat
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
