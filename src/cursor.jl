##
# layer 3 access
# read data as records, in the following forms:
# - plain Julia types (use isdefined to check if member is set, but can't do this check for members that are primitive type)
# - Thrift types (use Thrift.isfilled to check if member is set)
# - ProtoBuf types (use ProtoBuf.isfilled to check if member is set)
#
# Builder implementations do the appropriate translation from column chunks to types.
# RecordCursor uses Builders for the result types.

abstract type AbstractBuilder{T} end
#

##
# Row cursor iterates through row numbers of a column 
mutable struct RowCursor
    par::ParFile

    rows::UnitRange{Int}            # rows to scan over
    row::Int                        # current row
    
    rowgroups::Vector{RowGroup}     # row groups in range
    rg::Union{Int,Nothing}          # current row group
    rgrange::Union{UnitRange{Int},Nothing} # current rowrange

    function RowCursor(par::ParFile, rows::UnitRange{Int64}, col::AbstractString, row::Signed=first(rows))
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
    r = iterate(x, _start(x)) #! adding this here in case it also becomes an issue
    return r
end

##
# Column cursor iterates through all values of the column, including null values.
# Each iteration returns the value (as a Union{T,Nothing}), definition level, and repetition level for each value.
# Row can be deduced from repetition level.
mutable struct ColCursor{T}
    row::RowCursor
    colname::AbstractString
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

    function ColCursor{T}(row::RowCursor, colname::AbstractString) where T
        maxdefn = max_definition_level(schema(row.par), colname)
        new{T}(row, colname, maxdefn,nothing,nothing,nothing)
    end
end

function ColCursor(par::ParFile, rows::UnitRange{Int64}, colname::AbstractString, row::Signed=first(rows))
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
    r = iterate(x, _start(x)) #! here is the issue was x, _start(x)
    return r
end

##
# Record cursor iterates over multiple columns and returns rows as records
mutable struct RecCursor{T}
    colnames::Vector{AbstractString}
    colcursors::Vector{ColCursor}
    builder::T

    colstates::Vector{Tuple{Int,Int}}
    #colfilters::Vector{Function}
    #recfilter::Function
end

function RecCursor(par::ParFile, rows::UnitRange{Int64}, colnames::Vector{AbstractString}, builder::T, row::Signed=first(rows)) where {T <: AbstractBuilder}
    colcursors = [ColCursor(par, rows, colname, row) for colname in colnames]
    RecCursor{T}(colnames, colcursors, builder, Array{Tuple{Int,Int}}(undef, length(colcursors)))
end

function state(cursor::RecCursor)
    col1state = cursor.colstates[1]
    col1state[1] # return row as state
end

function _start(cursor::RecCursor)
    cursor.colstates = [_start(colcursor) for colcursor in cursor.colcursors]
    state(cursor)
end
_done(cursor::RecCursor, row::Signed) = _done(cursor.colcursors[1].row, row)

function _next(cursor::RecCursor{T}, row::Signed) where {T}
    states = cursor.colstates
    cursors = cursor.colcursors
    builder = cursor.builder

    row = init(cursor.builder)
    for colid in 1:length(states)                               # for each column
        colcursor = cursors[colid]
        colval, colstate = _next(colcursor, states[colid])       # for each value, defn level, repn level in column
        val, def, rep = colval
        update(builder, row, colcursor.colname, val, def, rep)  # update record
        states[colid] = colstate                                # set last state to states
    end
    row, state(cursor)
end

function Base.iterate(cursor::RecCursor, state)
    _done(cursor, state) && return nothing
    return _next(cursor, state)
end

function Base.iterate(cursor::RecCursor)
    r = iterate(cursor, _start(cursor))
    return r
end

##
# JuliaBuilder creates a plain Julia object
function default_init(::Type{T}) where {T}
    if T <: Array
        Array{eltype(T)}(0)
    else
        ccall(:jl_new_struct_uninit, Any, (Any,), T)::T
    end
end

mutable struct JuliaBuilder{T} <: AbstractBuilder{T}
    par::ParFile
    rowtype::Type{T}
    initfn::Function
    col_repeat_state::Dict{AbstractString,Int}
end
JuliaBuilder(par::ParFile, T::DataType, initfn::Function=default_init) = JuliaBuilder{T}(par, T, initfn, Dict{AbstractString,Int}())

function init(builder::JuliaBuilder{T}) where {T}
    empty!(builder.col_repeat_state)
    builder.initfn(T)::T
end

function update(builder::JuliaBuilder{T}, row::T, fqcolname::AbstractString, val, defn_level::Signed, repn_level::Signed) where {T}
    #@debug("updating $fqcolname")
    nameparts = split(fqcolname, '.')
    sch = builder.par.schema
    F = row  # the current field corresponding to the level in fqcolname
    Fdefn = 0
    Frepn = 0

    # for each name part of colname (a field)
    for idx in 1:length(nameparts)
        colname = join(nameparts[1:idx], '.')
        #@debug("updating part $colname of $fqcolname isnull:$(val === nothing), def:$(defn_level), rep:$(repn_level)")
        leaf = nameparts[idx]
        symleaf = Symbol(leaf)

        required = isrequired(sch, colname)         # determine whether field is optional and repeated
        repeated = isrepeated(sch, colname)
        required || (Fdefn += 1)                    # if field is optional, increment defn level
        repeated && (Frepn += 1)                    # if field can repeat, increment repn level

        defined = (val === nothing) ? isdefined(F, symleaf) : false
        mustdefine = defn_level >= Fdefn
        mustrepeat = repeated && (repn_level == Frepn)
        repkey = fqcolname * ":" * colname
        repidx = get(builder.col_repeat_state, repkey, 0)
        if mustrepeat
            repidx += 1
            builder.col_repeat_state[repkey] = repidx
        end
        nreps = defined ? length(getfield(F, symleaf)) : 0

        #@debug("repeat:$mustrepeat, nreps:$nreps, repidx:$repidx, defined:$defined, mustdefine:$mustdefine")
        if mustrepeat && (nreps < repidx)
            if !defined && mustdefine
                Vrep = builder.initfn(fieldtype(typeof(F), symleaf))
                setfield!(F, symleaf, Vrep)
            else
                Vrep = getfield(F, symleaf)
            end
            if length(Vrep) < repidx
                resize!(Vrep, repidx)
                if !isbits(eltype(Vrep))
                    Vrep[repidx] = builder.initfn(eltype(Vrep))
                end
            end
            F = Vrep[repidx]
        elseif !defined && mustdefine
            if idx == length(nameparts)
                V = val
            else
                V = builder.initfn(fieldtype(typeof(F), symleaf))
            end
            setfield!(F, symleaf, V)
            F = V
        end
    end
    nothing
end
