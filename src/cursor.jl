##
# layer 3 access
# read data as records, in the following forms:
# - plain Julia types (use isdefined to check if member is set, but can't do this check for members that are primitive type)
# - Thrift types (use Thrift.isfilled to check if member is set)
# - ProtoBuf types (use ProtoBuf.isfilled to check if member is set)
#
# Builder implementations do the appropriate translation from column chunks to types.
# RecordCursor uses Builders for the result types.

abstract AbstractBuilder{T}
#

##
# Row cursor iterates through row numbers of a column 
type RowCursor
    par::ParFile

    rows::Range                     # rows to scan over
    row::Int                        # current row
    
    rowgroups::Vector{RowGroup}     # row groups in range
    rg::Int                         # current row group
    rgrange::Range                  # current rowrange

    function RowCursor(par::ParFile, rows::Range, col::AbstractString, row::Int=first(rows))
        rgs = rowgroups(par, col, rows)
        cursor = new(par, rows, row, rgs)
        setrow(cursor, row)
        cursor
    end
end

function setrow(cursor::RowCursor, row::Int)
    cursor.row = row
    isdefined(cursor, :rgrange) && (row in cursor.rgrange) && return
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

function start(cursor::RowCursor)
    row = first(cursor.rows)
    setrow(cursor, row)
    row
end
done(cursor::RowCursor, row::Int) = (row > last(cursor.rows))
function next(cursor::RowCursor, row::Int)
    setrow(cursor, row)
    row, (row+1)
end

##
# Column cursor iterates through all values of the column, including null values.
# Each iteration returns the value (as a Nullable), definition level, and repetition level for each value.
# Row can be deduced from repetition level.
type ColCursor{T}
    row::RowCursor
    colname::AbstractString
    maxdefn::Int

    colchunks::Vector{ColumnChunk}
    cc::Int
    ccrange::Range

    vals::Vector{T}
    valpos::Int
    #valrange::UnitRange{Int}

    defn_levels::Vector{Int}
    repn_levels::Vector{Int}
    levelpos::Int
    levelrange::UnitRange{Int}

    function ColCursor(row::RowCursor, colname::AbstractString)
        maxdefn = max_definition_level(schema(row.par), colname)
        cursor = new(row, colname, maxdefn)
    end
end

function ColCursor(par::ParFile, rows::Range, colname::AbstractString, row::Int=first(rows))
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

function setrow{T}(cursor::ColCursor{T}, row::Int)
    par = cursor.row.par
    rg = cursor.row.rowgroups[cursor.row.rg]
    ccincr = (row - cursor.row.row) == 1 # whether this is just an increment within the column chunk
    setrow(cursor.row, row) # set the row cursor
    isdefined(cursor, :colchunks) || (cursor.colchunks = columns(par, rg, cursor.colname))

    # check if cursor is done
    if done(cursor.row, row)
        cursor.cc = length(cursor.colchunks) + 1
        cursor.ccrange = row:(row-1)
        cursor.vals = Array(T, 0)
        cursor.repn_levels = cursor.defn_levels = Int[]
        cursor.valpos = cursor.levelpos = 0
        cursor.levelrange = 0:-1 #cursor.valrange = 0:-1
        return
    end

    # find the column chunk with the row
    if !isdefined(cursor, :ccrange) || !(row in cursor.ccrange)
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

function start(cursor::ColCursor)
    row = start(cursor.row)
    setrow(cursor, row)
    row, cursor.levelpos
end
function done(cursor::ColCursor, rowandlevel::Tuple{Int,Int})
    row, levelpos = rowandlevel
    (levelpos > last(cursor.levelrange)) && done(cursor.row, row)
end
function next{T}(cursor::ColCursor{T}, rowandlevel::Tuple{Int,Int})
    # find values for current row and level in row
    row, levelpos = rowandlevel
    (levelpos == cursor.levelpos) || throw(InvalidStateException("Invalid column cursor state", :levelpos))

    maxdefn = cursor.maxdefn
    defn_level = isempty(cursor.defn_levels) ? maxdefn : cursor.defn_levels[levelpos]
    repn_level = isempty(cursor.repn_levels) ? 0 : cursor.repn_levels[levelpos]
    cursor.levelpos += 1
    if defn_level == maxdefn
        val = Nullable{T}(cursor.vals[cursor.valpos])
        cursor.valpos += 1
    else
        val = Nullable{T}()
    end

    # advance row
    if cursor.levelpos > last(cursor.levelrange)
        row += 1
        setrow(cursor, row)
    end

    (val, defn_level, repn_level), (row, cursor.levelpos)
end

##
# Record cursor iterates over multiple columns and returns rows as records
type RecCursor{T}
    colnames::Vector{AbstractString}
    colcursors::Vector{ColCursor}
    builder::T

    colstates::Vector{Tuple{Int,Int}}
    #colfilters::Vector{Function}
    #recfilter::Function
end

function RecCursor{T <: AbstractBuilder}(par::ParFile, rows::Range, colnames::Vector{AbstractString}, builder::T, row::Int=first(rows))
    colcursors = [ColCursor(par, rows, colname, row) for colname in colnames]
    RecCursor{T}(colnames, colcursors, builder, Array(Tuple{Int,Int}, length(colcursors)))
end

function state(cursor::RecCursor)
    col1state = cursor.colstates[1]
    col1state[1] # return row as state
end

function start(cursor::RecCursor)
    cursor.colstates = [start(colcursor) for colcursor in cursor.colcursors]
    state(cursor)
end
done(cursor::RecCursor, row::Int) = done(cursor.colcursors[1].row, row)

function next{T}(cursor::RecCursor{T}, row::Int)
    states = cursor.colstates
    cursors = cursor.colcursors
    builder = cursor.builder

    row = init(cursor.builder)
    for colid in 1:length(states)                               # for each column
        colcursor = cursors[colid]
        colval, colstate = next(colcursor, states[colid])       # for each value, defn level, repn level in column
        val, def, rep = colval
        update(builder, row, colcursor.colname, val, def, rep)  # update record
        states[colid] = colstate                                # set last state to states
    end
    row, state(cursor)
end

##
# JuliaBuilder creates a plain Julia object
function default_init{T}(::Type{T})
    if issubtype(T, Array)
        Array(eltype(T), 0)
    else
        ccall(:jl_new_struct_uninit, Any, (Any,), T)::T
    end
end

type JuliaBuilder{T} <: AbstractBuilder{T}
    par::ParFile
    rowtype::Type{T}
    initfn::Function
    col_repeat_state::Dict{AbstractString,Int}
end
JuliaBuilder(par::ParFile, T::DataType, initfn::Function=default_init) = JuliaBuilder{T}(par, T, initfn, Dict{AbstractString,Int}())

function init{T}(builder::JuliaBuilder{T})
    empty!(builder.col_repeat_state)
    builder.initfn(T)::T
end

function update{T}(builder::JuliaBuilder{T}, row::T, fqcolname::AbstractString, val::Nullable, defn_level::Int, repn_level::Int)
    #@logmsg("updating $fqcolname")
    nameparts = split(fqcolname, '.')
    sch = builder.par.schema
    F = row  # the current field corresponding to the level in fqcolname
    Fdefn = 0
    Frepn = 0

    # for each name part of colname (a field)
    for idx in 1:length(nameparts)
        colname = join(nameparts[1:idx], '.')
        #@logmsg("updating part $colname of $fqcolname isnull:$(isnull(val)), def:$(defn_level), rep:$(repn_level)")
        leaf = nameparts[idx]
        symleaf = Symbol(leaf)

        required = isrequired(sch, colname)         # determine whether field is optional and repeated
        repeated = isrepeated(sch, colname)
        required || (Fdefn += 1)                    # if field is optional, increment defn level
        repeated && (Frepn += 1)                    # if field can repeat, increment repn level

        defined = isnull(val) ? isdefined(F, symleaf) : false
        mustdefine = defn_level >= Fdefn
        mustrepeat = repeated && (repn_level == Frepn)
        repkey = fqcolname * ":" * colname
        repidx = get(builder.col_repeat_state, repkey, 0)
        if mustrepeat
            repidx += 1
            builder.col_repeat_state[repkey] = repidx
        end
        nreps = defined ? length(getfield(F, symleaf)) : 0

        #@logmsg("repeat:$mustrepeat, nreps:$nreps, repidx:$repidx, defined:$defined, mustdefine:$mustdefine")
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
                V = get(val)
            else
                V = builder.initfn(fieldtype(typeof(F), symleaf))
            end
            setfield!(F, symleaf, V)
            F = V
        end
    end
    nothing
end
