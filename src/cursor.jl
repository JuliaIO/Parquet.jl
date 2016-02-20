##
# layer 3 access
# read data as records, in the following forms:
# - plain Julia types (use isdefined to check if member is set, but can't do this check for members that are primitive type)
# - Thrift types (use Thrift.isfilled to check if member is set)
# - ProtoBuf types (use ProtoBuf.isfilled to check if member is set)
#
# Builder implementations do the appropriate translation from column chunks to types.
# Iterator implementations use Builders for the result types.

abstract SchemaBuilder
#abstract ParIterator
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
            valpos = sum(sub(defn_levels, 1:(levelpos-1)) .== maxdefn) + 1
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

type JuliaSchemaBuilder{T} <: SchemaBuilder
    par::ParFile
    cols::Vector{AbstractString}
    rows::Range

    row::RowCursor
    cursors::Vector{ColCursor}
end

function JuliaSchemaBuilder(par::ParFile, T::DataType, cols::Vector{AbstractString}=colnames(par), rows::Range=1:nrows(par))
    JuliaSchemaBuilder{T}(par, cols, rows)
end

function build{T}(bldr::JuliaSchemaBuilder{T}, obj::T=T())
    # filter out from the list of all row groups
    #   - those not in range
    #   - those with no matching columns
    # while index within row range
    #   get matching rowgroup
    #     for every column in cols
    #       advance cursor to matching row
    #       set value to instance 
end
