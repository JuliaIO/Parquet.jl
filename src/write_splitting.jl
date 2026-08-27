# Row-boundary ownership and slicing for row groups and data pages.

struct _WritePageCapacityError <: Exception
    resource::Symbol
    requested::Int64
    maximum::Int64
end

function _writecheckedint64(value::Integer, resource::Symbol, maximum::Int64)
    value >= 0 || throw(ArgumentError("$resource must be nonnegative"))
    value <= typemax(Int64) || throw(LimitError(resource, typemax(Int64), maximum))
    return Int64(value)
end

function _writedensecopy(values::AbstractVector, budget::_LiveByteBudget)
    T = Base.nonmissingtype(eltype(values))
    T === Missing && throw(ArgumentError(
        "a physical Parquet leaf cannot contain only missing dense values"))
    count = 0
    for value in values
        ismissing(value) || (count = Base.checked_add(count, 1))
    end
    charge = _reservearray!(budget, T, count)
    output = T[]
    sizehint!(output, count)
    for value in values
        ismissing(value) && continue
        push!(output, convert(T, value))
    end
    return output, charge
end

function _writenormalizedense(column::WriteColumn, rows::Int,
        budget::_LiveByteBudget)
    definitions = column.definitions
    values = column.values
    charge = Int64(0)
    if definitions === nothing && !iszero(column.max_definition_level)
        iszero(column.max_repetition_level) || throw(ArgumentError(
            "a repeated writer leaf with definition levels must provide its level stream"))
        column.max_definition_level == 1 || throw(ArgumentError(
            "a nested writer leaf with definition levels must provide its level stream"))
        length(values) == rows || throw(ArgumentError(
            "optional writer leaf input does not match the table row count"))
        definitioncharge = _reservearray!(budget, UInt64, rows)
        dense, densecharge = try
            _writedensecopy(values, budget)
        catch
            _release!(budget, definitioncharge)
            rethrow()
        end
        definitions = Vector{UInt64}(undef, rows)
        index = 0
        for value in values
            index += 1
            definitions[index] = ismissing(value) ? UInt64(0) : UInt64(1)
        end
        values = dense
        charge = _materializedsum(definitioncharge, densecharge)
    elseif Missing <: eltype(values) || !(values isa Vector)
        dense, densecharge = _writedensecopy(values, budget)
        if length(dense) != length(values)
            _release!(budget, densecharge)
            throw(ArgumentError(
                "writer dense physical values contain missing"))
        end
        values = dense
        charge = densecharge
    else
        for value in values
            ismissing(value) && throw(ArgumentError(
                "writer dense physical values contain missing"))
        end
    end
    values === column.values && definitions === column.definitions && return column, charge
    normalized = WriteColumn(column.name, values, column.physical,
        column.type_length, column.optional, column.logical, column.converted,
        column.path, column.repetitions, definitions,
        column.max_repetition_level, column.max_definition_level, column.rows,
        column.schema)
    return normalized, charge
end

function _writeentryoffsets(column::WriteColumn, rows::Int)
    entries = _columnentrycount(column)
    output = Vector{Int64}(undef, rows + 1)
    if column.repetitions === nothing
        entries == rows || throw(ArgumentError(
            "flat writer leaf has $entries level entries for $rows rows"))
        for row in 0:rows
            output[row + 1] = Int64(row)
        end
        return output
    end
    repetitions = column.repetitions
    length(repetitions) == entries || throw(ArgumentError(
        "writer repetition stream length changed"))
    row = 0
    for index in eachindex(repetitions)
        level = repetitions[index]
        level <= UInt64(column.max_repetition_level) || throw(ArgumentError(
            "writer repetition level exceeds its schema maximum"))
        if iszero(level)
            row += 1
            row <= rows || throw(ArgumentError(
                "writer leaf has more row boundaries than the table"))
            output[row] = Int64(index - firstindex(repetitions))
        end
    end
    row == rows || throw(ArgumentError(
        "writer leaf has $row row boundaries for $rows rows"))
    output[rows + 1] = Int64(entries)
    return output
end

function _writedenseoffsets(column::WriteColumn, entries::Vector{Int64})
    rows = length(entries) - 1
    output = Vector{Int64}(undef, rows + 1)
    output[1] = Int64(0)
    dense = Int64(0)
    definitions = column.definitions
    if definitions === nothing
        iszero(column.max_definition_level) || throw(ArgumentError(
            "writer leaf is missing its definition stream"))
        for row in 1:rows
            dense = Base.checked_add(dense, entries[row + 1] - entries[row])
            output[row + 1] = dense
        end
    else
        length(definitions) == last(entries) || throw(ArgumentError(
            "writer definition stream length changed"))
        maximum = UInt64(column.max_definition_level)
        position = 0
        for row in 1:rows
            stop = Int(entries[row + 1])
            while position < stop
                position += 1
                definition = definitions[position]
                definition <= maximum || throw(ArgumentError(
                    "writer definition level exceeds its schema maximum"))
                definition == maximum && (dense = Base.checked_add(dense, 1))
            end
            output[row + 1] = dense
        end
    end
    dense == length(column.values) || throw(ArgumentError(
        "writer dense-value count does not match its definition stream"))
    return output
end

function _writepayloadfixedwidth(column::WriteColumn)
    column.physical in (Metadata.Type.INT32, Metadata.Type.FLOAT) && return Int64(4)
    column.physical in (Metadata.Type.INT64, Metadata.Type.DOUBLE) && return Int64(8)
    if column.physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY
        width = column.type_length
        width === nothing && throw(ArgumentError(
            "fixed byte-array writer leaf has no width"))
        return Int64(width)
    end
    return nothing
end

function _writepayloadoffsets(column::WriteColumn, dense::Vector{Int64},
        limits::Limits)
    rows = length(dense) - 1
    output = Vector{Int64}(undef, rows + 1)
    physical = column.physical
    if physical == Metadata.Type.BOOLEAN
        for row in 0:rows
            output[row + 1] = cld(dense[row + 1], Int64(8))
        end
        return output
    end
    width = _writepayloadfixedwidth(column)
    if width !== nothing
        for row in 0:rows
            output[row + 1] = Base.checked_mul(dense[row + 1], width)
        end
        return output
    end
    physical == Metadata.Type.BYTE_ARRAY || throw(ArgumentError(
        "unsupported writer physical type $physical"))
    output[1] = Int64(0)
    bytes = Int64(0)
    position = 0
    for row in 1:rows
        stop = Int(dense[row + 1])
        while position < stop
            position += 1
            value = column.values[position]
            payload = value isa AbstractString ? ncodeunits(value) : length(value)
            _checklimit(:string_bytes, payload, limits.max_string_bytes)
            payload <= typemax(Int32) || throw(ArgumentError(
                "byte array exceeds Int32 length"))
            bytes = Base.checked_add(bytes, Base.checked_add(Int64(4),
                Int64(payload)))
        end
        output[row + 1] = bytes
    end
    return output
end

function _writevalidateprefixes(column::WriteColumn, rows::Int,
        entries::Vector{Int64}, dense::Vector{Int64}, payload::Vector{Int64})
    length(entries) == rows + 1 == length(dense) == length(payload) ||
        throw(AssertionError("writer row-boundary prefix lengths differ"))
    first(entries) == first(dense) == first(payload) == 0 ||
        throw(AssertionError("writer row-boundary prefixes do not start at zero"))
    issorted(entries) && issorted(dense) && issorted(payload) ||
        throw(AssertionError("writer row-boundary prefixes are not monotonic"))
    last(entries) == _columnentrycount(column) || throw(AssertionError(
        "writer entry prefix has the wrong terminal count"))
    last(dense) == length(column.values) || throw(AssertionError(
        "writer dense prefix has the wrong terminal count"))
    last(payload) == _writerrawpayloadbytes(column) || throw(AssertionError(
        "writer payload prefix has the wrong terminal byte count"))
    for row in 1:rows
        entries[row + 1] > entries[row] || throw(ArgumentError(
            "every top-level row must add a level entry to every leaf"))
        start = Int(entries[row]) + 1
        repetitions = column.repetitions
        repetitions === nothing || iszero(repetitions[start]) ||
            throw(ArgumentError("writer data-page row boundary has nonzero repetition"))
    end
    return
end

function _writeprepareleaf(node::SchemaNode, column::WriteColumn, rows::Int,
        limits::Limits, budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        normalized, densecharge = _writenormalizedense(column, rows, budget)
        prefixcharge = _materializedproduct(3,
            _materializedarraybytes(Int64, rows + 1))
        _reserve!(budget, prefixcharge)
        entries = _writeentryoffsets(normalized, rows)
        dense = _writedenseoffsets(normalized, entries)
        payload = _writepayloadoffsets(normalized, dense, limits)
        _writevalidateprefixes(normalized, rows, entries, dense, payload)
        return normalized, entries, dense, payload,
            _materializedsum(densecharge, prefixcharge)
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writerowgroupsize(value, rows::Int)
    value === nothing && return max(rows, 1)
    value isa Integer && !(value isa Bool) || throw(ArgumentError(
        "rowgroupsize must be a positive integer or nothing"))
    value > 0 || throw(ArgumentError(
        "rowgroupsize must be a positive integer or nothing"))
    value <= typemax(Int) || throw(ArgumentError(
        "rowgroupsize exceeds the Julia index range"))
    return Int(value)
end

function _writepagesize(value)
    value === nothing && return nothing
    value isa Integer && !(value isa Bool) || throw(ArgumentError(
        "pagesize must be a positive integer or nothing"))
    value > 0 || throw(ArgumentError(
        "pagesize must be a positive integer or nothing"))
    value <= typemax(Int64) || throw(ArgumentError(
        "pagesize exceeds Int64"))
    return Int64(value)
end

function _writeslicelevels(levels::Union{Nothing,AbstractVector{UInt64}}, first::Int,
        last::Int, budget::_LiveByteBudget)
    levels === nothing && return nothing, Int64(0)
    _reserveobjects!(budget)
    return @view(levels[(first + 1):last]), _MATERIALIZED_OBJECT_BYTES
end

function _writesliceprefixvalues(prefix::Vector{Int64}, firstrow::Int,
        lastrow::Int)
    count = lastrow - firstrow + 1
    base = prefix[firstrow + 1]
    output = Vector{Int64}(undef, count)
    for index in 0:(count - 1)
        output[index + 1] = prefix[firstrow + index + 1] - base
    end
    return output
end

function _writesliceprefix(prefix::Vector{Int64}, firstrow::Int, lastrow::Int,
        budget::_LiveByteBudget)
    count = lastrow - firstrow + 1
    charge = _reservearray!(budget, Int64, count)
    output = _writesliceprefixvalues(prefix, firstrow, lastrow)
    return output, charge
end

function _writesliceleaf(leaf::WriteLeafPlan, firstrow::Int, lastrow::Int,
        limits::Limits, budget::_LiveByteBudget)
    0 <= firstrow <= lastrow <= leaf.column.rows || throw(ArgumentError(
        "writer row-group slice is outside its leaf"))
    start = _budgetused(budget)
    try
        entrystart = Int(leaf.entry_offsets[firstrow + 1])
        entrystop = Int(leaf.entry_offsets[lastrow + 1])
        densestart = Int(leaf.dense_offsets[firstrow + 1])
        densestop = Int(leaf.dense_offsets[lastrow + 1])
        repetitions, repetitioncharge = _writeslicelevels(
            leaf.column.repetitions, entrystart, entrystop, budget)
        definitions, definitioncharge = _writeslicelevels(
            leaf.column.definitions, entrystart, entrystop, budget)
        repetitioncharge >= 0 && definitioncharge >= 0 || throw(AssertionError(
            "writer level slice charge is negative"))
        _reserveobjects!(budget, 2)
        values = @view leaf.column.values[(densestart + 1):densestop]
        entries, entrycharge = _writesliceprefix(leaf.entry_offsets,
            firstrow, lastrow, budget)
        dense, densecharge = _writesliceprefix(leaf.dense_offsets,
            firstrow, lastrow, budget)
        payloadcharge = _reservearray!(budget, Int64,
            lastrow - firstrow + 1)
        entrycharge >= 0 && densecharge >= 0 && payloadcharge >= 0 ||
            throw(AssertionError("writer prefix slice charge is negative"))
        rows = lastrow - firstrow
        source = leaf.column
        payload = source.physical == Metadata.Type.BOOLEAN ?
            _writepayloadoffsets(source, dense, limits) :
            _writesliceprefixvalues(leaf.payload_offsets, firstrow, lastrow)
        column = WriteColumn(source.name, values, source.physical,
            source.type_length, source.optional, source.logical,
            source.converted, leaf.path, repetitions, definitions,
            source.max_repetition_level, source.max_definition_level, rows,
            source.schema)
        _writevalidateprefixes(column, rows, entries, dense, payload)
        return WriteLeafPlan(leaf.ordinal, leaf.path, column, entries,
            dense, payload)
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writegroupcount(rows::Int, size::Int)
    iszero(rows) && return 0
    return Base.checked_add(fld(rows - 1, size), 1)
end

function _splitwriteplan(plan::WritePlan, rowgroupsize, limits::Limits,
        budget::_LiveByteBudget)
    size = _writerowgroupsize(rowgroupsize, plan.rows)
    isempty(plan.rowgroups) && return plan
    length(plan.rowgroups) == 1 || throw(AssertionError(
        "writer plan was split more than once"))
    source = only(plan.rowgroups)
    source.rows == plan.rows || throw(AssertionError(
        "writer source row group does not own every table row"))
    size >= plan.rows && return plan
    count = try
        _writegroupcount(plan.rows, size)
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64),
            limits.max_container_elements))
    end
    _checklimit(:container_elements, count, limits.max_container_elements)
    leafcount = try
        Base.checked_mul(count, length(source.leaves))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64),
            limits.max_container_elements))
    end
    _checklimit(:container_elements, leafcount, limits.max_container_elements)
    start = _budgetused(budget)
    try
        _reservearray!(budget, WriteRowGroupPlan, count)
        _reserveobjects!(budget, count + 1)
        groups = WriteRowGroupPlan[]
        sizehint!(groups, count)
        firstrow = 0
        while firstrow < plan.rows
            lastrow = min(Base.checked_add(firstrow, size), plan.rows)
            _reservearray!(budget, WriteLeafPlan, length(source.leaves))
            leaves = WriteLeafPlan[]
            sizehint!(leaves, length(source.leaves))
            for leaf in source.leaves
                push!(leaves, _writesliceleaf(leaf, firstrow, lastrow, limits,
                    budget))
            end
            push!(groups, WriteRowGroupPlan(lastrow - firstrow, leaves))
            firstrow = lastrow
        end
        length(groups) == count || throw(AssertionError(
            "writer row-group split count changed"))
        output = WritePlan(plan.elements, plan.schema, plan.rows, groups)
        obsolete = _materializedarraybytes(WriteRowGroupPlan, 1)
        obsolete = _materializedsum(obsolete,
            _materializedarraybytes(WriteLeafPlan, length(source.leaves)))
        obsolete = _materializedsum(obsolete,
            _materializedproduct(2, _MATERIALIZED_OBJECT_BYTES))
        prefixbytes = _materializedproduct(3,
            _materializedarraybytes(Int64, plan.rows + 1))
        obsolete = _materializedsum(obsolete,
            _materializedproduct(length(source.leaves), prefixbytes))
        _release!(budget, obsolete)
        return output
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

struct _WriteDataFrame
    bytes::Vector{UInt8}
    uncompressed_size::Int64
    first_row_index::Int64
end

struct _WriteChunkDictionary{V}
    values::V
    indices::Vector{UInt64}
    payload::Vector{UInt8}
    bitwidth::Int
end

function _writepageestimate(leaf::WriteLeafPlan, firstrow::Int, lastrow::Int)
    entries = leaf.entry_offsets[lastrow + 1] -
        leaf.entry_offsets[firstrow + 1]
    dense = leaf.dense_offsets[lastrow + 1] -
        leaf.dense_offsets[firstrow + 1]
    levels = Int64(0)
    !iszero(leaf.column.max_repetition_level) &&
        (levels = Base.checked_add(levels, Base.checked_mul(entries, Int64(8))))
    !iszero(leaf.column.max_definition_level) &&
        (levels = Base.checked_add(levels, Base.checked_mul(entries, Int64(8))))
    raw = if leaf.column.physical == Metadata.Type.BOOLEAN
        cld(dense, Int64(8))
    else
        leaf.payload_offsets[lastrow + 1] -
            leaf.payload_offsets[firstrow + 1]
    end
    return Base.checked_add(levels, raw)
end

function _writecandidateboundaries(leaf::WriteLeafPlan,
        pagesize::Union{Nothing,Int64})
    rows = leaf.column.rows
    iszero(rows) && return Tuple{Int,Int}[]
    pagesize === nothing && return Tuple{Int,Int}[(0, rows)]
    output = Tuple{Int,Int}[]
    firstrow = 0
    while firstrow < rows
        lastrow = firstrow
        while lastrow < rows
            candidate = lastrow + 1
            estimate = _writepageestimate(leaf, firstrow, candidate)
            lastrow > firstrow && estimate > pagesize && break
            lastrow = candidate
        end
        lastrow > firstrow || throw(AssertionError(
            "writer soft page planner did not consume one row"))
        push!(output, (firstrow, lastrow))
        firstrow = lastrow
    end
    return output
end

function _writepagecolumn(leaf::WriteLeafPlan, firstrow::Int, lastrow::Int)
    entrystart = Int(leaf.entry_offsets[firstrow + 1])
    entrystop = Int(leaf.entry_offsets[lastrow + 1])
    densestart = Int(leaf.dense_offsets[firstrow + 1])
    densestop = Int(leaf.dense_offsets[lastrow + 1])
    repetitions = leaf.column.repetitions === nothing ? nothing :
        @view leaf.column.repetitions[(entrystart + 1):entrystop]
    definitions = leaf.column.definitions === nothing ? nothing :
        @view leaf.column.definitions[(entrystart + 1):entrystop]
    values = @view leaf.column.values[(densestart + 1):densestop]
    source = leaf.column
    column = WriteColumn(source.name, values, source.physical,
        source.type_length, source.optional, source.logical, source.converted,
        source.path, repetitions, definitions, source.max_repetition_level,
        source.max_definition_level, lastrow - firstrow, source.schema)
    _columnentrycount(column) > 0 || throw(AssertionError(
        "a nonempty row slice produced an empty data page"))
    repetitions === nothing || iszero(first(repetitions)) ||
        throw(ArgumentError("writer data page does not begin at a row boundary"))
    actualrows = repetitions === nothing ? _columnentrycount(column) :
        count(iszero, repetitions)
    actualrows == column.rows || throw(ArgumentError(
        "writer page row count does not match its repetition stream"))
    return column, densestart, densestop
end

function _writecapacityargument(err::ArgumentError)
    message = err.msg
    message isa AbstractString || return false
    return message == "encoded Parquet values exceed Int32 bytes" ||
        message == "Parquet page exceeds Int32 bytes" ||
        message == "compressed Parquet page exceeds Int32 bytes"
end

function _writepagecapacity(error)
    if error isa LimitError && error.resource == :page_bytes
        return _WritePageCapacityError(:page_bytes, error.requested,
            error.maximum)
    end
    if error isa ArgumentError && _writecapacityargument(error)
        return _WritePageCapacityError(:page_bytes, typemax(Int64),
            typemax(Int32))
    end
    return nothing
end

function _writeencodedpage(leaf::WriteLeafPlan, firstrow::Int, lastrow::Int,
        encoding::Metadata.Encoding.T, dictionary,
        limits::Limits; checksum::Bool, codec::Metadata.CompressionCodec.T,
        compressionlevel::Union{Nothing,Integer}, pageversion::Symbol)
    column, densestart, densestop = _writepagecolumn(leaf, firstrow, lastrow)
    entries = _columnentrycount(column)
    entries <= typemax(Int32) || throw(_WritePageCapacityError(
        :page_values, Int64(entries), Int64(typemax(Int32))))
    column.rows <= typemax(Int32) || throw(_WritePageCapacityError(
        :page_rows, Int64(column.rows), Int64(typemax(Int32))))
    values = if dictionary === nothing
        try
            _encodedpayload(column, encoding, limits)
        catch err
            capacity = _writepagecapacity(err)
            capacity === nothing && rethrow()
            throw(capacity)
        end
    else
        indices = copy(@view dictionary.indices[(densestart + 1):densestop])
        _encodedictionaryindices(indices, dictionary.bitwidth)
    end
    try
        bytes, headerlength, payloadlength = _datapagebytes(column, values,
            encoding, pageversion, limits; checksum=checksum, codec=codec,
            compressionlevel=compressionlevel)
        length(bytes) <= typemax(Int32) || throw(_WritePageCapacityError(
            :page_frame_bytes, Int64(length(bytes)), Int64(typemax(Int32))))
        uncompressed = Base.checked_add(Int64(headerlength),
            Int64(payloadlength))
        return _WriteDataFrame(bytes, uncompressed, Int64(firstrow))
    catch err
        capacity = _writepagecapacity(err)
        capacity === nothing && rethrow()
        throw(capacity)
    end
end

function _writethrowcapacity(error::_WritePageCapacityError)
    throw(LimitError(error.resource, error.requested, error.maximum))
end

function _writeappendpages!(output::Vector{_WriteDataFrame},
        leaf::WriteLeafPlan, firstrow::Int, lastrow::Int,
        encoding::Metadata.Encoding.T, dictionary, limits::Limits;
        checksum::Bool, codec::Metadata.CompressionCodec.T,
        compressionlevel::Union{Nothing,Integer}, pageversion::Symbol)
    frame = try
        _writeencodedpage(leaf, firstrow, lastrow, encoding, dictionary,
            limits; checksum=checksum, codec=codec,
            compressionlevel=compressionlevel, pageversion=pageversion)
    catch err
        err isa _WritePageCapacityError || rethrow()
        lastrow - firstrow == 1 && _writethrowcapacity(err)
        middle = firstrow + fld(lastrow - firstrow, 2)
        _writeappendpages!(output, leaf, firstrow, middle, encoding,
            dictionary, limits; checksum=checksum, codec=codec,
            compressionlevel=compressionlevel, pageversion=pageversion)
        _writeappendpages!(output, leaf, middle, lastrow, encoding,
            dictionary, limits; checksum=checksum, codec=codec,
            compressionlevel=compressionlevel, pageversion=pageversion)
        return
    end
    push!(output, frame)
    return
end

function _writedataframes(leaf::WriteLeafPlan,
        pagesize::Union{Nothing,Int64}, encoding::Metadata.Encoding.T,
        dictionary, limits::Limits; checksum::Bool,
        codec::Metadata.CompressionCodec.T,
        compressionlevel::Union{Nothing,Integer}, pageversion::Symbol)
    frames = _WriteDataFrame[]
    for (firstrow, lastrow) in _writecandidateboundaries(leaf, pagesize)
        _writeappendpages!(frames, leaf, firstrow, lastrow, encoding,
            dictionary, limits; checksum=checksum, codec=codec,
            compressionlevel=compressionlevel, pageversion=pageversion)
    end
    isempty(frames) && !iszero(leaf.column.rows) && throw(AssertionError(
        "a nonempty leaf chunk produced no data pages"))
    return frames
end

function _writechunkencodings(column::WriteColumn,
        encoding::Metadata.Encoding.T; dictionary::Bool=false)
    dictionary && return Metadata.Encoding.T[Metadata.Encoding.PLAIN,
        Metadata.Encoding.RLE, Metadata.Encoding.RLE_DICTIONARY]
    output = Metadata.Encoding.T[]
    (!iszero(column.max_repetition_level) ||
        !iszero(column.max_definition_level)) &&
        encoding != Metadata.Encoding.RLE &&
        push!(output, Metadata.Encoding.RLE)
    push!(output, encoding)
    return output
end

function _writeaggregatepages(frames::Vector{_WriteDataFrame},
        column::WriteColumn, encoding::Metadata.Encoding.T,
        pageversion::Symbol; dictionarypage=nothing,
        dictionaryuncompressed::Int64=Int64(0),
        capturelocations::Bool=true)
    total = dictionarypage === nothing ? 0 : length(dictionarypage)
    uncompressed = dictionaryuncompressed
    for frame in frames
        total = Base.checked_add(total, length(frame.bytes))
        uncompressed = _addgroupsize(uncompressed, frame.uncompressed_size)
    end
    bytes = UInt8[]
    sizehint!(bytes, total)
    dictionarypage === nothing || append!(bytes, dictionarypage)
    dataoffset = Int64(length(bytes))
    locations = Metadata.PageLocation[]
    capturelocations && sizehint!(locations, length(frames))
    for frame in frames
        relative = Int64(length(bytes))
        append!(bytes, frame.bytes)
        capturelocations && push!(locations, Metadata.PageLocation(
            offset=relative,
            compressed_page_size=Int32(length(frame.bytes)),
            first_row_index=frame.first_row_index))
    end
    dictionary = dictionarypage !== nothing
    encodings = _writechunkencodings(column, encoding;
        dictionary=dictionary)
    pagecount = length(frames)
    pagecount <= typemax(Int32) || throw(LimitError(:container_elements,
        Int64(pagecount), Int64(typemax(Int32))))
    stats = Metadata.PageEncodingStats[]
    if dictionary
        push!(stats, Metadata.PageEncodingStats(
            page_type=Metadata.PageType.DICTIONARY_PAGE,
            encoding=Metadata.Encoding.PLAIN, count=Int32(1)))
    end
    push!(stats, Metadata.PageEncodingStats(
        page_type=_datapagetype(pageversion), encoding=encoding,
        count=Int32(pagecount)))
    return ColumnPages(bytes, uncompressed, dataoffset,
        dictionary ? Int64(0) : nothing, encodings, stats, locations)
end

function _writeencodedchunk(leaf::WriteLeafPlan,
        pagesize::Union{Nothing,Int64}, encoding::Metadata.Encoding.T,
        limits::Limits; checksum::Bool, codec::Metadata.CompressionCodec.T,
        compressionlevel::Union{Nothing,Integer}, pageversion::Symbol,
        capturelocations::Bool=true)
    frames = _writedataframes(leaf, pagesize, encoding, nothing, limits;
        checksum=checksum, codec=codec, compressionlevel=compressionlevel,
        pageversion=pageversion)
    return _writeaggregatepages(frames, leaf.column, encoding, pageversion;
        capturelocations=capturelocations)
end

function _writechunkdictionary(column::WriteColumn, limits::Limits)
    values, indices = _dictionaryentries(column)
    length(values) <= typemax(Int32) || throw(_WritePageCapacityError(
        :page_values, Int64(length(values)), Int64(typemax(Int32))))
    dictionarycolumn = WriteColumn(column.name, values, column.physical,
        column.type_length, false, column.logical, column.converted)
    payload = try
        _plainpayload(dictionarycolumn, limits)
    catch err
        capacity = _writepagecapacity(err)
        capacity === nothing && rethrow()
        throw(capacity)
    end
    bitwidth = _dictionarybitwidth(length(values))
    bitwidth <= 32 || throw(_WritePageCapacityError(:page_values,
        Int64(length(values)), Int64(typemax(UInt32))))
    return _WriteChunkDictionary(values, indices, payload, bitwidth)
end

function _writedictionaryframe(dictionary::_WriteChunkDictionary,
        limits::Limits; checksum::Bool, codec::Metadata.CompressionCodec.T,
        compressionlevel::Union{Nothing,Integer})
    header = Metadata.DictionaryPageHeader(
        num_values=Int32(length(dictionary.values)),
        encoding=Metadata.Encoding.PLAIN, is_sorted=false)
    try
        bytes, headerlength, payloadlength = _framedpage(dictionary.payload,
            Metadata.PageType.DICTIONARY_PAGE, limits; checksum=checksum,
            codec=codec, compressionlevel=compressionlevel,
            dictionary_header=header)
        return bytes, Base.checked_add(Int64(headerlength),
            Int64(payloadlength))
    catch err
        capacity = _writepagecapacity(err)
        capacity === nothing && rethrow()
        throw(capacity)
    end
end

function _writedictionarychunk(leaf::WriteLeafPlan,
        pagesize::Union{Nothing,Int64}, limits::Limits; checksum::Bool,
        codec::Metadata.CompressionCodec.T,
        compressionlevel::Union{Nothing,Integer}, pageversion::Symbol,
        capturelocations::Bool=true)
    dictionary = _writechunkdictionary(leaf.column, limits)
    dictionarypage, dictionaryuncompressed = _writedictionaryframe(dictionary,
        limits; checksum=checksum, codec=codec,
        compressionlevel=compressionlevel)
    frames = _writedataframes(leaf, pagesize,
        Metadata.Encoding.RLE_DICTIONARY, dictionary, limits;
        checksum=checksum, codec=codec, compressionlevel=compressionlevel,
        pageversion=pageversion)
    return _writeaggregatepages(frames, leaf.column,
        Metadata.Encoding.RLE_DICTIONARY, pageversion;
        dictionarypage=dictionarypage,
        dictionaryuncompressed=dictionaryuncompressed,
        capturelocations=capturelocations)
end

function _writeadaptivecapacity(error)
    error isa _WritePageCapacityError && return true
    error isa LimitError || return false
    return error.resource in (:page_bytes, :page_values, :page_rows,
        :page_frame_bytes)
end

function _writethrowadaptivecapacity(error)
    error isa _WritePageCapacityError && _writethrowcapacity(error)
    throw(error)
end

function _writesplitcolumnpages(leaf::WriteLeafPlan, limits::Limits;
        pagesize::Union{Nothing,Int64}, checksum::Bool, dictionary::Bool,
        codec::Metadata.CompressionCodec.T,
        compressionlevel::Union{Nothing,Integer}, pageversion::Symbol,
        encoding::Union{Nothing,Metadata.Encoding.T}=nothing,
        capturelocations::Bool=true)
    encoding === nothing || return _writeencodedchunk(leaf, pagesize,
        encoding, limits; checksum=checksum, codec=codec,
        compressionlevel=compressionlevel, pageversion=pageversion,
        capturelocations=capturelocations)
    dictionary || return _writeencodedchunk(leaf, pagesize,
        Metadata.Encoding.PLAIN, limits; checksum=checksum, codec=codec,
        compressionlevel=compressionlevel, pageversion=pageversion,
        capturelocations=capturelocations)
    leaf.column.physical == Metadata.Type.BOOLEAN &&
        return _writeencodedchunk(leaf, pagesize, Metadata.Encoding.PLAIN,
            limits; checksum=checksum, codec=codec,
            compressionlevel=compressionlevel, pageversion=pageversion,
            capturelocations=capturelocations)
    plain = nothing
    plainerror = nothing
    try
        plain = _writeencodedchunk(leaf, pagesize, Metadata.Encoding.PLAIN,
            limits; checksum=checksum, codec=codec,
            compressionlevel=compressionlevel, pageversion=pageversion,
            capturelocations=capturelocations)
    catch err
        _writeadaptivecapacity(err) || rethrow()
        plainerror = err
    end
    encoded = nothing
    encodederror = nothing
    encoded = try
        _writedictionarychunk(leaf, pagesize, limits; checksum=checksum,
            codec=codec, compressionlevel=compressionlevel,
            pageversion=pageversion,
            capturelocations=capturelocations)
    catch err
        _writeadaptivecapacity(err) || rethrow()
        encodederror = err
        nothing
    end
    plain === nothing && encoded === nothing &&
        _writethrowadaptivecapacity(something(plainerror, encodederror))
    plain === nothing && return encoded
    encoded === nothing && return plain
    length(encoded.bytes) < length(plain.bytes) || return plain
    return encoded
end

function _budgetedsplitcolumnpages(leaf::WriteLeafPlan, limits::Limits,
        budget::_LiveByteBudget; pagesize::Union{Nothing,Int64},
        checksum::Bool, dictionary::Bool,
        codec::Metadata.CompressionCodec.T,
        compressionlevel::Union{Nothing,Integer}, pageversion::Symbol,
        encoding::Union{Nothing,Metadata.Encoding.T}=nothing,
        capturelocations::Bool=true)
    working = _writerpageworkingbytes(leaf.column, dictionary)
    _reserve!(budget, working)
    pages = try
        _writesplitcolumnpages(leaf, limits; pagesize=pagesize,
            checksum=checksum, dictionary=dictionary, codec=codec,
            compressionlevel=compressionlevel, pageversion=pageversion,
            encoding=encoding, capturelocations=capturelocations)
    catch
        _release!(budget, working)
        rethrow()
    end
    live = _columnpageslivebytes(pages)
    live <= working || begin
        _release!(budget, working)
        throw(AssertionError(
            "writer split-page allocation exceeded its materialization preflight"))
    end
    _release!(budget, working - live)
    return pages, live
end
