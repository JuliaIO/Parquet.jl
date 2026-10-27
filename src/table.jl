import Tables

mutable struct Table{F<:File,C<:NamedTuple}
    file::F
    metadata::Metadata.FileMetaData
    schema::Schema
    columns::C
    rows::Int
    @atomic closed::Bool
end

function _writefieldsencoded(table::Table, limits::Limits, encoding,
        dictionary::Bool)
    return _provenancewritefields(table, limits, encoding, dictionary)
end

function _writekeyvaluemetadata(table::Table, limits::Limits)
    metadata = table.metadata.key_value_metadata
    metadata === nothing && return nothing
    return _rewritekeyvaluemetadata(metadata, limits)
end

function _readfilemetadata(file::File, limits::Limits,
    budget::_ByteBudget=_ByteBudget(limits))
    file.footer.encrypted && throw(FormatError("encrypted footers are not supported yet"))
    reader = Thrift.Reader(file.footer.bytes; limits=limits, budget=budget)
    metadata = Thrift.decode(reader, Metadata.FileMetaData)
    metadata.encryption_algorithm === nothing || throw(FormatError(
        "plaintext-footer encryption is not supported yet"))
    Thrift.remaining(reader) == 0 || throw(FormatError("file footer has trailing bytes"))
    return metadata
end

function _tablenames(schema::Schema, limits::Limits)
    return _internschemanames(String[node.element.name for node in schema.root.children],
        limits)
end

function _validatetablerowgroupmetadata(group::Metadata.RowGroup,
        rowindex::Int, leafcount::Int, footeroffset::Int64)
    group.total_byte_size >= 0 || throw(FormatError(
        "row group $rowindex has a negative total byte size"))
    compressed = group.total_compressed_size
    compressed === nothing || compressed >= 0 || throw(FormatError(
        "row group $rowindex has a negative compressed byte size"))
    offset = group.file_offset
    if offset !== nothing
        offset >= 0 || throw(FormatError(
            "row group $rowindex has a negative file offset"))
        (iszero(offset) || 4 <= offset < footeroffset) || throw(FormatError(
            "row group $rowindex file offset $offset is outside the file body"))
    end
    ordinal = group.ordinal
    ordinal === nothing || ordinal >= 0 || throw(FormatError(
        "row group $rowindex has a negative ordinal"))
    sorting = group.sorting_columns
    sorting === nothing && return
    for column in sorting
        index = Int64(column.column_idx)
        0 <= index < leafcount || throw(FormatError(
            "row group $rowindex sorting column index $index is outside " *
            "the schema leaf range"))
    end
    return
end

function _validatetablechunkranges(metadata::Metadata.FileMetaData,
        schema::Schema, footeroffset::Int64)
    leafcount = length(schema.leaves)
    for (rowindex, group) in enumerate(metadata.row_groups)
        _validatetablerowgroupmetadata(group, rowindex, leafcount,
            footeroffset)
        length(group.columns) == leafcount || throw(FormatError(
            "row group $rowindex has $(length(group.columns)) columns for " *
            "$leafcount schema leaves"))
        for (columnindex, (chunk, leaf)) in enumerate(zip(group.columns,
                schema.leaves))
            chunk.file_offset >= 0 || throw(FormatError(
                "row group $rowindex column chunk $columnindex has a " *
                "negative file offset"))
            _chunkrange(_chunkmetadata(chunk, leaf), footeroffset)
        end
    end
    return
end

# Tasks that decode `count` work items: up to `ntasks`, or one when the source does not
# allow concurrent reads.
function _readtasks(file::File, ntasks::Int, count::Int)
    ntasks >= 1 || throw(ArgumentError("ntasks must be at least 1, got $ntasks"))
    concurrentreads(file.source) || return 1
    return max(1, min(ntasks, count))
end

struct _ReadFailure
    error::Any
end

mutable struct _ReadQueue
    @atomic next::Int
end

# `map(index -> f(index, buffers), indices)`, where `buffers` are the `_PageBuffers` of
# the task that runs `f`. With `ntasks > 1`, that many tasks take the indices in order.
# An error from `f` is rethrown as itself rather than in a TaskFailedException.
function _readmap(f, indices, ntasks::Int)
    if ntasks <= 1
        buffers = _PageBuffers()
        return map(index -> f(index, buffers), indices)
    end
    items = collect(indices)
    results = Vector{Any}(undef, length(items))
    queue = _ReadQueue(1)
    workers = map(1:min(ntasks, length(items))) do _
        return errormonitor(Threads.@spawn _readworker(f, items, results, queue))
    end
    foreach(wait, workers)
    for result in results
        result isa _ReadFailure && throw(result.error)
    end
    return results
end

function _readworker(f, items::Vector, results::Vector{Any}, queue::_ReadQueue)
    buffers = _PageBuffers()
    while true
        index = (@atomic queue.next += 1) - 1
        index > length(items) && return
        results[index] = try
            f(items[index], buffers)
        catch err
            _ReadFailure(err)
        end
    end
end

# Index of each row group's first row in the table, minus one.
function _rowgroupstarts(metadata::Metadata.FileMetaData, rows::Int)
    starts = Vector{Int}(undef, length(metadata.row_groups))
    total = 0
    for (rowindex, group) in enumerate(metadata.row_groups)
        starts[rowindex] = total
        total += Int(group.num_rows)
    end
    total == rows || throw(FormatError(
        "row groups contain $total rows but $rows were validated"))
    return starts
end

# Whether every chunk of a flat field declares no more rows than its bytes could encode
# (`_claimcapacity`). Only then is the column allocated at full length up front, so its
# chunks can decode into it in any order.
function _flatchunksbacked(metadata::Metadata.FileMetaData, node::SchemaNode)
    index = Int(node.column_index)
    for group in metadata.row_groups
        md = group.columns[index].meta_data
        md === nothing && return false
        group.num_rows <= _claimcapacity(group.num_rows, md.total_compressed_size) ||
            return false
    end
    return true
end

function _flatcolumnvector(node::SchemaNode, rows::Int, limits::Limits)
    _, L = _valueconverter(node.element, limits)
    return Vector{_flatoutputtype(L, node)}(undef, rows)
end

# One row group's chunk of a flat field, decoded into `column[(offset + 1):]`. Returns
# the page buffers a STRING chunk's payloads point into, or `nothing`.
function _readtablechunk!(column::Vector, offset::Int, file::File,
        metadata::Metadata.FileMetaData, rowindex::Int, node::SchemaNode,
        limits::Limits, budget::_ByteBudget, buffers::_PageBuffers)
    group = metadata.row_groups[rowindex]
    md = _chunkmetadata(group.columns[Int(node.column_index)], node)
    md.num_values == group.num_rows || throw(FormatError(
        "flat column $(node.column_index) has $(md.num_values) values for " *
        "$(group.num_rows) rows"))
    start, stop = _chunkrange(md, file.footer.offset)
    convert, _ = _valueconverter(node.element, limits)
    _readflatchunk!(column, offset, _physicaleltype(md.type_), convert, file.source, md,
        node, start, stop, limits, budget, buffers)
    return convert isa _StringPayloads ? convert.buffers : nothing
end

# A STRING column from its payloads and each row group's page buffers. Payload buffer
# indices count from the row group's first buffer, so later row groups are rebased.
function _stringcolumn(payloads::Vector{DataStrings.StringPayload}, node::SchemaNode,
        chunks::Vector{Vector{Vector{UInt8}}}, starts::Vector{Int})
    pages = Vector{UInt8}[]
    for (rowindex, chunk) in enumerate(chunks)
        base = length(pages)
        if base > 0 && !isempty(chunk)
            last = rowindex == length(starts) ? length(payloads) : starts[rowindex + 1]
            for index in (starts[rowindex] + 1):last
                payload = payloads[index]
                DataStrings.payloadlength(payload) > DataStrings.INLINE_MAX || continue
                buffer = UInt64(DataStrings.payloadbufidx(payload) + base)
                payloads[index] = DataStrings.StringPayload(payload.a,
                    (payload.b & 0xffffffff00000000) | buffer)
            end
        end
        append!(pages, chunk)
    end
    T = node.max_definition_level == 0 ? DataStrings.DataString :
        Union{Missing,DataStrings.DataString}
    return DataStrings.StringVector{T}(payloads, pages, Val(:trusted))
end

function _finishflatcolumn(column::Vector, node::SchemaNode, chunks, starts::Vector{Int})
    column isa Vector{DataStrings.StringPayload} &&
        return _stringcolumn(column, node, chunks, starts)
    node.element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
        _logicalkind(node) === nothing || return column
    return FixedByteArrayVector{eltype(column)}(column, node.element.type_length)
end

# Columns of a table whose fields are all flat leaves. Each chunk decodes straight into
# its column, one task per chunk; a column whose chunks are not all backed by their
# bytes grows as its chunks decode in order, on one task.
function _readflattable(file::File, metadata::Metadata.FileMetaData, schema::Schema,
        rows::Int, limits::Limits, budget::_ByteBudget, ntasks::Int=Threads.nthreads())
    fields = schema.root.children
    starts = _rowgroupstarts(metadata, rows)
    columns = AbstractVector[]
    # STRING page buffers, per field and row group.
    pages = [Vector{Vector{UInt8}}[Vector{UInt8}[] for _ in starts] for _ in fields]
    tasks = Tuple{Int,Int}[]
    for (fieldindex, node) in enumerate(fields)
        node.max_definition_level in (0, 1) ||
            throw(FormatError("nested definition levels are not supported yet"))
        backed = _flatchunksbacked(metadata, node)
        column = _flatcolumnvector(node, backed ? rows : 0, limits)
        backed || sizehint!(column, _claimcapacity(rows, _checkedsourcelength(file.source)))
        push!(columns, column)
        if backed
            append!(tasks, (fieldindex, rowindex) for rowindex in eachindex(starts))
        else
            push!(tasks, (fieldindex, 0))
        end
    end
    _readmap(tasks, _readtasks(file, ntasks, length(tasks))) do (fieldindex, rowindex), buffers
        column = columns[fieldindex]
        node = fields[fieldindex]
        for index in (rowindex == 0 ? eachindex(starts) : (rowindex:rowindex))
            offset = rowindex == 0 ? length(column) : starts[index]
            kept = _readtablechunk!(column, offset, file, metadata, index, node, limits,
                budget, buffers)
            kept === nothing || (pages[fieldindex][index] = kept)
        end
        return nothing
    end
    for (fieldindex, node) in enumerate(fields)
        length(columns[fieldindex]) == rows || throw(FormatError(
            "flat column $(node.column_index) has $(length(columns[fieldindex])) " *
            "values for $rows rows"))
        columns[fieldindex] = _finishflatcolumn(columns[fieldindex], node,
            pages[fieldindex], starts)
    end
    return columns
end

# Copy of a Thrift struct with some fields replaced.
function _withfields(value::T; fields...) where {T}
    return T((haskey(fields, name) ? fields[name] : getfield(value, name)
        for name in fieldnames(T))...)
end

# Index of the last element in the preorder subtree that starts at `index`.
function _subtreeend(elements::Vector{Metadata.SchemaElement}, index::Int)
    last = index
    for _ in 1:something(elements[index].num_children, Int32(0))
        last = _subtreeend(elements, last + 1)
    end
    return last
end

# File metadata and schema restricted to the top-level fields at `positions`, in that
# order and renamed to `names`. Row groups keep only those fields' column chunks, so
# the chunks of every other field are never read.
function _selectfields(metadata::Metadata.FileMetaData, schema::Schema,
        positions::Vector{Int}, names::Vector{String}, limits::Limits)
    elements = metadata.schema
    leafcount = length(schema.leaves)
    elementranges = UnitRange{Int}[]
    leafranges = UnitRange{Int}[]
    element, leaf = 2, 1
    for child in schema.root.children
        last = _subtreeend(elements, element)
        count = _writeschemaleafcount(child)
        push!(elementranges, element:last)
        push!(leafranges, leaf:(leaf + count - 1))
        element, leaf = last + 1, leaf + count
    end
    selected = Metadata.SchemaElement[_withfields(first(elements);
        num_children=Int32(length(positions)))]
    leaves = Int[]
    for (position, name) in zip(positions, names)
        range = elementranges[position]
        push!(selected, _withfields(elements[first(range)]; name=name))
        append!(selected, @view elements[(first(range) + 1):last(range)])
        append!(leaves, leafranges[position])
    end
    groups = map(enumerate(metadata.row_groups)) do (rowindex, group)
        length(group.columns) == leafcount || throw(FormatError(
            "row group $rowindex has $(length(group.columns)) columns for " *
            "$leafcount schema leaves"))
        return _withfields(group; columns=group.columns[leaves],
            sorting_columns=nothing)
    end
    orders = metadata.column_orders
    if orders !== nothing
        length(orders) == leafcount || throw(FormatError(
            "file declares $(length(orders)) column orders for $leafcount leaves"))
        orders = orders[leaves]
    end
    selectedmetadata = _withfields(metadata; schema=selected, row_groups=groups,
        column_orders=orders)
    return selectedmetadata, Schema(selected; limits=limits)
end

function _isflattableplan(plan::_NestedSchemaPlan)
    for child in plan.root.children
        child isa _NestedLeafPlan || return false
        child.source.max_repetition_level == 0 || return false
    end
    return true
end

"""
    Parquet.Table(source; scan=nothing, ntasks=Threads.nthreads(), limits=Parquet.Limits())

Read a Parquet file from a path, a byte vector, or an `IO` into Tables.jl columns.

`scan` takes a `Tables.Scan` request. Only the top-level columns it selects or its
filter references are read; the column chunks of every other column are skipped
unread. The filter, row bounds, and renames are then applied to the columns read, and
the result keeps their stored Parquet schema (renamed), so writing it preserves their
annotations. Type overrides are not supported.

Column chunks decode on up to `ntasks` tasks; `ntasks=1` reads on the calling task.
The result does not depend on `ntasks`.
"""
function Table(input; scan::Union{Nothing,Tables.Scan}=nothing,
        ntasks::Integer=Threads.nthreads(), limits::Limits=Limits())
    ntasks >= 1 || throw(ArgumentError("ntasks must be at least 1, got $ntasks"))
    budget = _ByteBudget(limits)
    file = File(input; limits=limits, budget=budget)
    try
        metadata = _readfilemetadata(file, limits, budget)
        metadata.num_rows >= 0 || throw(FormatError("file metadata has a negative row count"))
        metadata.num_rows <= typemax(Int) || throw(FormatError("file row count overflows Int"))
        _checklimit(:container_elements, metadata.num_rows, limits.max_container_elements)
        schema = Schema(metadata; limits=limits)
        bound = nothing
        if scan !== nothing
            sourcenames = _tablenames(schema, limits)
            bound = Tables.resolve(scan, sourcenames)
            any(column -> column.type !== nothing, bound.columns) && throw(ArgumentError(
                "Parquet.Table does not apply scan type overrides; convert the " *
                "columns after reading"))
            positions = sort!(unique!(vcat(Int[column.index
                for column in bound.columns], bound.filtercols)))
            metadata, schema = _selectfields(metadata, schema, positions,
                String[String(sourcenames[position]) for position in positions],
                limits)
        end
        rows = _validatetablerowgroups(metadata, schema, limits)
        _checklimit(:container_elements, rows, limits.max_container_elements)
        _validatetablechunkranges(metadata, schema, file.footer.offset)
        nested = _nestedplan(schema; limits=limits)
        nested.plan_count > 0 || throw(AssertionError(
            "nested schema plan has no root"))
        names = _tablenames(schema, limits)
        if _isflattableplan(nested)
            columns = _readflattable(file, metadata, schema, rows, limits, budget,
                Int(ntasks))
        else
            root = _readnestedroot(file, metadata, schema, nested, limits,
                budget, Int(ntasks))
            columns = root.children
        end
        length(columns) == length(names) || throw(AssertionError(
            "table column count does not match its names"))
        named = NamedTuple{Tuple(names)}(Tuple(columns))
        if bound !== nothing
            named, rows, metadata, schema = _scanresidual(named, rows, bound,
                metadata, schema, limits)
        end
        table = Table(file, metadata, schema, named, rows, false)
        finalizer(close!, table)
        return table
    catch
        try
            close!(file)
        catch
        end
        rethrow()
    end
end

# Applies the filter, row bounds, order, and renames of a resolved scan to the columns
# read for it, and restricts the schema to the output columns.
function _scanresidual(columns::NamedTuple, rows::Int, bound::Tables.BoundScan,
        metadata::Metadata.FileMetaData, schema::Schema, limits::Limits)
    readpositions = sort!(unique!(vcat(Int[column.index for column in bound.columns],
        bound.filtercols)))
    positions = Int[searchsortedfirst(readpositions, column.index)
        for column in bound.columns]
    names = Symbol[column.name for column in bound.columns]
    readnames = keys(columns)
    bound.filter === nothing && bound.limit === nothing && iszero(bound.offset) &&
        positions == eachindex(readnames) && Tuple(names) == readnames &&
        return columns, rows, metadata, schema
    residual = Tables.Scan(; select=[readnames[position] => name
        for (position, name) in zip(positions, names)], filter=bound.filter,
        limit=bound.limit, offset=bound.offset, validate=bound.validate)
    # With no column read, a stand-in column carries the row count to the executor.
    result = Tables.scan(isempty(readnames) ? (rows=Base.OneTo(rows),) : columns,
        residual)
    outrows = Tables.rowcount(result)
    output = NamedTuple{Tuple(names)}(Tuple(Tables.getcolumn(result, name)
        for name in names))
    metadata, schema = _selectfields(metadata, schema, positions,
        String[String(name) for name in names], limits)
    return output, outrows, metadata, schema
end

function close!(table::Table)
    (@atomicswap table.closed = true) && return
    close!(table.file)
    return
end

function Base.close(table::Table)
    close!(table)
    return
end

function Base.length(table::Table)
    return table.rows
end

function Tables.istable(::Type{<:Table})
    return true
end

function Tables.columnaccess(::Type{<:Table})
    return true
end

function Tables.columns(table::Table)
    return table.columns
end

function Tables.columnnames(table::Table)
    return keys(table.columns)
end

function Tables.schema(table::Table)
    names = Tuple(keys(table.columns))
    types = Tuple(eltype(column) for column in values(table.columns))
    return Tables.Schema(names, types)
end

function Tables.rowcount(table::Table)
    return length(table)
end
