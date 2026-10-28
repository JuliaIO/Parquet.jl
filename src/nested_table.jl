# Whole-table nested leaf-stream assembly.

function _validatetablerowgroups(metadata::Metadata.FileMetaData,
    schema::Schema, limits::Limits)
    declared = metadata.num_rows
    declared >= 0 || throw(FormatError(
        "file metadata has a negative row count"))
    total = Int64(0)
    for (rowindex, group) in enumerate(metadata.row_groups)
        group.num_rows >= 0 || throw(FormatError(
            "row group $rowindex has a negative row count"))
        group.num_rows <= typemax(Int) || throw(FormatError(
            "row group $rowindex row count overflows"))
        length(group.columns) == length(schema.leaves) || throw(FormatError(
            "row group $rowindex has $(length(group.columns)) columns for " *
            "$(length(schema.leaves)) schema leaves"))
        total = try
            Base.checked_add(total, group.num_rows)
        catch err
            err isa OverflowError || rethrow()
            throw(FormatError("row group row count overflows Int64"))
        end
        (iszero(declared) || total <= declared) || throw(FormatError(
            "row groups contain more rows than file metadata declares"))
        _checklimit(:container_elements, total, limits.max_container_elements)
    end
    (iszero(declared) || total == declared) || throw(FormatError(
        "row groups contain $total rows but file metadata declares $declared"))
    total <= typemax(Int) || throw(FormatError(
        "table row count exceeds the Julia index range"))
    return Int(total)
end

# Whether a top-level field reads through the flat path (`_readflattable`).
_isflatfield(plan::_NestedPlan) =
    plan isa _NestedLeafPlan && plan.source.max_repetition_level == 0

# One leaf's chunks in row groups `groups`, decoded in order and assembled along its path.
function _readleafchunks(file::File, metadata::Metadata.FileMetaData,
        plan::_NestedSchemaPlan, path::_LeafPath, groups::UnitRange{Int}, limits::Limits,
        budget::_ByteBudget, buffers::_PageBuffers, options::_ReadOptions)
    node = plan.leaves[path.leaf].source
    index = Int(node.column_index)
    chunks = map(groups) do rowgroup
        group = metadata.row_groups[rowgroup]
        md = _chunkmetadata(group.columns[index], node)
        return group, md, _leafchunkrange(file.source, md, file.footer.offset, limits)
    end
    convert, L = _valueconverter(node, limits, options)
    rows = sum(chunk -> _claimcapacity(chunk[1].num_rows, chunk[3][2] - chunk[3][1]),
        chunks; init=0)
    entries = sum(chunk -> _claimcapacity(chunk[2].num_values, chunk[3][2] - chunk[3][1]),
        chunks; init=0)
    sink = _barrier(_leafsink, path, L, _scratch(buffers.dense, L), rows, entries, buffers)
    return _barrier(_readleafpiece!, sink, _physicaleltype(node.element.type_), convert,
        file, chunks, path, node, limits, budget, buffers, options)::_LeafPiece
end

# Decodes `chunks` (row group, chunk metadata, byte range) of `file`'s leaf at `path`
# into `sink` and returns the leaf's piece.
function _readleafpiece!(sink::_LeafSink{O,L}, ::Type{P}, convert, file::File,
        chunks::Vector, path::_LeafPath, node::SchemaNode, limits::Limits,
        budget::_ByteBudget, buffers::_PageBuffers, options::_ReadOptions) where {O,L,P}
    total = 0
    for (group, md, (start, stop)) in chunks
        state = sink.state
        before = state.slots[1]
        state.chunkstart = true
        try
            _readchunk!(sink, P, L, convert, file.source, md, node, start, stop, limits,
                budget, buffers; checksum=options.checksum)
            state.slots[1] - before == group.num_rows || throw(FormatError(
                "leaf $(path.leaf) has $(state.slots[1] - before) rows in a row group of " *
                "$(group.num_rows)"))
        catch err
            err isa FormatError || rethrow()
            throw(_chunkerror(err, file, group, node))
        end
        total += Int(group.num_rows)
    end
    counts = _finishleaf!(sink, total)
    piece = _leafpiece(sink.state, _leafvalues(sink, node, convert, options.stringtype),
        counts)
    _charge!(budget, sizeof(sink.output) + sum(sizeof, piece.offsets; init=0) +
        sum(bits -> sizeof(bits.chunks), piece.bits; init=0))
    return piece
end

_concatparts(::Type{V}, parts::Vector{AbstractVector}) where {V} =
    _concat(V[part for part in parts])

# An empty column of `node`'s rows.
function _emptycolumn(node::_AssemblyNode)
    T = node.eltype
    plan = node.plan
    plan isa _NestedLeafPlan && return _fromrows(_columntype(T), T[])
    children = AbstractVector[_emptycolumn(child) for child in node.children]
    if plan isa _NestedStructPlan
        names = String[_plansource(child).element.name for child in plan.children]
        return StructVector{T}(names, children, BitVector(), 0)
    end
    plan isa _NestedListPlan && return ListVector{T}([0], BitVector(), only(children))
    values = length(children) == 2 ? children[2] : Missing[]
    return MapVector{T}([0], BitVector(), children[1], values)
end

# The columns of a table with nested fields. Flat fields read as in `_readflattable`.
# Each nested leaf decodes and assembles its chunks on its own task, the largest first;
# then each field is built from its leaves' pieces. With fewer nested leaves than tasks,
# leaves split by row group for parallelism, and each field joins its row groups.
function _readnestedcolumns(file::File, metadata::Metadata.FileMetaData, schema::Schema,
        plan::_NestedSchemaPlan, rows::Int, limits::Limits, budget::_ByteBudget,
        ntasks::Int, options::_ReadOptions)
    plan.depth <= _NESTED_READ_MAX_DEPTH || throw(LimitError(:nested_read_depth,
        Int64(plan.depth), Int64(_NESTED_READ_MAX_DEPTH)))
    fields = plan.root.children
    columns = Vector{AbstractVector}(undef, length(fields))
    flat = Int[index for (index, field) in enumerate(fields) if _isflatfield(field)]
    if !isempty(flat)
        names = String[_plansource(fields[index]).element.name for index in flat]
        flatmetadata, flatschema = _selectfields(metadata, schema, flat, names, limits)
        columns[flat] = _readflattable(file, flatmetadata, flatschema, rows, limits,
            budget, ntasks, options)
    end
    nested = Int[index for index in eachindex(fields) if !_isflatfield(fields[index])]
    isempty(nested) && return columns
    assemblies = _FieldAssembly[_fieldassembly(fields[index], limits, options)
        for index in nested]
    groups = metadata.row_groups
    leaves = sum(field -> length(field.paths), assemblies)
    tasks = _readtasks(file, ntasks, leaves * max(1, length(groups)))
    ranges = tasks > leaves ? UnitRange{Int}[group:group for group in eachindex(groups)] :
        UnitRange{Int}[eachindex(groups)]
    work = Tuple{Int,Int,Int}[(field, position, range)
        for field in eachindex(assemblies)
        for position in eachindex(assemblies[field].paths) for range in eachindex(ranges)]
    cost(item) = sum(group -> groups[group].columns[assemblies[item[1]].paths[item[2]].leaf].
        meta_data.total_uncompressed_size, ranges[item[3]]; init=0)
    order = sortperm(work; by=cost, rev=true)
    results = _readmap(order, _readtasks(file, ntasks, length(work))) do index, buffers
        field, position, range = work[index]
        return _readleafchunks(file, metadata, plan, assemblies[field].paths[position],
            ranges[range], limits, budget, buffers, options)
    end
    pieces = Vector{_LeafPiece}(undef, length(work))
    pieces[order] = results
    starts = pushfirst!(cumsum(Int[length(field.paths) * length(ranges)
        for field in assemblies]), 0)
    fieldorder = sortperm(eachindex(assemblies); by=field -> starts[field + 1] - starts[field],
        rev=true)
    built = _readmap(fieldorder, _readtasks(file, ntasks, length(assemblies))) do field, _
        assembly = assemblies[field]
        isempty(groups) && return _emptycolumn(assembly.root)
        parts = AbstractVector[_buildfield(assembly, _LeafPiece[pieces[starts[field] +
                (position - 1) * length(ranges) + range]
                for position in eachindex(assembly.paths)],
            sum(group -> Int(groups[group].num_rows), ranges[range]; init=0))
            for range in eachindex(ranges)]
        length(parts) == 1 && return parts[1]
        # The column type is known at run time.
        column = _barrier(_concatparts, _columntype(assembly.root.eltype), parts)
        budget.maximum == typemax(Int64) || _charge!(budget, Base.summarysize(column))
        return column
    end
    columns[nested[fieldorder]] = built
    return columns
end
