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

# One leaf's chunk in row group `rowgroup`, decoded and assembled along its path.
function _readleafchunk(file::File, metadata::Metadata.FileMetaData,
        plan::_NestedSchemaPlan, path::_LeafPath, rowgroup::Int, limits::Limits,
        budget::_ByteBudget, buffers::_PageBuffers, options::_ReadOptions)
    node = plan.leaves[path.leaf].source
    group = metadata.row_groups[rowgroup]
    md = _chunkmetadata(group.columns[Int(node.column_index)], node)
    start, stop = _leafchunkrange(file.source, md, file.footer.offset, limits)
    convert, L = _valueconverter(node, limits, options)
    capacity = _claimcapacity(md.num_values, stop - start)
    sink = _leafsink(path, L, _scratch(buffers.dense, L), capacity)
    _readchunk!(sink, _physicaleltype(node.element.type_), L, convert, file.source, md,
        node, start, stop, limits, budget, buffers)
    counts = _finishleaf!(sink, group.num_rows)
    _charge!(budget, sizeof(sink.output) + sum(sizeof, sink.offsets; init=0) +
        sum(bits -> sizeof(bits.chunks), sink.bits; init=0))
    return _leafpiece(sink, _leafvalues(sink, node, convert), counts)
end

# An empty column of `node`'s rows.
function _emptycolumn(node::_AssemblyNode)
    T = node.eltype
    plan = node.plan
    plan isa _NestedLeafPlan && return _fromrows(_columntype(T), T[])
    children = AbstractVector[_emptycolumn(child) for child in node.children]
    if plan isa _NestedStructPlan
        names = String[child.source.element.name for child in plan.children]
        return StructVector{T}(names, children, BitVector(), 0)
    end
    plan isa _NestedListPlan && return ListVector{T}([0], BitVector(), only(children))
    values = length(children) == 2 ? children[2] : Missing[]
    return MapVector{T}([0], BitVector(), children[1], values)
end

# The columns of a table with nested fields. Flat fields read as in `_readflattable`.
# Each leaf chunk of a nested field decodes and assembles on its own task; then each
# nested field is built from its leaves' pieces, one row group at a time, and the row
# groups are joined. The largest pieces are scheduled first.
function _readnestedcolumns(file::File, metadata::Metadata.FileMetaData, schema::Schema,
        plan::_NestedSchemaPlan, rows::Int, limits::Limits, budget::_ByteBudget,
        ntasks::Int, options::_ReadOptions)
    plan.depth <= _NESTED_READ_MAX_DEPTH || throw(LimitError(:nested_read_depth,
        Int64(plan.depth), Int64(_NESTED_READ_MAX_DEPTH)))
    fields = plan.root.children
    columns = Vector{AbstractVector}(undef, length(fields))
    flat = Int[index for (index, field) in enumerate(fields) if _isflatfield(field)]
    if !isempty(flat)
        names = String[fields[index].source.element.name for index in flat]
        flatmetadata, flatschema = _selectfields(metadata, schema, flat, names, limits)
        columns[flat] = _readflattable(file, flatmetadata, flatschema, rows, limits,
            budget, ntasks, options)
    end
    nested = Int[index for index in eachindex(fields) if !_isflatfield(fields[index])]
    isempty(nested) && return columns
    assemblies = _FieldAssembly[_fieldassembly(fields[index], limits, options)
        for index in nested]
    groups = metadata.row_groups
    tasks = Tuple{Int,Int,Int}[(field, position, group)
        for field in eachindex(assemblies)
        for position in eachindex(assemblies[field].paths) for group in eachindex(groups)]
    cost(task) = groups[task[3]].columns[assemblies[task[1]].paths[task[2]].leaf].
        meta_data.total_uncompressed_size
    order = sortperm(tasks; by=cost, rev=true)
    results = _readmap(order, _readtasks(file, ntasks, length(tasks))) do index, buffers
        field, position, group = tasks[index]
        return _readleafchunk(file, metadata, plan, assemblies[field].paths[position],
            group, limits, budget, buffers, options)
    end
    pieces = Vector{_LeafPiece}(undef, length(tasks))
    pieces[order] = results
    starts = cumsum([0; [length(field.paths) * length(groups) for field in assemblies]])
    fieldorder = sortperm(eachindex(assemblies); by=field -> starts[field + 1] - starts[field],
        rev=true)
    built = _readmap(fieldorder, _readtasks(file, ntasks, length(assemblies))) do field, _
        assembly = assemblies[field]
        isempty(groups) && return _emptycolumn(assembly.root)
        parts = map(eachindex(groups)) do group
            leafpieces = _LeafPiece[pieces[starts[field] +
                (position - 1) * length(groups) + group]
                for position in eachindex(assembly.paths)]
            return _buildfield(assembly, leafpieces, Int(groups[group].num_rows))
        end
        length(parts) == 1 && return only(parts)
        column = _concat(_columntype(assembly.root.eltype)[part for part in parts])
        budget.maximum == typemax(Int64) || _charge!(budget, Base.summarysize(column))
        return column
    end
    columns[nested[fieldorder]] = built
    return columns
end
