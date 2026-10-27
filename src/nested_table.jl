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

function _nestedstreamentrycount(metadata::Metadata.FileMetaData,
    leaf::_NestedLeafPlan, limits::Limits)
    total = Int64(0)
    index = Int(leaf.source.column_index)
    for group in metadata.row_groups
        md = _chunkmetadata(group.columns[index], leaf.source)
        total = try
            Base.checked_add(total, md.num_values)
        catch err
            err isa OverflowError || rethrow()
            throw(LimitError(:container_elements, typemax(Int64),
                limits.max_container_elements))
        end
        _checklimit(:container_elements, total,
            limits.max_container_elements)
    end
    total <= typemax(Int) || throw(FormatError(
        "nested leaf entry count exceeds the Julia index range"))
    return Int(total)
end

# Checks that one chunk's entries, appended to `repetition` after `before`, start a row
# and hold `rows` rows.
function _checkchunkrows(repetition::Vector{UInt16}, before::Int, rows::Int64,
        rowindex::Int, leafindex::Int)
    after = length(repetition)
    after > before && !iszero(repetition[before + 1]) && throw(FormatError(
        "row group $rowindex leaf $leafindex starts with repetition level " *
        "$(repetition[before + 1])"))
    actual = count(iszero, view(repetition, (before + 1):after))
    actual == rows || throw(FormatError(
        "row group $rowindex leaf $leafindex has $actual rows but $rows were expected"))
    return
end

# One leaf's stream across all row groups, decoded into one set of arrays with the
# leaf's logical conversion applied, and the page buffers of a STRING leaf's payloads.
function _readnestedstream(file::File, metadata::Metadata.FileMetaData,
    schema::Schema, leaf::_NestedLeafPlan, rows::Int, limits::Limits,
    budget::_ByteBudget, entries::Int, buffers::_PageBuffers)
    node = leaf.source
    T = _physicaleltype(node.element.type_)
    convert, L = _valueconverter(node.element, limits)
    capacity = _claimcapacity(entries, _checkedsourcelength(file.source))
    sink = _StreamSink(sizehint!(UInt16[], capacity), sizehint!(UInt16[], capacity),
        sizehint!(L[], capacity))
    index = Int(node.column_index)
    for (rowindex, group) in enumerate(metadata.row_groups)
        md = _chunkmetadata(group.columns[index], node)
        start, stop = _leafchunkrange(file.source, md, file.footer.offset, limits)
        before = length(sink.repetition)
        _readchunk!(sink, T, L, convert, file.source, md, node, start, stop, limits,
            budget, buffers)
        if node.max_repetition_level == 0
            length(sink.repetition) - before == group.num_rows || throw(FormatError(
                "row group $rowindex leaf $index has $(length(sink.repetition) - before) " *
                "rows but $(group.num_rows) were expected"))
        else
            _checkchunkrows(sink.repetition, before, group.num_rows, rowindex, index)
        end
    end
    length(sink.repetition) == entries || throw(AssertionError(
        "nested leaf concatenation did not fill its level arrays"))
    stream = LeafStream(sink.repetition, sink.definition, sink.values,
        node.max_repetition_level, node.max_definition_level; expected_rows=rows)
    return stream, convert isa _StringPayloads ? convert.buffers : nothing
end

function _readnestedroot(file::File, metadata::Metadata.FileMetaData,
    schema::Schema, plan::_NestedSchemaPlan, limits::Limits, budget::_ByteBudget,
    ntasks::Int=Threads.nthreads())
    rows = _validatetablerowgroups(metadata, schema, limits)
    entries = Int[_nestedstreamentrycount(metadata, leaf, limits) for leaf in plan.leaves]
    tasks = _readtasks(file, ntasks, length(plan.leaves))
    results = _readmap(eachindex(plan.leaves), tasks) do index, buffers
        return _readnestedstream(file, metadata, schema, plan.leaves[index], rows,
            limits, budget, entries[index], buffers)
    end
    streams = LeafStream[first(result) for result in results]
    pages = Union{Nothing,Vector{Vector{UInt8}}}[last(result) for result in results]
    return _assemblenested(plan, streams, rows; limits=limits, budget=budget,
        strings=pages)
end
