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

function _nestedstreampayloadcharge(::Type{T}, retained::Int64,
    count::Int) where {T}
    structural = _materializedsum(_leaflevelbytes(count),
        _materializedarraybytes(T, count))
    retained >= structural || throw(AssertionError(
        "leaf stream retained less than its level and value vectors"))
    payload = retained - structural
    T === Vector{UInt8} || iszero(payload) || throw(AssertionError(
        "fixed-width leaf stream retained an unexpected payload charge"))
    return payload
end

function _appendnestedrowgroup!(repetition::Vector{UInt64},
    definition::Vector{UInt64}, values::Vector{T}, position::Int,
    file::File, metadata::Metadata.FileMetaData, schema::Schema,
    rowindex::Int, leafindex::Int, count::Int, rows::Int64,
    limits::Limits, budget::_LiveByteBudget) where {T}
    before = _budgetused(budget)
    try
        stream = readleafstream(file, metadata, schema, rowindex, leafindex;
            expected_rows=rows, limits=limits, budget=budget)
        retained = _budgetused(budget) - before
        length(stream) == count || throw(FormatError(
            "row group $rowindex leaf $leafindex produced $(length(stream)) " *
            "of $count declared entries"))
        count == 0 || begin
            copyto!(repetition, position, stream.repetition, 1, count)
            copyto!(definition, position, stream.definition, 1, count)
        end
        append!(values, stream.values)
        payload = _nestedstreampayloadcharge(T, retained, count)
        structural = _materializedsum(_leaflevelbytes(count),
            _materializedarraybytes(T, count))
        _release!(budget, structural)
        return position + count, payload
    catch
        retained = _budgetused(budget) - before
        retained >= 0 || throw(AssertionError(
            "leaf stream failure decreased the shared budget"))
        iszero(retained) || _release!(budget, retained)
        rethrow()
    end
end

function _readnestedstream(file::File, metadata::Metadata.FileMetaData,
    schema::Schema, leaf::_NestedLeafPlan, rows::Int, limits::Limits,
    budget::_LiveByteBudget, entries::Int)
    T = _physicaleltype(leaf.source.element.type_)
    repetition = Vector{UInt64}(undef, entries)
    definition = Vector{UInt64}(undef, entries)
    values = T[]
    sizehint!(values, entries)
    position = 1
    payloadcharge = Int64(0)
    index = Int(leaf.source.column_index)
    try
        for (rowindex, group) in enumerate(metadata.row_groups)
            md = _chunkmetadata(group.columns[index], leaf.source)
            count = Int(md.num_values)
            position, payload = _appendnestedrowgroup!(repetition,
                definition, values, position, file, metadata, schema,
                rowindex, index, count, group.num_rows, limits, budget)
            payloadcharge = _materializedsum(payloadcharge, payload)
        end
        position == entries + 1 || throw(AssertionError(
            "nested leaf concatenation did not fill its level arrays"))
        return LeafStream(repetition, definition, values,
            leaf.source.max_repetition_level,
            leaf.source.max_definition_level; expected_rows=rows),
            payloadcharge
    catch
        iszero(payloadcharge) || _release!(budget, payloadcharge)
        rethrow()
    end
end

function _nestedstreamlayout!(entries::Vector{Int},
    metadata::Metadata.FileMetaData, plan::_NestedSchemaPlan,
    limits::Limits)
    structural = Int64(0)
    for (index, leaf) in enumerate(plan.leaves)
        count = _nestedstreamentrycount(metadata, leaf, limits)
        entries[index] = count
        T = _physicaleltype(leaf.source.element.type_)
        charge = _materializedsum(_leaflevelbytes(count),
            _materializedarraybytes(T, count))
        structural = _materializedsum(structural, charge)
    end
    return structural
end

function _readnestedroot(file::File, metadata::Metadata.FileMetaData,
    schema::Schema, plan::_NestedSchemaPlan, limits::Limits,
    budget::_LiveByteBudget)
    rows = _validatetablerowgroups(metadata, schema, limits)
    count = length(plan.leaves)
    streamcharge = _materializedsum(
        _materializedarraybytes(LeafStream, count),
        _materializedproduct(count, _MATERIALIZED_OBJECT_BYTES))
    _reserve!(budget, streamcharge)
    streams = LeafStream[]
    sizehint!(streams, count)
    accountingcharge = _materializedsum(_materializedarraybytes(Int, count),
        _materializedarraybytes(Int64, count))
    try
        _reserve!(budget, accountingcharge)
    catch
        _release!(budget, streamcharge)
        rethrow()
    end
    entrycounts = zeros(Int, count)
    payloadcharges = zeros(Int64, count)
    assembled = false
    structuralcharge = Int64(0)
    structuralreserved = false
    try
        structuralcharge = _nestedstreamlayout!(entrycounts, metadata,
            plan, limits)
        _reserve!(budget, structuralcharge)
        structuralreserved = true
        for (index, leaf) in enumerate(plan.leaves)
            stream, payload = _readnestedstream(file, metadata, schema,
                leaf, rows, limits, budget, entrycounts[index])
            push!(streams, stream)
            payloadcharges[index] = payload
        end
        root = _assemblenested(plan, streams, rows;
            limits=limits, budget=budget)
        assembled = true
        for index in eachindex(streams)
            if _logicalkind(plan.leaves[index].source) !== nothing
                _release!(budget, payloadcharges[index])
                payloadcharges[index] = 0
            end
        end
        _release!(budget, structuralcharge)
        structuralreserved = false
        _release!(budget, _materializedsum(streamcharge, accountingcharge))
        return root
    catch
        if !assembled
            for charge in payloadcharges
                iszero(charge) || _release!(budget, charge)
            end
        end
        structuralreserved && _release!(budget, structuralcharge)
        _release!(budget, _materializedsum(streamcharge, accountingcharge))
        rethrow()
    end
end
