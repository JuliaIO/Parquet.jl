# Recursive Dremel assembly from aligned physical leaf streams.

mutable struct _NestedReadState
    plan::_NestedPlan
    children::Vector{_NestedReadState}
    force_required::Bool
    occurrences::Int64
    present::Int64
    entries::Int64
    fill_occurrences::Int64
    fill_present::Int64
    fill_entries::Int64
    buffer::Any
    validity::Union{Nothing,BitVector}
    names::Union{Nothing,Vector{String}}
    child_outputs::Union{Nothing,Vector{AbstractVector}}
    result::Any
end

mutable struct _NestedReadContext
    streams::Vector{LeafStream}
    positions::Vector{Int}
    dense_positions::Vector{Int}
    logical_values::Vector{AbstractVector}
    limits::Limits
    fill::Bool
end

function _nestedreadlabel(plan::_NestedPlan)
    path = getfield(plan, :source).path
    isempty(path) && return "schema root"
    return "nested field $(repr(join(path, ".")))"
end

function _nestedreadstate(plan::_NestedLeafPlan; force_required::Bool=false)
    return _NestedReadState(plan, _NestedReadState[], force_required,
        Int64(0), Int64(0), Int64(0), Int64(0), Int64(0), Int64(0), nothing,
        nothing, nothing, nothing, nothing)
end

function _nestedreadstate(plan::_NestedStructPlan; force_required::Bool=false)
    children = _NestedReadState[]
    sizehint!(children, length(plan.children))
    for child in plan.children
        push!(children, _nestedreadstate(child))
    end
    return _NestedReadState(plan, children, force_required, Int64(0), Int64(0),
        Int64(0), Int64(0), Int64(0), Int64(0), nothing, nothing, nothing,
        nothing, nothing)
end

function _nestedreadstate(plan::_NestedListPlan; force_required::Bool=false)
    child = _nestedreadstate(plan.element)
    return _NestedReadState(plan, _NestedReadState[child], force_required,
        Int64(0), Int64(0), Int64(0), Int64(0), Int64(0), Int64(0), nothing,
        nothing, nothing, nothing, nothing)
end

function _nestedreadstate(plan::_NestedMapPlan; force_required::Bool=false)
    count = plan.value === nothing ? 1 : 2
    children = Vector{_NestedReadState}(undef, count)
    children[1] = _nestedreadstate(plan.key; force_required=true)
    plan.value === nothing || (children[2] = _nestedreadstate(plan.value))
    return _NestedReadState(plan, children, force_required, Int64(0), Int64(0),
        Int64(0), Int64(0), Int64(0), Int64(0), nothing, nothing, nothing,
        nothing, nothing)
end

function _nestedreadplancount(plan::_NestedLeafPlan)
    return Int64(1)
end

function _nestedreadplancountsum(count::Int64, child::_NestedPlan)
    return try
        Base.checked_add(count, _nestedreadplancount(child))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64), typemax(Int64)))
    end
end

function _nestedreadplancount(plan::_NestedStructPlan)
    count = Int64(1)
    for child in plan.children
        count = _nestedreadplancountsum(count, child)
    end
    return count
end

function _nestedreadplancount(plan::_NestedListPlan)
    return _nestedreadplancountsum(Int64(1), plan.element)
end

function _nestedreadplancount(plan::_NestedMapPlan)
    count = _nestedreadplancountsum(Int64(1), plan.key)
    plan.value === nothing && return count
    return _nestedreadplancountsum(count, plan.value)
end

function _nestedreadint(value::Integer, label::AbstractString)
    value >= 0 || throw(FormatError("negative $label"))
    value <= typemax(Int) || throw(FormatError("$label exceeds the Julia index range"))
    return Int(value)
end

function _nestedreadincrement(value::Int64, limits::Limits)
    next = try
        Base.checked_add(value, Int64(1))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64),
            limits.max_container_elements))
    end
    _checklimit(:container_elements, next, limits.max_container_elements)
    return next
end

function _nestedreadrecord!(context::_NestedReadContext,
    state::_NestedReadState, field::Symbol)
    if context.fill
        fillfield = field === :occurrences ? :fill_occurrences :
            field === :present ? :fill_present : :fill_entries
        expected = getfield(state, field)
        next = getfield(state, fillfield) + Int64(1)
        next <= expected || throw(FormatError(
            "$(_nestedreadlabel(state.plan)) changed between nested reader passes"))
        setfield!(state, fillfield, next)
        return next
    end
    next = _nestedreadincrement(getfield(state, field), context.limits)
    setfield!(state, field, next)
    return next
end

function _nestedreadphysicalvalue(value, expected::Type,
    leaf::_NestedLeafPlan)
    value isa expected || throw(FormatError(
        "physical leaf $(repr(join(leaf.source.path, "."))) contains value type " *
        "$(typeof(value)); expected $expected"))
    if leaf.source.element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY
        width = leaf.source.element.type_length
        width !== nothing && length(value) == width || throw(FormatError(
            "fixed byte-array leaf $(repr(join(leaf.source.path, "."))) contains " *
            "a value with the wrong length"))
    end
    return
end

function _nestedreadvalidatestream(stream::LeafStream, leaf::_NestedLeafPlan,
    rows::Int, limits::Limits)
    repetitions = stream.repetition
    definitions = stream.definition
    length(repetitions) == length(definitions) || throw(FormatError(
        "leaf stream repetition and definition counts differ"))
    _checklimit(:container_elements, length(repetitions),
        limits.max_container_elements)
    _checklimit(:container_elements, length(stream.values),
        limits.max_container_elements)
    maxrepetition = UInt64(leaf.source.max_repetition_level)
    maxdefinition = UInt64(leaf.source.max_definition_level)
    rowcount = 0
    densecount = 0
    for index in eachindex(repetitions, definitions)
        repetition = repetitions[index]
        definition = definitions[index]
        repetition <= maxrepetition || throw(FormatError(
            "repetition level $repetition exceeds the schema maximum $maxrepetition"))
        definition <= maxdefinition || throw(FormatError(
            "definition level $definition exceeds the schema maximum $maxdefinition"))
        repetition <= definition || throw(FormatError(
            "repetition level $repetition exceeds definition level $definition"))
        iszero(repetition) && (rowcount += 1)
        definition == maxdefinition && (densecount += 1)
    end
    isempty(repetitions) || iszero(first(repetitions)) || throw(FormatError(
        "leaf stream starts with a nonzero repetition level"))
    rowcount == rows || throw(FormatError(
        "leaf stream has $rowcount rows but $rows were expected"))
    densecount == length(stream.values) || throw(FormatError(
        "leaf stream has $(length(stream.values)) dense values for $densecount present entries"))
    expected = _physicaleltype(leaf.source.element.type_)
    for value in stream.values
        _nestedreadphysicalvalue(value, expected, leaf)
    end
    return
end

function _nestedreadvalidatestreams(plan::_NestedSchemaPlan,
    streams::AbstractVector,
    rows::Int, limits::Limits)
    length(streams) == length(plan.leaves) || throw(FormatError(
        "nested reader received $(length(streams)) streams for $(length(plan.leaves)) leaves"))
    for (index, stream) in enumerate(streams)
        stream isa LeafStream || throw(FormatError(
            "nested reader stream $index is not a LeafStream"))
        _nestedreadvalidatestream(stream, plan.leaves[index], rows, limits)
    end
    return
end

function _nestedreadstreams(streams::AbstractVector)
    output = Vector{LeafStream}(undef, length(streams))
    for (index, stream) in enumerate(streams)
        output[index] = stream
    end
    return output
end

function _nestedreadcurrent(context::_NestedReadContext, leafindex::Int)
    stream = context.streams[leafindex]
    position = context.positions[leafindex]
    position <= length(stream) || throw(FormatError(
        "leaf $leafindex ends before its sibling occurrence"))
    return stream.repetition[position], stream.definition[position]
end

function _nestedreadstatus(context::_NestedReadContext, plan::_NestedPlan,
    startrepetition::UInt64)
    range = _nestedleafrange(plan)
    isempty(range) && return true
    expected = nothing
    for rawindex in range
        leafindex = Int(rawindex)
        repetition, definition = _nestedreadcurrent(context, leafindex)
        repetition == startrepetition || throw(FormatError(
            "$(_nestedreadlabel(plan)) has misaligned sibling repetition boundaries"))
        parent = UInt64(getfield(plan, :parent_definition))
        definition >= parent || throw(FormatError(
            "$(_nestedreadlabel(plan)) has a definition below its present ancestor"))
        present = definition >= UInt64(getfield(plan, :present_definition))
        if expected === nothing
            expected = present
        elseif expected != present
            throw(FormatError(
                "$(_nestedreadlabel(plan)) has inconsistent sibling presence"))
        end
    end
    return something(expected)
end

function _nestedreadentrystatus(context::_NestedReadContext,
    plan::Union{_NestedListPlan,_NestedMapPlan})
    expected = nothing
    for rawindex in plan.leaf_range
        leafindex = Int(rawindex)
        _, definition = _nestedreadcurrent(context, leafindex)
        present = definition >= UInt64(plan.entry_definition)
        if expected === nothing
            expected = present
        elseif expected != present
            throw(FormatError(
                "$(_nestedreadlabel(plan)) has inconsistent sibling entry presence"))
        end
    end
    return something(expected)
end

function _nestedreadparentrepetition(
    plan::Union{_NestedListPlan,_NestedMapPlan})
    plan.repetition_level > 0 || throw(FormatError(
        "$(_nestedreadlabel(plan)) has no repeated-entry level"))
    return UInt64(plan.repetition_level - Int16(1))
end

function _nestedreadconsume!(context::_NestedReadContext, plan::_NestedLeafPlan)
    leafindex = Int(first(plan.leaf_range))
    stream = context.streams[leafindex]
    position = context.positions[leafindex]
    position <= length(stream) || throw(FormatError("leaf $leafindex ends early"))
    definition = stream.definition[position]
    denseindex = 0
    if definition == UInt64(plan.source.max_definition_level)
        denseindex = context.dense_positions[leafindex] + 1
        denseindex <= length(stream.values) || throw(FormatError(
            "leaf $leafindex ends before its dense values"))
        context.dense_positions[leafindex] = denseindex
    end
    context.positions[leafindex] = position + 1
    return denseindex
end

function _nestedreadconsumeplaceholder!(context::_NestedReadContext,
    plan::_NestedPlan, leaves::Vector{_NestedLeafPlan})
    for rawindex in _nestedleafrange(plan)
        leafindex = Int(rawindex)
        stream = context.streams[leafindex]
        position = context.positions[leafindex]
        position <= length(stream) || throw(FormatError(
            "leaf $leafindex ends before its sibling placeholder"))
        definition = stream.definition[position]
        if definition == UInt64(leaves[leafindex].source.max_definition_level)
            denseindex = context.dense_positions[leafindex] + 1
            denseindex <= length(stream.values) || throw(FormatError(
                "leaf $leafindex ends before its dense values"))
            context.dense_positions[leafindex] = denseindex
        end
        context.positions[leafindex] = position + 1
    end
    return
end

function _nestedreadboundary(context::_NestedReadContext,
    range::UnitRange{Int32}, maximum::UInt64, label::AbstractString)
    isempty(range) && return nothing
    expected = nothing
    ended = nothing
    for rawindex in range
        leafindex = Int(rawindex)
        position = context.positions[leafindex]
        stream = context.streams[leafindex]
        atend = position > length(stream)
        if ended === nothing
            ended = atend
        elseif ended != atend
            throw(FormatError("$label has incomplete sibling streams"))
        end
        atend && continue
        repetition = stream.repetition[position]
        repetition <= maximum || throw(FormatError(
            "$label leaves an unconsumed repetition level $repetition"))
        if expected === nothing
            expected = repetition
        elseif expected != repetition
            throw(FormatError("$label has misaligned next sibling boundaries"))
        end
    end
    something(ended) && return nothing
    return something(expected)
end

function _nestedreadscan!(context::_NestedReadContext,
    state::_NestedReadState, startrepetition::UInt64,
    leaves::Vector{_NestedLeafPlan})
    return _nestedreadscan!(context, state, state.plan, startrepetition, leaves)
end

function _nestedreadscan!(context::_NestedReadContext,
    state::_NestedReadState, plan::_NestedLeafPlan, startrepetition::UInt64,
    leaves::Vector{_NestedLeafPlan})
    outputindex = _nestedreadrecord!(context, state, :occurrences)
    present = _nestedreadstatus(context, plan, startrepetition)
    denseindex = _nestedreadconsume!(context, plan)
    if present
        denseindex > 0 || throw(FormatError(
            "$(_nestedreadlabel(plan)) is present without a dense value"))
        _nestedreadrecord!(context, state, :present)
        if context.fill
            values = context.logical_values[Int(first(plan.leaf_range))]
            state.buffer[Int(outputindex)] = values[denseindex]
        end
    else
        denseindex == 0 || throw(FormatError(
            "$(_nestedreadlabel(plan)) is null but has a dense value"))
        if context.fill
            state.force_required && throw(FormatError(
                "$(_nestedreadlabel(plan)) is a null map key"))
            state.buffer[Int(outputindex)] = missing
        end
    end
    return present
end

function _nestedreadscan!(context::_NestedReadContext,
    state::_NestedReadState, plan::_NestedStructPlan, startrepetition::UInt64,
    leaves::Vector{_NestedLeafPlan})
    outputindex = _nestedreadrecord!(context, state, :occurrences)
    present = _nestedreadstatus(context, plan, startrepetition)
    boundary = UInt64(plan.source.max_repetition_level)
    if !present
        _nestedreadconsumeplaceholder!(context, plan, leaves)
        _nestedreadboundary(context, plan.leaf_range, boundary,
            _nestedreadlabel(plan))
        if context.fill && state.buffer !== nothing
            state.buffer[Int(outputindex) + 1] = state.fill_present
        end
        return false
    end
    _nestedreadrecord!(context, state, :present)
    for child in state.children
        _nestedreadscan!(context, child, startrepetition, leaves)
    end
    _nestedreadboundary(context, plan.leaf_range, boundary,
        _nestedreadlabel(plan))
    if context.fill && state.buffer !== nothing
        state.buffer[Int(outputindex) + 1] = state.fill_present
    end
    return true
end

function _nestedreadscan!(context::_NestedReadContext,
    state::_NestedReadState, plan::_NestedListPlan, startrepetition::UInt64,
    leaves::Vector{_NestedLeafPlan})
    outputindex = _nestedreadrecord!(context, state, :occurrences)
    present = _nestedreadstatus(context, plan, startrepetition)
    parentrepetition = _nestedreadparentrepetition(plan)
    if !present
        _nestedreadconsumeplaceholder!(context, plan, leaves)
        _nestedreadboundary(context, plan.leaf_range, parentrepetition,
            _nestedreadlabel(plan))
        if context.fill
            state.validity === nothing || (state.validity[Int(outputindex)] = false)
            state.buffer[Int(outputindex) + 1] = state.fill_entries
        end
        return false
    end
    _nestedreadrecord!(context, state, :present)
    nonempty = _nestedreadentrystatus(context, plan)
    if !nonempty
        _nestedreadconsumeplaceholder!(context, plan, leaves)
        _nestedreadboundary(context, plan.leaf_range, parentrepetition,
            _nestedreadlabel(plan))
        if context.fill
            state.validity === nothing || (state.validity[Int(outputindex)] = true)
            state.buffer[Int(outputindex) + 1] = state.fill_entries
        end
        return true
    end
    itemrepetition = startrepetition
    repeated = UInt64(plan.repetition_level)
    while true
        _nestedreadrecord!(context, state, :entries)
        _nestedreadscan!(context, state.children[1], itemrepetition, leaves)
        boundary = _nestedreadboundary(context, plan.leaf_range, repeated,
            _nestedreadlabel(plan))
        boundary == repeated || break
        itemrepetition = repeated
    end
    boundary = _nestedreadboundary(context, plan.leaf_range, parentrepetition,
        _nestedreadlabel(plan))
    if context.fill
        state.validity === nothing || (state.validity[Int(outputindex)] = true)
        state.buffer[Int(outputindex) + 1] = state.fill_entries
    end
    boundary === nothing || boundary <= parentrepetition || throw(FormatError(
        "$(_nestedreadlabel(plan)) continues after its final item"))
    return true
end

function _nestedreadscan!(context::_NestedReadContext,
    state::_NestedReadState, plan::_NestedMapPlan, startrepetition::UInt64,
    leaves::Vector{_NestedLeafPlan})
    outputindex = _nestedreadrecord!(context, state, :occurrences)
    present = _nestedreadstatus(context, plan, startrepetition)
    parentrepetition = _nestedreadparentrepetition(plan)
    if !present
        _nestedreadconsumeplaceholder!(context, plan, leaves)
        _nestedreadboundary(context, plan.leaf_range, parentrepetition,
            _nestedreadlabel(plan))
        if context.fill
            state.validity === nothing || (state.validity[Int(outputindex)] = false)
            state.buffer[Int(outputindex) + 1] = state.fill_entries
        end
        return false
    end
    _nestedreadrecord!(context, state, :present)
    nonempty = _nestedreadentrystatus(context, plan)
    if !nonempty
        _nestedreadconsumeplaceholder!(context, plan, leaves)
        _nestedreadboundary(context, plan.leaf_range, parentrepetition,
            _nestedreadlabel(plan))
        if context.fill
            state.validity === nothing || (state.validity[Int(outputindex)] = true)
            state.buffer[Int(outputindex) + 1] = state.fill_entries
        end
        return true
    end
    itemrepetition = startrepetition
    repeated = UInt64(plan.repetition_level)
    while true
        _nestedreadrecord!(context, state, :entries)
        keypresent = _nestedreadscan!(context, state.children[1],
            itemrepetition, leaves)
        keypresent || throw(FormatError(
            "$(_nestedreadlabel(plan)) contains a null map key"))
        length(state.children) == 1 || _nestedreadscan!(context,
            state.children[2], itemrepetition, leaves)
        boundary = _nestedreadboundary(context, plan.leaf_range, repeated,
            _nestedreadlabel(plan))
        boundary == repeated || break
        itemrepetition = repeated
    end
    boundary = _nestedreadboundary(context, plan.leaf_range, parentrepetition,
        _nestedreadlabel(plan))
    if context.fill
        state.validity === nothing || (state.validity[Int(outputindex)] = true)
        state.buffer[Int(outputindex) + 1] = state.fill_entries
    end
    boundary === nothing || boundary <= parentrepetition || throw(FormatError(
        "$(_nestedreadlabel(plan)) continues after its final entry"))
    return true
end

function _nestedreadpass!(context::_NestedReadContext,
    state::_NestedReadState, plan::_NestedSchemaPlan, rows::Int)
    for _ in 1:rows
        _nestedreadscan!(context, state, UInt64(0), plan.leaves)
    end
    boundary = _nestedreadboundary(context, plan.root.leaf_range, UInt64(0),
        "schema root")
    boundary === nothing || throw(FormatError(
        "nested leaf streams contain more rows than the metadata row count"))
    for index in eachindex(context.streams)
        context.positions[index] == length(context.streams[index]) + 1 ||
            throw(FormatError("nested reader did not consume every level entry in leaf $index"))
        context.dense_positions[index] == length(context.streams[index].values) ||
            throw(FormatError("nested reader did not consume every dense value in leaf $index"))
    end
    return
end

function _nestedreadverifycounts(state::_NestedReadState)
    state.present <= state.occurrences || throw(FormatError(
        "$(_nestedreadlabel(state.plan)) has more present values than occurrences"))
    plan = state.plan
    required = state.force_required ||
        getfield(plan, :parent_definition) == getfield(plan, :present_definition)
    required && state.present != state.occurrences && throw(FormatError(
        "$(_nestedreadlabel(plan)) is required but has null occurrences"))
    if plan isa _NestedStructPlan
        iszero(state.entries) || throw(FormatError(
            "$(_nestedreadlabel(plan)) records entries for a struct"))
        for child in state.children
            child.occurrences == state.present || throw(FormatError(
                "$(_nestedreadlabel(plan)) has a miscounted struct child"))
        end
    elseif plan isa _NestedListPlan
        length(state.children) == 1 || throw(FormatError(
            "$(_nestedreadlabel(plan)) has an invalid reader state"))
        state.children[1].occurrences == state.entries || throw(FormatError(
            "$(_nestedreadlabel(plan)) has a miscounted list element"))
    elseif plan isa _NestedMapPlan
        for child in state.children
            child.occurrences == state.entries || throw(FormatError(
                "$(_nestedreadlabel(plan)) has a miscounted map child"))
        end
    elseif !iszero(state.entries)
        throw(FormatError("$(_nestedreadlabel(plan)) records entries for a leaf"))
    end
    for child in state.children
        _nestedreadverifycounts(child)
    end
    return
end

function _nestedreadlogicaltype(plan::_NestedLeafPlan)
    physical = _physicaleltype(plan.source.element.type_)
    return _logicaleltype(plan.source, physical)
end

function _nestedreadoutputtype(state::_NestedReadState,
    plan::_NestedLeafPlan)
    logical = _nestedreadlogicaltype(plan)
    required = state.force_required || plan.parent_definition == plan.present_definition
    return required ? logical : Union{Missing,logical}
end

function _nestedreadindexbytes(terminal::Int64, count::Int64)
    T = _nestedindextype(terminal)
    return _materializedarraybytes(T, count)
end

function _nestedreadpassbytes(plan::_NestedSchemaPlan)
    leaves = Int64(length(plan.leaves))
    nodes = _nestedreadplancount(plan.root)
    nodes == plan.plan_count || throw(FormatError(
        "nested schema plan count does not match its topology"))
    nodes > 0 || throw(FormatError("nested schema plan has no root state"))
    edges = nodes - Int64(1)
    bytes = _materializedarraybytes(LeafStream, leaves)
    bytes = _materializedsum(bytes, _materializedarraybytes(Int, leaves))
    bytes = _materializedsum(bytes, _materializedarraybytes(Int, leaves))
    bytes = _materializedsum(bytes,
        _materializedarraybytes(AbstractVector, leaves))
    bytes = _materializedsum(bytes,
        _materializedproduct(_nestedreadplusone(nodes),
            _MATERIALIZED_OBJECT_BYTES))
    bytes = _materializedsum(bytes,
        _materializedproduct(nodes, _MATERIALIZED_ARRAY_HEADER_BYTES))
    bytes = _materializedsum(bytes,
        _materializedarraybytes(_NestedReadState, edges; header=false))
    return bytes
end

function _nestedreadplusone(value::Int64)
    return try
        Base.checked_add(value, Int64(1))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64), typemax(Int64)))
    end
end

function _nestedreadpayloadbytes(plan::_NestedLeafPlan, stream::LeafStream)
    _logicalkind(plan.source) === nothing && return Int64(0)
    logical = _nestedreadlogicaltype(plan)
    isbitstype(logical) && return Int64(0)
    bytes = Int64(0)
    for value in stream.values
        bytes = _materializedsum(bytes, _MATERIALIZED_OBJECT_BYTES)
        value isa AbstractVector{UInt8} || continue
        bytes = _materializedsum(bytes, length(value))
    end
    return bytes
end

function _nestedreadcharges(state::_NestedReadState, streams::Vector{LeafStream})
    plan = state.plan
    if plan isa _NestedLeafPlan
        outputtype = _nestedreadoutputtype(state, plan)
        final = _materializedarraybytes(outputtype, state.occurrences)
        plan.source.element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
            _logicalkind(plan.source) === nothing &&
            (final = _materializedsum(final, _MATERIALIZED_OBJECT_BYTES))
        temporary = Int64(0)
        if _logicalkind(plan.source) !== nothing
            logical = _nestedreadlogicaltype(plan)
            temporary = _materializedarraybytes(logical,
                length(streams[Int(first(plan.leaf_range))].values))
            final = _materializedsum(final,
                _nestedreadpayloadbytes(plan,
                    streams[Int(first(plan.leaf_range))]))
        end
        return final, temporary
    end
    final = _MATERIALIZED_OBJECT_BYTES
    temporary = Int64(0)
    count = state.occurrences
    if plan isa _NestedStructPlan
        final = _materializedsum(final,
            _materializedarraybytes(String, length(state.children)))
        final = _materializedsum(final,
            _materializedarraybytes(AbstractVector, length(state.children)))
        required = state.force_required || plan.parent_definition == plan.present_definition
        required || (final = _materializedsum(final,
            _nestedreadindexbytes(state.present,
                _nestedreadplusone(count))))
    else
        final = _materializedsum(final,
            _nestedreadindexbytes(state.entries,
                _nestedreadplusone(count)))
        required = state.force_required || plan.parent_definition == plan.present_definition
        required || (final = _materializedsum(final,
            _materializedbitbytes(count)))
    end
    for child in state.children
        childfinal, childtemporary = _nestedreadcharges(child, streams)
        final = _materializedsum(final, childfinal)
        temporary = _materializedsum(temporary, childtemporary)
    end
    return final, temporary
end

function _nestedreadindexarray(terminal::Int64, count::Int64)
    length = _nestedreadint(count, "nested index length")
    T = _nestedindextype(terminal)
    output = Vector{T}(undef, length)
    output[1] = zero(T)
    return output
end

function _nestedreadallocate!(state::_NestedReadState)
    plan = state.plan
    count = _nestedreadint(state.occurrences,
        "$(_nestedreadlabel(plan)) occurrence count")
    if plan isa _NestedLeafPlan
        T = _nestedreadoutputtype(state, plan)
        state.buffer = Vector{T}(undef, count)
        return
    elseif plan isa _NestedStructPlan
        required = state.force_required || plan.parent_definition == plan.present_definition
        state.buffer = required ? nothing :
            _nestedreadindexarray(state.present,
                _nestedreadplusone(state.occurrences))
        state.names = Vector{String}(undef, length(plan.children))
        state.child_outputs = Vector{AbstractVector}(undef, length(plan.children))
        for index in eachindex(plan.children)
            state.names[index] = plan.children[index].source.element.name
        end
    else
        state.buffer = _nestedreadindexarray(state.entries,
            _nestedreadplusone(state.occurrences))
        required = state.force_required || plan.parent_definition == plan.present_definition
        state.validity = required ? nothing : falses(count)
    end
    for child in state.children
        _nestedreadallocate!(child)
    end
    return
end

function _nestedreadlogicalvalues!(output::Vector{AbstractVector},
    plan::_NestedSchemaPlan, streams::Vector{LeafStream}, limits::Limits)
    for index in eachindex(streams)
        output[index] = _logicalvalues(plan.leaves[index].source,
            streams[index].values; limits=limits)
    end
    return output
end

function _nestedreadreset!(state::_NestedReadState)
    state.fill_occurrences = 0
    state.fill_present = 0
    state.fill_entries = 0
    for child in state.children
        _nestedreadreset!(child)
    end
    return
end

function _nestedreadverifyfills(state::_NestedReadState)
    state.fill_occurrences == state.occurrences || throw(FormatError(
        "$(_nestedreadlabel(state.plan)) occurrence count changed between passes"))
    state.fill_present == state.present || throw(FormatError(
        "$(_nestedreadlabel(state.plan)) presence count changed between passes"))
    state.fill_entries == state.entries || throw(FormatError(
        "$(_nestedreadlabel(state.plan)) entry count changed between passes"))
    for child in state.children
        _nestedreadverifyfills(child)
    end
    return
end

function _nestedreadfinalize!(state::_NestedReadState)
    for child in state.children
        _nestedreadfinalize!(child)
    end
    plan = state.plan
    if plan isa _NestedLeafPlan
        if plan.source.element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
                _logicalkind(plan.source) === nothing
            state.result = FixedByteArrayVector{eltype(state.buffer)}(
                state.buffer, plan.source.element.type_length)
        else
            state.result = state.buffer
        end
        return
    elseif plan isa _NestedStructPlan
        children = something(state.child_outputs)
        for index in eachindex(state.children)
            children[index] = state.children[index].result
        end
        names = something(state.names)
        ranks = state.buffer
        T = ranks === nothing ? StructValue : Union{Missing,StructValue}
        state.result = StructVector{T,typeof(ranks)}(
            names, ranks, children, Int(state.occurrences))
        return
    elseif plan isa _NestedListPlan
        values = state.children[1].result
        E = eltype(values)
        V = ListValue{E}
        T = state.validity === nothing ? V : Union{Missing,V}
        state.result = ListVector{T,E,eltype(state.buffer),typeof(state.validity)}(
            state.buffer, state.validity, values)
        return
    end
    keys = state.children[1].result
    Missing <: eltype(keys) && throw(FormatError(
        "$(_nestedreadlabel(plan)) exposes a nullable map key"))
    values = length(state.children) == 1 ? nothing : state.children[2].result
    K = eltype(keys)
    hasvalues = values !== nothing
    V = hasvalues ? eltype(values) : Missing
    R = MapValue{K,V,hasvalues}
    T = state.validity === nothing ? R : Union{Missing,R}
    state.result = MapVector{T,K,V,hasvalues,eltype(state.buffer),
        typeof(state.validity)}(state.buffer, state.validity, keys, values)
    return
end

function _assemblenested(plan::_NestedSchemaPlan, streams::AbstractVector,
    rows::Integer; limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    rowcount = _nestedreadint(rows, "nested row count")
    _checklimit(:container_elements, rowcount, limits.max_container_elements)
    _nestedreadvalidatestreams(plan, streams, rowcount, limits)
    passbytes = _nestedreadpassbytes(plan)
    _reserve!(budget, passbytes)
    finalreserved = Int64(0)
    try
        normalized = _nestedreadstreams(streams)
        state = _nestedreadstate(plan.root)
        positions = ones(Int, length(normalized))
        densepositions = zeros(Int, length(normalized))
        logical = Vector{AbstractVector}(undef, length(normalized))
        context = _NestedReadContext(normalized, positions, densepositions,
            logical, limits, false)
        _nestedreadpass!(context, state, plan, rowcount)
        _nestedreadverifycounts(state)
        finalbytes, temporarybytes = _nestedreadcharges(state, normalized)
        totalbytes = _materializedsum(finalbytes, temporarybytes)
        _reserve!(budget, totalbytes)
        finalreserved = totalbytes
        _nestedreadlogicalvalues!(logical, plan, normalized, limits)
        _nestedreadallocate!(state)
        _nestedreadreset!(state)
        fill!(positions, 1)
        fill!(densepositions, 0)
        context.fill = true
        _nestedreadpass!(context, state, plan, rowcount)
        _nestedreadverifyfills(state)
        _nestedreadfinalize!(state)
        _release!(budget, _materializedsum(passbytes, temporarybytes))
        return state.result
    catch
        iszero(finalreserved) || _release!(budget, finalreserved)
        _release!(budget, passbytes)
        rethrow()
    end
end

function _assemblenested(plan::_NestedSchemaPlan, streams::AbstractVector,
    rows::Integer, limits::Limits, budget::_LiveByteBudget)
    return _assemblenested(plan, streams, rows; limits=limits, budget=budget)
end
