# Column-at-a-time Dremel assembly from aligned physical leaf streams.
#
# A plan node's slots are computed from the levels of each leaf beneath it: entry `i`
# opens a slot when its repetition level is at most the level of the node's nearest
# repeated ancestor and its definition level shows the node's parent is present. Every
# leaf under a node must yield the same slots, presence, and list entry counts.

# Assembly recurses once per plan-tree level, so a plan deep enough to be built under a
# raised max_metadata_depth could overflow the stack. Such plans are rejected with a
# LimitError before any recursion.
const _NESTED_READ_MAX_DEPTH = 1024

function _nestedreadlabel(plan::_NestedPlan)
    path = getfield(plan, :source).path
    isempty(path) && return "schema root"
    return "nested field $(repr(join(path, ".")))"
end

function _nestedreadint(value::Integer, label::AbstractString)
    value >= 0 || throw(FormatError("negative $label"))
    value <= typemax(Int) || throw(FormatError("$label exceeds the Julia index range"))
    return Int(value)
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
    _nestedreadcheckvalues(stream.values, leaf)
    return
end

# Streams decoded by this package hold the leaf's logical values (or its physical values
# when it has no annotation), and PLAIN decoding fixes FIXED_LEN_BYTE_ARRAY widths;
# other streams are checked value by value.
function _nestedreadcheckvalues(values::Vector{T}, leaf::_NestedLeafPlan) where {T}
    expected = _physicaleltype(leaf.source.element.type_)
    logical = _nestedreadlogicaltype(leaf)
    T === logical && logical !== expected && return
    T === DataStrings.StringPayload && _logicalkind(leaf.source) === :string && return
    T === expected && leaf.source.element.type_ != Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
        return
    for value in values
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

function _nestedreadlogicaltype(plan::_NestedLeafPlan)
    physical = _physicaleltype(plan.source.element.type_)
    return _logicaleltype(plan.source, physical)
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

# One node's slots as seen through one leaf stream. `counts` holds each slot's entry
# count for lists and maps and is empty otherwise.
struct _NestedShape
    present::BitVector
    counts::Vector{Int}
end

struct _NestedShapeTree
    shape::_NestedShape
    children::Vector{_NestedShapeTree}
end

function _nestedlisting(plan::_NestedPlan)
    return plan isa Union{_NestedListPlan,_NestedMapPlan}
end

function _nestedshapebytes(slots::Integer, listing::Bool)
    bytes = _materializedsum(_materializedbitbytes(slots),
        _MATERIALIZED_OBJECT_BYTES)
    listing || return bytes
    return _materializedsum(bytes, _materializedarraybytes(Int, slots))
end

# Plan levels are schema levels, so they fit the UInt16 stream levels.
function _nestedshape(stream::LeafStream, plan::_NestedPlan,
        repetition::UInt64, parent::UInt64, budget::_LiveByteBudget)
    reps = stream.repetition
    defs = stream.definition
    length(reps) == length(defs) || throw(FormatError(
        "leaf stream repetition and definition counts differ"))
    repetition = UInt16(repetition)
    parent = UInt16(parent)
    present = UInt16(getfield(plan, :present_definition))
    listing = _nestedlisting(plan)
    level = listing ? UInt16(plan.repetition_level) : UInt16(0)
    entry = listing ? UInt16(plan.entry_definition) : UInt16(0)
    slots = 0
    @inbounds for i in eachindex(reps)
        slots += (reps[i] <= repetition) & (defs[i] >= parent)
    end
    charge = _nestedshapebytes(slots, listing)
    _reserve!(budget, charge)
    presence = falses(slots)
    counts = listing ? zeros(Int, slots) : Int[]
    slot = 0
    open = false
    # `slot` is at least 1 wherever `open` is true, which guards every slot access.
    @inbounds for i in eachindex(reps)
        r = reps[i]
        d = defs[i]
        if r <= repetition
            open = d >= parent
            open || continue
            slot += 1
            d >= present || continue
            presence[slot] = true
            listing && d >= entry && (counts[slot] = 1)
        else
            (open && presence[slot]) || throw(FormatError(
                "$(_nestedreadlabel(plan)) repeats inside a null or absent value"))
            if listing
                (d >= entry && counts[slot] > 0) || throw(FormatError(
                    "$(_nestedreadlabel(plan)) repeats inside an empty value"))
                r == level && (counts[slot] += 1)
            else
                d >= present || throw(FormatError(
                    "$(_nestedreadlabel(plan)) has a definition below its present ancestor"))
            end
        end
    end
    return _NestedShape(presence, counts), charge
end

# Slots of `plan`, verified against every leaf beneath it and against the `expected`
# slot count implied by its parent. Returns the shape tree and its budget charge.
function _nestedshapetree(plan::_NestedPlan, streams::Vector{LeafStream},
        repetition::UInt64, parent::UInt64, expected::Int, keyed::Bool,
        budget::_LiveByteBudget)
    label = plan
    range = _nestedleafrange(plan)
    charge = Int64(0)
    if isempty(range)
        plan isa _NestedStructPlan || throw(FormatError(
            "$(_nestedreadlabel(label)) has no leaf columns"))
        charge = _nestedshapebytes(expected, false)
        _reserve!(budget, charge)
        shape = _NestedShape(trues(expected), Int[])
    else
        shape, charge = _nestedshape(streams[first(range)], plan, repetition,
            parent, budget)
        for index in (first(range) + Int32(1)):last(range)
            other, othercharge = _nestedshape(streams[index], plan, repetition,
                parent, budget)
            same = other.present == shape.present && other.counts == shape.counts
            _release!(budget, othercharge)
            same || throw(FormatError(
                "$(_nestedreadlabel(label)) has misaligned sibling leaves"))
        end
    end
    length(shape.present) == expected || throw(FormatError(
        "$(_nestedreadlabel(label)) has $(length(shape.present)) occurrences " *
        "where its parent expects $expected"))
    keyed && !all(shape.present) && throw(FormatError(
        "$(_nestedreadlabel(label)) is a null map key"))
    children = _NestedShapeTree[]
    if plan isa _NestedStructPlan
        inner = UInt64(plan.present_definition)
        present = count(shape.present)
        for child in plan.children
            tree, childcharge = _nestedshapetree(child, streams, repetition,
                inner, present, false, budget)
            charge = _materializedsum(charge, childcharge)
            push!(children, tree)
        end
    elseif _nestedlisting(plan)
        level = UInt64(plan.repetition_level)
        entry = UInt64(plan.entry_definition)
        entries = sum(shape.counts; init=0)
        members = plan isa _NestedListPlan ? (plan.element,) :
            plan.value === nothing ? (plan.key,) : (plan.key, plan.value)
        for (index, child) in enumerate(members)
            iskey = plan isa _NestedMapPlan && index == 1
            tree, childcharge = _nestedshapetree(child, streams, level, entry,
                entries, iskey, budget)
            charge = _materializedsum(charge, childcharge)
            push!(children, tree)
        end
    end
    return _NestedShapeTree(shape, children), charge
end

function _nestedrequired(plan::_NestedPlan, keyed::Bool)
    return keyed || getfield(plan, :parent_definition) == getfield(plan, :present_definition)
end

function _nestedoffsets(counts::Vector{Int}, budget::_LiveByteBudget)
    total = sum(counts; init=0)
    I = _nestedindextype(total)
    _reservearray!(budget, I, length(counts) + 1)
    offsets = Vector{I}(undef, length(counts) + 1)
    offsets[1] = zero(I)
    running = 0
    for (index, count) in enumerate(counts)
        running += count
        offsets[index + 1] = I(running)
    end
    return offsets
end

function _nestedvalidity(present::BitVector, required::Bool,
        budget::_LiveByteBudget)
    required && return nothing
    _reserve!(budget, _materializedbitbytes(length(present)))
    return copy(present)
end

# A STRING leaf read from a table: its present values as a StringVector, spread over
# the leaf's occurrences with null payloads where it is absent.
function _nestedstrings(values::DataStrings.StringVector{DataStrings.DataString},
        presence::BitVector, required::Bool)
    required && return values
    payloads = Vector{DataStrings.StringPayload}(undef, length(presence))
    next = 0
    for index in eachindex(payloads, presence)
        if presence[index]
            next += 1
            payloads[index] = values.payloads[next]
        else
            payloads[index] = DataStrings.PAYLOAD_MISSING
        end
    end
    return DataStrings.StringVector{Union{Missing,DataStrings.DataString}}(payloads,
        values.buffers, Val(:trusted))
end

function _nestedbuild(plan::_NestedLeafPlan, tree::_NestedShapeTree,
        logical::Vector{AbstractVector}, keyed::Bool, budget::_LiveByteBudget)
    presence = tree.shape.present
    values = logical[Int(first(plan.leaf_range))]
    count(presence) == length(values) || throw(FormatError(
        "$(_nestedreadlabel(plan)) has $(length(values)) dense values for " *
        "$(count(presence)) present occurrences"))
    values isa DataStrings.StringVector &&
        return _nestedstrings(values, presence, _nestedrequired(plan, keyed))
    T = _nestedreadlogicaltype(plan)
    output = if _nestedrequired(plan, keyed)
        _reservearray!(budget, T, length(values))
        values isa Vector{T} ? values : convert(Vector{T}, values)
    else
        _reservearray!(budget, Union{Missing,T}, length(presence))
        _nestedfill(Union{Missing,T}, presence, values)
    end
    if plan.source.element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
            _logicalkind(plan.source) === nothing
        _reserveobjects!(budget)
        return FixedByteArrayVector{eltype(output)}(output,
            plan.source.element.type_length)
    end
    return output
end

function _nestedfill(::Type{T}, presence::BitVector, values::AbstractVector) where {T}
    output = Vector{T}(undef, length(presence))
    next = 0
    @inbounds for index in eachindex(output)
        if presence[index]
            next += 1
            output[index] = values[next]
        else
            output[index] = missing
        end
    end
    return output
end

function _nestedbuild(plan::_NestedStructPlan, tree::_NestedShapeTree,
        logical::Vector{AbstractVector}, keyed::Bool, budget::_LiveByteBudget)
    presence = tree.shape.present
    count = length(presence)
    _reserveobjects!(budget)
    _reservearray!(budget, String, length(plan.children))
    _reservearray!(budget, AbstractVector, length(plan.children))
    names = String[child.source.element.name for child in plan.children]
    children = Vector{AbstractVector}(undef, length(plan.children))
    for (index, child) in enumerate(plan.children)
        children[index] = _nestedbuild(child, tree.children[index], logical,
            false, budget)
    end
    if _nestedrequired(plan, keyed)
        return StructVector{StructValue,Nothing}(names, nothing, children, count)
    end
    I = _nestedindextype(Base.count(presence))
    _reservearray!(budget, I, count + 1)
    ranks = Vector{I}(undef, count + 1)
    ranks[1] = zero(I)
    for index in 1:count
        ranks[index + 1] = ranks[index] + I(presence[index])
    end
    return StructVector{Union{Missing,StructValue},Vector{I}}(names, ranks,
        children, count)
end

function _nestedbuild(plan::_NestedListPlan, tree::_NestedShapeTree,
        logical::Vector{AbstractVector}, keyed::Bool, budget::_LiveByteBudget)
    _reserveobjects!(budget)
    offsets = _nestedoffsets(tree.shape.counts, budget)
    validity = _nestedvalidity(tree.shape.present, _nestedrequired(plan, keyed),
        budget)
    values = _nestedbuild(plan.element, tree.children[1], logical, false, budget)
    E = eltype(values)
    T = validity === nothing ? ListValue{E} : Union{Missing,ListValue{E}}
    return ListVector{T,E,eltype(offsets),typeof(validity)}(offsets, validity,
        values)
end

function _nestedbuild(plan::_NestedMapPlan, tree::_NestedShapeTree,
        logical::Vector{AbstractVector}, keyed::Bool, budget::_LiveByteBudget)
    _reserveobjects!(budget)
    offsets = _nestedoffsets(tree.shape.counts, budget)
    validity = _nestedvalidity(tree.shape.present, _nestedrequired(plan, keyed),
        budget)
    keys = _nestedbuild(plan.key, tree.children[1], logical, true, budget)
    Missing <: eltype(keys) && throw(FormatError(
        "$(_nestedreadlabel(plan)) exposes a nullable map key"))
    values = plan.value === nothing ? nothing :
        _nestedbuild(plan.value, tree.children[2], logical, false, budget)
    K = eltype(keys)
    hasvalues = values !== nothing
    V = hasvalues ? eltype(values) : Missing
    R = MapValue{K,V,hasvalues}
    T = validity === nothing ? R : Union{Missing,R}
    return MapVector{T,K,V,hasvalues,eltype(offsets),typeof(validity)}(offsets,
        validity, keys, values)
end

# Logical conversions of each leaf's dense values. The converted arrays are temporary
# (leaf outputs charge their own arrays); the converted payload is retained.
function _nestedreadlogicalvalues(plan::_NestedSchemaPlan,
        streams::Vector{LeafStream}, limits::Limits, budget::_LiveByteBudget,
        strings)
    output = Vector{AbstractVector}(undef, length(streams))
    temporary = Int64(0)
    for (index, stream) in enumerate(streams)
        leaf = plan.leaves[index]
        if strings !== nothing && strings[index] !== nothing
            output[index] = DataStrings.StringVector{DataStrings.DataString}(
                stream.values, strings[index], Val(:trusted))
            continue
        end
        logical = _nestedreadlogicaltype(leaf)
        # Streams read from a table already hold logical values.
        if _logicalkind(leaf.source) === nothing || eltype(stream.values) === logical
            output[index] = stream.values
            continue
        end
        charge = _reservearray!(budget, logical, length(stream.values))
        temporary = _materializedsum(temporary, charge)
        _reserve!(budget, _nestedreadpayloadbytes(leaf, stream))
        output[index] = _logicalvalues(leaf.source, stream.values; limits=limits)
    end
    return output, temporary
end

# `strings` holds, per leaf, the page buffers of a STRING leaf whose stream values are
# DataStrings payloads (as `_readnestedstream` decodes them), or `nothing`.
function _assemblenested(plan::_NestedSchemaPlan, streams::AbstractVector,
    rows::Integer; limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits), strings=nothing)
    plan.depth <= _NESTED_READ_MAX_DEPTH || throw(LimitError(:nested_read_depth,
        Int64(plan.depth), Int64(_NESTED_READ_MAX_DEPTH)))
    rowcount = _nestedreadint(rows, "nested row count")
    _checklimit(:container_elements, rowcount, limits.max_container_elements)
    _nestedreadvalidatestreams(plan, streams, rowcount, limits)
    normalized = _nestedreadstreams(streams)
    start = _budgetused(budget)
    try
        tree, temporary = _nestedshapetree(plan.root, normalized, UInt64(0),
            UInt64(0), rowcount, false, budget)
        logical, logicalcharge = _nestedreadlogicalvalues(plan, normalized,
            limits, budget, strings)
        root = _nestedbuild(plan.root, tree, logical, false, budget)
        _release!(budget, _materializedsum(temporary, logicalcharge))
        return root
    catch
        used = _budgetused(budget) - start
        used > 0 && _release!(budget, used)
        rethrow()
    end
end

function _assemblenested(plan::_NestedSchemaPlan, streams::AbstractVector,
    rows::Integer, limits::Limits, budget::_LiveByteBudget)
    return _assemblenested(plan, streams, rows; limits=limits, budget=budget)
end
