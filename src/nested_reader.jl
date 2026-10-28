# Column-at-a-time Dremel assembly of nested fields.
#
# Slots: the rows of a field are space 0, and the entries of the j-th list or map on a
# leaf's path are space j. A node lives in the space of its nearest list or map ancestor,
# so a struct's fields share the struct's slots. A leaf entry with repetition level r and
# definition level d opens a slot in spaces r, r + 1, ... for as long as d reaches each
# space's entry level; at each slot it opens, the nodes of that space are present when d
# reaches their own level.
#
# Each leaf rebuilds its whole path from its own levels. The first leaf under a node keeps
# the node's validity bits and offsets; leaves that share a node compare slot counts and
# hashes of theirs, so leaves that disagree about a shared ancestor are rejected.

# Assembly recurses once per plan-tree level, so a plan deep enough to be built under a
# raised max_metadata_depth could overflow the stack. Such plans are rejected with a
# LimitError before any recursion.
const _NESTED_READ_MAX_DEPTH = 1024

function _nestedreadlabel(plan::_NestedPlan)
    path = getfield(plan, :source).path
    isempty(path) && return "schema root"
    return "nested field $(repr(join(path, ".")))"
end

function _nestedrequired(plan::_NestedPlan, keyed::Bool)
    return keyed || getfield(plan, :parent_definition) == getfield(plan, :present_definition)
end

const _ITEM_VALIDITY = 0x01   # a nullable node: one presence bit per slot
const _ITEM_OFFSETS = 0x02    # a list or map: where each slot's entries start
const _ITEM_KEY = 0x03        # a map key declared optional: it must be present

# What one leaf builds and checks. The items of space j are
# `first[j + 1]:(first[j + 2] - 1)`; a slot of space j >= 1 exists where the definition
# level reaches `entry[j]`, and `repeat[r + 1]` is the level an entry repeating at r
# needs (`entry[r]`, 0 for r = 0).
struct _LeafPath
    leaf::Int
    depth::Int
    entry::Vector{UInt16}
    repeat::Vector{UInt16}
    first::Vector{Int}
    kind::Vector{UInt8}
    space::Vector{Int}
    level::Vector{UInt16}
    node::Vector{Int}
    owned::Vector{Bool}
    shared::Vector{Bool}
    maxdefinition::UInt16
    nullable::Bool
    keyed::Bool
end

# A node of a field's assembly tree; `eltype` is its row type (with `Missing` when it is
# nullable), `depth` the levels of nested values in it, and `position` a leaf's index in
# the field's paths.
struct _AssemblyNode
    plan::_NestedPlan
    id::Int
    eltype::Type
    children::Vector{_AssemblyNode}
    depth::Int
    position::Int
end

# One field's assembly: its node tree, the paths of its leaves (in leaf order), and the
# (path, item) pairs that see each node's validity (`items[2id - 1]`) and offsets
# (`items[2id]`), the storing leaf first.
struct _FieldAssembly
    root::_AssemblyNode
    paths::Vector{_LeafPath}
    items::Vector{Vector{Tuple{Int,Int}}}
end

mutable struct _AssemblyBuilder
    limits::Limits
    options::_ReadOptions
    nodes::Int
    paths::Vector{_LeafPath}
end

# The element type a leaf reads as.
function _leafeltype(node::SchemaNode, limits::Limits, options::_ReadOptions)
    _, L = _valueconverter(node, limits, options)
    L === DataStrings.StringPayload || return L
    return _logicalkind(node) in (:string, :enum) ? options.stringtype :
        DataStrings.DataBytes
end

# The path of a leaf whose ancestors' items are `stack`, as (space, kind, level, node,
# first leaf, whether the node has several leaves) tuples in path order.
function _leafpath(plan::_NestedLeafPlan, entries::Vector{UInt16},
        stack::Vector{Tuple{Int,UInt8,Int,Int,Int,Bool}}, nullable::Bool, keyed::Bool)
    leaf = Int(first(plan.leaf_range))
    depth = length(entries)
    starts = Vector{Int}(undef, depth + 2)
    item = 1
    for space in 0:(depth + 1)
        while item <= length(stack) && stack[item][1] < space
            item += 1
        end
        starts[space + 1] = item
    end
    return _LeafPath(leaf, depth, copy(entries), UInt16[0x0000; entries], starts,
        UInt8[entry[2] for entry in stack], Int[entry[1] for entry in stack],
        UInt16[entry[3] for entry in stack], Int[entry[4] for entry in stack],
        Bool[entry[5] == leaf for entry in stack], Bool[entry[6] for entry in stack],
        UInt16(plan.source.max_definition_level), nullable, keyed)
end

function _assemblynode!(builder::_AssemblyBuilder, plan::_NestedPlan, keyed::Bool,
        entries::Vector{UInt16}, stack::Vector{Tuple{Int,UInt8,Int,Int,Int,Bool}})
    id = (builder.nodes += 1)
    required = _nestedrequired(plan, keyed)
    space = length(entries)
    range = _nestedleafrange(plan)
    firstleaf = isempty(range) ? 0 : Int(first(range))
    shared = length(range) > 1
    present = Int(getfield(plan, :present_definition))
    declared = getfield(plan, :parent_definition) != present
    if plan isa _NestedLeafPlan
        E = _leafeltype(plan.source, builder.limits, builder.options)
        push!(builder.paths, _leafpath(plan, entries, stack, !required, keyed && declared))
        return _AssemblyNode(plan, id, required ? E : Union{Missing,E}, _AssemblyNode[], 0,
            length(builder.paths))
    end
    pushed = 0
    if !required
        push!(stack, (space, _ITEM_VALIDITY, present, id, firstleaf, shared))
        pushed += 1
    elseif keyed && declared
        push!(stack, (space, _ITEM_KEY, present, id, firstleaf, shared))
        pushed += 1
    end
    if plan isa _NestedStructPlan
        children = _AssemblyNode[_assemblynode!(builder, child, false, entries, stack)
            for child in plan.children]
        R = _structrowtype(String[child.source.element.name for child in plan.children],
            Type[child.eltype for child in children],
            1 + maximum(child -> child.depth, children; init=0))
    else
        push!(stack, (space, _ITEM_OFFSETS, 0, id, firstleaf, shared))
        pushed += 1
        push!(entries, UInt16(getfield(plan, :entry_definition)))
        if plan isa _NestedListPlan
            children = _AssemblyNode[_assemblynode!(builder, plan.element, false, entries,
                stack)]
            R = ListValue{children[1].eltype}
        else
            mapplan = plan::_NestedMapPlan
            children = _AssemblyNode[_assemblynode!(builder, mapplan.key, true, entries,
                stack)]
            mapplan.value === nothing || push!(children, _assemblynode!(builder,
                something(mapplan.value), false, entries, stack))
            R = MapValue{children[1].eltype,
                length(children) == 2 ? children[2].eltype : Missing}
        end
        pop!(entries)
    end
    foreach(_ -> pop!(stack), 1:pushed)
    return _AssemblyNode(plan, id, required ? R : Union{Missing,R}, children,
        1 + maximum(child -> child.depth, children; init=0), 0)
end

function _fieldassembly(plan::_NestedPlan, limits::Limits, options::_ReadOptions)
    builder = _AssemblyBuilder(limits, options, 0, _LeafPath[])
    root = _assemblynode!(builder, plan, false, UInt16[],
        Tuple{Int,UInt8,Int,Int,Int,Bool}[])
    items = [Tuple{Int,Int}[] for _ in 1:(2 * builder.nodes)]
    for (position, path) in enumerate(builder.paths), item in eachindex(path.node)
        kind = path.kind[item]
        kind == _ITEM_KEY && continue
        push!(items[2 * path.node[item] - (kind == _ITEM_VALIDITY)], (position, item))
    end
    return _FieldAssembly(root, builder.paths, items)
end

# What a leaf's levels build: the slots opened in each space, and for each path item its
# validity bits or offsets (task scratch for items the leaf does not own) and, once
# finished, a hash of them. It does not depend on the leaf's value type, so its code
# compiles once.
mutable struct _PathState
    path::_LeafPath
    chunkstart::Bool
    previous::UInt16
    slots::Vector{Int}
    bounds::Vector{Int}
    hashes::Vector{UInt64}
    bits::Vector{BitVector}
    offsets::Vector{Vector{Int}}
end

# One leaf's decode state: its path state and its values per slot of its space (`null`
# where absent).
struct _LeafSink{O,L}
    state::_PathState
    output::Vector{O}
    dense::Vector{L}
    null::O
end

# The path state of a chunk of `rows` rows and at most `entries` entries (so at most that
# many slots in any space below the rows), with room reserved for the arrays it keeps and
# scratch from `buffers` for the others.
function _PathState(path::_LeafPath, rows::Int, entries::Int, buffers::_PageBuffers)
    items = length(path.kind)
    bits = Vector{BitVector}(undef, items)
    offsets = Vector{Vector{Int}}(undef, items)
    while length(buffers.bits) < items
        push!(buffers.bits, BitVector())
        push!(buffers.offsets, Int[])
    end
    for item in 1:items
        owned = path.owned[item]
        bits[item] = owned ? BitVector() : empty!(buffers.bits[item])
        offsets[item] = owned ? Int[] : empty!(buffers.offsets[item])
        owned || continue
        capacity = (path.space[item] == 0 ? rows : entries) + 1
        path.kind[item] == _ITEM_VALIDITY && sizehint!(bits[item], capacity)
        path.kind[item] == _ITEM_OFFSETS && sizehint!(offsets[item], capacity)
    end
    return _PathState(path, true, 0x0000, zeros(Int, path.depth + 1),
        zeros(Int, path.depth + 1), zeros(UInt64, items), bits, offsets)
end

@inline _mixhash(hash::UInt64, value::UInt64) = (hash ⊻ value) * 0x00000100000001b3

function _hashwords(words::Vector{<:Union{UInt64,Int}})
    hash = 0xcbf29ce484222325
    @inbounds for word in words
        hash = _mixhash(hash, word % UInt64)
    end
    return hash
end

@noinline function _nestedrepeaterror(path::_LeafPath, repetition::Integer)
    throw(FormatError("leaf $(path.leaf) repeats at level $repetition inside a null, " *
        "empty, or absent value"))
end

@noinline _nestedfirsterror(path::_LeafPath, repetition::Integer) = throw(FormatError(
    "leaf $(path.leaf) starts with repetition level $repetition"))

@noinline _nestedkeyerror(path::_LeafPath) = throw(FormatError(
    "leaf $(path.leaf) has a null map key"))

# Applies `count` entries (levels in `repetitions` and `definitions`, the `present`
# values in `source`) to the sink's slots: one pass over the levels per path node and
# one for the leaf's values.
function _assembleentries!(sink::_LeafSink, repetitions::Vector{UInt16},
        definitions::Vector{UInt16}, count::Int, present::Int, source)
    state = sink.state
    depth = state.path.depth
    ends = _assemblenodes!(state, repetitions, definitions, count)
    _growto!(sink.output, state.slots[depth + 1] + count)
    ends[depth + 1], next = _leafpass!(sink, definitions, count, source)
    next == present || throw(FormatError(
        "leaf $(state.path.leaf) has $present values for $next present entries"))
    copyto!(state.slots, ends)
    return
end

# The node passes of `count` entries; returns each space's slot count after them.
function _assemblenodes!(state::_PathState, repetitions::Vector{UInt16},
        definitions::Vector{UInt16}, count::Int)
    path = state.path
    slots = state.slots
    if path.depth > 0
        _checkrepetitions(path, repetitions, definitions, count, state.chunkstart,
            state.previous)
        if count > 0
            state.chunkstart = false
            state.previous = definitions[count]
        end
    end
    ends = copy(slots)
    bounds = _openbounds!(state.bounds, repetitions, count)
    item = 1
    while item <= length(path.kind)
        kind = path.kind[item]
        space = path.space[item]
        if kind == _ITEM_KEY
            _keypass(path, item, repetitions, definitions, count)
            item += 1
            continue
        end
        # A nullable list or map's validity and offsets take one pass.
        validity = kind == _ITEM_VALIDITY ? item : 0
        offsets = kind == _ITEM_OFFSETS ? item : 0
        if validity > 0 && item < length(path.kind) &&
                path.kind[item + 1] == _ITEM_OFFSETS && path.node[item + 1] == path.node[item]
            offsets = item + 1
        end
        validity > 0 && resize!(state.bits[validity], slots[space + 1] + bounds[space + 1])
        offsets > 0 &&
            _growto!(state.offsets[offsets], slots[space + 1] + bounds[space + 1] + 1)
        ends[space + 1], inner = _nodepass!(state, validity, offsets, repetitions,
            definitions, count)
        offsets > 0 && (ends[space + 2] = inner)
        item = max(validity, offsets) + 1
    end
    return ends
end

# How many of `count` entries repeat at most at each space: at most that many slots of
# the space open in them, so its arrays grow by that much (not by every entry).
function _openbounds!(bounds::Vector{Int}, repetitions::Vector{UInt16}, count::Int)
    fill!(bounds, 0)
    depth = length(bounds) - 1
    @inbounds for index in 1:count
        bounds[min(Int(repetitions[index]), depth) + 1] += 1
    end
    for space in 2:length(bounds)
        bounds[space] += bounds[space - 1]
    end
    return bounds
end

# A slot of `space` opens at an entry that repeats at most at `space` and is defined to
# at least `level`, the definition level of the space's slots (0 for rows).
@inline _opens(repetition::UInt16, definition::UInt16, space::Int, level::UInt16) =
    (repetition <= space) & (definition >= level)

_entrylevel(path::_LeafPath, space::Int) = space == 0 ? 0x0000 : path.entry[space]

# A chunk starts a row, and an entry that repeats list r continues an instance that has
# entries: it and the entry before it (in the same instance, or the one that opened it)
# are defined at least to the list's entry level. `previous` is the definition level of
# the entry before these; `chunkstart` says they begin a chunk.
function _checkrepetitions(path::_LeafPath, repetitions::Vector{UInt16},
        definitions::Vector{UInt16}, count::Int, chunkstart::Bool, previous::UInt16)
    count > 0 && chunkstart && !iszero(repetitions[1]) &&
        _nestedfirsterror(path, repetitions[1])
    levels = path.repeat
    bad = false
    @inbounds for index in 1:count
        definition = definitions[index]
        bad |= min(definition, previous) < levels[repetitions[index] + 1]
        previous = definition
    end
    bad || return
    for index in 2:count
        repetition = repetitions[index]
        min(definitions[index], definitions[index - 1]) < levels[repetition + 1] &&
            _nestedrepeaterror(path, repetition)
    end
    _nestedrepeaterror(path, repetitions[1])
end

# One node's slots in a page: its validity bits (item `validity`, or 0) and the offsets of
# its entries (item `offsets`, or 0). Returns the slot counts of the node's space and, for
# offsets, of the space of its entries.
function _nodepass!(state::_PathState, validity::Int, offsets::Int,
        repetitions::Vector{UInt16}, definitions::Vector{UInt16}, count::Int)
    path = state.path
    space = path.space[validity > 0 ? validity : offsets]
    slot = state.slots[space + 1]
    # Each kind of pass calls its own compiled loop: a `Val(validity > 0)` argument made
    # the compiler split the call into a slower generic loop (about 5x per entry).
    if offsets == 0
        return _nodestore!(state.bits[validity].chunks, Int[], Val(true), Val(false),
            repetitions, definitions, count, space,
            (_entrylevel(path, space), path.level[validity], 0x0000), slot, 0)
    end
    levels = (_entrylevel(path, space), validity > 0 ? path.level[validity] : 0x0000,
        path.entry[space + 1])
    if validity == 0
        return _nodestore!(UInt64[], state.offsets[offsets], Val(false), Val(true),
            repetitions, definitions, count, space, levels, slot, state.slots[space + 2])
    end
    return _nodestore!(state.bits[validity].chunks, state.offsets[offsets], Val(true),
        Val(true), repetitions, definitions, count, space, levels, slot,
        state.slots[space + 2])
end

function _nodestore!(chunks::Vector{UInt64}, offsets::Vector{Int}, ::Val{V}, ::Val{O},
        repetitions::Vector{UInt16}, definitions::Vector{UInt16}, count::Int,
        space::Int, (entrylevel, level, innerlevel)::NTuple{3,UInt16}, slot::Int,
        inner::Int) where {V,O}
    # Bits accumulate in `word`, the chunk holding the next slot's bit.
    shift = slot & 63
    chunk = (slot >> 6) + 1
    word = V && shift > 0 ? (@inbounds chunks[chunk]) & ((UInt64(1) << shift) - 1) :
        UInt64(0)
    # Branch-free: every entry stores the next slot's offset, which stays only if the
    # entry opens that slot (the offsets have room for one past the page's slots).
    @inbounds for index in 1:count
        repetition = repetitions[index]
        definition = definitions[index]
        opens = _opens(repetition, definition, space, entrylevel)
        O && (offsets[slot + 1] = inner)
        slot += opens
        if V
            word |= UInt64(opens & (definition >= level)) << shift
            shift += opens
            if shift == 64
                chunks[chunk] = word
                chunk += 1
                shift = 0
                word = UInt64(0)
            end
        end
        O && (inner += _opens(repetition, definition, space + 1, innerlevel))
    end
    V && shift > 0 && (@inbounds chunks[chunk] = word)
    return slot, inner
end

function _keypass(path::_LeafPath, item::Int, repetitions::Vector{UInt16},
        definitions::Vector{UInt16}, count::Int)
    space = path.space[item]
    entrylevel = _entrylevel(path, space)
    level = path.level[item]
    @inbounds for index in 1:count
        definition = definitions[index]
        _opens(repetitions[index], definition, space, entrylevel) &&
            definition < level && _nestedkeyerror(path)
    end
    return
end

# The leaf's values, one per slot of its space (every entry repeats at most at it).
function _leafpass!(sink::_LeafSink, definitions::Vector{UInt16}, count::Int, source)
    path = sink.state.path
    level = _entrylevel(path, path.depth)
    maxdefinition = path.maxdefinition
    output = sink.output
    null = sink.null
    slot = sink.state.slots[path.depth + 1]
    next = 0
    @inbounds for index in 1:count
        definition = definitions[index]
        definition >= level || continue
        slot += 1
        if definition == maxdefinition
            next += 1
            output[slot] = _value(source, next)
        else
            path.keyed && _nestedkeyerror(path)
            output[slot] = null
        end
    end
    return slot, next
end

# A page's present values: decoded (`_DenseValues`), PLAIN fixed-width values in the
# page, or dictionary entries by the page's indices.
struct _DenseValues{V<:AbstractVector}
    values::V
end

@inline _value(source::_DenseValues, next::Int) = @inbounds source.values[next]

struct _PlainValues{P,C}
    pointer::Ptr{P}
    convert::C
end

@inline _value(source::_PlainValues, next::Int) =
    _convertvalue(source.convert, ltoh(unsafe_load(source.pointer, next)))

struct _DictionaryValues{L}
    indices::Vector{UInt32}
    dictionary::Vector{L}
end

@inline function _value(source::_DictionaryValues, next::Int)
    raw = @inbounds source.indices[next]
    entries = UInt32(length(source.dictionary))
    raw < entries || _dictionaryindexerror(raw, entries)
    return @inbounds _dictionarycopy(source.dictionary[raw + 1])
end

# Closes a leaf's chunks at `rows` rows: trims its arrays and hashes the shared ones.
# Returns each path item's slot count.
function _finishleaf!(sink::_LeafSink, rows::Integer)
    counts = _finishpath!(sink.state, rows)
    resize!(sink.output, sink.state.slots[sink.state.path.depth + 1])
    return counts
end

function _finishpath!(state::_PathState, rows::Integer)
    path = state.path
    slots = state.slots
    slots[1] == rows || throw(FormatError(
        "leaf $(path.leaf) has $(slots[1]) rows but $rows were expected"))
    counts = Int[slots[space + 1] for space in path.space]
    for item in eachindex(path.kind)
        slot = counts[item]
        if path.kind[item] == _ITEM_VALIDITY
            bits = resize!(state.bits[item], slot)
            path.shared[item] && (state.hashes[item] = _hashwords(bits.chunks))
        elseif path.kind[item] == _ITEM_OFFSETS
            offsets = _growto!(state.offsets[item], slot + 1)
            offsets[slot + 1] = slots[path.space[item] + 2]
            resize!(offsets, slot + 1)
            path.shared[item] && (state.hashes[item] = _hashwords(offsets))
        end
    end
    return counts
end

# A decoded leaf chunk: its values, and for each path item its slot count and hash, and
# the validity bits or offsets where the leaf stores them.
struct _LeafPiece
    values::AbstractVector
    counts::Vector{Int}
    hashes::Vector{UInt64}
    bits::Vector{BitVector}
    offsets::Vector{Vector{Int}}
end

# The piece keeps only the arrays its leaf owns (the others are task scratch).
function _leafpiece(state::_PathState, values::AbstractVector, counts::Vector{Int})
    @nospecialize values
    owned = state.path.owned
    return _LeafPiece(values, counts, state.hashes,
        BitVector[owned[item] ? state.bits[item] : BitVector() for item in eachindex(owned)],
        Vector{Int}[owned[item] ? state.offsets[item] : Int[] for item in eachindex(owned)])
end

# The sink of a leaf whose values convert to `L`, for a chunk of `rows` rows and at most
# `entries` entries: string payloads are filled with missing or empty payloads, other
# values with `missing` or a placeholder.
function _leafsink(path::_LeafPath, ::Type{L}, dense::Vector{L}, rows::Int,
        entries::Int, buffers::_PageBuffers) where {L}
    state = _PathState(path, rows, entries, buffers)
    if L === DataStrings.StringPayload
        null = path.nullable ? DataStrings.PAYLOAD_MISSING :
            DataStrings.StringPayload(UInt64(0), UInt64(0))
        return _LeafSink(state, sizehint!(DataStrings.StringPayload[], entries), dense,
            null)
    end
    path.nullable && return _LeafSink(state, sizehint!(Union{Missing,L}[], entries),
        dense, missing)
    return _LeafSink(state, sizehint!(L[], entries), dense, _filler(L))
end

# The vector of a leaf's values: string payloads wrap their page buffers.
function _leafvalues(sink::_LeafSink, node::SchemaNode, convert, stringtype::Type)
    convert isa _StringPayloads || return sink.output
    return _stringcolumn(_payloadvector(node, sink.state.path.nullable, sink.output,
        convert.buffers), stringtype)
end

# The rows of field `field` for one row group, from the pieces of its leaves (in path
# order), checking that the leaves agree on every shared node.
function _buildfield(field::_FieldAssembly, pieces::AbstractVector{_LeafPiece}, rows::Int)
    for (index, items) in enumerate(field.items), (position, item) in items
        (owner, owneritem) = first(items)
        reference = pieces[owner]
        piece = pieces[position]
        piece.counts[item] == reference.counts[owneritem] &&
            piece.hashes[item] == reference.hashes[owneritem] || throw(FormatError(
                "$(_nestedreadlabel(_findnode(field.root, cld(index, 2)).plan)) has " *
                "misaligned sibling leaves"))
    end
    return _buildnode(field, field.root, pieces, rows)
end

function _findnode(node::_AssemblyNode, id::Int)
    node.id == id && return node
    for child in node.children
        found = _findnode(child, id)
        found === nothing || return found
    end
    return nothing
end

# The stored validity bits or offsets of node `id`.
function _nodearray(field::_FieldAssembly, pieces, id::Int, kind::UInt8)
    position, item = first(field.items[2 * id - (kind == _ITEM_VALIDITY)])
    field.paths[position].owned[item] || throw(AssertionError(
        "assembly node $id has no stored array"))
    return kind == _ITEM_VALIDITY ? pieces[position].bits[item] :
        pieces[position].offsets[item]
end

function _buildnode(field::_FieldAssembly, node::_AssemblyNode, pieces, count::Int)
    plan = node.plan
    T = node.eltype
    if plan isa _NestedLeafPlan
        values = pieces[node.position].values
        length(values) == count || throw(FormatError("$(_nestedreadlabel(plan)) has " *
            "$(length(values)) values where its parent expects $count"))
        return values
    end
    validity = Missing <: T ? _nodearray(field, pieces, node.id, _ITEM_VALIDITY) :
        BitVector()
    if plan isa _NestedStructPlan
        children = AbstractVector[_buildnode(field, child, pieces, count)
            for child in node.children]
        names = String[child.source.element.name for child in plan.children]
        return StructVector{T}(names, children, validity, count)
    end
    offsets = _nodearray(field, pieces, node.id, _ITEM_OFFSETS)
    length(offsets) == count + 1 || throw(FormatError("$(_nestedreadlabel(plan)) has " *
        "$(length(offsets) - 1) occurrences where its parent expects $count"))
    entries = last(offsets)
    children = AbstractVector[_buildnode(field, child, pieces, entries)
        for child in node.children]
    plan isa _NestedListPlan && return ListVector{T}(offsets, validity, only(children))
    values = length(children) == 2 ? children[2] : Vector{Missing}(undef, entries)
    return MapVector{T}(offsets, validity, children[1], values)
end

# One vector from the pieces of a column, in order.
_concat(pieces::AbstractVector{<:Vector}) = reduce(vcat, pieces)

function _concat(pieces::AbstractVector{C}) where {C<:Union{DataStrings.StringVector,
        DataStrings.BytesVector}}
    payloads = reduce(vcat, [piece.payloads for piece in pieces])
    buffers = Vector{UInt8}[]
    next = 0
    for piece in pieces
        base = length(buffers)
        stop = next + length(piece)
        if base > 0
            for index in (next + 1):stop
                payload = payloads[index]
                DataStrings.payloadlength(payload) > DataStrings.INLINE_MAX || continue
                buffer = UInt64(DataStrings.payloadbufidx(payload) + base)
                payloads[index] = DataStrings.StringPayload(payload.a,
                    (payload.b & 0xffffffff00000000) | buffer)
            end
        end
        append!(buffers, piece.buffers)
        next = stop
    end
    return C(payloads, buffers, Val(:trusted))
end

function _concatoffsets(pieces)
    offsets = Vector{Int}(undef, sum(piece -> length(piece.offsets) - 1, pieces) + 1)
    offsets[1] = 0
    next = 1
    for piece in pieces
        base = offsets[next]
        for index in 2:length(piece.offsets)
            next += 1
            offsets[next] = base + piece.offsets[index]
        end
    end
    return offsets
end

function _concat(pieces::AbstractVector{ListVector{T}}) where {T}
    C = _columntype(eltype(Base.nonmissingtype(T)))
    return ListVector{T}(_concatoffsets(pieces),
        reduce(vcat, [piece.validity for piece in pieces]),
        _concat(C[piece.values for piece in pieces]))
end

function _concat(pieces::AbstractVector{MapVector{T}}) where {T}
    K, V = fieldtypes(eltype(Base.nonmissingtype(T)))
    return MapVector{T}(_concatoffsets(pieces),
        reduce(vcat, [piece.validity for piece in pieces]),
        _concat(_columntype(K)[piece.keys for piece in pieces]),
        _concat(_columntype(V)[piece.values for piece in pieces]))
end

function _concat(pieces::AbstractVector{StructVector{T}}) where {T}
    head = first(pieces)
    children = AbstractVector[_concatchild([piece.children[index] for piece in pieces])
        for index in eachindex(head.children)]
    return StructVector{T}(head.names, children,
        reduce(vcat, [piece.validity for piece in pieces]),
        sum(piece -> piece.length, pieces))
end

# Children of struct pieces are `_columntype`s of their field types.
_concatchild(pieces::AbstractVector) =
    _concat(_columntype(eltype(first(pieces)))[piece for piece in pieces])

# Physical leaf streams (tests and fixtures) assemble through the same machinery: each
# stream is one chunk whose values convert as a read converts them.

function _nestedreadint(value::Integer, label::AbstractString)
    value >= 0 || throw(FormatError("negative $label"))
    value <= typemax(Int) || throw(FormatError("$label exceeds the Julia index range"))
    return Int(value)
end

function _nestedreadphysicalvalue(value, expected::Type, leaf::_NestedLeafPlan)
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
    _checklimit(:container_elements, length(repetitions), limits.max_container_elements)
    maxrepetition = leaf.source.max_repetition_level
    maxdefinition = leaf.source.max_definition_level
    dense = 0
    for index in eachindex(repetitions, definitions)
        repetitions[index] <= maxrepetition || throw(FormatError(
            "repetition level $(repetitions[index]) exceeds the schema maximum $maxrepetition"))
        definitions[index] <= maxdefinition || throw(FormatError(
            "definition level $(definitions[index]) exceeds the schema maximum $maxdefinition"))
        dense += definitions[index] == maxdefinition
    end
    dense == length(stream.values) || throw(FormatError(
        "leaf stream has $(length(stream.values)) dense values for $dense present entries"))
    expected = _physicaleltype(leaf.source.element.type_)
    for value in stream.values
        _nestedreadphysicalvalue(value, expected, leaf)
    end
    return
end

# The leaf piece of a physical stream.
function _streampiece(path::_LeafPath, leaf::_NestedLeafPlan, stream::LeafStream,
        rows::Int, limits::Limits, budget::_ByteBudget)
    convert, L = _valueconverter(leaf.source, limits, _ReadOptions())
    dense = L[_convertvalue(convert, value) for value in stream.values]
    count = length(stream.repetition)
    _chargearray!(budget, L, 2 * count)
    sink = _leafsink(path, L, dense, rows, count, _PageBuffers())
    _assembleentries!(sink, stream.repetition, stream.definition, count, length(dense),
        _DenseValues(dense))
    counts = _finishleaf!(sink, rows)
    return _leafpiece(sink.state, _leafvalues(sink, leaf.source, convert,
        DataStrings.DataString), counts)
end

"""
    _assemblenested(plan, streams, rows; limits, budget) -> StructVector

Assemble the schema root of `plan` from one physical `LeafStream` per leaf, converting
values as a read does.
"""
function _assemblenested(plan::_NestedSchemaPlan, streams::AbstractVector,
        rows::Integer; limits::Limits=Limits(), budget::_ByteBudget=_ByteBudget(limits))
    plan.depth <= _NESTED_READ_MAX_DEPTH || throw(LimitError(:nested_read_depth,
        Int64(plan.depth), Int64(_NESTED_READ_MAX_DEPTH)))
    rowcount = _nestedreadint(rows, "nested row count")
    _checklimit(:container_elements, rowcount, limits.max_container_elements)
    length(streams) == length(plan.leaves) || throw(FormatError(
        "nested reader received $(length(streams)) streams for $(length(plan.leaves)) leaves"))
    for (index, stream) in enumerate(streams)
        stream isa LeafStream || throw(FormatError(
            "nested reader stream $index is not a LeafStream"))
        _nestedreadvalidatestream(stream, plan.leaves[index], rowcount, limits)
    end
    field = _fieldassembly(plan.root, limits, _ReadOptions())
    pieces = _LeafPiece[_streampiece(path, plan.leaves[path.leaf], streams[path.leaf],
        rowcount, limits, budget) for path in field.paths]
    return _buildfield(field, pieces, rowcount)
end

_assemblenested(plan::_NestedSchemaPlan, streams::AbstractVector, rows::Integer,
    limits::Limits, budget::_ByteBudget) =
    _assemblenested(plan, streams, rows; limits=limits, budget=budget)

function _sinkpage!(decode, sink::_LeafSink, count::Int, present::Int,
        hasrepetition::Bool, hasdefinition::Bool, buffers::_PageBuffers)
    position = decode(sink.dense, 0)
    _assemblepage!(sink, count, present, hasrepetition, hasdefinition, buffers,
        _DenseValues(sink.dense))
    return position
end

function _assemblepage!(sink::_LeafSink, count::Int, present::Int, hasrepetition::Bool,
        hasdefinition::Bool, buffers::_PageBuffers, source)
    repetitions = buffers.repetition
    definitions = buffers.definition
    hasrepetition || fill!(view(_growto!(repetitions, count), 1:count), 0x0000)
    hasdefinition || fill!(view(_growto!(definitions, count), 1:count),
        sink.state.path.maxdefinition)
    _assembleentries!(sink, repetitions, definitions, count, present, source)
    return
end

# PLAIN fixed-width and dictionary pages place their values straight into the leaf's
# slots. A page reaching here decoded both level kinds its leaf has.
function _scatterpage!(sink::_LeafSink{O,L}, count::Int, present::Int, ::Type{P},
        convert::C, encoding::Metadata.Encoding.T, bytes::_ByteView, position::Int,
        stop::Int, dictionary::Vector{L}, hasdictionary::Bool,
        buffers::_PageBuffers) where {O,L,P,C}
    hasrepetition = sink.state.path.depth > 0
    hasdefinition = sink.state.path.maxdefinition > 0
    if encoding == Metadata.Encoding.PLAIN && P <: Union{Int32,Int64,Float32,Float64}
        total = Base.checked_mul(present, sizeof(P))
        _requirebytes(bytes, position, total)
        GC.@preserve bytes _assemblepage!(sink, count, present, hasrepetition,
            hasdefinition, buffers, _PlainValues{P,C}(Ptr{P}(pointer(bytes, position)),
                convert))
        return position + total
    elseif _isdictionaryencoding(encoding)
        hasdictionary || throw(FormatError(
            "dictionary-encoded data page has no dictionary page"))
        position = _dictionaryindices!(buffers.indices, present, bytes, position, stop)
        _assemblepage!(sink, count, present, hasrepetition, hasdefinition, buffers,
            _DictionaryValues(buffers.indices, dictionary))
        return position
    end
    return nothing
end
