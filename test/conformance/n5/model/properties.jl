const N5_PROPERTY_SEED = UInt64(0x4e355f4c49535435)
const N5_PROPERTY_CANDIDATE_LAST = 4095
const N5_PROPERTY_CASE_COUNT = 256
const N5_PROPERTY_CODEC_COUNT = 32
const N5_PROPERTY_MAX_AST_NODES = 64
const N5_PROPERTY_MAX_LEAVES = 16
const N5_PROPERTY_MAX_LEVEL_ENTRIES = 2048
const N5_PROPERTY_MAX_DENSE_VALUES = 2048
const N5_PROPERTY_MAX_PAYLOAD_BYTES = 256 * 1024
const N5_PROPERTY_MAX_COLLECTION_LENGTH = 5
const N5_PROPERTY_LIST_LAYOUTS =
    (:canonical, :rule1, :rule2, :rule3, :rule4, :rule5)
const N5_PROPERTY_LIST_ANNOTATIONS = (:modern, :legacy, :dual, :conflict)
const N5_PROPERTY_MAP_MARKERS = (:modern, :dual, :modern_alias, :conflict,
    :modern_primitive, :modern_unknown, :legacy, :alias)
const N5_PROPERTY_ENTRY_MARKERS = (:none, :marked, :empty, :future,
    :future_marked, :unknown_converted)
const N5_PROPERTY_CODECS =
    (:uncompressed, :snappy, :gzip, :brotli, :zstd, :lz4_raw)
const N5_PROPERTY_CODEC_CASE_IDS = (
    2, 4, 5, 6, 7, 8, 9, 10,
    11, 14, 15, 18, 28, 34, 38, 48,
    58, 62, 96, 106, 107, 117, 122, 123,
    144, 145, 148, 178, 191, 202, 249, 251,
)
const N5_PROPERTY_REJECTED_PREFIX = (
    (29, :depth),
    (63, :width),
    (95, :astnodes),
    (154, :leaves),
    (201, :levelentries),
    (254, :payloadbytes),
)

mutable struct N5SplitMix64
    state::UInt64
end

struct N5PropertyCase
    id::Int
    name::String
    pageversion::Symbol
    node::N5Node
    rows::Vector{Any}
    schema::Vector{MD.SchemaElement}
    paths::Vector{Vector{String}}
    streams::Vector{N5LeafStream}
    depth::Int
    width::Int
    astnodes::Int
    leaves::Int
    levelentries::Int
    densevalues::Int
    payloadbytes::Int
end

mutable struct N5PropertyBuildState
    rng::N5SplitMix64
    caseid::Int
    serial::Int
    maxwidth::Int
    listordinal::Int
    mapordinal::Int
end

mutable struct N5PropertyValueState
    rng::N5SplitMix64
end

mutable struct N5PropertyCoverage
    nodes::Dict{Symbol,Int}
    recursive_nodes::Dict{Symbol,Int}
    list_layouts::Dict{Symbol,Int}
    list_annotations::Dict{Symbol,Int}
    map_markers::Dict{Symbol,Int}
    entry_markers::Dict{Symbol,Int}
    repetitions::Dict{Symbol,Int}
    value_modes::Dict{Symbol,Int}
    name_modes::Dict{Symbol,Int}
    container_states::Dict{Symbol,Int}
    element_states::Dict{Symbol,Int}
    value_states::Dict{Symbol,Int}
    collection_lengths::Dict{Int,Int}
    depths::Dict{Int,Int}
    widths::Dict{Int,Int}
    row_counts::Dict{Int,Int}
    page_versions::Dict{Symbol,Int}
    physical_types::Dict{MD.Type.T,Int}
    duplicate_maps::Int
    complex_keys::Int
    complex_values::Int
    row_starts::Int
    row_continuations::Int
    rejected_candidates::Int
end

function N5PropertyCoverage()
    return N5PropertyCoverage(
        Dict{Symbol,Int}(), Dict{Symbol,Int}(), Dict{Symbol,Int}(),
        Dict{Symbol,Int}(), Dict{Symbol,Int}(), Dict{Symbol,Int}(),
        Dict{Symbol,Int}(), Dict{Symbol,Int}(), Dict{Symbol,Int}(),
        Dict{Symbol,Int}(), Dict{Symbol,Int}(), Dict{Symbol,Int}(),
        Dict{Int,Int}(), Dict{Int,Int}(), Dict{Int,Int}(), Dict{Int,Int}(),
        Dict{Symbol,Int}(), Dict{MD.Type.T,Int}(), 0, 0, 0, 0, 0, 0)
end

function _n5splitmixvalue(value::UInt64)
    mixed = value
    mixed = (mixed ⊻ (mixed >> 30)) * UInt64(0xbf58476d1ce4e5b9)
    mixed = (mixed ⊻ (mixed >> 27)) * UInt64(0x94d049bb133111eb)
    return mixed ⊻ (mixed >> 31)
end

function n5splitmix64!(rng::N5SplitMix64)
    rng.state += UInt64(0x9e3779b97f4a7c15)
    return _n5splitmixvalue(rng.state)
end

function n5propertyrng(id::Integer, lane::UInt64=UInt64(0))
    0 <= id <= N5_PROPERTY_CANDIDATE_LAST || throw(ArgumentError(
        "N5 property candidate ID is outside 0:4095"))
    identity = UInt64(id) * UInt64(0xd1342543de82ef95)
    state = _n5splitmixvalue(N5_PROPERTY_SEED ⊻ identity ⊻ lane)
    return N5SplitMix64(state)
end

function _n5propertychoice!(rng::N5SplitMix64, count::Int)
    count > 0 || throw(ArgumentError("N5 property choice is empty"))
    return Int(rem(n5splitmix64!(rng), UInt64(count))) + 1
end

function _n5propertycoin!(rng::N5SplitMix64)
    return isodd(n5splitmix64!(rng))
end

function _n5propertyname!(state::N5PropertyBuildState, prefix::String)
    state.serial += 1
    return string(prefix, "_", state.caseid, "_", state.serial)
end

function _n5propertyprimitive(state::N5PropertyBuildState, name::String,
        optional::Bool)
    choice = _n5propertychoice!(state.rng, 6)
    if choice == 1
        return n5primitive(name, MD.Type.INT32; optional=optional)
    elseif choice == 2
        return n5primitive(name, MD.Type.INT64; optional=optional)
    elseif choice == 3
        return n5primitive(name, MD.Type.FLOAT; optional=optional)
    elseif choice == 4
        return n5primitive(name, MD.Type.DOUBLE; optional=optional)
    elseif choice == 5
        return n5primitive(name, MD.Type.BOOLEAN; optional=optional)
    end
    logical = _n5propertycoin!(state.rng) ? :string : :none
    return n5primitive(name, MD.Type.BYTE_ARRAY; optional=optional,
        logical=logical)
end

function _n5propertyoptional(state::N5PropertyBuildState,
        optional::Union{Nothing,Bool})
    optional === nothing || return optional
    return _n5propertycoin!(state.rng)
end

function _n5propertystruct(state::N5PropertyBuildState, name::String,
        depth::Int, optional::Bool)
    childcount = state.maxwidth
    fields = N5Node[]
    sizehint!(fields, childcount)
    for index in 1:childcount
        childname = _n5propertyname!(state, "f")
        if index == 1 && depth > 1
            push!(fields, _n5propertynode(state, childname, depth - 1))
        else
            push!(fields, _n5propertyprimitive(state, childname,
                _n5propertycoin!(state.rng)))
        end
    end
    return n5struct(name, fields; optional=optional)
end

function _n5propertylist(state::N5PropertyBuildState, name::String,
        depth::Int, layout::Symbol, optional::Bool; rule3map::Bool=false,
        tuplewrapper::Bool=false)
    state.listordinal += 1
    annotation = N5_PROPERTY_LIST_ANNOTATIONS[mod(state.caseid +
        state.listordinal - 1, length(N5_PROPERTY_LIST_ANNOTATIONS)) + 1]
    if layout === :rule1
        element = _n5propertyprimitive(state,
            _n5propertyname!(state, "element"), false)
        return n5list(name, element; optional=optional, layout=:rule1,
            annotation=annotation)
    elseif layout === :rule2
        childcount = max(2, state.maxwidth)
        fields = N5Node[]
        sizehint!(fields, childcount)
        for index in 1:childcount
            childname = _n5propertyname!(state, "field")
            if index == 1 && depth > 2
                push!(fields, _n5propertynode(state, childname, depth - 2))
            else
                push!(fields, _n5propertyprimitive(state, childname,
                    _n5propertycoin!(state.rng)))
            end
        end
        element = n5struct(_n5propertyname!(state, "element"), fields)
        return n5list(name, element; optional=optional, layout=:rule2,
            wrapper="array", annotation=annotation)
    elseif layout === :rule3
        innername = _n5propertyname!(state, rule3map ? "map" : "array")
        if rule3map
            element = _n5propertymap(state, innername, max(depth - 1, 2), false)
        else
            element = _n5propertylist(state, innername, max(depth - 1, 2),
                :canonical, false)
        end
        return n5list(name, element; optional=optional, layout=:rule3,
            wrapper=rule3map ? "map" : "array", annotation=annotation)
    elseif layout === :rule4
        child = depth > 2 ? _n5propertynode(state,
            _n5propertyname!(state, "value"), depth - 2) :
            _n5propertyprimitive(state, _n5propertyname!(state, "value"),
                _n5propertycoin!(state.rng))
        wrapper = tuplewrapper ? string(name, "_tuple") : "array"
        element = n5struct(wrapper, N5Node[child])
        return n5list(name, element; optional=optional, layout=:rule4,
            wrapper=wrapper, annotation=annotation)
    end
    child = depth > 1 ? _n5propertynode(state,
        _n5propertyname!(state, "element"), depth - 1) :
        _n5propertyprimitive(state, _n5propertyname!(state, "element"), false)
    if layout === :rule5
        wrappers = ("Array", "ARRAY", string(name, "_Tuple"), "list")
        wrapper = wrappers[mod(state.caseid + state.listordinal - 1,
            length(wrappers)) + 1]
        return n5list(name, child; optional=optional, layout=:rule5,
            wrapper=wrapper, annotation=annotation)
    end
    return n5list(name, child; optional=optional, layout=:canonical,
        wrapper="list", annotation=annotation)
end

function _n5propertymap(state::N5PropertyBuildState, name::String,
        depth::Int, optional::Bool)
    state.mapordinal += 1
    ordinal = state.caseid + state.mapordinal - 1
    marker = N5_PROPERTY_MAP_MARKERS[mod(ordinal,
        length(N5_PROPERTY_MAP_MARKERS)) + 1]
    entrymarker = N5_PROPERTY_ENTRY_MARKERS[mod(div(ordinal,
        length(N5_PROPERTY_MAP_MARKERS)),
        length(N5_PROPERTY_ENTRY_MARKERS)) + 1]
    valuemode = (:absent, :required, :optional)[mod(div(ordinal, 3), 3) + 1]
    arbitrary = isodd(div(ordinal, 5))
    entryname = arbitrary ? _n5propertyname!(state, "entries") : "key_value"
    keyname = arbitrary ? _n5propertyname!(state, "left") : "key"
    valuename = arbitrary ? _n5propertyname!(state, "right") : "value"
    recursivekey = depth > 2 && (valuemode === :absent || isodd(ordinal))
    if recursivekey
        key = _n5propertynode(state, keyname, depth - 1; optional=false)
    else
        optionalkey = isodd(div(ordinal, 7))
        key = _n5propertyprimitive(state, keyname, optionalkey)
    end
    value = nothing
    if valuemode !== :absent
        valueoptional = valuemode === :optional
        if depth > 2 && !recursivekey
            value = _n5propertynode(state, valuename, depth - 1;
                optional=valueoptional)
        else
            value = _n5propertyprimitive(state, valuename, valueoptional)
        end
    end
    return n5map(name, key, value; optional=optional, marker=marker,
        entrymarker=entrymarker, entryname=entryname)
end

function _n5propertynode(state::N5PropertyBuildState, name::String,
        depth::Int; optional::Union{Nothing,Bool}=nothing)
    depth >= 1 || throw(ArgumentError("N5 property depth is below one"))
    optionalvalue = _n5propertyoptional(state, optional)
    depth == 1 && return _n5propertyprimitive(state, name, optionalvalue)
    kind = _n5propertychoice!(state.rng, 3)
    kind == 1 && return _n5propertystruct(state, name, depth, optionalvalue)
    if kind == 2
        layouts = depth >= 3 ? N5_PROPERTY_LIST_LAYOUTS :
            (:canonical, :rule1, :rule5)
        layout = layouts[_n5propertychoice!(state.rng, length(layouts))]
        return _n5propertylist(state, name, depth, layout, optionalvalue;
            rule3map=_n5propertycoin!(state.rng),
            tuplewrapper=_n5propertycoin!(state.rng))
    end
    return _n5propertymap(state, name, depth, optionalvalue)
end

function _n5propertyroot(state::N5PropertyBuildState)
    mode = mod(state.caseid, 16)
    targetdepth = mod(state.caseid, 6) + 1
    optional = isodd(div(state.caseid, 2))
    name = "field"
    mode == 0 && return _n5propertyprimitive(state, name, optional)
    mode in (1, 12, 15) && return _n5propertystruct(state, name,
        max(targetdepth, 2), optional)
    mode == 2 && return _n5propertylist(state, name, max(targetdepth, 2),
        :canonical, optional)
    mode == 3 && return _n5propertylist(state, name, 2, :rule1, optional)
    mode == 4 && return _n5propertylist(state, name, max(targetdepth, 3),
        :rule2, optional)
    mode == 5 && return _n5propertylist(state, name, max(targetdepth, 3),
        :rule3, optional)
    mode == 6 && return _n5propertylist(state, name, max(targetdepth, 3),
        :rule3, optional; rule3map=true)
    mode == 7 && return _n5propertylist(state, name, max(targetdepth, 3),
        :rule4, optional)
    mode == 8 && return _n5propertylist(state, name, max(targetdepth, 3),
        :rule4, optional; tuplewrapper=true)
    mode == 9 && return _n5propertylist(state, name, max(targetdepth, 2),
        :rule5, optional)
    mode in (10, 11, 14) && return _n5propertymap(state, name,
        max(targetdepth, 2), optional)
    return _n5propertylist(state, name, max(targetdepth, 2),
        N5_PROPERTY_LIST_LAYOUTS[_n5propertychoice!(state.rng,
            length(N5_PROPERTY_LIST_LAYOUTS))], optional;
        rule3map=_n5propertycoin!(state.rng),
        tuplewrapper=_n5propertycoin!(state.rng))
end

function _n5propertyphase(index::Int, salt::Int, count::Int)
    return mod(index - 1 + salt, count)
end

function _n5propertyprimitivevalue(state::N5PropertyValueState,
        node::N5Primitive)
    raw = n5splitmix64!(state.rng)
    if node.physical == MD.Type.INT32
        return Int32(Int(rem(raw, UInt64(2001))) - 1000)
    elseif node.physical == MD.Type.INT64
        return Int64(raw & UInt64(0x0000ffffffffffff)) - Int64(1 << 46)
    elseif node.physical == MD.Type.FLOAT
        return Float32(Int(rem(raw, UInt64(2001))) - 1000) / Float32(7)
    elseif node.physical == MD.Type.DOUBLE
        return Float64(Int(rem(raw, UInt64(2001))) - 1000) / 11.0
    elseif node.physical == MD.Type.BOOLEAN
        return isodd(raw)
    elseif node.physical == MD.Type.BYTE_ARRAY
        lengthvalue = Int(rem(raw >> 8, UInt64(8))) + 1
        bytes = UInt8[UInt8((raw >> (8 * mod(index, 8))) & 0x7f)
            for index in 0:(lengthvalue - 1)]
        if node.logical === :string
            for index in eachindex(bytes)
                bytes[index] = UInt8('a') + bytes[index] % UInt8(26)
            end
            return String(bytes)
        end
        return bytes
    end
    throw(ArgumentError("unsupported N5 property primitive $(node.physical)"))
end

function _n5propertybatch(state::N5PropertyValueState, node::N5Primitive,
        count::Int, salt::Int; forcepresent::Bool=false)
    values = Vector{Any}(undef, count)
    for index in 1:count
        absent = node.optional && !forcepresent &&
            iszero(_n5propertyphase(index, salt, 2))
        values[index] = absent ? missing : _n5propertyprimitivevalue(state, node)
    end
    return values
end

function _n5propertybatch(state::N5PropertyValueState, node::N5Struct,
        count::Int, salt::Int; forcepresent::Bool=false)
    present = Int[]
    for index in 1:count
        absent = node.optional && !forcepresent &&
            iszero(_n5propertyphase(index, salt, 2))
        absent || push!(present, index)
    end
    children = Vector{Vector{Any}}(undef, length(node.fields))
    for (index, field) in enumerate(node.fields)
        children[index] = _n5propertybatch(state, field, length(present),
            salt + 17 * index)
    end
    values = Vector{Any}(undef, count)
    fill!(values, missing)
    for (slot, index) in enumerate(present)
        values[index] = N5Record(Any[child[slot] for child in children])
    end
    return values
end

function _n5propertypresentlength(state::N5PropertyValueState,
        optionalchild::Bool)
    lengthvalue = _n5propertychoice!(state.rng,
        N5_PROPERTY_MAX_COLLECTION_LENGTH)
    optionalchild && (lengthvalue = max(lengthvalue, 3))
    return lengthvalue
end

function _n5propertybatch(state::N5PropertyValueState, node::N5List,
        count::Int, salt::Int; forcepresent::Bool=false)
    lengths = zeros(Int, count)
    missingrows = falses(count)
    for index in 1:count
        if node.optional && !forcepresent
            phase = _n5propertyphase(index, salt, 3)
            missingrows[index] = phase == 0
            phase == 2 && (lengths[index] = _n5propertypresentlength(state,
                node.element.optional))
        else
            phase = _n5propertyphase(index, salt, 2)
            phase == 1 && (lengths[index] = _n5propertypresentlength(state,
                node.element.optional))
        end
    end
    total = sum(lengths)
    elements = _n5propertybatch(state, node.element, total, salt + 31)
    values = Vector{Any}(undef, count)
    cursor = 1
    for index in 1:count
        if missingrows[index]
            values[index] = missing
        else
            lengthvalue = lengths[index]
            values[index] = lengthvalue == 0 ? [] :
                Any[elements[cursor:(cursor + lengthvalue - 1)]...]
            cursor += lengthvalue
        end
    end
    return values
end

function _n5propertymaplength(state::N5PropertyValueState, node::N5Map)
    optionalchild = node.value !== nothing && node.value.optional
    lengthvalue = _n5propertypresentlength(state, optionalchild)
    return max(lengthvalue, 2)
end

function _n5propertybatch(state::N5PropertyValueState, node::N5Map,
        count::Int, salt::Int; forcepresent::Bool=false)
    lengths = zeros(Int, count)
    missingrows = falses(count)
    for index in 1:count
        if node.optional && !forcepresent
            phase = _n5propertyphase(index, salt, 3)
            missingrows[index] = phase == 0
            phase == 2 && (lengths[index] = _n5propertymaplength(state, node))
        else
            phase = _n5propertyphase(index, salt, 2)
            phase == 1 && (lengths[index] = _n5propertymaplength(state, node))
        end
    end
    total = sum(lengths)
    keys = _n5propertybatch(state, node.key, total, salt + 43;
        forcepresent=true)
    mapvalues = node.value === nothing ? nothing :
        _n5propertybatch(state, node.value, total, salt + 59)
    cursor = 1
    for lengthvalue in lengths
        if lengthvalue >= 2
            keys[cursor + 1] = keys[cursor]
        end
        cursor += lengthvalue
    end
    values = Vector{Any}(undef, count)
    cursor = 1
    for index in 1:count
        if missingrows[index]
            values[index] = missing
            continue
        end
        entries = N5Entry[]
        sizehint!(entries, lengths[index])
        for offset in 0:(lengths[index] - 1)
            position = cursor + offset
            if mapvalues === nothing
                push!(entries, N5Entry(keys[position], nothing, false))
            else
                push!(entries, N5Entry(keys[position], mapvalues[position], true))
            end
        end
        values[index] = N5MapValue(entries)
        cursor += lengths[index]
    end
    return values
end

function _n5propertyempty(value)
    value isa AbstractVector && return isempty(value)
    value isa N5MapValue && return isempty(value.entries)
    return false
end

function _n5propertystatesvalid(node::N5Primitive, values::Vector{Any};
        forcepresent::Bool=false)
    if node.optional && !forcepresent && length(values) >= 2
        any(ismissing, values) && any(!ismissing, values) || return false
    end
    return forcepresent ? all(!ismissing, values) : true
end

function _n5propertystatesvalid(node::N5Struct, values::Vector{Any};
        forcepresent::Bool=false)
    if node.optional && !forcepresent && length(values) >= 2
        any(ismissing, values) && any(!ismissing, values) || return false
    end
    forcepresent && !all(!ismissing, values) && return false
    present = Any[value for value in values if !ismissing(value)]
    for (index, child) in enumerate(node.fields)
        childvalues = Any[value.values[index] for value in present]
        _n5propertystatesvalid(child, childvalues) || return false
    end
    return true
end

function _n5propertycontainerstates(node, values::Vector{Any},
        forcepresent::Bool)
    if node.optional && !forcepresent && length(values) >= 3
        any(ismissing, values) || return false
        any(value -> !ismissing(value) && _n5propertyempty(value), values) ||
            return false
        any(value -> !ismissing(value) && !_n5propertyempty(value), values) ||
            return false
    elseif (!node.optional || forcepresent) && length(values) >= 2
        all(!ismissing, values) || return false
        any(_n5propertyempty, values) || return false
        any(value -> !_n5propertyempty(value), values) || return false
    end
    return true
end

function _n5propertystatesvalid(node::N5List, values::Vector{Any};
        forcepresent::Bool=false)
    _n5propertycontainerstates(node, values, forcepresent) || return false
    elements = []
    for value in values
        ismissing(value) || append!(elements, value)
    end
    return _n5propertystatesvalid(node.element, elements)
end

function _n5propertystatesvalid(node::N5Map, values::Vector{Any};
        forcepresent::Bool=false)
    _n5propertycontainerstates(node, values, forcepresent) || return false
    keys = []
    mapvalues = []
    for value in values
        ismissing(value) && continue
        if !isempty(value.entries)
            length(value.entries) >= 2 || return false
            isequal(value.entries[1].key, value.entries[2].key) || return false
        end
        for entry in value.entries
            ismissing(entry.key) && return false
            push!(keys, entry.key)
            node.value === nothing || push!(mapvalues, entry.value)
        end
    end
    _n5propertystatesvalid(node.key, keys; forcepresent=true) || return false
    node.value === nothing && return true
    return _n5propertystatesvalid(node.value, mapvalues)
end

function n5propertyastnodes(node::N5Primitive)
    return 1
end

function n5propertyastnodes(node::N5Struct)
    return 1 + sum(n5propertyastnodes, node.fields; init=0)
end

function n5propertyastnodes(node::N5List)
    return 1 + n5propertyastnodes(node.element)
end

function n5propertyastnodes(node::N5Map)
    value = node.value === nothing ? 0 : n5propertyastnodes(node.value)
    return 1 + n5propertyastnodes(node.key) + value
end

function n5propertydepth(node::N5Primitive)
    return 1
end

function n5propertydepth(node::N5Struct)
    return 1 + maximum(n5propertydepth, node.fields)
end

function n5propertydepth(node::N5List)
    return 1 + n5propertydepth(node.element)
end

function n5propertydepth(node::N5Map)
    value = node.value === nothing ? 0 : n5propertydepth(node.value)
    return 1 + max(n5propertydepth(node.key), value)
end

function n5propertywidth(node::N5Primitive)
    return 1
end

function n5propertywidth(node::N5Struct)
    return max(length(node.fields), maximum(n5propertywidth, node.fields))
end

function n5propertywidth(node::N5List)
    return max(1, n5propertywidth(node.element))
end

function n5propertywidth(node::N5Map)
    own = node.value === nothing ? 1 : 2
    value = node.value === nothing ? 1 : n5propertywidth(node.value)
    return max(own, n5propertywidth(node.key), value)
end

function _n5propertyunarychain(name::String, depth::Int)
    node = n5primitive(string(name, "_leaf"), MD.Type.INT32)
    for level in 2:depth
        node = n5struct(string(name, "_level_", level), N5Node[node])
    end
    return node
end

function _n5propertyastcapnode()
    groups = N5Node[]
    for groupindex in 1:4
        fields = N5Node[]
        for fieldindex in 1:4
            name = string("cap_", groupindex, "_", fieldindex)
            push!(fields, _n5propertyunarychain(name, 4))
        end
        push!(groups, n5struct(string("cap_group_", groupindex), fields))
    end
    return n5struct("field", groups)
end

function _n5propertyleafcapnode()
    groups = N5Node[]
    for groupindex in 1:4
        fields = N5Node[n5primitive(
            string("cap_leaf_", groupindex, "_", fieldindex), MD.Type.INT32)
            for fieldindex in 1:4]
        push!(groups, n5struct(string("cap_group_", groupindex), fields))
    end
    branch = n5struct("cap_branch", groups)
    tail = n5primitive("cap_tail", MD.Type.INT32)
    return n5struct("field", N5Node[branch, tail])
end

function _n5propertynestedmissing(depth::Int)
    value = missing
    for _ in 1:depth
        value = Any[value, value, value, value, value]
    end
    return value
end

function _n5propertynestedvalue(depth::Int, value)
    iszero(depth) && return value
    missingbranch = _n5propertynestedmissing(depth - 1)
    return Any[_n5propertynestedvalue(depth - 1, value), missingbranch,
        missingbranch, missingbranch, missingbranch]
end

function _n5propertyhardcapcandidate(id::Int)
    if id == 29
        node = _n5propertyunarychain("field", 7)
        values = N5PropertyValueState(n5propertyrng(id,
            UInt64(0x56414c5545534e35)))
        return node, _n5propertybatch(values, node, 1, id)
    elseif id == 63
        fields = N5Node[n5primitive(string("wide_", index), MD.Type.INT32)
            for index in 1:5]
        node = n5struct("field", fields)
        values = N5PropertyValueState(n5propertyrng(id,
            UInt64(0x56414c5545534e35)))
        return node, _n5propertybatch(values, node, 1, id)
    elseif id == 95
        node = _n5propertyastcapnode()
        values = N5PropertyValueState(n5propertyrng(id,
            UInt64(0x56414c5545534e35)))
        return node, _n5propertybatch(values, node, 1, id)
    elseif id == 154
        node = _n5propertyleafcapnode()
        values = N5PropertyValueState(n5propertyrng(id,
            UInt64(0x56414c5545534e35)))
        return node, _n5propertybatch(values, node, 1, id)
    elseif id == 201
        node = n5primitive("entry", MD.Type.INT32; optional=true)
        for level in 1:5
            node = n5list(string("level_", level), node)
        end
        nested = _n5propertynestedmissing(5)
        values = Any[nested for _ in 1:24]
        rng = n5propertyrng(id, UInt64(0x56414c5545534e35))
        present = Int32(rem(n5splitmix64!(rng), UInt64(2001))) - Int32(1000)
        values[1] = _n5propertynestedvalue(5, present)
        return node, values
    elseif id == 254
        node = n5primitive("field", MD.Type.BYTE_ARRAY)
        rng = n5propertyrng(id, UInt64(0x56414c5545534e35))
        byte = UInt8(n5splitmix64!(rng) & UInt64(0xff))
        bytes = fill(byte, N5_PROPERTY_MAX_PAYLOAD_BYTES + 1)
        return node, Any[bytes]
    end
    return nothing
end

function _n5propertygeneratedcandidate(id::Int)
    hardcap = _n5propertyhardcapcandidate(id)
    hardcap !== nothing && return hardcap
    build = N5PropertyBuildState(n5propertyrng(id), id, 0,
        mod(div(id, 6), 4) + 1, 0, 0)
    node = _n5propertyroot(build)
    values = N5PropertyValueState(n5propertyrng(id,
        UInt64(0x56414c5545534e35)))
    return node, _n5propertybatch(values, node, mod(id, 25), id)
end

function n5propertycandidateassessment(id::Int)
    0 <= id <= N5_PROPERTY_CANDIDATE_LAST || throw(ArgumentError(
        "N5 property candidate ID is outside 0:4095"))
    node, rows = _n5propertygeneratedcandidate(id)
    statesvalid = _n5propertystatesvalid(node, rows)
    compiled = n5compile(node)
    streams = n5shred(compiled, rows)
    schema = n5schema(node)
    physical = n5physicalleaves(schema)
    astnodes = n5propertyastnodes(node)
    depth = n5propertydepth(node)
    width = n5propertywidth(node)
    leaves = length(compiled.leaves)
    levelentries = sum(stream -> length(stream.repetition), streams; init=0)
    densevalues = sum(stream -> length(stream.values), streams; init=0)
    payloadbytes = sum(length(_n5plainencode(stream.values, leaf))
        for (stream, leaf) in zip(streams, physical); init=0)
    reason = depth < 1 || depth > 6 ? :depth :
        width < 1 || width > 4 ? :width :
        astnodes > N5_PROPERTY_MAX_AST_NODES ? :astnodes :
        leaves > N5_PROPERTY_MAX_LEAVES ? :leaves :
        levelentries > N5_PROPERTY_MAX_LEVEL_ENTRIES ? :levelentries :
        densevalues > N5_PROPERTY_MAX_DENSE_VALUES ? :densevalues :
        payloadbytes > N5_PROPERTY_MAX_PAYLOAD_BYTES ? :payloadbytes :
        !statesvalid ? :semantic_states : nothing
    paths = Vector{String}[copy(leaf.path) for leaf in physical]
    metrics = (; depth, width, astnodes, leaves, levelentries, densevalues,
        payloadbytes)
    reason === nothing || return (; candidate=nothing, reason, metrics)
    candidate = (; id, node, rows, schema, paths, streams, depth, width, astnodes,
        leaves, levelentries, densevalues, payloadbytes)
    return (; candidate, reason=nothing, metrics)
end

function _n5propertycandidate(id::Int)
    return n5propertycandidateassessment(id).candidate
end

function n5propertycases()
    cases = N5PropertyCase[]
    rejected = 0
    for id in 0:N5_PROPERTY_CANDIDATE_LAST
        candidate = _n5propertycandidate(id)
        if candidate === nothing
            rejected += 1
            continue
        end
        pageversion = isodd(length(cases)) ? :v2 : :v1
        name = string("generated-", lpad(string(id), 4, '0'))
        push!(cases, N5PropertyCase(candidate.id, name, pageversion,
            candidate.node, candidate.rows, candidate.schema, candidate.paths,
            candidate.streams, candidate.depth, candidate.width,
            candidate.astnodes, candidate.leaves, candidate.levelentries,
            candidate.densevalues, candidate.payloadbytes))
        length(cases) == N5_PROPERTY_CASE_COUNT && break
    end
    length(cases) == N5_PROPERTY_CASE_COUNT || throw(ArgumentError(
        "N5 property schedule accepted $(length(cases)) of 256 cases"))
    return cases, rejected
end

function _n5propertybump!(counts::Dict{T,Int}, key::T) where {T}
    counts[key] = get(() -> 0, counts, key) + 1
    return
end

function _n5propertycoverast!(coverage::N5PropertyCoverage,
        node::N5Primitive, level::Int)
    _n5propertybump!(coverage.nodes, :primitive)
    level > 1 && _n5propertybump!(coverage.recursive_nodes, :primitive)
    _n5propertybump!(coverage.repetitions,
        node.optional ? :optional : :required)
    _n5propertybump!(coverage.physical_types, node.physical)
    return
end

function _n5propertycoverast!(coverage::N5PropertyCoverage,
        node::N5Struct, level::Int)
    _n5propertybump!(coverage.nodes, :struct)
    level > 1 && _n5propertybump!(coverage.recursive_nodes, :struct)
    _n5propertybump!(coverage.repetitions,
        node.optional ? :optional : :required)
    for child in node.fields
        _n5propertycoverast!(coverage, child, level + 1)
    end
    return
end

function _n5propertycoverast!(coverage::N5PropertyCoverage,
        node::N5List, level::Int)
    _n5propertybump!(coverage.nodes, :list)
    level > 1 && _n5propertybump!(coverage.recursive_nodes, :list)
    _n5propertybump!(coverage.repetitions,
        node.optional ? :optional : :required)
    _n5propertybump!(coverage.repetitions, :repeated)
    _n5propertybump!(coverage.list_layouts, node.layout)
    _n5propertybump!(coverage.list_annotations, node.annotation)
    _n5propertycoverast!(coverage, node.element, level + 1)
    return
end

function _n5propertycoverast!(coverage::N5PropertyCoverage,
        node::N5Map, level::Int)
    _n5propertybump!(coverage.nodes, :map)
    level > 1 && _n5propertybump!(coverage.recursive_nodes, :map)
    _n5propertybump!(coverage.repetitions,
        node.optional ? :optional : :required)
    _n5propertybump!(coverage.repetitions, :repeated)
    _n5propertybump!(coverage.map_markers, node.marker)
    _n5propertybump!(coverage.entry_markers, node.entrymarker)
    valuemode = node.value === nothing ? :absent :
        node.value.optional ? :optional : :required
    _n5propertybump!(coverage.value_modes, valuemode)
    names = node.entryname == "key_value" && node.key.name == "key" &&
        (node.value === nothing || node.value.name == "value") ?
        :canonical : :arbitrary
    _n5propertybump!(coverage.name_modes, names)
    node.key isa N5Primitive || (coverage.complex_keys += 1)
    node.value === nothing || node.value isa N5Primitive ||
        (coverage.complex_values += 1)
    _n5propertycoverast!(coverage, node.key, level + 1)
    node.value === nothing || _n5propertycoverast!(coverage,
        node.value, level + 1)
    return
end

function _n5propertycoverstate!(coverage::N5PropertyCoverage, value)
    if ismissing(value)
        _n5propertybump!(coverage.container_states, :null)
    elseif _n5propertyempty(value)
        _n5propertybump!(coverage.container_states, :empty)
    else
        _n5propertybump!(coverage.container_states, :present)
    end
    return
end

function _n5propertycovervalues!(coverage::N5PropertyCoverage,
        node::N5Primitive, values::Vector{Any})
    return
end

function _n5propertycovervalues!(coverage::N5PropertyCoverage,
        node::N5Struct, values::Vector{Any})
    present = Any[value for value in values if !ismissing(value)]
    for (index, child) in enumerate(node.fields)
        childvalues = Any[value.values[index] for value in present]
        _n5propertycovervalues!(coverage, child, childvalues)
    end
    return
end

function _n5propertycovervalues!(coverage::N5PropertyCoverage,
        node::N5List, values::Vector{Any})
    elements = []
    for value in values
        _n5propertycoverstate!(coverage, value)
        if !ismissing(value)
            _n5propertybump!(coverage.collection_lengths, length(value))
            append!(elements, value)
        end
    end
    if node.element.optional
        for value in elements
            _n5propertybump!(coverage.element_states,
                ismissing(value) ? :null : :present)
        end
    end
    _n5propertycovervalues!(coverage, node.element, elements)
    return
end

function _n5propertycovervalues!(coverage::N5PropertyCoverage,
        node::N5Map, values::Vector{Any})
    keys = []
    mapvalues = []
    for value in values
        _n5propertycoverstate!(coverage, value)
        ismissing(value) && continue
        _n5propertybump!(coverage.collection_lengths, length(value.entries))
        if length(value.entries) >= 2 &&
                isequal(value.entries[1].key, value.entries[2].key)
            coverage.duplicate_maps += 1
        end
        for entry in value.entries
            push!(keys, entry.key)
            node.value === nothing || push!(mapvalues, entry.value)
        end
    end
    _n5propertycovervalues!(coverage, node.key, keys)
    if node.value !== nothing
        if node.value.optional
            for value in mapvalues
                _n5propertybump!(coverage.value_states,
                    ismissing(value) ? :null : :present)
            end
        end
        _n5propertycovervalues!(coverage, node.value, mapvalues)
    end
    return
end

function n5propertycoverage(cases::Vector{N5PropertyCase}, rejected::Int)
    coverage = N5PropertyCoverage()
    coverage.rejected_candidates = rejected
    for case in cases
        _n5propertybump!(coverage.depths, case.depth)
        _n5propertybump!(coverage.widths, case.width)
        _n5propertybump!(coverage.row_counts, length(case.rows))
        _n5propertybump!(coverage.page_versions, case.pageversion)
        _n5propertycoverast!(coverage, case.node, 1)
        _n5propertycovervalues!(coverage, case.node, case.rows)
        for stream in case.streams
            coverage.row_starts += count(iszero, stream.repetition)
            coverage.row_continuations += count(!iszero, stream.repetition)
        end
    end
    return coverage
end

function _n5propertymissing!(missing::Vector{String}, counts, key, label::String)
    get(() -> 0, counts, key) > 0 || push!(missing, label)
    return
end

function n5propertymissingcoverage(coverage::N5PropertyCoverage)
    missing = String[]
    for kind in (:primitive, :struct, :list, :map)
        _n5propertymissing!(missing, coverage.nodes, kind, "node:$kind")
    end
    for kind in (:primitive, :struct, :list, :map)
        _n5propertymissing!(missing, coverage.recursive_nodes, kind,
            "recursive-node:$kind")
    end
    for layout in N5_PROPERTY_LIST_LAYOUTS
        _n5propertymissing!(missing, coverage.list_layouts, layout,
            "list-layout:$layout")
    end
    for annotation in N5_PROPERTY_LIST_ANNOTATIONS
        _n5propertymissing!(missing, coverage.list_annotations, annotation,
            "list-annotation:$annotation")
    end
    for marker in N5_PROPERTY_MAP_MARKERS
        _n5propertymissing!(missing, coverage.map_markers, marker,
            "map-marker:$marker")
    end
    for marker in N5_PROPERTY_ENTRY_MARKERS
        _n5propertymissing!(missing, coverage.entry_markers, marker,
            "entry-marker:$marker")
    end
    for repetition in (:required, :optional, :repeated)
        _n5propertymissing!(missing, coverage.repetitions, repetition,
            "repetition:$repetition")
    end
    for mode in (:absent, :required, :optional)
        _n5propertymissing!(missing, coverage.value_modes, mode,
            "map-value:$mode")
    end
    for mode in (:canonical, :arbitrary)
        _n5propertymissing!(missing, coverage.name_modes, mode,
            "map-names:$mode")
    end
    for state in (:null, :empty, :present)
        _n5propertymissing!(missing, coverage.container_states, state,
            "container:$state")
    end
    for state in (:null, :present)
        _n5propertymissing!(missing, coverage.element_states, state,
            "optional-element:$state")
        _n5propertymissing!(missing, coverage.value_states, state,
            "optional-value:$state")
    end
    for lengthvalue in 0:N5_PROPERTY_MAX_COLLECTION_LENGTH
        _n5propertymissing!(missing, coverage.collection_lengths, lengthvalue,
            "collection-length:$lengthvalue")
    end
    for depth in 1:6
        _n5propertymissing!(missing, coverage.depths, depth, "depth:$depth")
    end
    for width in 1:4
        _n5propertymissing!(missing, coverage.widths, width, "width:$width")
    end
    for rows in 0:24
        _n5propertymissing!(missing, coverage.row_counts, rows, "rows:$rows")
    end
    for version in (:v1, :v2)
        _n5propertymissing!(missing, coverage.page_versions, version,
            "page:$version")
    end
    for physical in (MD.Type.INT32, MD.Type.INT64, MD.Type.FLOAT,
            MD.Type.DOUBLE, MD.Type.BOOLEAN, MD.Type.BYTE_ARRAY)
        _n5propertymissing!(missing, coverage.physical_types, physical,
            "physical:$(physical.value)")
    end
    coverage.duplicate_maps > 0 || push!(missing, "duplicate-map-key")
    coverage.complex_keys > 0 || push!(missing, "recursive-map-key")
    coverage.complex_values > 0 || push!(missing, "recursive-map-value")
    coverage.row_starts > 0 || push!(missing, "row-start")
    coverage.row_continuations > 0 || push!(missing, "row-continuation")
    coverage.rejected_candidates > 0 || push!(missing, "rejected-candidate")
    return missing
end

function n5propertycodecsubset(cases::Vector{N5PropertyCase})
    byid = Dict(case.id => case for case in cases)
    subset = N5PropertyCase[]
    sizehint!(subset, N5_PROPERTY_CODEC_COUNT)
    for id in N5_PROPERTY_CODEC_CASE_IDS
        haskey(byid, id) || throw(ArgumentError(
            "N5 property codec case ID $id is not accepted"))
        case = byid[id]
        length(case.rows) > 0 && case.payloadbytes > 0 || throw(ArgumentError(
            "N5 property codec case ID $id has no writable payload"))
        push!(subset, case)
    end
    length(subset) == N5_PROPERTY_CODEC_COUNT || throw(ArgumentError(
        "N5 property codec subset does not contain 32 cases"))
    return subset
end
