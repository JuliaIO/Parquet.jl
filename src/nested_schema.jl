abstract type _NestedPlan end

struct _NestedLeafPlan <: _NestedPlan
    source::SchemaNode
    parent_definition::Int16
    present_definition::Int16
    leaf_range::UnitRange{Int32}
end

struct _NestedStructPlan <: _NestedPlan
    source::SchemaNode
    parent_definition::Int16
    present_definition::Int16
    children::Vector{_NestedPlan}
    leaf_range::UnitRange{Int32}
end

struct _NestedListPlan <: _NestedPlan
    source::SchemaNode
    entry::SchemaNode
    parent_definition::Int16
    present_definition::Int16
    entry_definition::Int16
    repetition_level::Int16
    element::_NestedPlan
    annotation::Symbol
    rule::UInt8
    leaf_range::UnitRange{Int32}
end

struct _NestedMapPlan <: _NestedPlan
    source::SchemaNode
    entry::SchemaNode
    parent_definition::Int16
    present_definition::Int16
    entry_definition::Int16
    repetition_level::Int16
    key::_NestedPlan
    value::Union{Nothing,_NestedPlan}
    annotation::Symbol
    optional_key::Bool
    entry_has_map_key_value::Bool
    leaf_range::UnitRange{Int32}
end

struct _NestedSchemaPlan
    source::Schema
    root::_NestedStructPlan
    leaves::Vector{_NestedLeafPlan}
    plan_count::Int64
end

mutable struct _NestedPlanBuilder
    limits::Limits
    budget::_LiveByteBudget
    count::Int64
    leaves::Vector{_NestedLeafPlan}
    leafmaximum::Int
end

function _nestedemptyrange()
    return Int32(1):Int32(0)
end

function _nestedleafordinalcount(count::Integer)
    count <= typemax(Int32) || throw(LimitError(:container_elements, count,
        Int64(typemax(Int32))))
    return Int32(count)
end

function _nestedleafrange(plan::_NestedPlan)
    return getfield(plan, :leaf_range)
end

function _nestedclaim!(builder::_NestedPlanBuilder)
    requested = try
        Base.checked_add(builder.count, Int64(1))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64),
            builder.limits.max_container_elements))
    end
    _checklimit(:container_elements, requested,
        builder.limits.max_container_elements)
    _reserveobjects!(builder.budget)
    builder.count = requested
    return
end

function _nestedmergeranges(children::Vector{_NestedPlan})
    firstleaf = Int32(0)
    lastleaf = Int32(0)
    for child in children
        range = _nestedleafrange(child)
        isempty(range) && continue
        if firstleaf == 0
            firstleaf = first(range)
        else
            first(range) == lastleaf + Int32(1) ||
                throw(FormatError("nested schema has noncontiguous descendant leaves"))
        end
        lastleaf = last(range)
    end
    firstleaf == 0 && return _nestedemptyrange()
    return firstleaf:lastleaf
end

function _nestedmergeranges(left::UnitRange{Int32}, right::UnitRange{Int32})
    isempty(left) && return right
    isempty(right) && return left
    first(right) == last(left) + Int32(1) ||
        throw(FormatError("nested schema has noncontiguous descendant leaves"))
    return first(left):last(right)
end

function _nestedmodernannotation(logical::Metadata.LogicalType)
    logical.STRING !== nothing && return :STRING
    logical.MAP !== nothing && return :MAP
    logical.LIST !== nothing && return :LIST
    logical.ENUM !== nothing && return :ENUM
    logical.DECIMAL !== nothing && return :DECIMAL
    logical.DATE !== nothing && return :DATE
    logical.TIME !== nothing && return :TIME
    logical.TIMESTAMP !== nothing && return :TIMESTAMP
    logical.INTEGER !== nothing && return :INTEGER
    logical.UNKNOWN !== nothing && return :UNKNOWN
    logical.JSON !== nothing && return :JSON
    logical.BSON !== nothing && return :BSON
    logical.UUID !== nothing && return :UUID
    logical.FLOAT16 !== nothing && return :FLOAT16
    logical.VARIANT !== nothing && return :VARIANT
    logical.GEOMETRY !== nothing && return :GEOMETRY
    logical.GEOGRAPHY !== nothing && return :GEOGRAPHY
    isempty(logical.unknown_fields) && return :EMPTY
    return :FUTURE
end

function _nestedknownconverted(converted::Metadata.ConvertedType.T)
    return Int32(0) <= converted.value <= Int32(21)
end

function _nestedrejectcollectionplacement(node::SchemaNode, location::String)
    element = node.element
    logical = element.logicalType
    if logical !== nothing &&
            (logical.LIST !== nothing || logical.MAP !== nothing)
        throw(FormatError("$location $(repr(element.name)) cannot carry a " *
            "LIST or MAP logical annotation"))
    end
    converted = element.converted_type
    if converted == Metadata.ConvertedType.LIST ||
            converted == Metadata.ConvertedType.MAP ||
            converted == Metadata.ConvertedType.MAP_KEY_VALUE
        throw(FormatError("$location $(repr(element.name)) cannot carry converted " *
            "LIST, MAP, or MAP_KEY_VALUE metadata"))
    end
    return
end

function _nestedvalidategroupmetadata(node::SchemaNode)
    element = node.element
    logical = element.logicalType
    if logical !== nothing
        annotation = _nestedmodernannotation(logical)
        annotation in (:VARIANT, :FUTURE, :EMPTY) && return
        annotation in (:LIST, :MAP) && return
        throw(FormatError("logical annotation $annotation on group " *
            "$(repr(element.name)) requires a primitive physical type"))
    end
    converted = element.converted_type
    converted === nothing && return
    converted in (Metadata.ConvertedType.LIST, Metadata.ConvertedType.MAP,
        Metadata.ConvertedType.MAP_KEY_VALUE) && return
    _nestedknownconverted(converted) || return
    throw(FormatError("converted annotation $(converted.value) on group " *
        "$(repr(element.name)) requires a primitive physical type"))
end

function _nestedgroupkind(node::SchemaNode)
    logical = node.element.logicalType
    if logical !== nothing
        _nestedvalidategroupmetadata(node)
        logical.LIST !== nothing && return (:list, :modern_list)
        logical.MAP !== nothing && return (:map, :modern_map)
        return (:struct, :ordinary)
    end
    _nestedvalidategroupmetadata(node)
    converted = node.element.converted_type
    converted == Metadata.ConvertedType.LIST && return (:list, :legacy_list)
    converted == Metadata.ConvertedType.MAP && return (:map, :legacy_map)
    converted == Metadata.ConvertedType.MAP_KEY_VALUE &&
        return (:map, :legacy_map_key_value)
    return (:struct, :ordinary)
end

function _nestedthresholds(node::SchemaNode; owned_repeated::Bool=false)
    repetition = node.element.repetition_type
    if owned_repeated
        repetition == Metadata.FieldRepetitionType.REPEATED ||
            throw(FormatError("nested repeated entry $(repr(node.element.name)) " *
                "is not marked REPEATED"))
        return (node.max_definition_level, node.max_definition_level)
    elseif repetition == Metadata.FieldRepetitionType.REQUIRED
        return (node.max_definition_level, node.max_definition_level)
    elseif repetition == Metadata.FieldRepetitionType.OPTIONAL
        node.max_definition_level > 0 ||
            throw(FormatError("optional field $(repr(node.element.name)) has an " *
                "invalid definition level"))
        return (node.max_definition_level - Int16(1), node.max_definition_level)
    elseif repetition == Metadata.FieldRepetitionType.REPEATED
        throw(FormatError("repeated field $(repr(node.element.name)) was not " *
            "normalized as a collection"))
    end
    throw(FormatError("field $(repr(node.element.name)) has no valid repetition type"))
end

function _nestedlistrule(outer::SchemaNode, entry::SchemaNode)
    entry.element.type_ === nothing || return UInt8(1)
    count = length(entry.children)
    count == 0 && throw(FormatError("LIST field $(repr(outer.element.name)) has a " *
        "zero-field repeated wrapper"))
    count >= 2 && return UInt8(2)
    onlychild = entry.children[1]
    onlychild.element.repetition_type == Metadata.FieldRepetitionType.REPEATED &&
        return UInt8(3)
    entry.element.name == "array" && return UInt8(4)
    entry.element.name == string(outer.element.name, "_tuple") && return UInt8(5)
    return UInt8(6)
end

function _nestedmapentrymarker(entry::SchemaNode)
    logical = entry.element.logicalType
    if logical !== nothing
        annotation = _nestedmodernannotation(logical)
        annotation in (:FUTURE, :EMPTY) && return false
        throw(FormatError("MAP entry group $(repr(entry.element.name)) cannot carry " *
            "logical annotation $annotation"))
    end
    converted = entry.element.converted_type
    converted === nothing && return false
    converted == Metadata.ConvertedType.MAP_KEY_VALUE && return true
    _nestedknownconverted(converted) || return false
    throw(FormatError("MAP entry group $(repr(entry.element.name)) cannot carry " *
        "converted annotation $(converted.value)"))
end

function _nestedvalidatemapchild(node::SchemaNode, role::String)
    repetition = node.element.repetition_type
    repetition == Metadata.FieldRepetitionType.REPEATED && throw(FormatError(
        "MAP $role $(repr(node.element.name)) cannot be REPEATED"))
    repetition in (Metadata.FieldRepetitionType.REQUIRED,
        Metadata.FieldRepetitionType.OPTIONAL) && return
    throw(FormatError("MAP $role $(repr(node.element.name)) has no valid repetition type"))
end

const _NESTED_FRAME_ROOT = UInt8(1)
const _NESTED_FRAME_STRUCT = UInt8(2)
const _NESTED_FRAME_LIST = UInt8(3)
const _NESTED_FRAME_MAP = UInt8(4)
const _NESTED_FRAME_REPEATED = UInt8(5)

mutable struct _NestedCompileFrame
    node::SchemaNode
    parent::Union{Nothing,_NestedCompileFrame}
    depth::Int
    mode::UInt8
    annotation::Symbol
    rule::UInt8
    parent_definition::Int16
    present_definition::Int16
    entry::Union{Nothing,SchemaNode}
    children::Union{Nothing,Vector{_NestedPlan}}
    firstplan::Union{Nothing,_NestedPlan}
    secondplan::Union{Nothing,_NestedPlan}
    expected::Int
    completed::Int
    optional_key::Bool
    entry_has_map_key_value::Bool
end

function _nestedframe(builder::_NestedPlanBuilder, node::SchemaNode, parent,
        depth::Int, mode::UInt8, annotation::Symbol, rule::UInt8,
        parent_definition::Int16, present_definition::Int16,
        entry::Union{Nothing,SchemaNode},
        children::Union{Nothing,Vector{_NestedPlan}}, expected::Int;
        optional_key::Bool=false, entry_has_map_key_value::Bool=false)
    _reserveobjects!(builder.budget)
    return _NestedCompileFrame(node, parent, depth, mode, annotation, rule,
        parent_definition, present_definition, entry, children, nothing,
        nothing, expected, 0, optional_key, entry_has_map_key_value)
end

function _nestedcheckdepth(builder::_NestedPlanBuilder, depth::Int)
    _checklimit(:metadata_depth, depth, builder.limits.max_metadata_depth)
    return
end

function _nestedstartroot(builder::_NestedPlanBuilder, node::SchemaNode)
    _nestedcheckdepth(builder, 1)
    _nestedclaim!(builder)
    count = length(node.children)
    _checklimit(:container_elements, count,
        builder.limits.max_container_elements)
    _reservearray!(builder.budget, _NestedPlan, count)
    children = _NestedPlan[]
    sizehint!(children, count)
    return _nestedframe(builder, node, nothing, 1, _NESTED_FRAME_ROOT,
        :ordinary, UInt8(0), Int16(0), Int16(0), nothing, children, count)
end

function _nestedstartleaf(builder::_NestedPlanBuilder, node::SchemaNode,
        parentframe, depth::Int, owned_repeated::Bool)
    _nestedrejectcollectionplacement(node, "primitive field")
    if node.element.repetition_type == Metadata.FieldRepetitionType.REPEATED &&
            !owned_repeated
        node.max_definition_level > 0 || throw(FormatError(
            "repeated field $(repr(node.element.name)) has an invalid definition level"))
        _nestedcheckdepth(builder, depth)
        _nestedclaim!(builder)
        definition = node.max_definition_level - Int16(1)
        frame = _nestedframe(builder, node, parentframe, depth,
            _NESTED_FRAME_REPEATED, :unannotated_repeated, UInt8(0),
            definition, definition, node, nothing, 1)
        return (nothing, frame)
    end
    parent, present = _nestedthresholds(node; owned_repeated=owned_repeated)
    node.column_index > 0 || throw(FormatError(
        "primitive field $(repr(node.element.name)) has no column index"))
    length(builder.leaves) < builder.leafmaximum || throw(FormatError(
        "nested schema contains more physical leaf occurrences than its raw leaf list"))
    _nestedcheckdepth(builder, depth)
    _nestedclaim!(builder)
    range = node.column_index:node.column_index
    plan = _NestedLeafPlan(node, parent, present, range)
    push!(builder.leaves, plan)
    return (plan, nothing)
end

function _nestedstartstruct(builder::_NestedPlanBuilder, node::SchemaNode,
        parentframe, depth::Int, owned_repeated::Bool)
    parent, present = _nestedthresholds(node; owned_repeated=owned_repeated)
    _nestedcheckdepth(builder, depth)
    _nestedclaim!(builder)
    count = length(node.children)
    _checklimit(:container_elements, count,
        builder.limits.max_container_elements)
    _reservearray!(builder.budget, _NestedPlan, count)
    children = _NestedPlan[]
    sizehint!(children, count)
    frame = _nestedframe(builder, node, parentframe, depth,
        _NESTED_FRAME_STRUCT, :ordinary, UInt8(0), parent, present, nothing,
        children, count)
    return (nothing, frame)
end

function _nestedstartlist(builder::_NestedPlanBuilder, node::SchemaNode,
        parentframe, depth::Int, annotation::Symbol, owned_repeated::Bool)
    parent, present = _nestedthresholds(node; owned_repeated=owned_repeated)
    length(node.children) == 1 || throw(FormatError(
        "LIST field $(repr(node.element.name)) must have exactly one child"))
    entry = node.children[1]
    entry.element.repetition_type == Metadata.FieldRepetitionType.REPEATED ||
        throw(FormatError("LIST child $(repr(entry.element.name)) must be REPEATED"))
    rule = _nestedlistrule(node, entry)
    if rule == UInt8(6)
        kind, _ = _nestedgroupkind(entry)
        kind === :struct || throw(FormatError(
            "annotated repeated LIST wrapper $(repr(entry.element.name)) cannot be unwrapped"))
    end
    _nestedcheckdepth(builder, depth)
    _nestedcheckdepth(builder, depth + 1)
    _nestedclaim!(builder)
    frame = _nestedframe(builder, node, parentframe, depth, _NESTED_FRAME_LIST,
        annotation, rule, parent, present, entry, nothing, 1)
    return (nothing, frame)
end

function _nestedstartmap(builder::_NestedPlanBuilder, node::SchemaNode,
        parentframe, depth::Int, annotation::Symbol, owned_repeated::Bool)
    parent, present = _nestedthresholds(node; owned_repeated=owned_repeated)
    length(node.children) == 1 || throw(FormatError(
        "MAP field $(repr(node.element.name)) must have exactly one child"))
    entry = node.children[1]
    entry.element.type_ === nothing || throw(FormatError(
        "MAP entry $(repr(entry.element.name)) must be a group"))
    entry.element.repetition_type == Metadata.FieldRepetitionType.REPEATED ||
        throw(FormatError("MAP entry $(repr(entry.element.name)) must be REPEATED"))
    count = length(entry.children)
    1 <= count <= 2 || throw(FormatError(
        "MAP entry $(repr(entry.element.name)) must have one or two children"))
    marker = _nestedmapentrymarker(entry)
    keynode = entry.children[1]
    _nestedvalidatemapchild(keynode, "key")
    valuenode = count == 2 ? entry.children[2] : nothing
    valuenode === nothing || _nestedvalidatemapchild(valuenode, "value")
    _nestedcheckdepth(builder, depth)
    _nestedcheckdepth(builder, depth + 1)
    _nestedclaim!(builder)
    optionalkey = keynode.element.repetition_type ==
        Metadata.FieldRepetitionType.OPTIONAL
    frame = _nestedframe(builder, node, parentframe, depth, _NESTED_FRAME_MAP,
        annotation, UInt8(0), parent, present, entry, nothing, count;
        optional_key=optionalkey, entry_has_map_key_value=marker)
    return (nothing, frame)
end

function _nestedstartnode(builder::_NestedPlanBuilder, node::SchemaNode,
        parentframe, depth::Int; owned_repeated::Bool=false)
    node.element.type_ === nothing || return _nestedstartleaf(builder, node,
        parentframe, depth, owned_repeated)
    kind, annotation = _nestedgroupkind(node)
    repetition = node.element.repetition_type
    if kind !== :struct
        if repetition == Metadata.FieldRepetitionType.REPEATED &&
                !owned_repeated
            throw(FormatError("annotated collection $(repr(node.element.name)) " *
                "cannot be REPEATED outside a parent LIST compatibility form"))
        end
        kind === :list && return _nestedstartlist(builder, node, parentframe,
            depth, annotation, owned_repeated)
        return _nestedstartmap(builder, node, parentframe, depth, annotation,
            owned_repeated)
    end
    if repetition == Metadata.FieldRepetitionType.REPEATED && !owned_repeated
        node.max_definition_level > 0 || throw(FormatError(
            "repeated field $(repr(node.element.name)) has an invalid definition level"))
        _nestedcheckdepth(builder, depth)
        _nestedclaim!(builder)
        definition = node.max_definition_level - Int16(1)
        frame = _nestedframe(builder, node, parentframe, depth,
            _NESTED_FRAME_REPEATED, :unannotated_repeated, UInt8(0),
            definition, definition, node, nothing, 1)
        return (nothing, frame)
    end
    return _nestedstartstruct(builder, node, parentframe, depth,
        owned_repeated)
end

function _nestedframenext(frame::_NestedCompileFrame)
    if frame.mode in (_NESTED_FRAME_ROOT, _NESTED_FRAME_STRUCT)
        children = frame.children::Vector{_NestedPlan}
        node = frame.node.children[frame.completed + 1]
        length(children) == frame.completed || throw(AssertionError(
            "nested struct frame result count is inconsistent"))
        return (node, frame.depth + 1, false)
    elseif frame.mode == _NESTED_FRAME_LIST
        entry = frame.entry::SchemaNode
        if frame.rule == UInt8(6)
            return (entry.children[1], frame.depth + 2, false)
        end
        return (entry, frame.depth + 1, true)
    elseif frame.mode == _NESTED_FRAME_MAP
        entry = frame.entry::SchemaNode
        return (entry.children[frame.completed + 1], frame.depth + 2, false)
    end
    return (frame.node, frame.depth, true)
end

function _nestedframeaccept!(frame::_NestedCompileFrame, plan::_NestedPlan)
    if frame.mode in (_NESTED_FRAME_ROOT, _NESTED_FRAME_STRUCT)
        push!(frame.children::Vector{_NestedPlan}, plan)
    elseif frame.completed == 0
        frame.firstplan = plan
    else
        frame.secondplan = plan
    end
    frame.completed += 1
    return
end

function _nestedfinishframe(frame::_NestedCompileFrame)
    if frame.mode in (_NESTED_FRAME_ROOT, _NESTED_FRAME_STRUCT)
        children = frame.children::Vector{_NestedPlan}
        range = _nestedmergeranges(children)
        if frame.mode == _NESTED_FRAME_STRUCT && isempty(range) &&
                frame.present_definition != frame.parent_definition
            throw(FormatError("optional leafless group " *
                "$(repr(frame.node.element.name)) has no physical leaf that " *
                "records its presence"))
        end
        return _NestedStructPlan(frame.node, frame.parent_definition,
            frame.present_definition, children, range)
    elseif frame.mode == _NESTED_FRAME_LIST
        entry = frame.entry::SchemaNode
        element = frame.firstplan::_NestedPlan
        range = _nestedleafrange(element)
        isempty(range) && throw(FormatError("LIST field " *
            "$(repr(frame.node.element.name)) has no physical leaf that " *
            "records its entries"))
        return _NestedListPlan(frame.node, entry, frame.parent_definition,
            frame.present_definition, entry.max_definition_level,
            entry.max_repetition_level, element, frame.annotation, frame.rule,
            range)
    elseif frame.mode == _NESTED_FRAME_MAP
        entry = frame.entry::SchemaNode
        key = frame.firstplan::_NestedPlan
        value = frame.secondplan
        range = value === nothing ? _nestedleafrange(key) :
            _nestedmergeranges(_nestedleafrange(key), _nestedleafrange(value))
        isempty(range) && throw(FormatError("MAP field " *
            "$(repr(frame.node.element.name)) has no physical leaf that " *
            "records its entries"))
        return _NestedMapPlan(frame.node, entry, frame.parent_definition,
            frame.present_definition, entry.max_definition_level,
            entry.max_repetition_level, key, value, frame.annotation,
            frame.optional_key, frame.entry_has_map_key_value, range)
    end
    element = frame.firstplan::_NestedPlan
    range = _nestedleafrange(element)
    isempty(range) && throw(FormatError("repeated field " *
        "$(repr(frame.node.element.name)) has no physical leaf that records " *
        "its entries"))
    return _NestedListPlan(frame.node, frame.node, frame.parent_definition,
        frame.present_definition, frame.node.max_definition_level,
        frame.node.max_repetition_level, element, frame.annotation, frame.rule,
        range)
end

function _nestedcompileiterative(builder::_NestedPlanBuilder,
    rootnode::SchemaNode)
    current = _nestedstartroot(builder, rootnode)
    pending::Union{Nothing,_NestedPlan} = nothing
    while true
        if pending !== nothing
            _nestedframeaccept!(current, pending)
            pending = nothing
        end
        if current.completed == current.expected
            pending = _nestedfinishframe(current)
            parent = current.parent
            _release!(builder.budget, _MATERIALIZED_OBJECT_BYTES)
            parent === nothing && return pending::_NestedStructPlan
            current = parent::_NestedCompileFrame
            continue
        end
        node, depth, owned = _nestedframenext(current)
        pending, childframe = _nestedstartnode(builder, node, current, depth;
            owned_repeated=owned)
        childframe === nothing || (current = childframe::_NestedCompileFrame)
    end
end

function _nestedvalidateleafplans(schema::Schema, root::_NestedStructPlan,
    leaves::Vector{_NestedLeafPlan})
    length(leaves) == length(schema.leaves) || throw(FormatError(
        "nested schema does not cover every physical leaf exactly once"))
    for index in eachindex(leaves)
        plan = leaves[index]
        expected = schema.leaves[index]
        plan.source.column_index == index || throw(FormatError(
            "nested schema physical leaves are not in column order"))
        plan.source.path == expected.path && plan.source.element == expected.element ||
            throw(FormatError("nested schema leaf plan does not match the raw schema"))
    end
    leafcount = _nestedleafordinalcount(length(leaves))
    expectedrange = iszero(leafcount) ? _nestedemptyrange() :
        Int32(1):leafcount
    root.leaf_range == expectedrange || throw(FormatError(
        "nested schema root does not have a contiguous physical leaf range"))
    return
end

function _nestedplan(schema::Schema; limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    start = _budgetused(budget)
    try
        rootnode = schema.root
        rootnode.element.type_ === nothing ||
            throw(FormatError("schema root must be a group"))
        _nestedrejectcollectionplacement(rootnode, "schema root")
        kind, _ = _nestedgroupkind(rootnode)
        kind === :struct || throw(FormatError(
            "schema root must be an ordinary group"))
        _nestedleafordinalcount(length(schema.leaves))
        _reservearray!(budget, _NestedLeafPlan, length(schema.leaves))
        leaves = _NestedLeafPlan[]
        sizehint!(leaves, length(schema.leaves))
        _reserveobjects!(budget)
        builder = _NestedPlanBuilder(limits, budget, Int64(0), leaves,
            length(schema.leaves))
        root = _nestedcompileiterative(builder, rootnode)
        _nestedvalidateleafplans(schema, root, builder.leaves)
        _reserveobjects!(budget)
        plan = _NestedSchemaPlan(schema, root, builder.leaves, builder.count)
        _release!(budget, _MATERIALIZED_OBJECT_BYTES)
        return plan
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end
