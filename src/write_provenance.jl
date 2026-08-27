# Exact schema-bearing writes for materialized Parquet.Table values.

abstract type _ProvenanceBinding end

struct _ProvenanceLeafBinding <: _ProvenanceBinding
    plan::_NestedLeafPlan
    values::AbstractVector
    force_required::Bool
    snapshot::_NestedWriteVectorSnapshot
end

struct _ProvenanceStructBinding <: _ProvenanceBinding
    plan::_NestedStructPlan
    values::StructVector
    children::Vector{_ProvenanceBinding}
    force_required::Bool
    snapshot::_NestedWriteVectorSnapshot
end

struct _ProvenanceListBinding <: _ProvenanceBinding
    plan::_NestedListPlan
    values::ListVector
    element::_ProvenanceBinding
    force_required::Bool
    snapshot::_NestedWriteVectorSnapshot
end

struct _ProvenanceMapBinding <: _ProvenanceBinding
    plan::_NestedMapPlan
    values::MapVector
    key::_ProvenanceBinding
    value::Union{Nothing,_ProvenanceBinding}
    force_required::Bool
    snapshot::_NestedWriteVectorSnapshot
end

mutable struct _ProvenanceKeyReplayFrame
    parent::Union{Nothing,_ProvenanceKeyReplayFrame}
    binding::_ProvenanceBinding
    snapshot::_NestedWriteKeySnapshot
    repetition::UInt64
    depth::Int
    position::Int
end

mutable struct _ProvenanceSchemaCompareFrame
    parent::Union{Nothing,_ProvenanceSchemaCompareFrame}
    fresh::SchemaNode
    stored::SchemaNode
    depth::Int
    position::Int
end

mutable struct _ProvenancePlanWalkFrame
    parent::Union{Nothing,_ProvenancePlanWalkFrame}
    plan::_NestedPlan
    position::Int
    expected::Int
end

mutable struct _ProvenanceSchemaWalkFrame
    parent::Union{Nothing,_ProvenanceSchemaWalkFrame}
    node::SchemaNode
    depth::Int
    position::Int
end

const _PROVENANCE_BIND_STRUCT = UInt8(1)
const _PROVENANCE_BIND_LIST = UInt8(2)
const _PROVENANCE_BIND_MAP = UInt8(3)

mutable struct _ProvenanceBindFrame
    parent::Union{Nothing,_ProvenanceBindFrame}
    plan::_NestedPlan
    values::AbstractVector
    force_required::Bool
    mode::UInt8
    children::Union{Nothing,Vector{_ProvenanceBinding}}
    firstbinding::Union{Nothing,_ProvenanceBinding}
    secondbinding::Union{Nothing,_ProvenanceBinding}
    expected::Int
    completed::Int
end

mutable struct _ProvenanceValidateFrame
    parent::Union{Nothing,_ProvenanceValidateFrame}
    binding::_ProvenanceBinding
    childcount::Int
    position::Int
    expected::Int
end

mutable struct _ProvenanceShredFrame
    parent::Union{Nothing,_ProvenanceShredFrame}
    binding::_ProvenanceBinding
    repetition::UInt64
    childindex::Int
    position::Int
    last::Int
    witness::_NestedWriteRowWitness
end

function _provenanceframesrelease!(budget::_LiveByteBudget, count::Int)
    iszero(count) && return
    _release!(budget,
        _materializedproduct(count, _MATERIALIZED_OBJECT_BYTES))
    return
end

struct _ProvenanceWriteFields <: AbstractVector{WriteFieldPlan}
    fields::Vector{WriteFieldPlan}
    elements::Vector{Metadata.SchemaElement}
    schema::Schema
end

function Base.IndexStyle(::Type{_ProvenanceWriteFields})
    return IndexLinear()
end

function Base.size(fields::_ProvenanceWriteFields)
    return size(fields.fields)
end

function Base.getindex(fields::_ProvenanceWriteFields, index::Int)
    return fields.fields[index]
end

function _provenanceexact(left::Thrift.RawField, right::Thrift.RawField)
    return left.id == right.id && left.type == right.type &&
        left.previd == right.previd && left.headerlength == right.headerlength &&
        left.bytes == right.bytes
end

function _provenanceexact(left::Tuple, right::Tuple)
    length(left) == length(right) || return false
    for index in eachindex(left, right)
        _provenanceexact(left[index], right[index]) || return false
    end
    return true
end

function _provenanceexact(left, right)
    typeof(left) === typeof(right) || return false
    T = typeof(left)
    if isstructtype(T) && hasfield(T, :unknown_fields)
        for index in 1:fieldcount(T)
            _provenanceexact(getfield(left, index), getfield(right, index)) ||
                return false
        end
        return true
    end
    return isequal(left, right)
end

function _provenanceclone(value::Thrift.RawField)
    return Thrift.RawField(value.id, value.type, value.previd,
        value.headerlength, copy(value.bytes))
end

function _provenanceclone(value::Tuple)
    return map(_provenanceclone, value)
end

function _provenanceclone(value)
    T = typeof(value)
    if isstructtype(T) && hasfield(T, :unknown_fields)
        fields = ntuple(index -> _provenanceclone(getfield(value, index)),
            fieldcount(T))
        return T(fields...)
    end
    return value
end

function _provenanceclonecharge(value::Thrift.RawField)
    return _materializedsum(_MATERIALIZED_OBJECT_BYTES,
        _materializedarraybytes(UInt8, length(value.bytes)))
end

function _provenanceclonecharge(value::Tuple)
    bytes = Int64(0)
    for item in value
        bytes = _materializedsum(bytes, _provenanceclonecharge(item))
    end
    return bytes
end

function _provenanceclonecharge(value)
    T = typeof(value)
    isstructtype(T) && hasfield(T, :unknown_fields) || return Int64(0)
    bytes = _MATERIALIZED_OBJECT_BYTES
    for index in 1:fieldcount(T)
        bytes = _materializedsum(bytes,
            _provenanceclonecharge(getfield(value, index)))
    end
    return bytes
end

function _provenancecomparelabel(label::String, index::Int)
    iszero(index) && return label
    return "$label $index"
end

function _provenancecompareschemadirect(fresh::SchemaNode,
        stored::SchemaNode, label::String, labelindex::Int)
    _provenanceexact(fresh.element, stored.element) || throw(ArgumentError(
        "$(_provenancecomparelabel(label, labelindex)) has a SchemaElement " *
        "that differs from table.metadata.schema"))
    fresh.path == stored.path || throw(ArgumentError(
        "$(_provenancecomparelabel(label, labelindex)) has a path that differs " *
        "from table.metadata.schema"))
    fresh.max_definition_level == stored.max_definition_level ||
        throw(ArgumentError(
            "$(_provenancecomparelabel(label, labelindex)) has a changed definition level"))
    fresh.max_repetition_level == stored.max_repetition_level ||
        throw(ArgumentError(
            "$(_provenancecomparelabel(label, labelindex)) has a changed repetition level"))
    fresh.column_index == stored.column_index || throw(ArgumentError(
        "$(_provenancecomparelabel(label, labelindex)) has a changed physical leaf ordinal"))
    length(fresh.children) == length(stored.children) || throw(ArgumentError(
        "$(_provenancecomparelabel(label, labelindex)) has changed child topology"))
    return
end

function _provenancecompareframe(fresh::SchemaNode, stored::SchemaNode,
        label::String, labelindex::Int, parent, depth::Int, limits::Limits,
        budget::_LiveByteBudget)
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    _checklimit(:container_elements, length(fresh.children),
        limits.max_container_elements)
    _provenancecompareschemadirect(fresh, stored, label, labelindex)
    _reserveobjects!(budget)
    return _ProvenanceSchemaCompareFrame(parent, fresh, stored, depth, 0)
end

function _provenancecompareschemanode(fresh::SchemaNode,
        stored::SchemaNode, label::String, labelindex::Int, limits::Limits,
        budget::_LiveByteBudget)
    start = _budgetused(budget)
    current::Union{Nothing,_ProvenanceSchemaCompareFrame} = nothing
    activeframes = 0
    try
        current = _provenancecompareframe(fresh, stored, label, labelindex,
            nothing, 1, limits, budget)
        activeframes = 1
        while true
            if current.position == length(current.fresh.children)
                parent = current.parent
                _release!(budget, _MATERIALIZED_OBJECT_BYTES)
                activeframes -= 1
                if parent === nothing
                    current = nothing
                    return
                end
                current = parent::_ProvenanceSchemaCompareFrame
                continue
            end
            current.position += 1
            index = current.position
            child = current.fresh.children[index]
            storedchild = current.stored.children[index]
            childdepth = _nestedwritedepthadd(current.depth, 1, limits)
            current = _provenancecompareframe(child, storedchild,
                "stored schema node", 0, current, childdepth, limits, budget)
            activeframes += 1
        end
    finally
        _provenanceframesrelease!(budget, activeframes)
        used = _budgetused(budget)
        used >= start || throw(AssertionError(
            "schema comparison released caller-owned budget"))
        used > start && _release!(budget, used - start)
    end
end

function _provenancecompareschema(fresh::Schema, stored::Schema,
        limits::Limits, budget::_LiveByteBudget)
    _provenancecompareschemanode(fresh.root, stored.root,
        "stored schema root", 0, limits, budget)
    length(fresh.leaves) == length(stored.leaves) || throw(ArgumentError(
        "stored schema has changed physical leaf ordering"))
    for index in eachindex(fresh.leaves, stored.leaves)
        _provenancecompareschemanode(fresh.leaves[index], stored.leaves[index],
            "stored schema leaf", index, limits, budget)
    end
    return
end

function _provenancefreshschema(table::Table, limits::Limits,
        budget::_LiveByteBudget)
    source = table.metadata.schema
    _checklimit(:container_elements, length(source), limits.max_container_elements)
    charge = _materializedarraybytes(Metadata.SchemaElement, length(source))
    for element in source
        charge = _materializedsum(charge, _provenanceclonecharge(element))
    end
    _reserve!(budget, charge)
    elements = Vector{Metadata.SchemaElement}(undef, length(source))
    for index in eachindex(source)
        elements[index] = _provenanceclone(source[index])
    end
    schema = Schema(elements; limits=limits, budget=budget)
    _provenancecompareschema(schema, table.schema, limits, budget)
    return elements, schema
end

function _provenancerequired(plan::_NestedPlan, force_required::Bool)
    return force_required || getfield(plan, :parent_definition) ==
        getfield(plan, :present_definition)
end

function _provenancepathlabel(prefix::String, node::SchemaNode,
        suffix::String)
    return "$prefix $(repr(join(node.path, ".")))$suffix"
end

function _provenancecheckvector(values::AbstractVector, count::Int,
        prefix::String, node::SchemaNode, suffix::String, limits::Limits)
    length(values) == count || throw(ArgumentError(
        "$(_provenancepathlabel(prefix, node, suffix)) has " *
        "$(length(values)) values; expected $count"))
    axes(values, 1) == Base.OneTo(count) || throw(ArgumentError(
        "$(_provenancepathlabel(prefix, node, suffix)) must use one-based " *
        "contiguous axes"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    return
end

function _provenancecontaineradd(value::Int, increment::Int, limits::Limits)
    requested = try
        Base.checked_add(value, increment)
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64),
            limits.max_container_elements))
    end
    _checklimit(:container_elements, requested, limits.max_container_elements)
    return requested
end

function _provenanceleaflogicaltype(plan::_NestedLeafPlan)
    element = plan.source.element
    physical = _physicaleltype(element.type_)
    return _logicaleltype(element, physical)
end

function _provenanceleafexpectedtype(plan::_NestedLeafPlan,
        force_required::Bool)
    logical = _provenanceleaflogicaltype(plan)
    _provenancerequired(plan, force_required) && return logical
    return Union{Missing,logical}
end

function _provenancevalidateleaf(binding::_ProvenanceLeafBinding, count::Int,
        limits::Limits)
    plan = binding.plan
    values = binding.values
    _provenancecheckvector(values, count, "physical leaf", plan.source, "",
        limits)
    expected = _provenanceleafexpectedtype(plan, binding.force_required)
    eltype(values) == expected || throw(ArgumentError(
        "physical leaf $(repr(join(plan.source.path, "."))) has logical " *
        "element type $(eltype(values)); expected $expected"))
    element = plan.source.element
    fixed = element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
        _logicalkind(element) === nothing
    if fixed
        values isa FixedByteArrayVector || throw(ArgumentError(
            "raw fixed leaf $(repr(join(plan.source.path, "."))) lost its width"))
        values.width == element.type_length || throw(ArgumentError(
            "raw fixed leaf $(repr(join(plan.source.path, "."))) changed width"))
    else
        values isa FixedByteArrayVector && throw(ArgumentError(
            "logical leaf $(repr(join(plan.source.path, "."))) has an unexpected " *
            "raw fixed-width wrapper"))
    end
    return
end

function _provenancecheckindices(indices::AbstractVector{<:Integer}, count::Int,
        prefix::String, node::SchemaNode, suffix::String, limits::Limits;
        ranks::Bool=false)
    expected = _provenancecontaineradd(count, 1, limits)
    _provenancecheckvector(indices, expected, prefix, node, suffix, limits)
    iszero(first(indices)) || throw(ArgumentError(
        "$(_provenancepathlabel(prefix, node, suffix)) must start at zero"))
    previous = Int64(0)
    for index in 2:length(indices)
        value = indices[index]
        value >= previous || throw(ArgumentError(
            "$(_provenancepathlabel(prefix, node, suffix)) must be nondecreasing"))
        value <= typemax(Int) || throw(ArgumentError(
            "$(_provenancepathlabel(prefix, node, suffix)) exceeds the Julia index range"))
        ranks && value - previous > 1 && throw(ArgumentError(
            "$(_provenancepathlabel(prefix, node, suffix)) has a rank difference above one"))
        previous = Int64(value)
    end
    _checklimit(:container_elements, previous, limits.max_container_elements)
    return Int(previous)
end

function _provenancevalidatevalidity(validity::BitVector,
        offsets::AbstractVector{<:Integer}, count::Int, prefix::String,
        node::SchemaNode, limits::Limits)
    _provenancecheckvector(validity, count, prefix, node, " validity", limits)
    for index in 1:count
        validity[index] || offsets[index] == offsets[index + 1] ||
            throw(ArgumentError("null $(_provenancepathlabel(prefix, node, "")) " *
                "row $index has a nonempty span"))
    end
    return
end

function _provenancevalidatestruct(binding::_ProvenanceStructBinding,
        count::Int, limits::Limits)
    plan = binding.plan
    values = binding.values
    length(values) == count && values.rows == count || throw(ArgumentError(
        "struct $(repr(join(plan.source.path, "."))) changed its row count"))
    length(values.names) == length(plan.children) || throw(ArgumentError(
        "struct $(repr(join(plan.source.path, "."))) changed its field count"))
    length(values.children) == length(plan.children) || throw(ArgumentError(
        "struct $(repr(join(plan.source.path, "."))) changed its child count"))
    for index in eachindex(plan.children)
        values.names[index] == plan.children[index].source.element.name ||
            throw(ArgumentError("struct $(repr(join(plan.source.path, "."))) " *
                "changed field name or order at position $index"))
    end
    required = _provenancerequired(plan, binding.force_required)
    if required
        values.ranks === nothing || throw(ArgumentError(
            "required struct $(repr(join(plan.source.path, "."))) gained validity"))
        childcount = count
    else
        values.ranks === nothing && throw(ArgumentError(
            "optional struct $(repr(join(plan.source.path, "."))) lost validity"))
        childcount = _provenancecheckindices(values.ranks, count, "struct",
            plan.source, " ranks", limits; ranks=true)
    end
    return childcount
end

function _provenancevalidatelist(binding::_ProvenanceListBinding, count::Int,
        limits::Limits)
    plan = binding.plan
    values = binding.values
    _provenancecheckvector(values, count, "list", plan.source, "", limits)
    entries = _provenancecheckindices(values.offsets, count, "list",
        plan.source, " offsets", limits)
    required = _provenancerequired(plan, binding.force_required)
    if required
        values.validity === nothing || throw(ArgumentError(
            "required list $(repr(join(plan.source.path, "."))) gained validity"))
    else
        values.validity === nothing && throw(ArgumentError(
            "optional list $(repr(join(plan.source.path, "."))) lost validity"))
        _provenancevalidatevalidity(values.validity, values.offsets, count,
            "list", plan.source, limits)
    end
    length(values.values) == entries || throw(ArgumentError(
        "list $(repr(join(plan.source.path, "."))) terminal offset changed"))
    return entries
end

function _provenancevalidatemap(binding::_ProvenanceMapBinding, count::Int,
        limits::Limits)
    plan = binding.plan
    values = binding.values
    _provenancecheckvector(values, count, "map", plan.source, "", limits)
    entries = _provenancecheckindices(values.offsets, count, "map",
        plan.source, " offsets", limits)
    required = _provenancerequired(plan, binding.force_required)
    if required
        values.validity === nothing || throw(ArgumentError(
            "required map $(repr(join(plan.source.path, "."))) gained validity"))
    else
        values.validity === nothing && throw(ArgumentError(
            "optional map $(repr(join(plan.source.path, "."))) lost validity"))
        _provenancevalidatevalidity(values.validity, values.offsets, count,
            "map", plan.source, limits)
    end
    length(values.keys) == entries || throw(ArgumentError(
        "map $(repr(join(plan.source.path, "."))) key count changed"))
    if plan.value === nothing
        values.values === nothing || throw(ArgumentError(
            "key-only map $(repr(join(plan.source.path, "."))) gained values"))
    else
        values.values === nothing && throw(ArgumentError(
            "map $(repr(join(plan.source.path, "."))) lost its values"))
        length(values.values) == entries || throw(ArgumentError(
            "map $(repr(join(plan.source.path, "."))) value count changed"))
    end
    return entries
end

function _provenancevalidateframe(binding::_ProvenanceBinding,
        childcount::Int, expected::Int, parent, budget::_LiveByteBudget)
    _reserveobjects!(budget)
    return _ProvenanceValidateFrame(parent, binding, childcount, 0, expected)
end

function _provenancevalidatestart(binding::_ProvenanceLeafBinding,
        count::Int, limits::Limits, ::Union{Nothing,_ProvenanceValidateFrame},
        ::_LiveByteBudget)
    _provenancevalidateleaf(binding, count, limits)
    return nothing
end

function _provenancevalidatestart(binding::_ProvenanceStructBinding,
        count::Int, limits::Limits, parent, budget::_LiveByteBudget)
    childcount = _provenancevalidatestruct(binding, count, limits)
    isempty(binding.children) && return nothing
    return _provenancevalidateframe(binding, childcount,
        length(binding.children), parent, budget)
end

function _provenancevalidatestart(binding::_ProvenanceListBinding,
        count::Int, limits::Limits, parent, budget::_LiveByteBudget)
    childcount = _provenancevalidatelist(binding, count, limits)
    return _provenancevalidateframe(binding, childcount, 1, parent, budget)
end

function _provenancevalidatestart(binding::_ProvenanceMapBinding,
        count::Int, limits::Limits, parent, budget::_LiveByteBudget)
    childcount = _provenancevalidatemap(binding, count, limits)
    expected = binding.value === nothing ? 1 : 2
    return _provenancevalidateframe(binding, childcount, expected, parent,
        budget)
end

function _provenancevalidatenext(frame::_ProvenanceValidateFrame)
    frame.position += 1
    binding = frame.binding
    if binding isa _ProvenanceStructBinding
        child = binding.children[frame.position]
        values = binding.values
        values.children[frame.position] === _provenancesource(child) ||
            throw(ArgumentError("struct " *
                "$(repr(join(binding.plan.source.path, "."))) changed child " *
                "identity at position $(frame.position)"))
        return child
    elseif binding isa _ProvenanceListBinding
        return binding.element
    elseif binding isa _ProvenanceMapBinding
        frame.position == 1 && return binding.key
        return something(binding.value)
    end
    throw(AssertionError("unknown schema-bearing validation binding"))
end

function _provenancevalidate(binding::_ProvenanceBinding, count::Int,
        limits::Limits, budget::_LiveByteBudget)
    start = _budgetused(budget)
    current::Union{Nothing,_ProvenanceValidateFrame} = nothing
    activeframes = 0
    try
        current = _provenancevalidatestart(binding, count, limits, nothing,
            budget)
        current === nothing && return
        activeframes = 1
        while true
            if current.position == current.expected
                parent = current.parent
                _release!(budget, _MATERIALIZED_OBJECT_BYTES)
                activeframes -= 1
                if parent === nothing
                    current = nothing
                    return
                end
                current = parent::_ProvenanceValidateFrame
                continue
            end
            child = _provenancevalidatenext(current)
            childframe = _provenancevalidatestart(child, current.childcount,
                limits, current, budget)
            if childframe !== nothing
                activeframes += 1
                current = childframe::_ProvenanceValidateFrame
            end
        end
    finally
        _provenanceframesrelease!(budget, activeframes)
        used = _budgetused(budget)
        used >= start || throw(AssertionError(
            "schema-bearing validation released caller-owned budget"))
        used > start && _release!(budget, used - start)
    end
end

function _provenancebindstart(plan::_NestedLeafPlan, values,
        force_required::Bool, ::Union{Nothing,_ProvenanceBindFrame},
        limits::Limits, budget::_LiveByteBudget,
        topology::_NestedWriteTopologySnapshot)
    depth = _nestedwritedepthadd(length(plan.source.path), 1, limits)
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    values isa AbstractVector || throw(ArgumentError(
        "physical leaf $(repr(join(plan.source.path, "."))) is not a vector"))
    _reserveobjects!(budget)
    return (_ProvenanceLeafBinding(plan, values, force_required,
        _nestedwritesnapshotfor(topology, values)), nothing)
end

function _provenancebindframe(parent, plan::_NestedPlan,
        values::AbstractVector, force_required::Bool, mode::UInt8,
        children::Union{Nothing,Vector{_ProvenanceBinding}}, expected::Int,
        budget::_LiveByteBudget)
    _reserveobjects!(budget)
    return _ProvenanceBindFrame(parent, plan, values, force_required, mode,
        children, nothing, nothing, expected, 0)
end

function _provenancebindstart(plan::_NestedStructPlan, values,
        force_required::Bool, parent, limits::Limits,
        budget::_LiveByteBudget, ::_NestedWriteTopologySnapshot)
    depth = _nestedwritedepthadd(length(plan.source.path), 1, limits)
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    values isa StructVector || throw(ArgumentError(
        "schema struct $(repr(join(plan.source.path, "."))) requires StructVector"))
    length(values.children) == length(plan.children) || throw(ArgumentError(
        "schema struct $(repr(join(plan.source.path, "."))) has a changed child count"))
    _checklimit(:container_elements, length(plan.children),
        limits.max_container_elements)
    _reservearray!(budget, _ProvenanceBinding, length(plan.children))
    children = _ProvenanceBinding[]
    sizehint!(children, length(plan.children))
    return (nothing, _provenancebindframe(parent, plan, values,
        force_required, _PROVENANCE_BIND_STRUCT, children,
        length(plan.children), budget))
end

function _provenancebindstart(plan::_NestedListPlan, values,
        force_required::Bool, parent, limits::Limits,
        budget::_LiveByteBudget, ::_NestedWriteTopologySnapshot)
    depth = _nestedwritedepthadd(length(plan.source.path), 1, limits)
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    values isa ListVector || throw(ArgumentError(
        "schema list $(repr(join(plan.source.path, "."))) requires ListVector"))
    return (nothing, _provenancebindframe(parent, plan, values,
        force_required, _PROVENANCE_BIND_LIST, nothing, 1, budget))
end

function _provenancebindstart(plan::_NestedMapPlan, values,
        force_required::Bool, parent, limits::Limits,
        budget::_LiveByteBudget, ::_NestedWriteTopologySnapshot)
    depth = _nestedwritedepthadd(length(plan.source.path), 1, limits)
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    values isa MapVector || throw(ArgumentError(
        "schema map $(repr(join(plan.source.path, "."))) requires MapVector"))
    if plan.value === nothing
        values.values === nothing || throw(ArgumentError(
            "key-only schema map $(repr(join(plan.source.path, "."))) gained " *
            "a value vector"))
        expected = 1
    else
        values.values === nothing && throw(ArgumentError(
            "schema map $(repr(join(plan.source.path, "."))) lost its value vector"))
        expected = 2
    end
    return (nothing, _provenancebindframe(parent, plan, values,
        force_required, _PROVENANCE_BIND_MAP, nothing, expected, budget))
end

function _provenancebindnext(frame::_ProvenanceBindFrame)
    plan = frame.plan
    values = frame.values
    index = frame.completed + 1
    if frame.mode == _PROVENANCE_BIND_STRUCT
        structplan = plan::_NestedStructPlan
        structvalues = values::StructVector
        return structplan.children[index], structvalues.children[index], false
    elseif frame.mode == _PROVENANCE_BIND_LIST
        return (plan::_NestedListPlan).element,
            (values::ListVector).values, false
    elseif frame.mode == _PROVENANCE_BIND_MAP
        mapplan = plan::_NestedMapPlan
        mapvalues = values::MapVector
        index == 1 && return mapplan.key, mapvalues.keys, true
        return something(mapplan.value), something(mapvalues.values), false
    end
    throw(AssertionError("unknown schema-bearing binding frame"))
end

function _provenancebindaccept!(frame::_ProvenanceBindFrame,
        binding::_ProvenanceBinding)
    if frame.mode == _PROVENANCE_BIND_STRUCT
        push!(frame.children::Vector{_ProvenanceBinding}, binding)
    elseif frame.completed == 0
        frame.firstbinding = binding
    else
        frame.secondbinding = binding
    end
    frame.completed += 1
    return
end

function _provenancebindfinish(frame::_ProvenanceBindFrame,
        budget::_LiveByteBudget, topology::_NestedWriteTopologySnapshot)
    _reserveobjects!(budget)
    if frame.mode == _PROVENANCE_BIND_STRUCT
        return _ProvenanceStructBinding(frame.plan::_NestedStructPlan,
            frame.values::StructVector,
            frame.children::Vector{_ProvenanceBinding}, frame.force_required,
            _nestedwritesnapshotfor(topology, frame.values))
    elseif frame.mode == _PROVENANCE_BIND_LIST
        return _ProvenanceListBinding(frame.plan::_NestedListPlan,
            frame.values::ListVector,
            frame.firstbinding::_ProvenanceBinding, frame.force_required,
            _nestedwritesnapshotfor(topology, frame.values))
    end
    return _ProvenanceMapBinding(frame.plan::_NestedMapPlan,
        frame.values::MapVector, frame.firstbinding::_ProvenanceBinding,
        frame.secondbinding, frame.force_required,
        _nestedwritesnapshotfor(topology, frame.values))
end

function _provenancebind(plan::_NestedPlan, values,
        limits::Limits, budget::_LiveByteBudget,
        topology::_NestedWriteTopologySnapshot;
        force_required::Bool=false)
    pending, current = _provenancebindstart(plan, values, force_required,
        nothing, limits, budget, topology)
    pending === nothing || return pending::_ProvenanceBinding
    active::Union{Nothing,_ProvenanceBindFrame} =
        current::_ProvenanceBindFrame
    activeframes = 1
    try
        while true
            if pending !== nothing
                _provenancebindaccept!(active, pending)
                pending = nothing
            end
            if active.completed == active.expected
                pending = _provenancebindfinish(active, budget, topology)
                parent = active.parent
                _release!(budget, _MATERIALIZED_OBJECT_BYTES)
                activeframes -= 1
                parent === nothing && begin
                    active = nothing
                    return pending::_ProvenanceBinding
                end
                active = parent::_ProvenanceBindFrame
                continue
            end
            childplan, childvalues, required = _provenancebindnext(active)
            pending, childframe = _provenancebindstart(childplan, childvalues,
                required, active, limits, budget, topology)
            if childframe !== nothing
                activeframes += 1
                active = childframe::_ProvenanceBindFrame
            end
        end
    finally
        _provenanceframesrelease!(budget, activeframes)
    end
end

function _provenanceplanframe(plan::_NestedPlan, parent,
        budget::_LiveByteBudget)
    expected = plan isa _NestedStructPlan ? length(plan.children) :
        plan isa _NestedListPlan ? 1 : plan isa _NestedMapPlan ?
        (plan.value === nothing ? 1 : 2) : 0
    iszero(expected) && return nothing
    _reserveobjects!(budget)
    return _ProvenancePlanWalkFrame(parent, plan, 0, expected)
end

function _provenanceplannext(frame::_ProvenancePlanWalkFrame)
    frame.position += 1
    plan = frame.plan
    plan isa _NestedStructPlan && return plan.children[frame.position]
    plan isa _NestedListPlan && return plan.element
    plan isa _NestedMapPlan && frame.position == 1 && return plan.key
    plan isa _NestedMapPlan && return something(plan.value)
    throw(AssertionError("unknown schema-bearing plan walk frame"))
end

function _provenancerejectleafless(plan::_NestedPlan,
        budget::_LiveByteBudget)
    isempty(_nestedleafrange(plan)) && throw(ArgumentError(
        "schema-bearing writes do not support zero-leaf group " *
        repr(join(plan.source.path, "."))))
    current::Union{Nothing,_ProvenancePlanWalkFrame} =
        _provenanceplanframe(plan, nothing, budget)
    activeframes = current === nothing ? 0 : 1
    try
        current === nothing && return
        while true
            if current.position == current.expected
                parent = current.parent
                _release!(budget, _MATERIALIZED_OBJECT_BYTES)
                activeframes -= 1
                if parent === nothing
                    current = nothing
                    return
                end
                current = parent::_ProvenancePlanWalkFrame
                continue
            end
            child = _provenanceplannext(current)
            isempty(_nestedleafrange(child)) && throw(ArgumentError(
                "schema-bearing writes do not support zero-leaf group " *
                repr(join(child.source.path, "."))))
            childframe = _provenanceplanframe(child, current, budget)
            if childframe !== nothing
                activeframes += 1
                current = childframe::_ProvenancePlanWalkFrame
            end
        end
    finally
        _provenanceframesrelease!(budget, activeframes)
    end
end

function _provenancebindings(table::Table, semantic::_NestedSchemaPlan,
        limits::Limits, budget::_LiveByteBudget,
        topology::_NestedWriteTopologySnapshot)
    start = _budgetused(budget)
    try
        columns = table.columns
        values = Base.values(columns)
        children = semantic.root.children
        length(values) == length(children) == length(topology.names) ||
            throw(ArgumentError(
                "table columns no longer match the stored Parquet schema"))
        _reservearray!(budget, _ProvenanceBinding, length(children))
        bindings = _ProvenanceBinding[]
        sizehint!(bindings, length(children))
        for index in eachindex(children)
            topology.names[index] == children[index].source.element.name ||
                throw(ArgumentError("table column name or order no longer matches " *
                    "the stored Parquet schema at position $index"))
            push!(bindings, _provenancebind(children[index], values[index],
                limits, budget, topology))
        end
        return bindings
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _provenancesource(binding::_ProvenanceLeafBinding)
    return binding.values
end

function _provenancesource(binding::_ProvenanceStructBinding)
    return binding.values
end

function _provenancesource(binding::_ProvenanceListBinding)
    return binding.values
end

function _provenancesource(binding::_ProvenanceMapBinding)
    return binding.values
end

function _nestedwritekeywitnesssnapshot(binding::_ProvenanceBinding)
    return binding.snapshot
end

function _nestedwritekeywitnessroot(binding::_ProvenanceLeafBinding, value)
    if ismissing(value)
        _provenancerequired(binding.plan, binding.force_required) && throw(
            ArgumentError("required schema-bearing Parquet MAP-key leaf is missing"))
        return
    end
    expected = _provenanceleaflogicaltype(binding.plan)
    value isa expected && !_nestedwritekeyrecursive(value) || throw(
        ArgumentError(
            "schema-bearing Parquet MAP key contains $(typeof(value)); expected $expected"))
    return
end

function _nestedwritekeywitnessroot(binding::_ProvenanceStructBinding, value)
    if ismissing(value)
        _provenancerequired(binding.plan, binding.force_required) && throw(ArgumentError(
            "required schema-bearing Parquet MAP-key struct is missing"))
        return
    end
    value isa StructValue || throw(ArgumentError(
        "schema-bearing Parquet MAP key requires StructValue"))
    return
end

function _nestedwritekeywitnessroot(binding::_ProvenanceListBinding, value)
    if ismissing(value)
        _provenancerequired(binding.plan, binding.force_required) && throw(ArgumentError(
            "required schema-bearing Parquet MAP-key LIST is missing"))
        return
    end
    value isa ListValue || throw(ArgumentError(
        "schema-bearing Parquet MAP key requires ListValue"))
    return
end

function _nestedwritekeywitnessroot(binding::_ProvenanceMapBinding, value)
    if ismissing(value)
        _provenancerequired(binding.plan, binding.force_required) && throw(ArgumentError(
            "required schema-bearing Parquet MAP-key MAP is missing"))
        return
    end
    value isa MapValue || throw(ArgumentError(
        "schema-bearing Parquet MAP key requires MapValue"))
    return
end

function _nestedwritekeywitnesssource(binding::_ProvenanceStructBinding,
        index::Int)
    index <= length(binding.children) || return nothing
    return _provenancesource(binding.children[index])
end

function _nestedwritekeywitnesschild(binding::_ProvenanceStructBinding,
        index::Int)
    index <= length(binding.children) || return nothing
    return binding.children[index]
end

function _nestedwritekeywitnesslist(binding::_ProvenanceListBinding)
    return binding.element
end

function _nestedwritekeywitnessmapkey(binding::_ProvenanceMapBinding)
    return binding.key
end

function _nestedwritekeywitnessmapvalue(binding::_ProvenanceMapBinding)
    return binding.value
end

function _nestedwritekeystoredsource(binding::_ProvenanceStructBinding,
        index::Int)
    return _nestedwritekeywitnesssource(binding, index)
end

function _provenancevalidatetop(table::Table,
        semantic::_NestedSchemaPlan, bindings::Vector{_ProvenanceBinding},
        rows::Int, limits::Limits, topology::_NestedWriteTopologySnapshot,
        budget::_LiveByteBudget)
    table.rows == rows || throw(ArgumentError(
        "table row count changed during schema-bearing write"))
    columns = table.columns
    values = Base.values(columns)
    length(values) == length(bindings) == length(semantic.root.children) ==
        length(topology.names) ||
        throw(ArgumentError("table column count changed during schema-bearing write"))
    for index in eachindex(bindings)
        child = semantic.root.children[index]
        topology.names[index] == child.source.element.name ||
            throw(ArgumentError(
                "table column name or order changed during schema-bearing write"))
        values[index] === _provenancesource(bindings[index]) ||
            throw(ArgumentError("table column identity changed during schema-bearing write"))
        _provenancevalidate(bindings[index], rows, limits, budget)
    end
    return
end

function _provenancetopology(table::Table, limits::Limits,
        budget::_LiveByteBudget)
    columns = table.columns
    raw_names = keys(columns)
    raw_values = Base.values(columns)
    count = length(raw_values)
    retained = _materializedsum(_materializedarraybytes(String, count),
        _materializedarraybytes(AbstractVector, count))
    for raw in raw_names
        retained = _materializedsum(retained,
            _materializedsum(_MATERIALIZED_OBJECT_BYTES,
                _writecolumnnamebytes(raw)))
    end
    _reserve!(budget, retained)
    names = String[]
    values = AbstractVector[]
    sizehint!(names, count)
    sizehint!(values, count)
    for index in eachindex(raw_values)
        value = raw_values[index]
        value isa AbstractVector || throw(ArgumentError(
            "schema-bearing Parquet columns must be vectors"))
        push!(names, String(raw_names[index]))
        push!(values, value)
    end
    return _nestedwritetopology(nothing, names, values, limits, budget;
        retainedcharge=retained)
end

function _provenancevalidateelements(elements::Vector{Metadata.SchemaElement},
        current::Vector{Metadata.SchemaElement})
    length(elements) == length(current) || throw(ArgumentError(
        "stored Parquet SchemaElement count changed during write"))
    for index in eachindex(elements, current)
        _provenanceexact(elements[index], current[index]) || throw(ArgumentError(
            "stored Parquet SchemaElement changed during write"))
    end
    return
end

function _provenancebarrier!(table::Table, elements::Vector{Metadata.SchemaElement},
        schema::Schema, semantic::_NestedSchemaPlan,
        bindings::Vector{_ProvenanceBinding}, rows::Int, limits::Limits,
        topology::_NestedWriteTopologySnapshot, budget::_LiveByteBudget)
    _provenancevalidateelements(elements, table.metadata.schema)
    _provenancecompareschema(schema, table.schema, limits, budget)
    _provenancevalidatetop(table, semantic, bindings, rows, limits, topology,
        budget)
    _nestedwritebarrier!(topology, nothing, budget)
    return
end

function _provenanceleafvalue(binding::_ProvenanceLeafBinding, index::Int)
    values = binding.values
    checkbounds(Bool, values, index) || throw(ArgumentError(
        "schema-bearing physical leaf index exceeds its vector"))
    return values[index]
end

function _provenanceleafaccess(binding::_ProvenanceLeafBinding, index::Int)
    return _nestedwriterowaccess(binding.values, index, binding.snapshot)
end

function _provenancekeyroot(binding::_ProvenanceLeafBinding, value)
    expected = _provenanceleaflogicaltype(binding.plan)
    value isa expected || throw(ArgumentError(
        "schema-bearing Parquet MAP key contains $(typeof(value)); expected $expected"))
    return
end

function _provenancekeyroot(::_ProvenanceStructBinding, value)
    value isa StructValue || throw(ArgumentError(
        "schema-bearing Parquet MAP key requires StructValue"))
    return
end

function _provenancekeyroot(::_ProvenanceListBinding, value)
    value isa ListValue || throw(ArgumentError(
        "schema-bearing Parquet MAP key requires ListValue"))
    return
end

function _provenancekeyroot(::_ProvenanceMapBinding, value)
    value isa MapValue || throw(ArgumentError(
        "schema-bearing Parquet MAP key requires MapValue"))
    return
end

function _provenancebindingvalue(binding::_ProvenanceBinding, index::Int)
    values = _provenancesource(binding)
    checkbounds(Bool, values, index) || throw(ArgumentError(
        "schema-bearing nested value index exceeds its vector"))
    return _nestedwriterowaccess(values, index, binding.snapshot)
end

function _provenancetracesnapshotkey!(::Nothing,
        ::_NestedWriteKeySnapshot)
    return
end

function _provenancetracesnapshotkey!(trace::_NestedWriteTrace,
        snapshot::_NestedWriteKeySnapshot)
    if trace.capturing
        _nestedwritetracecapture!(trace, _NestedWriteTraceEvent(
            _NESTED_WRITE_TRACE_KEY, Int64(0), Int64(0), Int64(0), Int64(0),
            snapshot))
        return
    end
    expected = _nestedwritetracenext!(trace)
    expected.kind == _NESTED_WRITE_TRACE_KEY && iszero(expected.a) &&
        iszero(expected.b) && iszero(expected.c) && iszero(expected.d) &&
        expected.value === snapshot || throw(ArgumentError(
            "schema-bearing Parquet MAP-key replay changed its nested key topology"))
    return
end

function _provenancekeysnapshotpayload(element::Metadata.SchemaElement,
        snapshot::_NestedWriteKeySnapshot, limits::Limits)
    snapshot.kind in (_NESTED_WRITE_KEY_STRING,
        _NESTED_WRITE_KEY_BYTES) || return _nestedwriteleafpayload(element,
            snapshot.value, limits)
    bytes = snapshot.value::Vector{UInt8}
    kind = _logicalkind(element)
    if kind in (:string, :enum)
        _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
        isvalid(String, bytes) || throw(ArgumentError(
            "schema-bearing Parquet MAP key contains invalid UTF-8"))
        return Int64(length(bytes))
    elseif kind === :json
        _validatejson(bytes, limits, ArgumentError)
        return Int64(length(bytes))
    elseif kind === :bson
        _validatebson(bytes, limits, ArgumentError)
        return Int64(length(bytes))
    elseif element.type_ == Metadata.Type.BYTE_ARRAY
        _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
        return Int64(length(bytes))
    elseif element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY
        return _nestedwritefixedpayload(element, bytes, limits)
    end
    throw(ArgumentError(
        "schema-bearing Parquet MAP-key snapshot has incompatible physical bytes"))
end

function _provenancekeysnapshotphysical(element::Metadata.SchemaElement,
        snapshot::_NestedWriteKeySnapshot, limits::Limits)
    snapshot.kind in (_NESTED_WRITE_KEY_STRING,
        _NESTED_WRITE_KEY_BYTES) && return snapshot.value
    return _nestedwritenormalizekeyphysical(element, snapshot.value, limits)
end

function _provenancekeyleafcheck(binding::_ProvenanceLeafBinding,
        snapshot::_NestedWriteKeySnapshot)
    _nestedwritekeymissing(snapshot) && return
    expected = _provenanceleaflogicaltype(binding.plan)
    kind = snapshot.kind
    if kind == _NESTED_WRITE_KEY_SCALAR
        snapshot.value isa expected || throw(ArgumentError(
            "schema-bearing Parquet MAP-key snapshot does not match its leaf binding"))
    elseif kind == _NESTED_WRITE_KEY_STRING
        snapshot.source_type <: AbstractString || throw(ArgumentError(
            "schema-bearing Parquet MAP-key snapshot does not match its STRING binding"))
    elseif kind == _NESTED_WRITE_KEY_BYTES
        snapshot.source_type <: expected || throw(ArgumentError(
            "schema-bearing Parquet MAP-key snapshot does not match its binary binding"))
    else
        throw(ArgumentError(
            "schema-bearing Parquet MAP-key snapshot does not match its leaf binding"))
    end
    return
end

function _provenanceshredkeyleaf!(context,
        binding::_ProvenanceLeafBinding,
        snapshot::_NestedWriteKeySnapshot, repetition::UInt64)
    _provenancekeyleafcheck(binding, snapshot)
    plan = binding.plan
    leafindex = Int(first(plan.leaf_range))
    present = !_nestedwritekeymissing(snapshot)
    _nestedwritetraceleaf!(context.trace, snapshot.value, present)
    if !present
        _provenancerequired(plan, binding.force_required) && throw(
            ArgumentError(
                "required schema-bearing Parquet MAP-key leaf is missing"))
        _nestedwriterecord!(context, leafindex, repetition,
            UInt64(plan.parent_definition), Int64(0), false, nothing)
        return
    end
    element = plan.source.element
    payload = _provenancekeysnapshotpayload(element, snapshot, context.limits)
    if context isa _NestedWriteEmitContext
        _nestedwritepreflightemit(context, leafindex, payload)
        physical = _provenancekeysnapshotphysical(element, snapshot,
            context.limits)
        _nestedwriterecord!(context, leafindex, repetition,
            UInt64(plan.present_definition), payload, true, physical)
    else
        _nestedwriterecord!(context, leafindex, repetition,
            UInt64(plan.present_definition), payload, true, snapshot.value)
    end
    return
end

function _provenancekeymissingcontainer!(context,
        binding::_ProvenanceBinding, snapshot::_NestedWriteKeySnapshot,
        repetition::UInt64, kind::UInt8)
    _nestedwritekeymissing(snapshot) || return false
    _provenancerequired(binding.plan, binding.force_required) && throw(
        ArgumentError("required schema-bearing Parquet MAP key became null"))
    if kind == _NESTED_WRITE_TRACE_STRUCT
        _nestedwritetracestruct!(context.trace, missing, false,
            length(binding.plan.children))
    else
        _nestedwritetraceevent!(context.trace, kind, Int64(0), Int64(0),
            Int64(0), Int64(0))
    end
    _nestedwritemarker!(context, binding.plan.leaf_range, repetition,
        UInt64(binding.plan.parent_definition))
    return true
end

function _provenancekeyreplayenter!(context,
        binding::_ProvenanceLeafBinding, snapshot::_NestedWriteKeySnapshot,
        repetition::UInt64, ::Int,
        ::Union{Nothing,_ProvenanceKeyReplayFrame})
    _provenanceshredkeyleaf!(context, binding, snapshot, repetition)
    return true
end

function _provenancekeyreplayframe!(context, binding::_ProvenanceBinding,
        snapshot::_NestedWriteKeySnapshot, repetition::UInt64, depth::Int,
        parent::Union{Nothing,_ProvenanceKeyReplayFrame})
    trace = context.trace
    trace === nothing && throw(AssertionError(
        "schema-bearing MAP-key replay requires a writer trace"))
    _nestedwritekeyframecharge!(trace)
    try
        return _ProvenanceKeyReplayFrame(parent, binding, snapshot,
            repetition, depth, 0)
    catch
        _nestedwritekeyframefree!(trace)
        rethrow()
    end
end

function _provenancekeyreplayenter!(context,
        binding::_ProvenanceStructBinding, snapshot::_NestedWriteKeySnapshot,
        repetition::UInt64, depth::Int,
        parent::Union{Nothing,_ProvenanceKeyReplayFrame})
    _provenancekeymissingcontainer!(context, binding, snapshot, repetition,
        _NESTED_WRITE_TRACE_STRUCT) && return true
    snapshot.kind == _NESTED_WRITE_KEY_STRUCT || throw(ArgumentError(
        "schema-bearing Parquet MAP-key snapshot is not a struct"))
    length(snapshot.children) == length(binding.children) || throw(
        ArgumentError(
            "schema-bearing Parquet MAP-key struct changed its child count"))
    _nestedwritetracestruct!(context.trace, snapshot, true,
        length(binding.children))
    isempty(snapshot.children) && return true
    return _provenancekeyreplayframe!(context, binding, snapshot, repetition,
        depth, parent)
end

function _provenancekeyreplayenter!(context,
        binding::_ProvenanceListBinding, snapshot::_NestedWriteKeySnapshot,
        repetition::UInt64, depth::Int,
        parent::Union{Nothing,_ProvenanceKeyReplayFrame})
    _provenancekeymissingcontainer!(context, binding, snapshot, repetition,
        _NESTED_WRITE_TRACE_LIST) && return true
    snapshot.kind == _NESTED_WRITE_KEY_LIST || throw(ArgumentError(
        "schema-bearing Parquet MAP-key snapshot is not a LIST"))
    count = length(snapshot.children)
    _nestedwritetraceevent!(context.trace, _NESTED_WRITE_TRACE_LIST,
        Int64(1), Int64(count), Int64(snapshot.first), Int64(snapshot.last))
    if iszero(count)
        _nestedwritemarker!(context, binding.plan.leaf_range, repetition,
            UInt64(binding.plan.present_definition))
        return true
    end
    return _provenancekeyreplayframe!(context, binding, snapshot, repetition,
        depth, parent)
end

function _provenancekeyreplayenter!(context,
        binding::_ProvenanceMapBinding, snapshot::_NestedWriteKeySnapshot,
        repetition::UInt64, depth::Int,
        parent::Union{Nothing,_ProvenanceKeyReplayFrame})
    _provenancekeymissingcontainer!(context, binding, snapshot, repetition,
        _NESTED_WRITE_TRACE_MAP) && return true
    snapshot.kind == _NESTED_WRITE_KEY_MAP || throw(ArgumentError(
        "schema-bearing Parquet MAP-key snapshot is not a MAP"))
    iseven(length(snapshot.children)) || throw(ArgumentError(
        "schema-bearing Parquet MAP-key snapshot has an incomplete entry"))
    count = length(snapshot.children) ÷ 2
    _nestedwritetraceevent!(context.trace, _NESTED_WRITE_TRACE_MAP,
        Int64(1), Int64(count), Int64(snapshot.first), Int64(snapshot.last))
    if iszero(count)
        _nestedwritemarker!(context, binding.plan.leaf_range, repetition,
            UInt64(binding.plan.present_definition))
        return true
    end
    return _provenancekeyreplayframe!(context, binding, snapshot, repetition,
        depth, parent)
end

function _provenancekeyreplaynext(frame::_ProvenanceKeyReplayFrame)
    while true
        frame.position += 1
        snapshot = frame.snapshot
        frame.position <= length(snapshot.children) || return nothing
        binding = frame.binding
        if binding isa _ProvenanceStructBinding
            return binding.children[frame.position],
                snapshot.children[frame.position], frame.repetition
        elseif binding isa _ProvenanceListBinding
            repetition = frame.position == 1 ? frame.repetition :
                UInt64(binding.plan.repetition_level)
            return binding.element, snapshot.children[frame.position],
                repetition
        elseif binding isa _ProvenanceMapBinding
            entry = (frame.position + 1) ÷ 2
            repetition = entry == 1 ? frame.repetition :
                UInt64(binding.plan.repetition_level)
            child = snapshot.children[frame.position]
            isodd(frame.position) && return binding.key, child, repetition
            childbinding = binding.value
            if childbinding === nothing
                _nestedwritekeymissing(child) || throw(ArgumentError(
                    "key-only schema-bearing MAP-key snapshot contains a value"))
                continue
            end
            return childbinding, child, repetition
        end
        throw(AssertionError("unknown schema-bearing MAP-key replay binding"))
    end
end

function _provenanceshredkey!(context, binding::_ProvenanceBinding,
        snapshot::_NestedWriteKeySnapshot, repetition::UInt64)
    trace = context.trace
    started = _provenancekeyreplayenter!(context, binding, snapshot,
        repetition, 1, nothing)
    started === true && return
    frame = started::_ProvenanceKeyReplayFrame
    try
        while true
            next = _provenancekeyreplaynext(frame)
            if next !== nothing
                childbinding, childsnapshot, childrepetition = next
                if frame.binding isa _ProvenanceMapBinding &&
                        isodd(frame.position)
                    _provenancetracesnapshotkey!(trace, childsnapshot)
                end
                depth = _nestedwritekeynextdepth(frame.depth, context.limits)
                _checklimit(:metadata_depth, depth,
                    context.limits.max_metadata_depth)
                started = _provenancekeyreplayenter!(context, childbinding,
                    childsnapshot, childrepetition, depth, frame)
                started === true || (frame = started)
                continue
            end
            parent = frame.parent
            _nestedwritekeyframefree!(trace)
            if parent === nothing
                frame = nothing
                return
            end
            frame = parent
        end
    finally
        while frame !== nothing
            parent = frame.parent
            _nestedwritekeyframefree!(trace)
            frame = parent
        end
    end
end

function _provenanceshred!(context::_NestedWriteCountContext,
        binding::_ProvenanceLeafBinding, index::Int, repetition::UInt64)
    plan = binding.plan
    value, row_witness = _provenanceleafaccess(binding, index)
    _nestedwriterowcheck(row_witness)
    leafindex = Int(first(plan.leaf_range))
    present = !ismissing(value)
    _nestedwritetraceleaf!(context.trace, value, present)
    if !present
        _provenancerequired(plan, binding.force_required) && throw(ArgumentError(
            "required physical leaf $(repr(join(plan.source.path, "."))) is missing"))
        _nestedwriterecord!(context, leafindex, repetition,
            UInt64(plan.parent_definition), Int64(0), false, nothing)
        return
    end
    expected = _provenanceleaflogicaltype(plan)
    value isa expected || throw(ArgumentError(
        "physical leaf $(repr(join(plan.source.path, "."))) contains " *
        "$(typeof(value)); expected $expected"))
    payload = _nestedwriteleafpayload(plan.source.element, value, context.limits)
    _nestedwriterecord!(context, leafindex, repetition,
        UInt64(plan.present_definition), payload, true, value)
    _nestedwriterowcheck(row_witness)
    return
end

function _provenanceshred!(context::_NestedWriteEmitContext,
        binding::_ProvenanceLeafBinding, index::Int, repetition::UInt64)
    plan = binding.plan
    value, row_witness = _provenanceleafaccess(binding, index)
    _nestedwriterowcheck(row_witness)
    leafindex = Int(first(plan.leaf_range))
    present = !ismissing(value)
    _nestedwritetraceleaf!(context.trace, value, present)
    if !present
        _provenancerequired(plan, binding.force_required) && throw(ArgumentError(
            "required physical leaf $(repr(join(plan.source.path, "."))) is missing"))
        _nestedwriterecord!(context, leafindex, repetition,
            UInt64(plan.parent_definition), Int64(0), false, nothing)
        return
    end
    expected = _provenanceleaflogicaltype(plan)
    value isa expected || throw(ArgumentError(
        "physical leaf $(repr(join(plan.source.path, "."))) contains " *
        "$(typeof(value)); expected $expected"))
    element = plan.source.element
    payload = _nestedwriteleafpayload(element, value, context.limits)
    _nestedwritepreflightemit(context, leafindex, payload)
    physical = _nestedwritenormalizephysical(element, value, context.limits)
    _nestedwriterecord!(context, leafindex, repetition,
        UInt64(plan.present_definition), payload, true, physical)
    _nestedwriterowcheck(row_witness)
    return
end

function _provenanceshredenter!(context, binding::_ProvenanceLeafBinding,
        index::Int, repetition::UInt64,
        ::Union{Nothing,_ProvenanceShredFrame})
    _provenanceshred!(context, binding, index, repetition)
    return true
end

function _provenanceshredframe(context, binding::_ProvenanceBinding,
        repetition::UInt64, childindex::Int, position::Int, last::Int,
        witness::_NestedWriteRowWitness, parent)
    trace = context.trace
    trace === nothing && throw(AssertionError(
        "schema-bearing traversal requires a writer trace"))
    _reserveobjects!(trace.budget)
    try
        return _ProvenanceShredFrame(parent, binding, repetition, childindex,
            position, last, witness)
    catch
        _release!(trace.budget, _MATERIALIZED_OBJECT_BYTES)
        rethrow()
    end
end

function _provenanceshredframefree!(context)
    trace = context.trace
    trace === nothing && throw(AssertionError(
        "schema-bearing traversal requires a writer trace"))
    _release!(trace.budget, _MATERIALIZED_OBJECT_BYTES)
    return
end

function _provenanceshredframesfree!(context, count::Int)
    iszero(count) && return
    trace = context.trace
    trace === nothing && throw(AssertionError(
        "schema-bearing traversal requires a writer trace"))
    _release!(trace.budget,
        _materializedproduct(count, _MATERIALIZED_OBJECT_BYTES))
    return
end

function _provenancestructindex(binding::_ProvenanceStructBinding, index::Int)
    witness = _nestedwriterowwitness(binding.snapshot, index)
    witness === nothing && throw(AssertionError(
        "schema-bearing struct has no row witness"))
    return witness.present ? witness.last : 0, witness
end

function _provenanceshredenter!(context,
        binding::_ProvenanceStructBinding, index::Int,
        repetition::UInt64, parent)
    childindex, row_witness = _provenancestructindex(binding, index)
    _nestedwriterowcheck(row_witness)
    present = !iszero(childindex)
    _nestedwritetracestruct!(context.trace, binding.values, present,
        length(binding.children))
    if iszero(childindex)
        _provenancerequired(binding.plan, binding.force_required) &&
            throw(ArgumentError("required struct became null during schema-bearing write"))
        _nestedwritemarker!(context, binding.plan.leaf_range, repetition,
            UInt64(binding.plan.parent_definition))
        return true
    end
    values = binding.values
    childcount = length(values.children)
    length(binding.children) == childcount || throw(ArgumentError(
        "schema-bearing struct changed its child count during access"))
    iszero(childcount) && return true
    return _provenanceshredframe(context, binding, repetition, childindex, 0,
        childcount, row_witness, parent)
end

function _provenancelistspan(binding::_ProvenanceListBinding, index::Int)
    witness = _nestedwriterowwitness(binding.snapshot, index)
    witness === nothing && throw(AssertionError(
        "schema-bearing list has no row witness"))
    witness.present || return 0, -1, witness
    return witness.first, witness.last, witness
end

function _provenanceshredenter!(context, binding::_ProvenanceListBinding,
        index::Int, repetition::UInt64, parent)
    firstentry, lastentry, row_witness = _provenancelistspan(binding, index)
    _nestedwriterowcheck(row_witness)
    present = !iszero(firstentry)
    count = present ? max(lastentry - firstentry + 1, 0) : 0
    _nestedwritetraceevent!(context.trace, _NESTED_WRITE_TRACE_LIST,
        present ? Int64(1) : Int64(0), Int64(count), Int64(firstentry),
        Int64(lastentry))
    if iszero(firstentry)
        _provenancerequired(binding.plan, binding.force_required) &&
            throw(ArgumentError("required list became null during schema-bearing write"))
        _nestedwritemarker!(context, binding.plan.leaf_range, repetition,
            UInt64(binding.plan.parent_definition))
        return true
    elseif firstentry > lastentry
        _nestedwritemarker!(context, binding.plan.leaf_range, repetition,
            UInt64(binding.plan.present_definition))
        return true
    end
    return _provenanceshredframe(context, binding, repetition, 0,
        firstentry - 1, lastentry, row_witness, parent)
end

function _provenancemapspan(binding::_ProvenanceMapBinding, index::Int)
    witness = _nestedwriterowwitness(binding.snapshot, index)
    witness === nothing && throw(AssertionError(
        "schema-bearing map has no row witness"))
    witness.present || return 0, -1, witness
    return witness.first, witness.last, witness
end

function _provenanceshredenter!(context, binding::_ProvenanceMapBinding,
        index::Int, repetition::UInt64, parent)
    firstentry, lastentry, row_witness = _provenancemapspan(binding, index)
    _nestedwriterowcheck(row_witness)
    present = !iszero(firstentry)
    count = present ? max(lastentry - firstentry + 1, 0) : 0
    _nestedwritetraceevent!(context.trace, _NESTED_WRITE_TRACE_MAP,
        present ? Int64(1) : Int64(0), Int64(count), Int64(firstentry),
        Int64(lastentry))
    if iszero(firstentry)
        _provenancerequired(binding.plan, binding.force_required) &&
            throw(ArgumentError("required map became null during schema-bearing write"))
        _nestedwritemarker!(context, binding.plan.leaf_range, repetition,
            UInt64(binding.plan.parent_definition))
        return true
    elseif firstentry > lastentry
        _nestedwritemarker!(context, binding.plan.leaf_range, repetition,
            UInt64(binding.plan.present_definition))
        return true
    end
    return _provenanceshredframe(context, binding, repetition, 0,
        firstentry - 1, lastentry, row_witness, parent)
end

function _provenanceshrednext(context, frame::_ProvenanceShredFrame)
    binding = frame.binding
    if binding isa _ProvenanceStructBinding
        frame.position += 1
        frame.position <= frame.last || return nothing
        _nestedwriterowcheck(frame.witness)
        values = binding.values
        length(values.children) == frame.last || throw(ArgumentError(
            "schema-bearing struct changed its child count during access"))
        child = binding.children[frame.position]
        values.children[frame.position] === _provenancesource(child) || throw(
            ArgumentError(
                "schema-bearing struct changed child identity during access"))
        return child, frame.childindex, frame.repetition
    elseif binding isa _ProvenanceListBinding
        frame.position += 1
        frame.position <= frame.last || return nothing
        _nestedwriterowcheck(frame.witness)
        repetition = frame.position == frame.witness.first ?
            frame.repetition : UInt64(binding.plan.repetition_level)
        return binding.element, frame.position, repetition
    elseif binding isa _ProvenanceMapBinding
        while true
            frame.position += 1
            frame.position <= frame.last || return nothing
            _nestedwriterowcheck(frame.witness)
            repetition = frame.position == frame.witness.first ?
                frame.repetition : UInt64(binding.plan.repetition_level)
            keyvalue, key_witness = _provenancebindingvalue(binding.key,
                frame.position)
            _nestedwriterowcheck(key_witness)
            _nestedwriterowcheck(frame.witness)
            ismissing(keyvalue) && throw(ArgumentError(
                "map $(repr(join(binding.plan.source.path, "."))) contains a null key"))
            expected = _nestedwritetracekey!(context.trace, keyvalue, nothing,
                context.limits, binding.key, key_witness)
            _nestedwriterowcheck(frame.witness)
            _provenanceshredkey!(context, binding.key, expected, repetition)
            _nestedwriterowcheck(frame.witness)
            binding.value === nothing && continue
            return something(binding.value), frame.position, repetition
        end
    end
    throw(AssertionError("unknown schema-bearing shred frame"))
end

function _provenanceshrediterative!(context,
        binding::_ProvenanceBinding, index::Int, repetition::UInt64)
    started = _provenanceshredenter!(context, binding, index, repetition,
        nothing)
    started === true && return
    current::Union{Nothing,_ProvenanceShredFrame} =
        started::_ProvenanceShredFrame
    activeframes = 1
    try
        while true
            next = _provenanceshrednext(context, current)
            if next !== nothing
                child, childindex, childrepetition = next
                started = _provenanceshredenter!(context, child, childindex,
                    childrepetition, current)
                if started !== true
                    activeframes += 1
                    current = started::_ProvenanceShredFrame
                end
                continue
            end
            parent = current.parent
            _provenanceshredframefree!(context)
            activeframes -= 1
            if parent === nothing
                current = nothing
                return
            end
            current = parent::_ProvenanceShredFrame
        end
    finally
        _provenanceshredframesfree!(context, activeframes)
    end
end

function _provenanceshred!(context, binding::_ProvenanceStructBinding,
        index::Int, repetition::UInt64)
    return _provenanceshrediterative!(context, binding, index, repetition)
end

function _provenanceshred!(context, binding::_ProvenanceListBinding,
        index::Int, repetition::UInt64)
    return _provenanceshrediterative!(context, binding, index, repetition)
end

function _provenanceshred!(context, binding::_ProvenanceMapBinding,
        index::Int, repetition::UInt64)
    return _provenanceshrediterative!(context, binding, index, repetition)
end

function _provenancepass!(context, bindings::Vector{_ProvenanceBinding}, rows::Int)
    for row in 1:rows
        for binding in bindings
            _provenanceshred!(context, binding, row, UInt64(0))
        end
        _nestedwritefinishrow!(context, eachindex(context isa
            _NestedWriteCountContext ? context.counts : context.builders), row)
    end
    return
end

function _provenanceschemawalkframe(node::SchemaNode, parent, depth::Int,
        limits::Limits, budget::_LiveByteBudget)
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    _checklimit(:container_elements, length(node.children),
        limits.max_container_elements)
    isempty(node.children) && return nothing
    _reserveobjects!(budget)
    return _ProvenanceSchemaWalkFrame(parent, node, depth, 0)
end

function _provenanceschemawalknext(frame::_ProvenanceSchemaWalkFrame)
    frame.position += 1
    return frame.node.children[frame.position]
end

function _provenancefragmentcount(node::SchemaNode, limits::Limits,
        budget::_LiveByteBudget)
    count = Int64(1)
    _checklimit(:container_elements, count, limits.max_container_elements)
    depth = _nestedwritedepthadd(length(node.path), 1, limits)
    current::Union{Nothing,_ProvenanceSchemaWalkFrame} =
        _provenanceschemawalkframe(node, nothing, depth, limits, budget)
    activeframes = current === nothing ? 0 : 1
    try
        current === nothing && return Int(count)
        while true
            if current.position == length(current.node.children)
                parent = current.parent
                _release!(budget, _MATERIALIZED_OBJECT_BYTES)
                activeframes -= 1
                if parent === nothing
                    current = nothing
                    return Int(count)
                end
                current = parent::_ProvenanceSchemaWalkFrame
                continue
            end
            child = _provenanceschemawalknext(current)
            count = try
                Base.checked_add(count, Int64(1))
            catch err
                err isa OverflowError || rethrow()
                throw(LimitError(:container_elements, typemax(Int64),
                    limits.max_container_elements))
            end
            _checklimit(:container_elements, count,
                limits.max_container_elements)
            childdepth = _nestedwritedepthadd(current.depth, 1, limits)
            childframe = _provenanceschemawalkframe(child, current, childdepth,
                limits, budget)
            if childframe !== nothing
                activeframes += 1
                current = childframe::_ProvenanceSchemaWalkFrame
            end
        end
    finally
        _provenanceframesrelease!(budget, activeframes)
    end
end

function _provenancefragmentappend!(elements::Vector{Metadata.SchemaElement},
        node::SchemaNode, limits::Limits, budget::_LiveByteBudget)
    push!(elements, node.element)
    depth = _nestedwritedepthadd(length(node.path), 1, limits)
    current::Union{Nothing,_ProvenanceSchemaWalkFrame} =
        _provenanceschemawalkframe(node, nothing, depth, limits, budget)
    activeframes = current === nothing ? 0 : 1
    try
        current === nothing && return
        while true
            if current.position == length(current.node.children)
                parent = current.parent
                _release!(budget, _MATERIALIZED_OBJECT_BYTES)
                activeframes -= 1
                if parent === nothing
                    current = nothing
                    return
                end
                current = parent::_ProvenanceSchemaWalkFrame
                continue
            end
            child = _provenanceschemawalknext(current)
            push!(elements, child.element)
            childdepth = _nestedwritedepthadd(current.depth, 1, limits)
            childframe = _provenanceschemawalkframe(child, current, childdepth,
                limits, budget)
            if childframe !== nothing
                activeframes += 1
                current = childframe::_ProvenanceSchemaWalkFrame
            end
        end
    finally
        _provenanceframesrelease!(budget, activeframes)
    end
end

function _provenancefragment(node::SchemaNode, limits::Limits,
        budget::_LiveByteBudget)
    count = _provenancefragmentcount(node, limits, budget)
    _reservearray!(budget, Metadata.SchemaElement, count)
    elements = Metadata.SchemaElement[]
    sizehint!(elements, count)
    _provenancefragmentappend!(elements, node, limits, budget)
    return elements
end

function _provenancefinishfields(semantic::_NestedSchemaPlan,
        builders::Vector{_NestedWriteLeafBuilder}, rows::Int, limits::Limits,
        budget::_LiveByteBudget)
    children = semantic.root.children
    _reservearray!(budget, WriteFieldPlan, length(children))
    fields = WriteFieldPlan[]
    sizehint!(fields, length(children))
    for child in children
        range = _nestedleafrange(child)
        isempty(range) && throw(ArgumentError(
            "schema-bearing writes do not support zero-leaf fields"))
        fragment = _provenancefragment(child.source, limits, budget)
        _reservearray!(budget, WriteColumn, length(range))
        leaves = WriteColumn[]
        sizehint!(leaves, length(range))
        for rawindex in range
            index = Int(rawindex)
            push!(leaves, _nestedwritecolumn(builders[index],
                semantic.leaves[index], rows, budget))
        end
        _reserveobjects!(budget)
        push!(fields, WriteFieldPlan(fragment, leaves))
    end
    return fields
end

function _provenancewritefields(table::Table, limits::Limits,
        budget::_LiveByteBudget, encoding, dictionary::Bool)
    start = _budgetused(budget)
    try
        rows = table.rows
        rows >= 0 || throw(ArgumentError("Parquet row count must be nonnegative"))
        _checklimit(:container_elements, rows, limits.max_container_elements)
        elements, schema = _provenancefreshschema(table, limits, budget)
        semantic = _nestedplan(schema; limits=limits, budget=budget)
        _provenancerejectleafless(semantic.root, budget)
        topology = _provenancetopology(table, limits, budget)
        bindings = _provenancebindings(table, semantic, limits, budget,
            topology)
        _provenancebarrier!(table, elements, schema, semantic, bindings, rows,
            limits, topology, budget)
        choices, choicecharge = _preflightwriteencoding(semantic, encoding,
            dictionary, budget)
        _provenancebarrier!(table, elements, schema, semantic, bindings, rows,
            limits, topology, budget)
        trace = _nestedwritetrace(budget, topology)
        counts = _nestedwritecounts(length(semantic.leaves), budget)
        countcontext, boundarycharge = _nestedwritecountcontext(counts,
            semantic, rows, limits, budget, trace)
        _provenancebarrier!(table, elements, schema, semantic, bindings, rows,
            limits, topology, budget)
        _provenancepass!(countcontext, bindings, rows)
        _provenancebarrier!(table, elements, schema, semantic, bindings, rows,
            limits, topology, budget)
        _nestedwritevalidateboundaries(countcontext, rows)
        _nestedwritepreflightrows(countcontext, choices)
        _release!(budget, choicecharge)
        choices = nothing
        builders = _nestedwritebuilders(counts, semantic, budget)
        _provenancebarrier!(table, elements, schema, semantic, bindings, rows,
            limits, topology, budget)
        emitcontext = _NestedWriteEmitContext(builders, counts, limits,
            countcontext, trace)
        _nestedwritetracecompare!(trace)
        _provenancepass!(emitcontext, bindings, rows)
        _nestedwritetracefinishcompare!(trace)
        _provenancebarrier!(table, elements, schema, semantic, bindings, rows,
            limits, topology, budget)
        _nestedwritevalidatebuilders(builders, counts, semantic, rows)
        fields = _provenancefinishfields(semantic, builders, rows, limits,
            budget)
        _provenancebarrier!(table, elements, schema, semantic, bindings, rows,
            limits, topology, budget)
        _release!(budget, boundarycharge)
        countcontext = nothing
        emitcontext = nothing
        _nestedwritetracerelease!(trace)
        trace = nothing
        _nestedwritetopologyrelease!(topology, budget)
        topology = nothing
        _reserveobjects!(budget)
        return _ProvenanceWriteFields(fields, elements, schema), rows
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _provenancevalidatefieldnode(field::WriteFieldPlan,
        node::SchemaNode, index::Int)
    index <= length(field.schema) || throw(ArgumentError(
        "schema-bearing writer field fragment is truncated"))
    _provenanceexact(field.schema[index], node.element) ||
        throw(ArgumentError("schema-bearing writer SchemaElement changed"))
    return index + 1
end

function _provenancevalidatefieldfragment(field::WriteFieldPlan,
        node::SchemaNode, index::Int, limits::Limits,
        budget::_LiveByteBudget)
    next = _provenancevalidatefieldnode(field, node, index)
    depth = _nestedwritedepthadd(length(node.path), 1, limits)
    current::Union{Nothing,_ProvenanceSchemaWalkFrame} =
        _provenanceschemawalkframe(node, nothing, depth, limits, budget)
    activeframes = current === nothing ? 0 : 1
    try
        current === nothing && return next
        while true
            if current.position == length(current.node.children)
                parent = current.parent
                _release!(budget, _MATERIALIZED_OBJECT_BYTES)
                activeframes -= 1
                if parent === nothing
                    current = nothing
                    return next
                end
                current = parent::_ProvenanceSchemaWalkFrame
                continue
            end
            child = _provenanceschemawalknext(current)
            next = _provenancevalidatefieldnode(field, child, next)
            childdepth = _nestedwritedepthadd(current.depth, 1, limits)
            childframe = _provenanceschemawalkframe(child, current, childdepth,
                limits, budget)
            if childframe !== nothing
                activeframes += 1
                current = childframe::_ProvenanceSchemaWalkFrame
            end
        end
    finally
        _provenanceframesrelease!(budget, activeframes)
    end
end

function _provenancevalidatefieldfragments(fields::_ProvenanceWriteFields,
        limits::Limits, budget::_LiveByteBudget)
    length(fields.fields) == length(fields.schema.root.children) ||
        throw(ArgumentError("schema-bearing writer field count changed"))
    for (field, node) in zip(fields.fields, fields.schema.root.children)
        next = _provenancevalidatefieldfragment(field, node, 1, limits,
            budget)
        next == length(field.schema) + 1 || throw(ArgumentError(
            "schema-bearing writer field fragment has trailing elements"))
    end
    return
end

function _writeplanleaves(fields::_ProvenanceWriteFields, schema::Schema,
        rows::Int, limits::Limits, budget::_LiveByteBudget)
    schema === fields.schema || throw(ArgumentError(
        "schema-bearing writer did not use its operation-owned schema"))
    return _writeplanleaves(fields.fields, schema, rows, limits, budget)
end

function _writeplan(fields::_ProvenanceWriteFields, rows::Int, limits::Limits,
        budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        rows >= 0 || throw(ArgumentError(
            "Parquet row count must be nonnegative"))
        _provenancevalidatefieldfragments(fields, limits, budget)
        leaves, leafcharge = _writeplanleaves(fields.fields, fields.schema,
            rows, limits, budget)
        _reservearray!(budget, WriteRowGroupPlan, iszero(rows) ? 0 : 1)
        _reserveobjects!(budget, 2)
        rowgroups = iszero(rows) ? WriteRowGroupPlan[] :
            WriteRowGroupPlan[WriteRowGroupPlan(rows, leaves)]
        plan = WritePlan(fields.elements, fields.schema, rows, rowgroups)
        iszero(rows) && _release!(budget, leafcharge)
        return plan
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end
