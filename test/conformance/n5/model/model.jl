abstract type N5Node end

struct N5Primitive <: N5Node
    name::String
    physical::MD.Type.T
    optional::Bool
    logical::Symbol
end

struct N5Struct <: N5Node
    name::String
    optional::Bool
    fields::Vector{N5Node}
end

struct N5List <: N5Node
    name::String
    optional::Bool
    element::N5Node
    layout::Symbol
    wrapper::String
    annotation::Symbol
end

struct N5Map <: N5Node
    name::String
    optional::Bool
    key::N5Node
    value::Union{Nothing,N5Node}
    marker::Symbol
    entrymarker::Symbol
    entryname::String
end

struct N5Record
    values::Vector{Any}
end

function n5record(values...)
    return N5Record(Any[values...])
end

function Base.:(==)(left::N5Record, right::N5Record)
    return left.values == right.values
end

function Base.isequal(left::N5Record, right::N5Record)
    return isequal(left.values, right.values)
end

struct N5Entry
    key::Any
    value::Any
    hasvalue::Bool
end

function Base.:(==)(left::N5Entry, right::N5Entry)
    return left.hasvalue == right.hasvalue && left.key == right.key &&
        left.value == right.value
end

function Base.isequal(left::N5Entry, right::N5Entry)
    return left.hasvalue == right.hasvalue && isequal(left.key, right.key) &&
        isequal(left.value, right.value)
end

struct N5MapValue
    entries::Vector{N5Entry}
end

function n5mapvalue(pairs::Pair...)
    return N5MapValue(N5Entry[N5Entry(pair.first, pair.second, true)
        for pair in pairs])
end

function n5keyset(keys...)
    return N5MapValue(N5Entry[N5Entry(key, nothing, false) for key in keys])
end

function n5maplookup(value::N5MapValue, key)
    for index in length(value.entries):-1:1
        entry = value.entries[index]
        isequal(entry.key, key) && return entry.hasvalue ? entry.value : missing
    end
    throw(KeyError(key))
end

function Base.:(==)(left::N5MapValue, right::N5MapValue)
    return left.entries == right.entries
end

function Base.isequal(left::N5MapValue, right::N5MapValue)
    return isequal(left.entries, right.entries)
end

abstract type N5PlanNode end

struct N5LeafPlan <: N5PlanNode
    node::N5Primitive
    leaf::Int
    parent_definition::Int
    present_definition::Int
    repetition_depth::Int
end

struct N5StructPlan <: N5PlanNode
    node::N5Struct
    parent_definition::Int
    present_definition::Int
    repetition_depth::Int
    children::Vector{N5PlanNode}
    leaves::UnitRange{Int}
end

struct N5ListPlan <: N5PlanNode
    node::N5List
    parent_definition::Int
    present_definition::Int
    entry_definition::Int
    entry_repetition::Int
    element::N5PlanNode
    leaves::UnitRange{Int}
end

struct N5MapPlan <: N5PlanNode
    node::N5Map
    parent_definition::Int
    present_definition::Int
    entry_definition::Int
    entry_repetition::Int
    key::N5PlanNode
    value::Union{Nothing,N5PlanNode}
    leaves::UnitRange{Int}
end

struct N5LeafSpec
    name::String
    physical::MD.Type.T
    logical::Symbol
    max_repetition::Int
    max_definition::Int
end

struct N5Compiled
    root::N5PlanNode
    leaves::Vector{N5LeafSpec}
end

struct N5LeafStream
    repetition::Vector{UInt64}
    definition::Vector{UInt64}
    values::Vector{Any}
    max_repetition::Int
    max_definition::Int
end

function Base.:(==)(left::N5LeafStream, right::N5LeafStream)
    return left.repetition == right.repetition &&
        left.definition == right.definition && left.values == right.values &&
        left.max_repetition == right.max_repetition &&
        left.max_definition == right.max_definition
end

function Base.isequal(left::N5LeafStream, right::N5LeafStream)
    return isequal(left.repetition, right.repetition) &&
        isequal(left.definition, right.definition) &&
        isequal(left.values, right.values) &&
        left.max_repetition == right.max_repetition &&
        left.max_definition == right.max_definition
end

mutable struct N5LeafBuilder
    repetition::Vector{UInt64}
    definition::Vector{UInt64}
    values::Vector{Any}
end

struct N5Absent end

const N5_ABSENT = N5Absent()

struct N5ExpandedLeaf
    repetition::Vector{UInt64}
    definition::Vector{UInt64}
    entries::Vector{Any}
    max_repetition::Int
    max_definition::Int
end

struct N5PhysicalLeaf
    element::MD.SchemaElement
    path::Vector{String}
    max_repetition::Int
    max_definition::Int
end

abstract type N5ExpectedVector end

struct N5ExpectedLeafVector <: N5ExpectedVector
    physical::MD.Type.T
    logical::Symbol
    nullable::Bool
    values::Vector{Any}
end

struct N5ExpectedStructVector <: N5ExpectedVector
    names::Vector{String}
    ranks::Union{Nothing,Vector{Int}}
    children::Vector{N5ExpectedVector}
    rows::Int
end

struct N5ExpectedListVector <: N5ExpectedVector
    offsets::Vector{Int}
    validity::Union{Nothing,BitVector}
    values::N5ExpectedVector
end

struct N5ExpectedMapVector <: N5ExpectedVector
    offsets::Vector{Int}
    validity::Union{Nothing,BitVector}
    keys::N5ExpectedVector
    values::Union{Nothing,N5ExpectedVector}
end

struct N5ExpectedTableVector
    names::Vector{String}
    children::Vector{N5ExpectedVector}
    rows::Int
end

function n5primitive(name::AbstractString, physical::MD.Type.T;
        optional::Bool=false, logical::Symbol=:none)
    logical in (:none, :string) || throw(ArgumentError(
        "unsupported N5 primitive logical annotation $logical"))
    logical === :string && physical != MD.Type.BYTE_ARRAY && throw(ArgumentError(
        "N5 STRING primitive must use BYTE_ARRAY"))
    return N5Primitive(String(name), physical, optional, logical)
end

function n5struct(name::AbstractString, fields::Vector{N5Node};
        optional::Bool=false)
    isempty(fields) && throw(ArgumentError("N5 struct must have at least one field"))
    return N5Struct(String(name), optional, fields)
end

function n5list(name::AbstractString, element::N5Node; optional::Bool=false,
        layout::Symbol=:canonical, wrapper::AbstractString="list",
        annotation::Symbol=:dual)
    layout in (:canonical, :rule1, :rule2, :rule3, :rule4, :rule5) ||
        throw(ArgumentError("unsupported N5 LIST layout $layout"))
    annotation in (:modern, :legacy, :dual, :conflict) ||
        throw(ArgumentError("unsupported N5 LIST annotation $annotation"))
    layout === :rule1 && (!(element isa N5Primitive) || element.optional) &&
        throw(ArgumentError("LIST rule 1 needs a required primitive element"))
    layout === :rule2 && !(element isa N5Struct) &&
        throw(ArgumentError("LIST rule 2 needs a struct element"))
    layout === :rule3 && !(element isa Union{N5List,N5Map}) &&
        throw(ArgumentError("LIST rule 3 needs a repeated collection child"))
    layout === :rule4 && (!(element isa N5Struct) || length(element.fields) != 1) &&
        throw(ArgumentError("LIST rule 4 needs a one-field struct element"))
    return N5List(String(name), optional, element, layout, String(wrapper), annotation)
end

function n5map(name::AbstractString, key::N5Node,
        value::Union{Nothing,N5Node}; optional::Bool=false,
        marker::Symbol=:modern, entrymarker::Symbol=:none,
        entryname::AbstractString="key_value")
    marker in (:modern, :legacy, :alias, :dual, :conflict,
        :modern_alias, :modern_primitive, :modern_unknown) ||
        throw(ArgumentError("unsupported N5 MAP marker $marker"))
    entrymarker in (:none, :marked, :empty, :future, :future_marked,
        :unknown_converted) ||
        throw(ArgumentError("unsupported N5 MAP entry marker $entrymarker"))
    return N5Map(String(name), optional, key, value, marker, entrymarker,
        String(entryname))
end

function _n5expectedvector(node::N5Primitive, values::Vector{Any};
        force_required::Bool=false)
    for value in values
        ismissing(value) && (!node.optional || force_required) && throw(ArgumentError(
            "required N5 vector leaf $(repr(node.name)) is null"))
    end
    return N5ExpectedLeafVector(node.physical, node.logical,
        node.optional && !force_required, copy(values))
end

function _n5expectedvector(node::N5Struct, values::Vector{Any};
        force_required::Bool=false)
    required = force_required || !node.optional
    ranks = required ? nothing : Int[0]
    childvalues = [Any[] for _ in node.fields]
    present = 0
    for value in values
        if ismissing(value)
            required && throw(ArgumentError(
                "required N5 vector struct $(repr(node.name)) is null"))
        else
            value isa N5Record || throw(ArgumentError(
                "N5 vector struct $(repr(node.name)) needs an N5Record"))
            length(value.values) == length(node.fields) || throw(ArgumentError(
                "N5 vector struct $(repr(node.name)) has the wrong field count"))
            for index in eachindex(node.fields)
                push!(childvalues[index], value.values[index])
            end
            present += 1
        end
        ranks === nothing || push!(ranks, present)
    end
    children = N5ExpectedVector[_n5expectedvector(field, childvalues[index])
        for (index, field) in enumerate(node.fields)]
    return N5ExpectedStructVector(String[field.name for field in node.fields],
        ranks, children, length(values))
end

function _n5expectedvector(node::N5List, values::Vector{Any};
        force_required::Bool=false)
    required = force_required || !node.optional
    offsets = Int[0]
    validity = required ? nothing : BitVector()
    elements = Any[]
    for value in values
        if ismissing(value)
            required && throw(ArgumentError(
                "required N5 vector list $(repr(node.name)) is null"))
            push!(validity, false)
        else
            value isa AbstractVector || throw(ArgumentError(
                "N5 vector list $(repr(node.name)) needs a vector"))
            validity === nothing || push!(validity, true)
            append!(elements, value)
        end
        push!(offsets, length(elements))
    end
    child = _n5expectedvector(node.element, elements)
    return N5ExpectedListVector(offsets, validity, child)
end

function _n5expectedvector(node::N5Map, values::Vector{Any};
        force_required::Bool=false)
    required = force_required || !node.optional
    offsets = Int[0]
    validity = required ? nothing : BitVector()
    keys = Any[]
    mapvalues = Any[]
    for value in values
        if ismissing(value)
            required && throw(ArgumentError(
                "required N5 vector map $(repr(node.name)) is null"))
            push!(validity, false)
        else
            value isa N5MapValue || throw(ArgumentError(
                "N5 vector map $(repr(node.name)) needs an N5MapValue"))
            validity === nothing || push!(validity, true)
            for entry in value.entries
                ismissing(entry.key) && throw(ArgumentError(
                    "N5 vector map $(repr(node.name)) has a null key"))
                push!(keys, entry.key)
                node.value === nothing || begin
                    entry.hasvalue || throw(ArgumentError(
                        "N5 vector map $(repr(node.name)) is missing a value field"))
                    push!(mapvalues, entry.value)
                end
            end
        end
        push!(offsets, length(keys))
    end
    key = _n5expectedvector(node.key, keys; force_required=true)
    value = node.value === nothing ? nothing :
        _n5expectedvector(node.value, mapvalues)
    return N5ExpectedMapVector(offsets, validity, key, value)
end

function n5expectedvectortree(node::N5Node, rows::AbstractVector)
    values = Any[rows...]
    child = _n5expectedvector(node, values)
    return N5ExpectedTableVector(String[node.name], N5ExpectedVector[child],
        length(values))
end

function _n5range(firstleaf::Int, leaves::Vector{N5LeafSpec})
    return firstleaf:length(leaves)
end

function _n5compile!(leaves::Vector{N5LeafSpec}, node::N5Primitive,
        definition::Int, repetition::Int)
    parent = definition
    present = definition + Int(node.optional)
    push!(leaves, N5LeafSpec(node.name, node.physical, node.logical,
        repetition, present))
    return N5LeafPlan(node, length(leaves), parent, present, repetition)
end

function _n5compile!(leaves::Vector{N5LeafSpec}, node::N5Struct,
        definition::Int, repetition::Int)
    firstleaf = length(leaves) + 1
    parent = definition
    present = definition + Int(node.optional)
    children = N5PlanNode[]
    sizehint!(children, length(node.fields))
    for field in node.fields
        push!(children, _n5compile!(leaves, field, present, repetition))
    end
    return N5StructPlan(node, parent, present, repetition, children,
        _n5range(firstleaf, leaves))
end

function _n5compile!(leaves::Vector{N5LeafSpec}, node::N5List,
        definition::Int, repetition::Int)
    firstleaf = length(leaves) + 1
    parent = definition
    present = definition + Int(node.optional)
    entrydefinition = present + 1
    entryrepetition = repetition + 1
    element = _n5compile!(leaves, node.element, entrydefinition,
        entryrepetition)
    return N5ListPlan(node, parent, present, entrydefinition,
        entryrepetition, element, _n5range(firstleaf, leaves))
end

function _n5compile!(leaves::Vector{N5LeafSpec}, node::N5Map,
        definition::Int, repetition::Int)
    firstleaf = length(leaves) + 1
    parent = definition
    present = definition + Int(node.optional)
    entrydefinition = present + 1
    entryrepetition = repetition + 1
    key = _n5compile!(leaves, node.key, entrydefinition, entryrepetition)
    value = node.value === nothing ? nothing : _n5compile!(leaves,
        node.value, entrydefinition, entryrepetition)
    return N5MapPlan(node, parent, present, entrydefinition,
        entryrepetition, key, value, _n5range(firstleaf, leaves))
end

function n5compile(node::N5Node)
    leaves = N5LeafSpec[]
    root = _n5compile!(leaves, node, 0, 0)
    return N5Compiled(root, leaves)
end

function _n5planleaves(plan::N5LeafPlan)
    return plan.leaf:plan.leaf
end

function _n5planleaves(plan::Union{N5StructPlan,N5ListPlan,N5MapPlan})
    return plan.leaves
end

function _n5emitnull!(builders::Vector{N5LeafBuilder}, plan::N5PlanNode,
        repetition::Int, definition::Int)
    for leaf in _n5planleaves(plan)
        push!(builders[leaf].repetition, UInt64(repetition))
        push!(builders[leaf].definition, UInt64(definition))
    end
    return
end

function _n5shred!(builders::Vector{N5LeafBuilder}, plan::N5LeafPlan,
        value, repetition::Int)
    builder = builders[plan.leaf]
    push!(builder.repetition, UInt64(repetition))
    if value === missing
        plan.node.optional || throw(ArgumentError(
            "required N5 primitive $(repr(plan.node.name)) is null"))
        push!(builder.definition, UInt64(plan.parent_definition))
        return
    end
    push!(builder.definition, UInt64(plan.present_definition))
    push!(builder.values, value)
    return
end

function _n5shred!(builders::Vector{N5LeafBuilder}, plan::N5StructPlan,
        value, repetition::Int)
    if value === missing
        plan.node.optional || throw(ArgumentError(
            "required N5 struct $(repr(plan.node.name)) is null"))
        _n5emitnull!(builders, plan, repetition, plan.parent_definition)
        return
    end
    value isa N5Record || throw(ArgumentError(
        "N5 struct $(repr(plan.node.name)) needs an N5Record"))
    length(value.values) == length(plan.children) || throw(ArgumentError(
        "N5 struct $(repr(plan.node.name)) has the wrong field count"))
    for (child, fieldvalue) in zip(plan.children, value.values)
        _n5shred!(builders, child, fieldvalue, repetition)
    end
    return
end

function _n5shred!(builders::Vector{N5LeafBuilder}, plan::N5ListPlan,
        value, repetition::Int)
    if value === missing
        plan.node.optional || throw(ArgumentError(
            "required N5 LIST $(repr(plan.node.name)) is null"))
        _n5emitnull!(builders, plan, repetition, plan.parent_definition)
        return
    end
    value isa AbstractVector || throw(ArgumentError(
        "N5 LIST $(repr(plan.node.name)) needs a vector"))
    if isempty(value)
        _n5emitnull!(builders, plan, repetition, plan.present_definition)
        return
    end
    for index in eachindex(value)
        entryrepetition = index == firstindex(value) ? repetition :
            plan.entry_repetition
        _n5shred!(builders, plan.element, value[index], entryrepetition)
    end
    return
end

function _n5shred!(builders::Vector{N5LeafBuilder}, plan::N5MapPlan,
        value, repetition::Int)
    if value === missing
        plan.node.optional || throw(ArgumentError(
            "required N5 MAP $(repr(plan.node.name)) is null"))
        _n5emitnull!(builders, plan, repetition, plan.parent_definition)
        return
    end
    value isa N5MapValue || throw(ArgumentError(
        "N5 MAP $(repr(plan.node.name)) needs an N5MapValue"))
    if isempty(value.entries)
        _n5emitnull!(builders, plan, repetition, plan.present_definition)
        return
    end
    for index in eachindex(value.entries)
        entry = value.entries[index]
        entry.key === missing && throw(ArgumentError("N5 MAP key is null"))
        entryrepetition = index == firstindex(value.entries) ? repetition :
            plan.entry_repetition
        _n5shred!(builders, plan.key, entry.key, entryrepetition)
        if plan.value === nothing
            entry.hasvalue && throw(ArgumentError(
                "key-only N5 MAP entry carries a value"))
        else
            entry.hasvalue || throw(ArgumentError("N5 MAP entry omits its value"))
            _n5shred!(builders, plan.value, entry.value, entryrepetition)
        end
    end
    return
end

function n5shred(compiled::N5Compiled, rows::AbstractVector)
    builders = N5LeafBuilder[N5LeafBuilder(UInt64[], UInt64[], [])
        for _ in compiled.leaves]
    for row in rows
        _n5shred!(builders, compiled.root, row, 0)
    end
    streams = N5LeafStream[]
    sizehint!(streams, length(builders))
    for (builder, leaf) in zip(builders, compiled.leaves)
        push!(streams, N5LeafStream(builder.repetition, builder.definition,
            builder.values, leaf.max_repetition, leaf.max_definition))
    end
    return streams
end

function _n5expand(stream::N5LeafStream)
    length(stream.repetition) == length(stream.definition) ||
        throw(ArgumentError("N5 stream level lengths differ"))
    entries = Vector{Any}(undef, length(stream.definition))
    fill!(entries, N5_ABSENT)
    dense = 1
    for index in eachindex(stream.definition)
        if stream.definition[index] == UInt64(stream.max_definition)
            dense <= length(stream.values) || throw(ArgumentError(
                "N5 stream dense values underflow"))
            entries[index] = stream.values[dense]
            dense += 1
        end
    end
    dense == length(stream.values) + 1 || throw(ArgumentError(
        "N5 stream dense values overflow"))
    return N5ExpandedLeaf(stream.repetition, stream.definition, entries,
        stream.max_repetition, stream.max_definition)
end

function _n5rowranges(stream::N5ExpandedLeaf, rows::Int)
    rows == 0 && return UnitRange{Int}[]
    starts = Int[]
    for index in eachindex(stream.repetition)
        iszero(stream.repetition[index]) && push!(starts, index)
    end
    length(starts) == rows || throw(ArgumentError(
        "N5 stream has $(length(starts)) rows, expected $rows"))
    ranges = UnitRange{Int}[]
    sizehint!(ranges, rows)
    for index in eachindex(starts)
        stop = index == length(starts) ? length(stream.repetition) :
            starts[index + 1] - 1
        push!(ranges, starts[index]:stop)
    end
    return ranges
end

function _n5driver(plan::N5PlanNode)
    return first(_n5planleaves(plan))
end

function _n5firstdefinition(streams::Vector{N5ExpandedLeaf},
        ranges::Vector{UnitRange{Int}}, plan::N5PlanNode)
    leaf = _n5driver(plan)
    range = ranges[leaf]
    isempty(range) && throw(ArgumentError("N5 assembler received an empty range"))
    return Int(streams[leaf].definition[first(range)])
end

function _n5partitions(stream::N5ExpandedLeaf, range::UnitRange{Int},
        repetition::Int)
    isempty(range) && throw(ArgumentError("N5 assembler cannot partition an empty range"))
    starts = Int[first(range)]
    for index in (first(range) + 1):last(range)
        stream.repetition[index] <= UInt64(repetition) && push!(starts, index)
    end
    ranges = UnitRange{Int}[]
    sizehint!(ranges, length(starts))
    for index in eachindex(starts)
        stop = index == length(starts) ? last(range) : starts[index + 1] - 1
        push!(ranges, starts[index]:stop)
    end
    return ranges
end

function _n5entryranges(streams::Vector{N5ExpandedLeaf},
        ranges::Vector{UnitRange{Int}}, plan::N5PlanNode, repetition::Int)
    leaves = _n5planleaves(plan)
    partitions = Vector{Vector{UnitRange{Int}}}(undef, length(leaves))
    count = 0
    for (slot, leaf) in enumerate(leaves)
        current = _n5partitions(streams[leaf], ranges[leaf], repetition)
        if slot == 1
            count = length(current)
        else
            length(current) == count || throw(ArgumentError(
                "N5 sibling occurrence counts differ"))
        end
        partitions[slot] = current
    end
    output = Vector{Vector{UnitRange{Int}}}(undef, count)
    for occurrence in 1:count
        current = copy(ranges)
        for (slot, leaf) in enumerate(leaves)
            current[leaf] = partitions[slot][occurrence]
        end
        output[occurrence] = current
    end
    return output
end

function _n5assemble(plan::N5LeafPlan, streams::Vector{N5ExpandedLeaf},
        ranges::Vector{UnitRange{Int}})
    range = ranges[plan.leaf]
    length(range) == 1 || throw(ArgumentError(
        "N5 primitive occurrence has $(length(range)) level entries"))
    index = first(range)
    definition = Int(streams[plan.leaf].definition[index])
    if definition < plan.present_definition
        plan.node.optional || throw(ArgumentError(
            "required N5 primitive is absent"))
        return missing
    end
    value = streams[plan.leaf].entries[index]
    value isa N5Absent && throw(ArgumentError("N5 primitive dense value is absent"))
    return value
end

function _n5assemble(plan::N5StructPlan, streams::Vector{N5ExpandedLeaf},
        ranges::Vector{UnitRange{Int}})
    definition = _n5firstdefinition(streams, ranges, plan)
    if definition < plan.present_definition
        plan.node.optional || throw(ArgumentError("required N5 struct is absent"))
        return missing
    end
    values = Any[_n5assemble(child, streams, ranges) for child in plan.children]
    return N5Record(values)
end

function _n5assemble(plan::N5ListPlan, streams::Vector{N5ExpandedLeaf},
        ranges::Vector{UnitRange{Int}})
    definition = _n5firstdefinition(streams, ranges, plan)
    if definition < plan.present_definition
        plan.node.optional || throw(ArgumentError("required N5 LIST is absent"))
        return missing
    end
    definition < plan.entry_definition && return []
    entries = _n5entryranges(streams, ranges, plan,
        plan.entry_repetition)
    return Any[_n5assemble(plan.element, streams, entry) for entry in entries]
end

function _n5assemble(plan::N5MapPlan, streams::Vector{N5ExpandedLeaf},
        ranges::Vector{UnitRange{Int}})
    definition = _n5firstdefinition(streams, ranges, plan)
    if definition < plan.present_definition
        plan.node.optional || throw(ArgumentError("required N5 MAP is absent"))
        return missing
    end
    definition < plan.entry_definition && return N5MapValue(N5Entry[])
    rangesbyentry = _n5entryranges(streams, ranges, plan,
        plan.entry_repetition)
    entries = N5Entry[]
    sizehint!(entries, length(rangesbyentry))
    for entryranges in rangesbyentry
        key = _n5assemble(plan.key, streams, entryranges)
        key === missing && throw(ArgumentError("N5 MAP key is null"))
        if plan.value === nothing
            push!(entries, N5Entry(key, nothing, false))
        else
            value = _n5assemble(plan.value, streams, entryranges)
            push!(entries, N5Entry(key, value, true))
        end
    end
    return N5MapValue(entries)
end

function n5assemble(compiled::N5Compiled, streams::Vector{N5LeafStream},
        rows::Integer)
    length(streams) == length(compiled.leaves) || throw(ArgumentError(
        "N5 assembler leaf count differs"))
    rowcount = Int(rows)
    rowcount >= 0 || throw(ArgumentError("N5 row count is negative"))
    expanded = N5ExpandedLeaf[_n5expand(stream) for stream in streams]
    ranges = Vector{Vector{UnitRange{Int}}}(undef, length(expanded))
    for index in eachindex(expanded)
        ranges[index] = _n5rowranges(expanded[index], rowcount)
    end
    output = []
    sizehint!(output, rowcount)
    for row in 1:rowcount
        rowranges = UnitRange{Int}[ranges[leaf][row] for leaf in eachindex(ranges)]
        push!(output, _n5assemble(compiled.root, expanded, rowranges))
    end
    return output
end

function _n5repetition(optional::Bool)
    return optional ? MD.FieldRepetitionType.OPTIONAL :
        MD.FieldRepetitionType.REQUIRED
end

function _n5repetition(optional::Bool, override::Union{Nothing,Symbol})
    override === nothing && return _n5repetition(optional)
    override === :repeated && return MD.FieldRepetitionType.REPEATED
    throw(ArgumentError("unsupported N5 repetition override $override"))
end

function _n5primitiveannotations(node::N5Primitive)
    node.logical === :none && return (nothing, nothing)
    logical = MD.LogicalType(STRING=MD.StringType())
    return (logical, MD.ConvertedType.UTF8)
end

function _n5listannotations(annotation::Symbol)
    logical = annotation in (:modern, :dual, :conflict) ?
        MD.LogicalType(LIST=MD.ListType()) : nothing
    converted = annotation in (:legacy, :dual) ? MD.ConvertedType.LIST :
        annotation === :conflict ? MD.ConvertedType.MAP : nothing
    return (logical, converted)
end

function _n5mapannotations(marker::Symbol)
    logical = marker in (:modern, :dual, :conflict, :modern_alias,
        :modern_primitive, :modern_unknown) ?
        MD.LogicalType(MAP=MD.MapType()) : nothing
    converted = marker in (:legacy, :dual) ? MD.ConvertedType.MAP :
        marker === :alias ? MD.ConvertedType.MAP_KEY_VALUE :
        marker === :conflict ? MD.ConvertedType.LIST :
        marker === :modern_alias ? MD.ConvertedType.MAP_KEY_VALUE :
        marker === :modern_primitive ? MD.ConvertedType.UTF8 :
        marker === :modern_unknown ? MD.ConvertedType.T(99) : nothing
    return (logical, converted)
end

function _n5futurelogical(id::Int16=Int16(2555))
    return MD.LogicalType(unknown_fields=(
        TH.RawField(id, TH.STRUCT, UInt8[0x00]),))
end

function _n5mapentryannotations(marker::Symbol)
    marker === :none && return (nothing, nothing)
    marker === :marked && return (nothing, MD.ConvertedType.MAP_KEY_VALUE)
    marker === :empty && return (MD.LogicalType(), nothing)
    marker === :future && return (_n5futurelogical(), nothing)
    marker === :future_marked && return (_n5futurelogical(),
        MD.ConvertedType.MAP_KEY_VALUE)
    marker === :unknown_converted && return (nothing, MD.ConvertedType.T(99))
    throw(ArgumentError("unsupported N5 MAP entry marker $marker"))
end

function _n5schemafield!(schema::Vector{MD.SchemaElement}, node::N5Primitive;
        repetition::Union{Nothing,Symbol}=nothing,
        name::Union{Nothing,String}=nothing)
    logical, converted = _n5primitiveannotations(node)
    push!(schema, MD.SchemaElement(type_=node.physical,
        repetition_type=_n5repetition(node.optional, repetition),
        name=something(name, node.name), converted_type=converted,
        logicalType=logical))
    return
end

function _n5schemafield!(schema::Vector{MD.SchemaElement}, node::N5Struct;
        repetition::Union{Nothing,Symbol}=nothing,
        name::Union{Nothing,String}=nothing)
    push!(schema, MD.SchemaElement(
        repetition_type=_n5repetition(node.optional, repetition),
        name=something(name, node.name), num_children=Int32(length(node.fields))))
    for field in node.fields
        _n5schemafield!(schema, field)
    end
    return
end

function _n5schemalistentry!(schema::Vector{MD.SchemaElement}, node::N5List)
    if node.layout === :rule1
        _n5schemafield!(schema, node.element; repetition=:repeated)
    elseif node.layout === :rule2
        element = node.element::N5Struct
        _n5schemafield!(schema, element; repetition=:repeated,
            name=node.wrapper)
    elseif node.layout === :rule3
        _n5schemafield!(schema, node.element; repetition=:repeated,
            name=node.wrapper)
    elseif node.layout === :rule4
        element = node.element::N5Struct
        _n5schemafield!(schema, element; repetition=:repeated,
            name=node.wrapper)
    elseif node.layout === :rule5
        push!(schema, MD.SchemaElement(
            repetition_type=MD.FieldRepetitionType.REPEATED,
            name=node.wrapper, num_children=Int32(1)))
        _n5schemafield!(schema, node.element)
    else
        push!(schema, MD.SchemaElement(
            repetition_type=MD.FieldRepetitionType.REPEATED,
            name=node.wrapper, num_children=Int32(1)))
        _n5schemafield!(schema, node.element; name="element")
    end
    return
end

function _n5schemafield!(schema::Vector{MD.SchemaElement}, node::N5List;
        repetition::Union{Nothing,Symbol}=nothing,
        name::Union{Nothing,String}=nothing)
    logical, converted = _n5listannotations(node.annotation)
    push!(schema, MD.SchemaElement(
        repetition_type=_n5repetition(node.optional, repetition),
        name=something(name, node.name), num_children=Int32(1),
        converted_type=converted, logicalType=logical))
    _n5schemalistentry!(schema, node)
    return
end

function _n5schemafield!(schema::Vector{MD.SchemaElement}, node::N5Map;
        repetition::Union{Nothing,Symbol}=nothing,
        name::Union{Nothing,String}=nothing)
    logical, converted = _n5mapannotations(node.marker)
    push!(schema, MD.SchemaElement(
        repetition_type=_n5repetition(node.optional, repetition),
        name=something(name, node.name), num_children=Int32(1),
        converted_type=converted, logicalType=logical))
    childcount = node.value === nothing ? 1 : 2
    entrylogical, entryconverted = _n5mapentryannotations(node.entrymarker)
    push!(schema, MD.SchemaElement(
        repetition_type=MD.FieldRepetitionType.REPEATED,
        name=node.entryname, num_children=Int32(childcount),
        converted_type=entryconverted, logicalType=entrylogical))
    _n5schemafield!(schema, node.key)
    node.value === nothing || _n5schemafield!(schema, node.value)
    return
end

function n5schema(node::N5Node; rootname::AbstractString="schema")
    schema = MD.SchemaElement[MD.SchemaElement(name=String(rootname),
        num_children=Int32(1))]
    _n5schemafield!(schema, node)
    return schema
end

function _n5physicalnode!(leaves::Vector{N5PhysicalLeaf},
        schema::Vector{MD.SchemaElement}, index::Int, path::Vector{String},
        repetition::Int, definition::Int)
    index <= length(schema) || throw(ArgumentError("N5 schema ends early"))
    element = schema[index]
    nextpath = copy(path)
    push!(nextpath, element.name)
    fieldrepetition = element.repetition_type
    nextrepetition = repetition + Int(fieldrepetition ==
        MD.FieldRepetitionType.REPEATED)
    nextdefinition = definition + Int(fieldrepetition in
        (MD.FieldRepetitionType.OPTIONAL, MD.FieldRepetitionType.REPEATED))
    if element.type_ !== nothing
        push!(leaves, N5PhysicalLeaf(element, nextpath, nextrepetition,
            nextdefinition))
        return index + 1
    end
    children = something(element.num_children, Int32(0))
    children >= 0 || throw(ArgumentError("N5 schema has negative child count"))
    cursor = index + 1
    for _ in 1:Int(children)
        cursor = _n5physicalnode!(leaves, schema, cursor, nextpath,
            nextrepetition, nextdefinition)
    end
    return cursor
end

function n5physicalleaves(schema::Vector{MD.SchemaElement})
    isempty(schema) && throw(ArgumentError("N5 schema is empty"))
    root = schema[1]
    root.type_ === nothing || throw(ArgumentError("N5 root is primitive"))
    root.num_children == 1 || throw(ArgumentError("N5 model needs one root field"))
    leaves = N5PhysicalLeaf[]
    cursor = _n5physicalnode!(leaves, schema, 2, String[], 0, 0)
    cursor == length(schema) + 1 || throw(ArgumentError("N5 schema has trailing nodes"))
    return leaves
end
