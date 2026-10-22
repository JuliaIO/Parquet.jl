struct SchemaNode
    element::Metadata.SchemaElement
    path::Vector{String}
    max_definition_level::Int16
    max_repetition_level::Int16
    column_index::Int32
    children::Vector{SchemaNode}
end

struct Schema
    root::SchemaNode
    leaves::Vector{SchemaNode}
end

function _schemalevels(element::Metadata.SchemaElement, definition::Integer,
    repetition::Integer)
    kind = element.repetition_type
    kind === nothing && throw(FormatError("schema element $(repr(element.name)) has no repetition type"))
    if kind == Metadata.FieldRepetitionType.REQUIRED
        return (definition, repetition)
    elseif kind == Metadata.FieldRepetitionType.OPTIONAL
        return (definition + 1, repetition)
    elseif kind == Metadata.FieldRepetitionType.REPEATED
        return (definition + 1, repetition + 1)
    end
    throw(FormatError("schema element $(repr(element.name)) has an unknown repetition type $(kind.value)"))
end

function _schemachildcount(element::Metadata.SchemaElement)
    count = element.num_children
    count === nothing && throw(FormatError("group schema element $(repr(element.name)) has no child count"))
    count >= 0 || throw(FormatError("group schema element $(repr(element.name)) has a negative child count"))
    return Int(count)
end

function _validateschemashape(element::Metadata.SchemaElement, limits::Limits)
    if element.type_ === nothing
        return _schemachildcount(element)
    end
    children = something(element.num_children, Int32(0))
    children == 0 || throw(FormatError("primitive schema element $(repr(element.name)) has children"))
    if element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY
        length = element.type_length
        length !== nothing && length > 0 ||
            throw(FormatError("fixed-length schema element $(repr(element.name)) has no positive length"))
        _checklimit(:string_bytes, length, limits.max_string_bytes)
    end
    return 0
end

mutable struct _SchemaParseFrame
    element::Metadata.SchemaElement
    path::Vector{String}
    definition::Int16
    repetition::Int16
    children::Vector{SchemaNode}
    expected::Int
    completed::Int
    parent::Union{Nothing,_SchemaParseFrame}
end

function _schemapath(parent::Vector{String}, name::String,
    budget::_LiveByteBudget)
    count = length(parent) + 1
    _reservearray!(budget, String, count)
    path = Vector{String}(undef, count)
    copyto!(path, 1, parent, 1, length(parent))
    path[end] = name
    return path
end

function _schemaparsestart(elements::Vector{Metadata.SchemaElement}, index::Int,
        parentpath::Vector{String}, definition::Integer, repetition::Integer,
        depth::Int,
        leaves::Vector{SchemaNode}, limits::Limits, budget::_LiveByteBudget,
        parent; root::Bool=false)
    index <= length(elements) || throw(FormatError("flattened schema ends before all declared children"))
    element = elements[index]
    children = _validateschemashape(element, limits)
    nextdefinition, nextrepetition = root ? (definition, repetition) :
        _schemalevels(element, definition, repetition)
    if element.type_ === nothing
        children <= length(elements) - index || throw(FormatError(
            "group schema element $(repr(element.name)) declares more direct " *
            "children than remain in the flattened schema"))
    end
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    nextdefinition <= typemax(Int16) || throw(FormatError("schema definition level exceeds Int16"))
    nextrepetition <= typemax(Int16) || throw(FormatError("schema repetition level exceeds Int16"))
    path = root ? parentpath : _schemapath(parentpath, element.name, budget)
    if element.type_ !== nothing
        ordinal = try
            Base.checked_add(length(leaves), 1)
        catch err
            err isa OverflowError || rethrow()
            throw(LimitError(:container_elements, typemax(Int64),
                Int64(typemax(Int32))))
        end
        ordinal <= typemax(Int32) || throw(LimitError(:container_elements,
            ordinal, Int64(typemax(Int32))))
        column = Int32(ordinal)
        _reservearray!(budget, SchemaNode, 0)
        _reserveobjects!(budget)
        node = SchemaNode(element, path, Int16(nextdefinition), Int16(nextrepetition), column, SchemaNode[])
        push!(leaves, node)
        return (node, nothing, index + 1)
    end
    _checklimit(:container_elements, children,
        limits.max_container_elements)
    _reservearray!(budget, SchemaNode, children)
    nodes = SchemaNode[]
    sizehint!(nodes, children)
    _reserveobjects!(budget)
    frame = _SchemaParseFrame(element, path, Int16(nextdefinition),
        Int16(nextrepetition), nodes, children, 0, parent)
    return (nothing, frame, index + 1)
end

function _parseschema(elements::Vector{Metadata.SchemaElement},
        leaves::Vector{SchemaNode}, rootpath::Vector{String}, limits::Limits,
        budget::_LiveByteBudget)
    node, frame, nextindex = _schemaparsestart(elements, 1, rootpath, 0, 0, 1,
        leaves, limits, budget, nothing; root=true)
    node === nothing || throw(AssertionError("schema root parser returned a primitive"))
    current = frame::_SchemaParseFrame
    while true
        if current.completed == current.expected
            _reserveobjects!(budget)
            completed = SchemaNode(current.element, current.path,
                current.definition, current.repetition, Int32(0), current.children)
            parent = current.parent
            _release!(budget, _MATERIALIZED_OBJECT_BYTES)
            parent === nothing && return (completed, nextindex)
            current = parent::_SchemaParseFrame
            push!(current.children, completed)
            current.completed += 1
            continue
        end
        child, childframe, nextindex = _schemaparsestart(elements, nextindex,
            current.path, current.definition, current.repetition,
            length(current.path) + 2, leaves, limits, budget, current)
        if childframe === nothing
            push!(current.children, child::SchemaNode)
            current.completed += 1
        else
            current = childframe::_SchemaParseFrame
        end
    end
end

function Schema(elements::Vector{Metadata.SchemaElement}; limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    start = _budgetused(budget)
    try
        isempty(elements) && throw(FormatError("file metadata has an empty schema"))
        elements[1].type_ === nothing || throw(FormatError("schema root must be a group"))
        rootrepetition = elements[1].repetition_type
        (rootrepetition === nothing ||
            rootrepetition == Metadata.FieldRepetitionType.REQUIRED) ||
            throw(FormatError("schema root can only use the legacy REQUIRED marker"))
        rootchildren = _validateschemashape(elements[1], limits)
        rootchildren <= length(elements) - 1 || throw(FormatError(
            "group schema element $(repr(elements[1].name)) declares more direct " *
            "children than remain in the flattened schema"))
        iszero(rootchildren) && length(elements) > 1 && throw(FormatError(
            "flattened schema has unclaimed elements"))
        _checklimit(:container_elements, length(elements), limits.max_container_elements)
        _reservearray!(budget, SchemaNode, length(elements))
        leaves = SchemaNode[]
        sizehint!(leaves, length(elements))
        _reservearray!(budget, String, 0)
        rootpath = String[]
        root, nextindex = _parseschema(elements, leaves, rootpath, limits, budget)
        nextindex == length(elements) + 1 || throw(FormatError(
            "flattened schema has unclaimed elements"))
        _reserveobjects!(budget)
        return Schema(root, leaves)
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function Schema(metadata::Metadata.FileMetaData; limits::Limits=Limits(),
    budget::_LiveByteBudget=_LiveByteBudget(limits))
    return Schema(metadata.schema; limits=limits, budget=budget)
end
