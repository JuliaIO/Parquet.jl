# Iterative declared-type inference and Dremel shredding for canonical nested writes.

abstract type _NestedWriteShape end

mutable struct _NestedWriteLeafAggregate
    seen::Bool
    adjusted::Union{Nothing,Bool}
    scale::Union{Nothing,Int32}
    precision::Int32
end

struct _NestedWriteLeafShape <: _NestedWriteShape
    name::String
    value_type::Type
    optional::Bool
    explicit::Union{Nothing,_ScalarLogicalColumnSpec}
    fixed_width::Union{Nothing,Int32}
    aggregate::_NestedWriteLeafAggregate
    source_snapshot::Any
end


function _NestedWriteLeafShape(name::String, value_type::Type, optional::Bool,
        explicit::Union{Nothing,_ScalarLogicalColumnSpec},
        fixed_width::Union{Nothing,Int32},
        aggregate::_NestedWriteLeafAggregate)
    return _NestedWriteLeafShape(name, value_type, optional, explicit,
        fixed_width, aggregate, nothing)
end

struct _NestedWriteStructShape <: _NestedWriteShape
    name::String
    value_type::Type
    optional::Bool
    names::Vector{String}
    children::Vector{_NestedWriteShape}
    source_children::Union{Nothing,Vector{AbstractVector}}
    source_snapshot::Any
    package_owned::Bool
end

struct _NestedWriteListShape <: _NestedWriteShape
    name::String
    value_type::Type
    optional::Bool
    element::_NestedWriteShape
    source_snapshot::Any
end

struct _NestedWriteMapShape <: _NestedWriteShape
    name::String
    value_type::Type
    optional::Bool
    key::_NestedWriteShape
    value::_NestedWriteShape
    source_snapshot::Any
    package_owned::Bool
    source_has_values::Bool
end

abstract type _NestedWriteNodePlan end

struct _NestedWriteLeafPlan <: _NestedWriteNodePlan
    semantic::_NestedLeafPlan
    shape::_NestedWriteLeafShape
end

struct _NestedWriteStructPlan <: _NestedWriteNodePlan
    semantic::_NestedStructPlan
    shape::_NestedWriteStructShape
    children::Vector{_NestedWriteNodePlan}
end

struct _NestedWriteListPlan <: _NestedWriteNodePlan
    semantic::_NestedListPlan
    shape::_NestedWriteListShape
    element::_NestedWriteNodePlan
end

struct _NestedWriteMapPlan <: _NestedWriteNodePlan
    semantic::_NestedMapPlan
    shape::_NestedWriteMapShape
    key::_NestedWriteNodePlan
    value::_NestedWriteNodePlan
end

mutable struct _NestedWriteLeafCount
    entries::Int64
    dense::Int64
    payload_bytes::Int64
end

mutable struct _NestedWriteLeafBuilder
    repetition::Vector{UInt64}
    definition::Vector{UInt64}
    values::Vector
    entry_position::Int
    dense_position::Int
    payload_position::Int64
end

const _NESTED_WRITE_TRACE_LEAF = UInt8(1)
const _NESTED_WRITE_TRACE_STRUCT = UInt8(2)
const _NESTED_WRITE_TRACE_LIST = UInt8(3)
const _NESTED_WRITE_TRACE_MAP = UInt8(4)
const _NESTED_WRITE_TRACE_KEY = UInt8(5)
const _NESTED_WRITE_TRACE_SOURCE = UInt8(6)
const _NESTED_WRITE_TRACE_CHUNK = 128

const _NESTED_WRITE_KEY_SCALAR = UInt8(1)
const _NESTED_WRITE_KEY_BYTES = UInt8(2)
const _NESTED_WRITE_KEY_STRING = UInt8(3)
const _NESTED_WRITE_KEY_TUPLE = UInt8(4)
const _NESTED_WRITE_KEY_NAMED_TUPLE = UInt8(5)
const _NESTED_WRITE_KEY_LIST = UInt8(6)
const _NESTED_WRITE_KEY_STRUCT = UInt8(7)
const _NESTED_WRITE_KEY_MAP = UInt8(8)
const _NESTED_WRITE_KEY_PAIR = UInt8(9)

struct _NestedWriteKeySnapshot
    kind::UInt8
    source_type::Type
    first::Int
    last::Int
    value::Any
    names::Any
    children::Vector{_NestedWriteKeySnapshot}
end

const _NESTED_WRITE_EMPTY_KEYS = _NestedWriteKeySnapshot[]

struct _NestedWriteKeyCollectionState
    count::Int
    first::Int
    last::Int
    indexed::Bool
    source::Any
end

mutable struct _NestedWriteKeyCaptureFrame
    parent::Union{Nothing,_NestedWriteKeyCaptureFrame}
    parent_slot::Int
    value::Any
    shape::Union{Nothing,_NestedWriteShape}
    witness::Any
    row_witness::Any
    state::_NestedWriteKeyCollectionState
    depth::Int
    kind::UInt8
    children::Vector{_NestedWriteKeySnapshot}
    names::Any
    position::Int
    iterator::Any
    iteration::Any
    pending::Any
    next_value::Any
    next_shape::Union{Nothing,_NestedWriteShape}
    next_witness::Any
    next_row_witness::Any
end

mutable struct _NestedWriteKeyCompareFrame
    parent::Union{Nothing,_NestedWriteKeyCompareFrame}
    expected::_NestedWriteKeySnapshot
    current::Any
    shape::Union{Nothing,_NestedWriteShape}
    witness::Any
    row_witness::Any
    state::_NestedWriteKeyCollectionState
    depth::Int
    position::Int
    iterator::Any
    iteration::Any
    pending::Any
    next_expected::Union{Nothing,_NestedWriteKeySnapshot}
    next_current::Any
    next_shape::Union{Nothing,_NestedWriteShape}
    next_witness::Any
    next_row_witness::Any
end

struct _NestedWriteTraceEvent
    kind::UInt8
    a::Int64
    b::Int64
    c::Int64
    d::Int64
    value::Any
end

mutable struct _NestedWriteTraceChunk
    events::Vector{_NestedWriteTraceEvent}
    used::Int
    next::Union{Nothing,_NestedWriteTraceChunk}
end

mutable struct _NestedWriteTrace
    budget::_LiveByteBudget
    first::Union{Nothing,_NestedWriteTraceChunk}
    last::Union{Nothing,_NestedWriteTraceChunk}
    current::Union{Nothing,_NestedWriteTraceChunk}
    position::Int
    capturing::Bool
    charge::Int64
    topology::Any
end

const _NESTED_WRITE_VECTOR_GENERIC = UInt8(0)
const _NESTED_WRITE_VECTOR_LOGICAL = UInt8(1)
const _NESTED_WRITE_VECTOR_FIXED = UInt8(2)
const _NESTED_WRITE_VECTOR_LIST = UInt8(3)
const _NESTED_WRITE_VECTOR_STRUCT = UInt8(4)
const _NESTED_WRITE_VECTOR_MAP = UInt8(5)

struct _NestedWriteVectorSnapshot
    source::AbstractVector
    kind::UInt8
    count::Int
    first::Int
    last::Int
    primary::Any
    secondary::Any
    tertiary::Any
    quaternary::Any
    copy1::Any
    copy2::Any
    copy3::Any
    scalar::Int64
end

mutable struct _NestedWriteSnapshotLookup
    sources::Vector{Union{Nothing,AbstractVector}}
    snapshots::Vector{Union{Nothing,_NestedWriteVectorSnapshot}}
    count::Int
    charge::Int64
end

mutable struct _NestedWriteDictEntry
    key::Any
    value::Any
    next::Any
end

mutable struct _NestedWriteDictDependency
    snapshot::Any
    next::Any
end

struct _NestedWriteStructViewSnapshot
    source::StructValue
    names::Vector{String}
    children::Vector{AbstractVector}
    names_copy::Vector{String}
    children_copy::Vector{AbstractVector}
    index::Int
end

mutable struct _NestedWriteIdentitySet
    sources::Vector{Union{Nothing,AbstractVector}}
    count::Int
end

mutable struct _NestedWriteDictWork
    value::Any
    shape::Union{Nothing,_NestedWriteShape}
    depth::Int
    source::Bool
    next::Any
end

mutable struct _NestedWriteDictMaterialization
    first::Any
    last::Any
    dependencies::Any
    dependency_last::Any
    dependency_sources::Any
    preflight_sources::Any
    count::Int
    charge::Int64
end

mutable struct _NestedWriteTopologySnapshot
    input_columns::Union{Nothing,AbstractVector}
    input_count::Int
    input_first::Int
    input_last::Int
    names::Vector{String}
    values::Vector{AbstractVector}
    nodes::Vector{_NestedWriteVectorSnapshot}
    lookup::_NestedWriteSnapshotLookup
    base_nodes::Int
    charge::Int64
end

struct _NestedWriteRowWitness
    snapshot::_NestedWriteVectorSnapshot
    index::Int
    first::Int
    last::Int
    present::Bool
end

struct _NestedWriteSourceFrame
    values::AbstractVector
    depth::Int
    position::Int
    count::Int
    exit::Bool
end

const _NESTED_WRITE_SHAPE_STRUCT = UInt8(1)
const _NESTED_WRITE_SHAPE_LIST = UInt8(2)
const _NESTED_WRITE_SHAPE_MAP = UInt8(3)

struct _NestedWriteShapeFrame
    kind::UInt8
    name::String
    value_type::Type
    optional::Bool
    source::Any
    depth::Int
    names::Union{Nothing,Vector{String}}
    source_children::Union{Nothing,Vector{AbstractVector}}
    child_types::Any
    first_source::Any
    second_source::Any
    children::Union{Nothing,Vector{_NestedWriteShape}}
    first::Union{Nothing,_NestedWriteShape}
    second::Union{Nothing,_NestedWriteShape}
    position::Int
    package_owned::Bool
    source_has_values::Bool
end

struct _NestedWriteSchemaFrame
    shape::_NestedWriteShape
    fragments::Union{Nothing,Vector{Vector{Metadata.SchemaElement}}}
    first::Union{Nothing,Vector{Metadata.SchemaElement}}
    second::Union{Nothing,Vector{Metadata.SchemaElement}}
    position::Int
    count::Int
end

struct _NestedWriteBindFrame
    shape::_NestedWriteShape
    semantic::_NestedPlan
    children::Union{Nothing,Vector{_NestedWriteNodePlan}}
    first::Union{Nothing,_NestedWriteNodePlan}
    second::Union{Nothing,_NestedWriteNodePlan}
    position::Int
end

const _NESTED_WRITE_SCAN_ENTER = UInt8(1)
const _NESTED_WRITE_SCAN_POSTCHECK = UInt8(2)
const _NESTED_WRITE_SCAN_STRUCT = UInt8(3)
const _NESTED_WRITE_SCAN_LIST = UInt8(4)
const _NESTED_WRITE_SCAN_MAP_DICT = UInt8(5)
const _NESTED_WRITE_SCAN_MAP_VIEW_KEY = UInt8(6)
const _NESTED_WRITE_SCAN_MAP_VIEW_VALUE = UInt8(7)
const _NESTED_WRITE_SCAN_MAP_ITER_KEY = UInt8(8)
const _NESTED_WRITE_SCAN_MAP_ITER_VALUE = UInt8(9)
const _NESTED_WRITE_SCAN_MAP_ITER_NEXT = UInt8(10)
const _NESTED_WRITE_SCAN_KEYASSERT = UInt8(11)
const _NESTED_WRITE_SCAN_DICT_RELEASE = UInt8(12)

struct _NestedWriteScanAction
    kind::UInt8
    shape::_NestedWriteShape
    value::Any
    row_witness::Any
    other_witness::Any
    state::Any
    expected::Union{Nothing,_NestedWriteKeySnapshot}
    materialization::Union{Nothing,_NestedWriteDictMaterialization}
    snapshot1::Any
    snapshot2::Any
    position::Int
    count::Int
    first::Int
    last::Int
end

const _NESTED_WRITE_SHRED_ENTER = UInt8(1)
const _NESTED_WRITE_SHRED_POSTCHECK = UInt8(2)
const _NESTED_WRITE_SHRED_STRUCT = UInt8(3)
const _NESTED_WRITE_SHRED_LIST = UInt8(4)
const _NESTED_WRITE_SHRED_MAP_DICT = UInt8(5)
const _NESTED_WRITE_SHRED_MAP_VIEW_KEY = UInt8(6)
const _NESTED_WRITE_SHRED_MAP_VIEW_VALUE = UInt8(7)
const _NESTED_WRITE_SHRED_MAP_ITER_KEY = UInt8(8)
const _NESTED_WRITE_SHRED_MAP_ITER_VALUE = UInt8(9)
const _NESTED_WRITE_SHRED_MAP_ITER_NEXT = UInt8(10)
const _NESTED_WRITE_SHRED_KEYASSERT = UInt8(11)
const _NESTED_WRITE_SHRED_DICT_RELEASE = UInt8(12)

struct _NestedWriteShredAction
    kind::UInt8
    plan::_NestedWriteNodePlan
    value::Any
    expected::Union{Nothing,_NestedWriteKeySnapshot}
    keymode::Bool
    repetition::UInt64
    row_witness::Any
    other_witness::Any
    state::Any
    nested::Union{Nothing,_NestedWriteKeySnapshot}
    materialization::Union{Nothing,_NestedWriteDictMaterialization}
    snapshot1::Any
    snapshot2::Any
    position::Int
    count::Int
    first::Int
    last::Int
    rawvalue::Any
    value_witness::Any
    phase::UInt8
end

mutable struct _NestedWritePassStack{T}
    frames::Vector{T}
    highwater::Int
    processing::Bool
    charge::Int64
end

function Base.isempty(stack::_NestedWritePassStack)
    return isempty(stack.frames)
end

function Base.lastindex(stack::_NestedWritePassStack)
    return lastindex(stack.frames)
end

function Base.getindex(stack::_NestedWritePassStack, index::Int)
    return stack.frames[index]
end

function Base.setindex!(stack::_NestedWritePassStack{T}, frame::T,
        index::Int) where {T}
    stack.frames[index] = frame
    return frame
end

function _NestedWriteShredAction(kind::UInt8, plan::_NestedWriteNodePlan,
        value, expected::Union{Nothing,_NestedWriteKeySnapshot}, keymode::Bool,
        repetition::UInt64, row_witness, other_witness, state,
        nested::Union{Nothing,_NestedWriteKeySnapshot},
        materialization::Union{Nothing,_NestedWriteDictMaterialization},
        snapshot1, snapshot2, position::Int, count::Int, first::Int, last::Int,
        phase::UInt8)
    return _NestedWriteShredAction(kind, plan, value, expected, keymode,
        repetition, row_witness, other_witness, state, nested,
        materialization, snapshot1, snapshot2, position, count, first, last,
        nothing, nothing, phase)
end

function _nestedwritepassstackstart(::Type{T},
        budget::_LiveByteBudget) where {T}
    charge = _materializedsum(_materializedarraybytes(T, 0),
        _MATERIALIZED_OBJECT_BYTES)
    _reserve!(budget, charge)
    try
        return _NestedWritePassStack(T[], 0, false, charge)
    catch
        _release!(budget, charge)
        rethrow()
    end
end

function _nestedwritestackpush!(stack::_NestedWritePassStack{T}, frame::T,
        budget::_LiveByteBudget) where {T}
    iszero(stack.charge) && throw(AssertionError(
        "nested writer cannot reuse a released pass stack"))
    active = length(stack.frames) + 1 + (stack.processing ? 1 : 0)
    added = active > stack.highwater
    framecharge = added ? _materializedarraybytes(T, 1; header=false) :
        Int64(0)
    added && _reserve!(budget, framecharge)
    try
        push!(stack.frames, frame)
    catch
        added && _release!(budget, framecharge)
        rethrow()
    end
    if added
        stack.highwater = active
        stack.charge = _materializedsum(stack.charge, framecharge)
    end
    return
end

function _nestedwritepassstackpop!(stack::_NestedWritePassStack)
    frame = pop!(stack.frames)
    stack.processing = true
    return frame
end

function _nestedwritepassstackprocessed!(stack::_NestedWritePassStack)
    stack.processing || throw(AssertionError(
        "nested writer pass stack has no active action"))
    stack.processing = false
    return
end

function _nestedwritepassstackclear!(stack::_NestedWritePassStack)
    empty!(stack.frames)
    stack.processing = false
    return
end

function _nestedwritepassstackrelease!(stack::_NestedWritePassStack,
        budget::_LiveByteBudget)
    _nestedwritepassstackclear!(stack)
    charge = stack.charge
    stack.highwater = 0
    stack.charge = Int64(0)
    _release!(budget, charge)
    return
end

function _nestedwritestackpop!(stack::_NestedWritePassStack,
        ::_LiveByteBudget)
    return pop!(stack.frames)
end

function _nestedwritedepthadd(depth::Int, increment::Int, limits::Limits)
    requested = try
        Base.checked_add(depth, increment)
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:metadata_depth, typemax(Int64),
            limits.max_metadata_depth))
    end
    return requested
end

function _nestedwritesourcechildcount(values::AbstractVector)
    Base.@nospecialize values
    values isa Union{LogicalColumn,FixedByteArrayVector,ListVector} && return 1
    values isa StructVector && return length(values.children)
    values isa MapVector && return values.values === nothing ? 1 : 2
    return 0
end

function _nestedwritesourcechild(values::AbstractVector, position::Int,
        count::Int)
    Base.@nospecialize values
    _nestedwritesourcechildcount(values) == count || throw(ArgumentError(
        "nested Parquet vector child count changed during traversal"))
    1 <= position <= count || throw(AssertionError(
        "nested Parquet source traversal has an invalid child position"))
    if values isa Union{LogicalColumn,FixedByteArrayVector,ListVector}
        return values.values
    elseif values isa StructVector
        return values.children[position]
    end
    map = values::MapVector
    position == 1 && return map.keys
    return something(map.values)
end

function _nestedwritesourceschedule!(
        stack::_NestedWritePassStack{_NestedWriteSourceFrame},
        frame::_NestedWriteSourceFrame, limits::Limits,
        budget::_LiveByteBudget; postorder::Bool=false)
    position = frame.position + 1
    position > frame.count && return false
    child = _nestedwritesourcechild(frame.values, position, frame.count)
    childdepth = _nestedwritedepthadd(frame.depth, 1, limits)
    _checklimit(:metadata_depth, childdepth, limits.max_metadata_depth)
    if position < frame.count
        _nestedwritestackpush!(stack, _NestedWriteSourceFrame(frame.values,
            frame.depth, position, frame.count, false), budget)
    elseif postorder
        _nestedwritestackpush!(stack, _NestedWriteSourceFrame(frame.values,
            frame.depth, frame.count, frame.count, true), budget)
    end
    _nestedwritestackpush!(stack, _NestedWriteSourceFrame(child, childdepth,
        0, -1, false), budget)
    return true
end

function _nestedwritetrace(budget::_LiveByteBudget)
    return _nestedwritetrace(budget, nothing)
end

function _nestedwritetrace(budget::_LiveByteBudget, topology)
    charge = _reserveobjects!(budget)
    return _NestedWriteTrace(budget, nothing, nothing, nothing, 1, true,
        charge, topology)
end

function _nestedwritetracereserve!(trace::_NestedWriteTrace, bytes::Int64)
    _reserve!(trace.budget, bytes)
    trace.charge = _materializedsum(trace.charge, bytes)
    return
end

function _nestedwritetraceunreserve!(trace::_NestedWriteTrace, bytes::Int64)
    _release!(trace.budget, bytes)
    trace.charge = Base.checked_sub(trace.charge, bytes)
    return
end

function _nestedwritetracechunk!(trace::_NestedWriteTrace)
    charge = _materializedsum(
        _materializedarraybytes(_NestedWriteTraceEvent,
            _NESTED_WRITE_TRACE_CHUNK), _MATERIALIZED_OBJECT_BYTES)
    _nestedwritetracereserve!(trace, charge)
    events = Vector{_NestedWriteTraceEvent}(undef,
        _NESTED_WRITE_TRACE_CHUNK)
    chunk = _NestedWriteTraceChunk(events, 0, nothing)
    if trace.last === nothing
        trace.first = chunk
    else
        something(trace.last).next = chunk
    end
    trace.last = chunk
    return chunk
end

function _nestedwritetracecapture!(trace::_NestedWriteTrace,
        event::_NestedWriteTraceEvent)
    chunk = trace.last
    if chunk === nothing || chunk.used == length(chunk.events)
        chunk = _nestedwritetracechunk!(trace)
    end
    chunk.used += 1
    chunk.events[chunk.used] = event
    return
end

function _nestedwritetracenext!(trace::_NestedWriteTrace)
    chunk = trace.current
    chunk === nothing && throw(ArgumentError(
        "nested Parquet input added occurrences between writer passes"))
    position = trace.position
    position <= chunk.used || throw(AssertionError(
        "nested writer trace cursor exceeded its chunk"))
    event = chunk.events[position]
    position += 1
    if position > chunk.used
        trace.current = chunk.next
        trace.position = 1
    else
        trace.position = position
    end
    return event
end

function _nestedwritetracepeekkey(trace::_NestedWriteTrace)
    chunk = trace.current
    position = trace.position
    while chunk !== nothing
        while position <= chunk.used
            event = chunk.events[position]
            event.kind == _NESTED_WRITE_TRACE_KEY && return event
            event.kind == _NESTED_WRITE_TRACE_SOURCE || throw(ArgumentError(
                "nested Parquet input changed its MAP-key trace topology"))
            position += 1
        end
        chunk = chunk.next
        position = 1
    end
    throw(ArgumentError(
        "nested Parquet input removed a MAP-key occurrence between writer passes"))
end

function _nestedwritetraceevent!(trace::Nothing, ::UInt8, ::Int64, ::Int64,
        ::Int64, ::Int64, value=nothing)
    return
end

function _nestedwritetraceevent!(trace::_NestedWriteTrace, kind::UInt8,
        a::Int64, b::Int64, c::Int64, d::Int64, value=nothing)
    if trace.capturing
        _nestedwritetracecapture!(trace,
            _NestedWriteTraceEvent(kind, a, b, c, d, value))
        return
    end
    expected = _nestedwritetracenext!(trace)
    expected.kind == kind && expected.a == a && expected.b == b &&
        expected.c == c && expected.d == d || throw(ArgumentError(
            "nested Parquet input changed its occurrence topology between writer passes"))
    kind in (_NESTED_WRITE_TRACE_KEY, _NESTED_WRITE_TRACE_SOURCE) && throw(
        AssertionError(
            "nested Parquet identity events require bounded comparison"))
    isequal(expected.value, value) || throw(ArgumentError(
        "nested Parquet input changed its occurrence metadata between writer passes"))
    return
end

function _nestedwritetracecompare!(trace::_NestedWriteTrace)
    trace.capturing = false
    trace.current = trace.first
    trace.position = 1
    return
end

function _nestedwritetracefinishcompare!(trace::_NestedWriteTrace)
    trace.current === nothing || throw(ArgumentError(
        "nested Parquet input removed occurrences between writer passes"))
    return
end

function _nestedwritetracerelease!(trace::_NestedWriteTrace)
    charge = trace.charge
    iszero(charge) || _release!(trace.budget, charge)
    trace.charge = Int64(0)
    trace.first = nothing
    trace.last = nothing
    trace.current = nothing
    return
end

function _nestedwritekeynextdepth(depth::Int, limits::Limits)
    depth < typemax(Int) || throw(LimitError(:metadata_depth,
        typemax(Int64), limits.max_metadata_depth))
    return depth + 1
end

function _nestedwritekeychilddepth(count::Int, depth::Int, limits::Limits)
    iszero(count) && return depth
    nextdepth = _nestedwritekeynextdepth(depth, limits)
    _checklimit(:metadata_depth, nextdepth, limits.max_metadata_depth)
    return nextdepth
end

function _nestedwritekeyrecursive(value)
    value isa Union{Pair,NamedTuple,StructValue,MapValue,AbstractDict} &&
        return true
    value isa Tuple && return !_nestedwriteisfixedtuple(typeof(value))
    value isa AbstractVector && !(value isa AbstractVector{UInt8}) && return true
    return false
end

function _nestedwritekeycaptureactive(value,
        frame::Union{Nothing,_NestedWriteKeyCaptureFrame})
    current = frame
    while current !== nothing
        current.value === value && return true
        current = current.parent
    end
    return false
end

function _nestedwritekeycompareactive(value,
        frame::Union{Nothing,_NestedWriteKeyCompareFrame})
    current = frame
    while current !== nothing
        current.current === value && return true
        current = current.parent
    end
    return false
end

function _nestedwritekeyframecharge!(trace::_NestedWriteTrace)
    _nestedwritetracereserve!(trace, _MATERIALIZED_OBJECT_BYTES)
    return
end

function _nestedwritekeyframefree!(trace::_NestedWriteTrace)
    _nestedwritetraceunreserve!(trace, _MATERIALIZED_OBJECT_BYTES)
    return
end

function _nestedwritekeycollectionfree!(trace::_NestedWriteTrace,
        state::_NestedWriteKeyCollectionState)
    source = state.source
    source isa _NestedWriteDictMaterialization &&
        _nestedwritedictrelease!(trace, source)
    return
end

function _nestedwritekeycaptureframefree!(trace::_NestedWriteTrace,
        frame::_NestedWriteKeyCaptureFrame)
    _nestedwritekeycollectionfree!(trace, frame.state)
    _nestedwritekeyframefree!(trace)
    return
end

function _nestedwritekeycompareframefree!(trace::_NestedWriteTrace,
        frame::_NestedWriteKeyCompareFrame)
    _nestedwritekeycollectionfree!(trace, frame.state)
    _nestedwritekeyframefree!(trace)
    return
end

function _nestedwritekeydeclared(shape::Nothing, value)
    return
end

function _nestedwritekeydeclared(shape::_NestedWriteShape, value)
    if ismissing(value)
        getfield(shape, :optional) && return
    elseif value isa getfield(shape, :value_type)
        return
    end
    throw(ArgumentError("Parquet MAP key field $(repr(getfield(shape, :name))) " *
        "contains $(typeof(value)); expected $(getfield(shape, :value_type))"))
end

function _nestedwritekeywitnesssource(::Nothing, ::Int)
    return nothing
end

function _nestedwritekeywitnesschild(::Nothing, ::Int)
    return nothing
end

function _nestedwritekeywitnesslist(::Nothing)
    return nothing
end

function _nestedwritekeywitnessmapkey(::Nothing)
    return nothing
end

function _nestedwritekeywitnessmapvalue(::Nothing)
    return nothing
end

function _nestedwritekeywitnessroot(::Nothing, value)
    return
end

function _nestedwritekeywitnesssnapshot(::Nothing)
    return nothing
end

function _nestedwritekeyrowsnapshot(shape, witness,
        trace::_NestedWriteTrace, source::AbstractVector, limits::Limits)
    stored = _nestedwritekeywitnesssnapshot(witness)
    if stored === nothing && shape !== nothing
        stored = _nestedwritesourcesnapshot(shape)
    end
    return _nestedwritetraceviewsnapshot!(trace, source, stored, limits)
end

function _nestedwritesourcesnapshot(shape::_NestedWriteLeafShape)
    return shape.source_snapshot
end

function _nestedwritesourcesnapshot(shape::_NestedWriteStructShape)
    return shape.source_snapshot
end

function _nestedwritesourcesnapshot(shape::_NestedWriteListShape)
    return shape.source_snapshot
end

function _nestedwritesourcesnapshot(shape::_NestedWriteMapShape)
    return shape.source_snapshot
end

function _nestedwriteoccurrencesnapshot!(trace, source::AbstractVector,
        shape::Union{Nothing,_NestedWriteShape}, limits::Limits)
    stored = shape === nothing ? nothing : _nestedwritesourcesnapshot(shape)
    if stored !== nothing && stored.source !== source
        stored = nothing
    end
    return _nestedwritetraceviewsnapshot!(trace, source, stored, limits)
end

function _nestedwriteispackagevector(value)
    return value isa Union{LogicalColumn,FixedByteArrayVector,ListVector,
        StructVector,MapVector}
end

function _nestedwritekeystoredsource(::Nothing, ::Int)
    return nothing
end

function _nestedwritekeystoredsource(sources::Vector{AbstractVector},
        index::Int)
    index <= length(sources) || return nothing
    return sources[index]
end

function _nestedwritekeycount(value)
    if value isa AbstractVector
        return _nestedvectorcount(value, "nested Parquet MAP key")
    end
    raw = length(value)
    raw isa Int || throw(ArgumentError(
        "nested Parquet MAP key length is not an Int"))
    raw >= 0 || throw(ArgumentError(
        "nested Parquet MAP key length must be nonnegative"))
    return raw
end

function _nestedwritekeyindexedstate(value)
    count = _nestedwritekeycount(value)
    first, last = value isa AbstractVector ?
        _nestedvectoraxes(value, "nested Parquet MAP key") :
        (_nestedvectoraxisvalue(firstindex(value),
            "nested Parquet MAP key first index"),
         _nestedvectoraxisvalue(lastindex(value),
            "nested Parquet MAP key last index"))
    return _NestedWriteKeyCollectionState(count, first, last, true, value)
end

function _nestedwritekeycollectionstate(value)
    return nothing
end

function _nestedwritekeycollectionstate(value::Union{Pair,NamedTuple,
        StructValue})
    return _NestedWriteKeyCollectionState(
        _nestedwritekeycount(value), 0, 0, false, nothing)
end

function _nestedwritekeycollectionstate(value::Tuple)
    return _nestedwritekeyindexedstate(value)
end

function _nestedwritekeycollectionstate(value::AbstractVector)
    return _nestedwritekeyindexedstate(value)
end

function _nestedwritekeycollectionstate(value::AbstractString)
    return _nestedwritekeyindexedstate(codeunits(value))
end

function _nestedwritekeycollectionstate(value::JSONValue)
    return _nestedwritekeyindexedstate(value.bytes)
end

function _nestedwritekeycollectionstate(value::BSONValue)
    return _nestedwritekeyindexedstate(value.bytes)
end

function _nestedwritekeyvalidatedcount(value, shape,
        state::Union{Nothing,_NestedWriteKeyCollectionState}, limits::Limits)
    state === nothing && return
    iscontainer = value isa Union{Pair,NamedTuple,StructValue,MapValue,
        AbstractDict}
    iscontainer |= value isa Tuple &&
        !_nestedwriteisfixedtuple(typeof(value))
    iscontainer |= value isa AbstractVector &&
        !(value isa AbstractVector{UInt8})
    iscontainer || return
    _checklimit(:container_elements, state.count,
        limits.max_container_elements)
    return
end

function _nestedwritekeydecimalpreflight(shape, value::Decimal,
        limits::Limits)
    digits = _decimaldigits(value.unscaled)
    digits <= typemax(Int32) || throw(ArgumentError(
        "DECIMAL MAP-key precision exceeds Int32"))
    precision = max(Int32(1), Int32(digits), value.scale)
    if shape isa _NestedWriteLeafShape &&
            shape.explicit isa _DecimalLogicalColumnSpec
        spec = shape.explicit
        value.scale == spec.scale || throw(ArgumentError(
            "DECIMAL MAP key requires scale $(spec.scale), got $(value.scale)"))
        _checkdecimalvalue(value.unscaled, spec.precision, shape.name,
            ArgumentError)
        precision = spec.precision
    end
    precision > 18 || return
    width = _decimalwritewidth(precision, limits)
    _checklimit(:decimal_bytes, width, limits.max_decimal_bytes)
    _twoscomplementwidth(value.unscaled) <= width || throw(ArgumentError(
        "DECIMAL MAP key does not fit its declared byte width"))
    return
end

function _nestedwritekeyleafpreflight(shape, value,
        state::Union{Nothing,_NestedWriteKeyCollectionState}, limits::Limits)
    ismissing(value) && return
    if shape isa _NestedWriteLeafShape && shape.fixed_width !== nothing
        width = Int(shape.fixed_width)
        _checklimit(:string_bytes, width, limits.max_string_bytes)
        state === nothing || state.count == width || throw(ArgumentError(
            "fixed byte-array MAP key has a value with the wrong width"))
        return
    elseif value isa AbstractString
        state === nothing && throw(AssertionError(
            "string MAP key has no captured collection state"))
        _checklimit(:string_bytes, state.count, limits.max_string_bytes)
    elseif value isa AbstractVector{UInt8}
        state === nothing && throw(AssertionError(
            "byte MAP key has no captured collection state"))
        _checklimit(:string_bytes, state.count, limits.max_string_bytes)
    elseif value isa JSONValue
        state === nothing && throw(AssertionError(
            "JSON MAP key has no captured collection state"))
        _checklimit(:string_bytes, state.count, limits.max_string_bytes)
    elseif value isa BSONValue
        state === nothing && throw(AssertionError(
            "BSON MAP key has no captured collection state"))
        _checklimit(:string_bytes, state.count, limits.max_string_bytes)
    elseif value isa Decimal
        _nestedwritekeydecimalpreflight(shape, value, limits)
    elseif value isa Tuple && _nestedwriteisfixedtuple(typeof(value))
        state === nothing && throw(AssertionError(
            "fixed-byte MAP key has no captured collection state"))
        _checklimit(:string_bytes, state.count, limits.max_string_bytes)
    end
    return
end

function _nestedwritekeyvectorcheck(value,
        state::_NestedWriteKeyCollectionState)
    state.indexed || throw(AssertionError(
        "nested Parquet MAP key has no captured axes"))
    value === state.source || throw(AssertionError(
        "nested Parquet MAP key checked a different captured source"))
    count = value isa AbstractVector ?
        _nestedvectorcount(value, "nested Parquet MAP key") : length(value)
    first, last = value isa AbstractVector ?
        _nestedvectoraxes(value, "nested Parquet MAP key") :
        (firstindex(value), lastindex(value))
    count == state.count && first == state.first && last == state.last || throw(ArgumentError(
            "nested Parquet MAP key changed length or axes while it was copied"))
    return
end

function _nestedwritekeyshape(shape::Nothing, ::Int)
    return nothing
end

function _nestedwritekeyshape(shape::_NestedWriteStructShape, index::Int)
    index <= length(shape.children) || throw(ArgumentError(
        "nested Parquet MAP key changed its struct field count"))
    return shape.children[index]
end

function _nestedwritekeybytescalar(byte)
    return try
        UInt8(byte)
    catch error
        error isa Union{InexactError,MethodError} || rethrow()
        throw(ArgumentError(
            "nested Parquet MAP key contains a value that is not a UInt8"))
    end
end

function _nestedwritekeychildren(trace::_NestedWriteTrace, count::Int)
    charge = _materializedarraybytes(_NestedWriteKeySnapshot, count)
    _nestedwritetracereserve!(trace, charge)
    return Vector{_NestedWriteKeySnapshot}(undef, count)
end

function _nestedwritekeypaircount(count::Int, trace::_NestedWriteTrace)
    count = try
        _materializedproduct(count, 2)
    catch error
        error isa LimitError || rethrow()
        throw(LimitError(:materialized_bytes, typemax(Int64),
            trace.budget.maximum))
    end
    count <= typemax(Int) || throw(LimitError(:materialized_bytes,
        typemax(Int64), trace.budget.maximum))
    return Int(count)
end

function _nestedwritekeynode!(trace::_NestedWriteTrace)
    _nestedwritetracereserve!(trace, _MATERIALIZED_OBJECT_BYTES)
    return
end

function _nestedwritekeycopybytes(value,
        state::_NestedWriteKeyCollectionState, action::String)
    _nestedwritekeyvectorcheck(value, state)
    output = Vector{UInt8}(undef, state.count)
    position = 0
    for byte in value
        _nestedwritekeyvectorcheck(value, state)
        position += 1
        position <= length(output) || throw(ArgumentError(
            "nested Parquet MAP key changed length while it was $action"))
        output[position] = _nestedwritekeybytescalar(byte)
    end
    _nestedwritekeyvectorcheck(value, state)
    position == length(output) || throw(ArgumentError(
        "nested Parquet MAP key changed length while it was $action"))
    return output
end

function _nestedwritekeybytes(value, trace::_NestedWriteTrace,
        state::_NestedWriteKeyCollectionState)
    charge = _materializedarraybytes(UInt8, state.count)
    _nestedwritetracereserve!(trace, charge)
    return _nestedwritekeycopybytes(value, state, "copied")
end

function _nestedwritekeysnapshot(value, trace::_NestedWriteTrace,
        shape::Union{Nothing,_NestedWriteShape}, limits::Limits)
    return _nestedwritekeysnapshotiterative(value, trace, shape, limits,
        nothing, nothing)
end

function _nestedwritekeysnapshot(value, trace::_NestedWriteTrace,
        shape::Union{Nothing,_NestedWriteShape}, limits::Limits, witness)
    return _nestedwritekeysnapshotiterative(value, trace, shape, limits,
        witness, nothing)
end

function _nestedwritekeysnapshot(value, trace::_NestedWriteTrace,
        shape::Union{Nothing,_NestedWriteShape}, limits::Limits, witness,
        row_witness)
    return _nestedwritekeysnapshotiterative(value, trace, shape, limits,
        witness, row_witness)
end

function _nestedwritekeysnapshotnode(value::AbstractString,
        trace::_NestedWriteTrace, ::Limits,
        ::Union{Nothing,_NestedWriteShape}, ::Int,
        ::Nothing,
        state::_NestedWriteKeyCollectionState)
    source = state.source
    _nestedwritekeynode!(trace)
    bytes = _nestedwritekeybytes(source, trace, state)
    isvalid(String, bytes) || throw(ArgumentError(
        "Parquet MAP key contains invalid UTF-8"))
    return _NestedWriteKeySnapshot(_NESTED_WRITE_KEY_STRING, typeof(value),
        state.first, state.last, bytes, nothing, _NESTED_WRITE_EMPTY_KEYS)
end

function _nestedwritekeysnapshotnode(value::AbstractVector{UInt8},
        trace::_NestedWriteTrace, ::Limits,
        ::Union{Nothing,_NestedWriteShape}, ::Int,
        ::Nothing,
        state::_NestedWriteKeyCollectionState)
    _nestedwritekeynode!(trace)
    bytes = _nestedwritekeybytes(value, trace, state)
    return _NestedWriteKeySnapshot(_NESTED_WRITE_KEY_BYTES, typeof(value),
        state.first, state.last, bytes, nothing,
        _NESTED_WRITE_EMPTY_KEYS)
end

function _nestedwritekeysnapshotnode(value::JSONValue,
        trace::_NestedWriteTrace, ::Limits,
        ::Union{Nothing,_NestedWriteShape}, ::Int,
        ::Nothing,
        state::_NestedWriteKeyCollectionState)
    _nestedwritekeynode!(trace)
    bytes = _nestedwritekeybytes(state.source, trace, state)
    return _NestedWriteKeySnapshot(_NESTED_WRITE_KEY_BYTES, JSONValue,
        1, length(bytes), bytes, nothing, _NESTED_WRITE_EMPTY_KEYS)
end

function _nestedwritekeysnapshotnode(value::BSONValue,
        trace::_NestedWriteTrace, ::Limits,
        ::Union{Nothing,_NestedWriteShape}, ::Int,
        ::Nothing,
        state::_NestedWriteKeyCollectionState)
    _nestedwritekeynode!(trace)
    bytes = _nestedwritekeybytes(state.source, trace, state)
    return _NestedWriteKeySnapshot(_NESTED_WRITE_KEY_BYTES, BSONValue,
        1, length(bytes), bytes, nothing, _NESTED_WRITE_EMPTY_KEYS)
end

function _nestedwritekeysnapshotnode(value::Decimal,
        trace::_NestedWriteTrace, ::Limits,
        ::Union{Nothing,_NestedWriteShape}, ::Int,
        ::Nothing, ::Nothing)
    _nestedwritekeynode!(trace)
    bits = ndigits(value.unscaled; base=2)
    payload = cld(bits, 8)
    charge = _materializedsum(_MATERIALIZED_OBJECT_BYTES,
        _materializedarraybytes(UInt8, payload))
    _nestedwritetracereserve!(trace, charge)
    return _NestedWriteKeySnapshot(_NESTED_WRITE_KEY_SCALAR, Decimal, 1, 1,
        copy(value), nothing, _NESTED_WRITE_EMPTY_KEYS)
end

function _nestedwritekeysnapshotnode(value::Tuple, trace::_NestedWriteTrace,
        ::Limits, shape::Union{Nothing,_NestedWriteShape}, ::Int, ::Nothing,
        state::_NestedWriteKeyCollectionState)
    _nestedwriteisfixedtuple(typeof(value)) || throw(AssertionError(
        "recursive Tuple MAP keys require the iterative frame engine"))
    shape isa Union{Nothing,_NestedWriteLeafShape} || throw(ArgumentError(
        "nested Parquet MAP key does not match its declared leaf topology"))
    shape === nothing || _nestedwriteisfixedtuple(shape.value_type) ||
        throw(ArgumentError(
            "nested Parquet MAP key does not match its declared leaf topology"))
    _nestedwritekeynode!(trace)
    bytes = _nestedwritekeybytes(value, trace, state)
    return _NestedWriteKeySnapshot(_NESTED_WRITE_KEY_BYTES, typeof(value),
        state.first, state.last, bytes, nothing, _NESTED_WRITE_EMPTY_KEYS)
end

function _nestedwritekeysnapshotnode(value, trace::_NestedWriteTrace,
        ::Limits, shape::Union{Nothing,_NestedWriteShape}, ::Int,
        ::Nothing, ::Nothing)
    shape isa Union{Nothing,_NestedWriteLeafShape} || throw(ArgumentError(
        "nested Parquet MAP key does not match its declared leaf topology"))
    value === nothing && throw(ArgumentError(
        "nothing is not a stable Parquet MAP key"))
    type = typeof(value)
    Base.ismutabletype(type) && throw(ArgumentError(
        "Parquet MAP key type $type does not have stable copy semantics"))
    (isbitstype(type) || fieldcount(type) == 0) || throw(ArgumentError(
        "Parquet MAP key type $type does not have stable copy semantics"))
    _nestedwritekeynode!(trace)
    return _NestedWriteKeySnapshot(_NESTED_WRITE_KEY_SCALAR, type, 1, 1,
        value, nothing, _NESTED_WRITE_EMPTY_KEYS)
end

function _nestedwritekeybytesequal(expected::Vector{UInt8}, current)
    state = _nestedwritekeyindexedstate(current)
    return _nestedwritekeybytesequal(expected, current, state)
end

function _nestedwritekeybytesequal(expected::Vector{UInt8}, current,
        state::_NestedWriteKeyCollectionState)
    length(expected) == state.count || return false
    _nestedwritekeyvectorcheck(current, state)
    position = 0
    for byte in current
        _nestedwritekeyvectorcheck(current, state)
        position += 1
        position <= length(expected) || return false
        expected[position] == _nestedwritekeybytescalar(byte) || return false
    end
    _nestedwritekeyvectorcheck(current, state)
    return position == length(expected)
end

function _nestedwritekeyequal(expected::_NestedWriteKeySnapshot, current,
        trace::_NestedWriteTrace, limits::Limits)
    return _nestedwritekeyequaliterative(expected, current, trace, limits,
        nothing, nothing, nothing)
end

function _nestedwritekeyequal(expected::_NestedWriteKeySnapshot, current,
        trace::_NestedWriteTrace, limits::Limits, shape, witness, row_witness)
    return _nestedwritekeyequaliterative(expected, current, trace, limits,
        shape, witness, row_witness)
end

function _nestedwritekeycapturecheck(frame::_NestedWriteKeyCaptureFrame)
    _nestedwriterowcheck(frame.row_witness)
    value = frame.value
    kind = frame.kind
    if kind == _NESTED_WRITE_KEY_STRUCT
        value.children isa Vector{AbstractVector} &&
            length(value.names) == length(frame.children) &&
            length(value.children) == length(frame.children) ||
            throw(ArgumentError(
                "nested Parquet MAP key changed its struct backing metadata while it was copied"))
    elseif kind == _NESTED_WRITE_KEY_LIST
        value isa ListValue && _validatelistvalue(value)
        _nestedwritekeyvectorcheck(value, frame.state)
    elseif kind == _NESTED_WRITE_KEY_MAP
        if value isa MapValue
            _validatemapvalue(value)
            _nestedwritekeyvectorcheck(value, frame.state)
        end
    end
    return
end

function _nestedwritekeycaptureframe(value, trace::_NestedWriteTrace,
        limits::Limits, shape::Union{Nothing,_NestedWriteShape}, depth::Int,
        parent::Union{Nothing,_NestedWriteKeyCaptureFrame}, parent_slot::Int,
        state::_NestedWriteKeyCollectionState, witness, row_witness)
    count = state.count
    kind = UInt8(0)
    names = nothing
    childcount = count
    if value isa Pair
        shape === nothing || throw(ArgumentError(
            "nested Parquet MAP key does not match its declared topology"))
        count == 2 || throw(ArgumentError(
            "nested Parquet MAP key changed its Pair length"))
        kind = _NESTED_WRITE_KEY_PAIR
    elseif value isa NamedTuple
        shape isa Union{Nothing,_NestedWriteStructShape} || throw(ArgumentError(
            "nested Parquet MAP key does not match its declared struct topology"))
        shape === nothing || length(shape.children) == count || throw(
            ArgumentError("nested Parquet MAP key changed its struct field count"))
        kind = _NESTED_WRITE_KEY_NAMED_TUPLE
        names = keys(value)
    elseif value isa Tuple
        shape === nothing || throw(ArgumentError(
            "nested Parquet MAP key does not match its declared topology"))
        kind = _NESTED_WRITE_KEY_TUPLE
    elseif value isa StructValue
        shape isa Union{Nothing,_NestedWriteStructShape} || throw(ArgumentError(
            "nested Parquet MAP key does not match its declared struct topology"))
        _validatestructvalue(value)
        if shape !== nothing
            value.names == shape.names || throw(ArgumentError(
                "nested Parquet MAP key changed its struct field names"))
            count == length(shape.children) || throw(ArgumentError(
                "nested Parquet MAP key changed its struct field count"))
        end
        namesbefore = value.names
        length(namesbefore) == count || throw(ArgumentError(
            "nested Parquet MAP key changed its struct field names"))
        names = namesbefore
        kind = _NESTED_WRITE_KEY_STRUCT
    elseif value isa MapValue
        shape isa Union{Nothing,_NestedWriteMapShape} || throw(ArgumentError(
            "nested Parquet MAP key does not match its declared MAP topology"))
        _validatemapvalue(value)
        kind = _NESTED_WRITE_KEY_MAP
        childcount = _nestedwritekeypaircount(count, trace)
    elseif value isa AbstractDict
        shape isa Union{Nothing,_NestedWriteMapShape} || throw(ArgumentError(
            "nested Parquet MAP key does not match its declared MAP topology"))
        kind = _NESTED_WRITE_KEY_MAP
        childcount = _nestedwritekeypaircount(count, trace)
    elseif value isa AbstractVector
        shape isa Union{Nothing,_NestedWriteListShape} || throw(ArgumentError(
            "nested Parquet MAP key does not match its declared LIST topology"))
        value isa ListValue && _validatelistvalue(value)
        kind = _NESTED_WRITE_KEY_LIST
    else
        throw(AssertionError("unknown recursive nested writer MAP-key type"))
    end
    _nestedwritekeychilddepth(count, depth, limits)
    _nestedwritekeycaptureactive(value, parent) && throw(ArgumentError(
        "cyclic nested Parquet MAP key"))
    container_snapshot = kind == _NESTED_WRITE_KEY_LIST &&
        _nestedwriteispackagevector(value) ?
        _nestedwriteoccurrencesnapshot!(trace, value, shape, limits) : nothing
    _nestedwritekeyframecharge!(trace)
    try
        frame = _NestedWriteKeyCaptureFrame(parent, parent_slot, value, shape,
            witness, row_witness, state, depth, kind,
            _NESTED_WRITE_EMPTY_KEYS, nothing, 0, nothing, nothing, nothing,
            nothing, nothing, nothing, nothing)
        frame.pending = container_snapshot
        _nestedwritekeynode!(trace)
        frame.children = _nestedwritekeychildren(trace, childcount)
        if kind == _NESTED_WRITE_KEY_STRUCT
            namecharge = _materializedarraybytes(String, count)
            _nestedwritetracereserve!(trace, namecharge)
            frame.names = copy(names)
        else
            frame.names = names
        end
        frame.iterator = if kind == _NESTED_WRITE_KEY_LIST
            eachindex(value)
        elseif kind == _NESTED_WRITE_KEY_MAP && value isa AbstractDict
            state.source.first
        else
            nothing
        end
        if iszero(childcount)
            snapshot = _nestedwritekeycapturefinish(frame, trace)
            _nestedwritekeycaptureframefree!(trace, frame)
            return snapshot
        end
        return frame
    catch
        _nestedwritekeycollectionfree!(trace, state)
        _nestedwritekeyframefree!(trace)
        rethrow()
    end
end

function _nestedwritekeycapturestart(value, trace::_NestedWriteTrace,
        limits::Limits, shape::Union{Nothing,_NestedWriteShape}, depth::Int,
        parent::Union{Nothing,_NestedWriteKeyCaptureFrame}, parent_slot::Int,
        witness, row_witness)
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    _nestedwriterowcheck(row_witness)
    _nestedwritekeywitnessroot(witness, value)
    _nestedwritekeydeclared(shape, value)
    if value isa AbstractDict
        _nestedwritekeycaptureactive(value, parent) && throw(ArgumentError(
            "cyclic nested Parquet MAP key"))
        keyshape = shape isa _NestedWriteMapShape ? shape.key : nothing
        valueshape = shape isa _NestedWriteMapShape ? shape.value : nothing
        materialization = _nestedwritedictmaterialize(trace, value, keyshape,
            valueshape, limits, depth)
        state = _NestedWriteKeyCollectionState(materialization.count, 0, 0,
            false, materialization)
        try
            _nestedwritekeyvalidatedcount(value, shape, state, limits)
            _nestedwritekeyleafpreflight(shape, value, state, limits)
        catch
            _nestedwritedictrelease!(trace, materialization)
            rethrow()
        end
        try
            return _nestedwritekeycaptureframe(value, trace, limits, shape,
                depth, parent, parent_slot, state, witness, row_witness)
        catch
            _nestedwritedictrelease!(trace, materialization)
            rethrow()
        end
    end
    state = _nestedwritekeycollectionstate(value)
    _nestedwritekeyvalidatedcount(value, shape, state, limits)
    _nestedwritekeyleafpreflight(shape, value, state, limits)
    if !_nestedwritekeyrecursive(value)
        return _nestedwritekeysnapshotnode(value, trace, limits, shape,
            depth, nothing, state)
    end
    state === nothing && throw(AssertionError(
        "recursive nested writer MAP key has no collection state"))
    return _nestedwritekeycaptureframe(value, trace, limits, shape, depth,
        parent, parent_slot, state, witness, row_witness)
end

function _nestedwritekeycapturesetnext!(frame::_NestedWriteKeyCaptureFrame,
        value, shape::Union{Nothing,_NestedWriteShape}, witness, row_witness)
    frame.position += 1
    frame.position <= length(frame.children) || throw(ArgumentError(
        "nested Parquet MAP key changed length while it was copied"))
    frame.next_value = value
    frame.next_shape = shape
    frame.next_witness = witness
    frame.next_row_witness = row_witness
    return true
end

function _nestedwritekeycaptureadvance!(frame::_NestedWriteKeyCaptureFrame,
        trace::_NestedWriteTrace, limits::Limits)
    _nestedwritekeycapturecheck(frame)
    frame.position == length(frame.children) && return false
    value = frame.value
    shape = frame.shape
    kind = frame.kind
    if kind == _NESTED_WRITE_KEY_PAIR
        child = iszero(frame.position) ? value.first : value.second
        return _nestedwritekeycapturesetnext!(frame, child, nothing, nothing,
            nothing)
    elseif kind == _NESTED_WRITE_KEY_TUPLE
        index = frame.position + 1
        child = value[index]
        return _nestedwritekeycapturesetnext!(frame, child, nothing, nothing,
            nothing)
    elseif kind == _NESTED_WRITE_KEY_NAMED_TUPLE
        index = frame.position + 1
        childshape = _nestedwritekeyshape(shape, index)
        child = getfield(value, index)
        return _nestedwritekeycapturesetnext!(frame, child, childshape,
            nothing, nothing)
    elseif kind == _NESTED_WRITE_KEY_STRUCT
        index = frame.position + 1
        childshape = _nestedwritekeyshape(shape, index)
        expectedchild = if shape isa _NestedWriteStructShape
            something(shape.source_children)[index]
        else
            _nestedwritekeywitnesssource(frame.witness, index)
        end
        childvector = _nestedwritestructaccesschild(value, value.names,
            value.children, length(frame.children), index,
            frame.names[index], expectedchild)
        childwitness = _nestedwritekeywitnesschild(frame.witness, index)
        child, row_witness = _nestedwriterowaccess(childvector, value.index,
            _nestedwritekeyrowsnapshot(childshape, childwitness, trace,
                childvector, limits))
        return _nestedwritekeycapturesetnext!(frame, child, childshape,
            childwitness, row_witness)
    elseif kind == _NESTED_WRITE_KEY_LIST
        result = iszero(frame.position) ? iterate(frame.iterator) :
            iterate(frame.iterator, frame.iteration)
        result === nothing && throw(ArgumentError(
            "nested Parquet MAP key changed length while it was copied"))
        index, iteration = result
        frame.iteration = iteration
        childshape = shape === nothing ? nothing : shape.element
        childwitness = _nestedwritekeywitnesslist(frame.witness)
        if value isa ListValue
            physical = value.first + index - 1
            child, row_witness = _nestedwriterowaccess(value.values, physical,
                _nestedwritekeyrowsnapshot(childshape, childwitness, trace,
                    value.values, limits))
        elseif frame.pending isa _NestedWriteVectorSnapshot
            child, row_witness = _nestedwriterowaccess(value, index,
                frame.pending)
        else
            child = _nestedwritelistaccessitem(value, index)
            row_witness = nothing
        end
        _nestedwritekeycapturecheck(frame)
        return _nestedwritekeycapturesetnext!(frame, child, childshape,
            childwitness, row_witness)
    elseif kind == _NESTED_WRITE_KEY_MAP && value isa MapValue
        if iseven(frame.position)
            entry = frame.position ÷ 2 + 1
            childshape = shape === nothing ? nothing : shape.key
            childwitness = _nestedwritekeywitnessmapkey(frame.witness)
            physical = value.first + entry - 1
            child, row_witness = _nestedwriterowaccess(value.keys, physical,
                _nestedwritekeyrowsnapshot(childshape, childwitness, trace,
                    value.keys, limits))
            frame.pending = physical
            return _nestedwritekeycapturesetnext!(frame, child,
                childshape, childwitness, row_witness)
        end
        childshape = shape === nothing ? nothing : shape.value
        childwitness = _nestedwritekeywitnessmapvalue(frame.witness)
        if value.values === nothing
            child = missing
            row_witness = nothing
        else
            child, row_witness = _nestedwriterowaccess(value.values,
                frame.pending, _nestedwritekeyrowsnapshot(childshape,
                    childwitness, trace, value.values, limits))
        end
        return _nestedwritekeycapturesetnext!(frame, child, childshape,
            childwitness, row_witness)
    elseif kind == _NESTED_WRITE_KEY_MAP
        if iseven(frame.position)
            entry = frame.iterator
            entry isa _NestedWriteDictEntry || throw(ArgumentError(
                "nested Parquet MAP key changed length while it was copied"))
            frame.iterator = entry.next
            frame.pending = entry
            childshape = shape === nothing ? nothing : shape.key
            childwitness = _nestedwritekeywitnessmapkey(frame.witness)
            return _nestedwritekeycapturesetnext!(frame, entry.key,
                childshape, childwitness, nothing)
        end
        entry = frame.pending
        entry isa _NestedWriteDictEntry || throw(ArgumentError(
            "nested Parquet MAP key changed its entry while it was copied"))
        childshape = shape === nothing ? nothing : shape.value
        childwitness = _nestedwritekeywitnessmapvalue(frame.witness)
        return _nestedwritekeycapturesetnext!(frame, entry.value, childshape,
            childwitness, nothing)
    end
    throw(AssertionError("unknown recursive nested writer MAP-key kind"))
end

function _nestedwritekeycapturefinish(frame::_NestedWriteKeyCaptureFrame,
        trace::_NestedWriteTrace)
    frame.position == length(frame.children) || throw(ArgumentError(
        "nested Parquet MAP key changed length while it was copied"))
    if frame.kind == _NESTED_WRITE_KEY_MAP && frame.value isa AbstractDict
        frame.iterator === nothing || throw(
            ArgumentError("nested Parquet MAP key changed length while it was copied"))
    elseif frame.kind == _NESTED_WRITE_KEY_LIST
        result = iszero(frame.position) ? iterate(frame.iterator) :
            iterate(frame.iterator, frame.iteration)
        result === nothing || throw(
            ArgumentError("nested Parquet MAP key changed length while it was copied"))
    end
    stored = if frame.kind == _NESTED_WRITE_KEY_MAP
        true
    elseif frame.kind == _NESTED_WRITE_KEY_STRUCT && frame.witness !== nothing
        frame.witness
    elseif frame.kind == _NESTED_WRITE_KEY_STRUCT &&
            frame.shape isa _NestedWriteStructShape
        frame.shape.source_children
    else
        nothing
    end
    return _NestedWriteKeySnapshot(frame.kind, typeof(frame.value),
        frame.kind in (_NESTED_WRITE_KEY_LIST, _NESTED_WRITE_KEY_MAP) ?
            frame.state.first : 1,
        frame.kind in (_NESTED_WRITE_KEY_LIST, _NESTED_WRITE_KEY_MAP) ?
            frame.state.last : length(frame.children),
        stored, frame.names, frame.children)
end

function _nestedwritekeycapturecleanup!(trace::_NestedWriteTrace,
        frame::Union{Nothing,_NestedWriteKeyCaptureFrame})
    current = frame
    while current !== nothing
        parent = current.parent
        _nestedwritekeycaptureframefree!(trace, current)
        current = parent
    end
    return
end

function _nestedwritekeysnapshotiterative(value, trace::_NestedWriteTrace,
        shape::Union{Nothing,_NestedWriteShape}, limits::Limits, witness,
        row_witness)
    started = _nestedwritekeycapturestart(value, trace, limits, shape, 1,
        nothing, 0, witness, row_witness)
    started isa _NestedWriteKeySnapshot && return started
    current = started::_NestedWriteKeyCaptureFrame
    try
        while true
            if _nestedwritekeycaptureadvance!(current, trace, limits)
                depth = _nestedwritekeynextdepth(current.depth, limits)
                started = _nestedwritekeycapturestart(current.next_value,
                    trace, limits, current.next_shape, depth, current,
                    current.position, current.next_witness,
                    current.next_row_witness)
                if started isa _NestedWriteKeySnapshot
                    current.children[current.position] = started
                    _nestedwriterowcheck(current.next_row_witness)
                else
                    current = started::_NestedWriteKeyCaptureFrame
                end
                continue
            end
            snapshot = _nestedwritekeycapturefinish(current, trace)
            parent = current.parent
            slot = current.parent_slot
            _nestedwritekeycaptureframefree!(trace, current)
            if parent === nothing
                current = nothing
                return snapshot
            end
            _nestedwriterowcheck(current.row_witness)
            current = parent
            current.children[slot] = snapshot
        end
    finally
        _nestedwritekeycapturecleanup!(trace, current)
    end
end

function _nestedwritekeycomparebytes(expected::_NestedWriteKeySnapshot,
        current)
    source = if current isa AbstractString
        codeunits(current)
    elseif current isa Union{JSONValue,BSONValue}
        current.bytes
    else
        current
    end
    state = _nestedwritekeyindexedstate(source)
    length(expected.value) == state.count &&
        expected.first == state.first && expected.last == state.last || throw(
            ArgumentError(
                "nested Parquet MAP key changed length or axes while it was compared"))
    return _nestedwritekeybytesequal(expected.value, source, state)
end

function _nestedwritekeycompareleaf(expected::_NestedWriteKeySnapshot,
        current, limits::Limits)
    kind = expected.kind
    if kind == _NESTED_WRITE_KEY_SCALAR
        typeof(current) === expected.source_type || return false
        return isequal(expected.value, current)
    elseif kind == _NESTED_WRITE_KEY_STRING
        current isa AbstractString || return false
        typeof(current) === expected.source_type || return false
        return _nestedwritekeycomparebytes(expected, current)
    elseif kind == _NESTED_WRITE_KEY_BYTES
        if expected.source_type === JSONValue
            current isa JSONValue || return false
        elseif expected.source_type === BSONValue
            current isa BSONValue || return false
        else
            current isa Union{AbstractVector{UInt8},Tuple} || return false
            typeof(current) === expected.source_type || return false
        end
        return _nestedwritekeycomparebytes(expected, current)
    end
    return nothing
end

function _nestedwritekeycomparecheck(frame::_NestedWriteKeyCompareFrame)
    _nestedwriterowcheck(frame.row_witness)
    expected = frame.expected
    current = frame.current
    kind = expected.kind
    if kind == _NESTED_WRITE_KEY_STRUCT
        current.names == expected.names &&
            length(current.children) == length(expected.children) || return false
    elseif kind == _NESTED_WRITE_KEY_LIST
        current isa ListValue && _validatelistvalue(current)
        _nestedwritekeyvectorcheck(current, frame.state)
        frame.state.first == expected.first &&
            frame.state.last == expected.last || return false
    elseif kind == _NESTED_WRITE_KEY_MAP
        if current isa MapValue
            _validatemapvalue(current)
            _nestedwritekeyvectorcheck(current, frame.state)
            frame.state.first == expected.first &&
                frame.state.last == expected.last || return false
        end
    end
    return true
end

function _nestedwritekeycompareinitialize!(frame::_NestedWriteKeyCompareFrame,
        limits::Limits)
    expected = frame.expected
    current = frame.current
    kind = expected.kind
    childcount = length(expected.children)
    if kind == _NESTED_WRITE_KEY_PAIR
        current isa Pair || return false
        typeof(current) === expected.source_type || return false
    elseif kind == _NESTED_WRITE_KEY_TUPLE
        current isa Tuple || return false
        typeof(current) === expected.source_type || return false
    elseif kind == _NESTED_WRITE_KEY_NAMED_TUPLE
        current isa NamedTuple || return false
        typeof(current) === expected.source_type || return false
        keys(current) == expected.names || return false
    elseif kind == _NESTED_WRITE_KEY_STRUCT
        current isa StructValue || return false
        expected.source_type === StructValue || return false
        _validatestructvalue(current)
        current.names == expected.names || return false
    elseif kind == _NESTED_WRITE_KEY_LIST
        current isa AbstractVector || return false
        typeof(current) === expected.source_type || return false
        current isa ListValue && _validatelistvalue(current)
    elseif kind == _NESTED_WRITE_KEY_MAP
        current isa Union{MapValue,AbstractDict} || return false
        typeof(current) === expected.source_type || return false
        iseven(childcount) || return false
    else
        return false
    end
    state = current isa AbstractDict ? frame.state :
        _nestedwritekeycollectionstate(current)
    state === nothing && return false
    _nestedwritekeyvalidatedcount(current, nothing, state, limits)
    if kind == _NESTED_WRITE_KEY_PAIR
        state.count == 2 == childcount || return false
    elseif kind in (_NESTED_WRITE_KEY_TUPLE,
            _NESTED_WRITE_KEY_NAMED_TUPLE, _NESTED_WRITE_KEY_STRUCT,
            _NESTED_WRITE_KEY_LIST)
        state.count == childcount || return false
    else
        state.count == childcount ÷ 2 || return false
    end
    if kind == _NESTED_WRITE_KEY_LIST
        state.first == expected.first && state.last == expected.last ||
            return false
    elseif kind == _NESTED_WRITE_KEY_MAP && current isa MapValue
        _validatemapvalue(current)
        state.first == expected.first && state.last == expected.last ||
            return false
    end
    frame.state = state
    frame.iterator = if kind == _NESTED_WRITE_KEY_LIST
        eachindex(current)
    elseif kind == _NESTED_WRITE_KEY_MAP && current isa AbstractDict
        state.source.first
    else
        nothing
    end
    return true
end

function _nestedwritekeycompareframe(expected::_NestedWriteKeySnapshot,
        current, trace::_NestedWriteTrace, limits::Limits, depth::Int,
        parent::Union{Nothing,_NestedWriteKeyCompareFrame}, shape, witness,
        row_witness)
    recursive = _nestedwritekeyrecursive(current)
    recursive || return false
    isempty(expected.children) ||
        _nestedwritekeychilddepth(length(expected.children), depth, limits)
    _nestedwritekeycompareactive(current, parent) && throw(ArgumentError(
        "cyclic nested Parquet MAP key"))
    container_snapshot = expected.kind == _NESTED_WRITE_KEY_LIST &&
        _nestedwriteispackagevector(current) ?
        _nestedwriteoccurrencesnapshot!(trace, current, shape, limits) : nothing
    materialization = if current isa AbstractDict
        keyshape = shape isa _NestedWriteMapShape ? shape.key : nothing
        valueshape = shape isa _NestedWriteMapShape ? shape.value : nothing
        _nestedwritedictmaterialize(trace, current, keyshape, valueshape,
            limits, depth)
    else
        nothing
    end
    state = materialization === nothing ?
        _NestedWriteKeyCollectionState(0, 0, 0, false, nothing) :
        _NestedWriteKeyCollectionState(materialization.count, 0, 0, false,
            materialization)
    try
        _nestedwritekeyframecharge!(trace)
    catch
        _nestedwritekeycollectionfree!(trace, state)
        rethrow()
    end
    try
        frame = _NestedWriteKeyCompareFrame(parent, expected, current, shape,
            witness, row_witness, state, depth, 0, nothing, nothing, nothing,
            nothing, nothing, nothing, nothing, nothing)
        frame.pending = container_snapshot
        if !_nestedwritekeycompareinitialize!(frame, limits)
            _nestedwritekeycompareframefree!(trace, frame)
            return false
        end
        return frame
    catch
        _nestedwritekeycollectionfree!(trace, state)
        _nestedwritekeyframefree!(trace)
        rethrow()
    end
end

function _nestedwritekeyfamilymatches(expected::_NestedWriteKeySnapshot,
        current)
    kind = expected.kind
    if kind == _NESTED_WRITE_KEY_SCALAR
        return !_nestedwritekeyrecursive(current) &&
            typeof(current) === expected.source_type
    elseif kind == _NESTED_WRITE_KEY_STRING
        return current isa AbstractString &&
            typeof(current) === expected.source_type
    elseif kind == _NESTED_WRITE_KEY_BYTES
        return !_nestedwritekeyrecursive(current) &&
            typeof(current) === expected.source_type
    elseif kind == _NESTED_WRITE_KEY_PAIR
        return current isa Pair && typeof(current) === expected.source_type
    elseif kind == _NESTED_WRITE_KEY_TUPLE
        return current isa Tuple && typeof(current) === expected.source_type
    elseif kind == _NESTED_WRITE_KEY_NAMED_TUPLE
        return current isa NamedTuple &&
            typeof(current) === expected.source_type
    elseif kind == _NESTED_WRITE_KEY_STRUCT
        return current isa StructValue
    elseif kind == _NESTED_WRITE_KEY_LIST
        return current isa AbstractVector &&
            !(current isa AbstractVector{UInt8}) &&
            typeof(current) === expected.source_type
    elseif kind == _NESTED_WRITE_KEY_MAP
        return current isa Union{MapValue,AbstractDict} &&
            typeof(current) === expected.source_type
    end
    return false
end

function _nestedwritekeycomparestart(expected::_NestedWriteKeySnapshot,
        current, trace::_NestedWriteTrace, limits::Limits, depth::Int,
        parent::Union{Nothing,_NestedWriteKeyCompareFrame}, shape, witness,
        row_witness)
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    _nestedwriterowcheck(row_witness)
    _nestedwritekeywitnessroot(witness, current)
    shape === nothing || _nestedwritekeydeclared(shape, current)
    _nestedwritekeyfamilymatches(expected, current) || return false
    _nestedwritekeyrecursive(current) && return _nestedwritekeycompareframe(
        expected, current, trace, limits, depth, parent, shape, witness,
        row_witness)
    leaf = _nestedwritekeycompareleaf(expected, current, limits)
    return leaf === nothing ? false : leaf
end

function _nestedwritekeycomparesetnext!(frame::_NestedWriteKeyCompareFrame,
        current, shape, witness, row_witness)
    frame.position += 1
    frame.position <= length(frame.expected.children) || return false
    frame.next_expected = frame.expected.children[frame.position]
    frame.next_current = current
    frame.next_shape = shape
    frame.next_witness = witness
    frame.next_row_witness = row_witness
    return true
end

function _nestedwritekeycompareadvance!(frame::_NestedWriteKeyCompareFrame,
        trace::_NestedWriteTrace, limits::Limits)
    _nestedwritekeycomparecheck(frame) || return false
    frame.position == length(frame.expected.children) && return nothing
    current = frame.current
    kind = frame.expected.kind
    if kind == _NESTED_WRITE_KEY_PAIR
        child = iszero(frame.position) ? current.first : current.second
        return _nestedwritekeycomparesetnext!(frame, child, nothing, nothing,
            nothing)
    elseif kind == _NESTED_WRITE_KEY_TUPLE
        return _nestedwritekeycomparesetnext!(frame,
            current[frame.position + 1], nothing, nothing, nothing)
    elseif kind == _NESTED_WRITE_KEY_NAMED_TUPLE
        shape = frame.shape
        childshape = shape === nothing ? nothing :
            _nestedwritekeyshape(shape, frame.position + 1)
        return _nestedwritekeycomparesetnext!(frame,
            getfield(current, frame.position + 1), childshape, nothing,
            nothing)
    elseif kind == _NESTED_WRITE_KEY_STRUCT
        index = frame.position + 1
        expectedchild = _nestedwritekeystoredsource(frame.expected.value,
            index)
        childvector = _nestedwritestructaccesschild(current, current.names,
            current.children, length(frame.expected.children), index,
            frame.expected.names[index], expectedchild)
        childshape = frame.shape === nothing ? nothing :
            _nestedwritekeyshape(frame.shape, index)
        childwitness = _nestedwritekeywitnesschild(frame.witness, index)
        child, row_witness = _nestedwriterowaccess(childvector, current.index,
            _nestedwritekeyrowsnapshot(childshape, childwitness, trace,
                childvector, limits))
        return _nestedwritekeycomparesetnext!(frame, child, childshape,
            childwitness, row_witness)
    elseif kind == _NESTED_WRITE_KEY_LIST
        result = iszero(frame.position) ? iterate(frame.iterator) :
            iterate(frame.iterator, frame.iteration)
        result === nothing && return false
        index, iteration = result
        frame.iteration = iteration
        childshape = frame.shape === nothing ? nothing : frame.shape.element
        childwitness = _nestedwritekeywitnesslist(frame.witness)
        if current isa ListValue
            physical = current.first + index - 1
            child, row_witness = _nestedwriterowaccess(current.values,
                physical, _nestedwritekeyrowsnapshot(childshape,
                    childwitness, trace, current.values, limits))
        elseif frame.pending isa _NestedWriteVectorSnapshot
            child, row_witness = _nestedwriterowaccess(current, index,
                frame.pending)
        else
            child = _nestedwritelistaccessitem(current, index)
            row_witness = nothing
        end
        _nestedwritekeycomparecheck(frame) || return false
        return _nestedwritekeycomparesetnext!(frame, child, childshape,
            childwitness, row_witness)
    elseif kind == _NESTED_WRITE_KEY_MAP && current isa MapValue
        if iseven(frame.position)
            entry = frame.position ÷ 2 + 1
            childshape = frame.shape === nothing ? nothing : frame.shape.key
            childwitness = _nestedwritekeywitnessmapkey(frame.witness)
            physical = current.first + entry - 1
            child, row_witness = _nestedwriterowaccess(current.keys,
                physical, _nestedwritekeyrowsnapshot(childshape,
                    childwitness, trace, current.keys, limits))
            frame.pending = physical
            return _nestedwritekeycomparesetnext!(frame, child, childshape,
                childwitness, row_witness)
        end
        childshape = frame.shape === nothing ? nothing : frame.shape.value
        childwitness = _nestedwritekeywitnessmapvalue(frame.witness)
        if current.values === nothing
            child = missing
            row_witness = nothing
        else
            child, row_witness = _nestedwriterowaccess(current.values,
                frame.pending, _nestedwritekeyrowsnapshot(childshape,
                    childwitness, trace, current.values, limits))
        end
        return _nestedwritekeycomparesetnext!(frame, child, childshape,
            childwitness, row_witness)
    elseif kind == _NESTED_WRITE_KEY_MAP
        if iseven(frame.position)
            entry = frame.iterator
            entry isa _NestedWriteDictEntry || return false
            frame.iterator = entry.next
            frame.pending = entry
            childshape = frame.shape === nothing ? nothing : frame.shape.key
            childwitness = _nestedwritekeywitnessmapkey(frame.witness)
            return _nestedwritekeycomparesetnext!(frame, entry.key,
                childshape, childwitness, nothing)
        end
        entry = frame.pending
        entry isa _NestedWriteDictEntry || return false
        childshape = frame.shape === nothing ? nothing : frame.shape.value
        childwitness = _nestedwritekeywitnessmapvalue(frame.witness)
        return _nestedwritekeycomparesetnext!(frame, entry.value,
            childshape, childwitness, nothing)
    end
    throw(AssertionError("unknown recursive nested writer MAP-key kind"))
end

function _nestedwritekeycomparefinish(frame::_NestedWriteKeyCompareFrame,
        trace::_NestedWriteTrace)
    frame.position == length(frame.expected.children) || return false
    if frame.expected.kind == _NESTED_WRITE_KEY_MAP &&
            frame.current isa AbstractDict
        frame.iterator === nothing || return false
    elseif frame.expected.kind == _NESTED_WRITE_KEY_LIST
        result = iszero(frame.position) ? iterate(frame.iterator) :
            iterate(frame.iterator, frame.iteration)
        result === nothing || return false
    end
    return true
end

function _nestedwritekeycomparecleanup!(trace::_NestedWriteTrace,
        frame::Union{Nothing,_NestedWriteKeyCompareFrame})
    current = frame
    while current !== nothing
        parent = current.parent
        _nestedwritekeycompareframefree!(trace, current)
        current = parent
    end
    return
end

function _nestedwritekeyequaliterative(expected::_NestedWriteKeySnapshot,
        current, trace::_NestedWriteTrace, limits::Limits, shape, witness,
        row_witness)
    started = _nestedwritekeycomparestart(expected, current, trace, limits, 1,
        nothing, shape, witness, row_witness)
    started isa Bool && return started
    frame = started::_NestedWriteKeyCompareFrame
    try
        while true
            advanced = _nestedwritekeycompareadvance!(frame, trace, limits)
            advanced === false && return false
            if advanced === true
                child = something(frame.next_expected)
                depth = _nestedwritekeynextdepth(frame.depth, limits)
                started = _nestedwritekeycomparestart(child,
                    frame.next_current, trace, limits, depth, frame,
                    frame.next_shape, frame.next_witness,
                    frame.next_row_witness)
                started === false && return false
                if started === true
                    _nestedwriterowcheck(frame.next_row_witness)
                    continue
                end
                frame = started::_NestedWriteKeyCompareFrame
                continue
            end
            _nestedwritekeycomparefinish(frame, trace) || return false
            parent = frame.parent
            _nestedwritekeycompareframefree!(trace, frame)
            if parent === nothing
                frame = nothing
                return true
            end
            _nestedwriterowcheck(frame.row_witness)
            frame = parent
        end
    finally
        _nestedwritekeycomparecleanup!(trace, frame)
    end
end

function _nestedwritetracekey!(::Nothing, value,
        ::Union{Nothing,_NestedWriteShape}, ::Limits)
    return nothing
end

function _nestedwritetracekey!(::Nothing, value,
        ::Union{Nothing,_NestedWriteShape}, ::Limits, witness)
    return nothing
end

function _nestedwritetracekey!(::Nothing, value,
        ::Union{Nothing,_NestedWriteShape}, ::Limits, witness, row_witness)
    return nothing
end

function _nestedwritetracekey!(trace::_NestedWriteTrace, value,
        shape::Union{Nothing,_NestedWriteShape}, limits::Limits)
    return _nestedwritetracekey!(trace, value, shape, limits, nothing)
end

function _nestedwritetracekey!(trace::_NestedWriteTrace, value,
        shape::Union{Nothing,_NestedWriteShape}, limits::Limits, witness)
    return _nestedwritetracekey!(trace, value, shape, limits, witness,
        nothing)
end

function _nestedwritetracekey!(trace::_NestedWriteTrace, value,
        shape::Union{Nothing,_NestedWriteShape}, limits::Limits, witness,
        row_witness)
    _nestedwriterowcheck(row_witness)
    _nestedwritekeywitnessroot(witness, value)
    _nestedwritekeydeclared(shape, value)
    if trace.capturing
        snapshot = _nestedwritekeysnapshot(value, trace, shape, limits,
            witness, row_witness)
        _nestedwritetracecapture!(trace, _NestedWriteTraceEvent(
            _NESTED_WRITE_TRACE_KEY, Int64(0), Int64(0), Int64(0), Int64(0),
            snapshot))
        return snapshot
    end
    expected = _nestedwritetracepeekkey(trace)
    expected.kind == _NESTED_WRITE_TRACE_KEY && iszero(expected.a) &&
        iszero(expected.b) && iszero(expected.c) && iszero(expected.d) ||
        throw(ArgumentError(
            "nested Parquet input changed its occurrence topology between writer passes"))
    snapshot = expected.value::_NestedWriteKeySnapshot
    _nestedwritekeyequal(snapshot, value, trace, limits, shape, witness,
        row_witness) ||
        throw(ArgumentError(
            "nested Parquet MAP keys changed content or order between writer passes"))
    consumed = _nestedwritetracenext!(trace)
    consumed === expected || throw(ArgumentError(
        "nested Parquet input changed its MAP-key trace topology"))
    return snapshot
end

function _nestedwritetracekey!(trace, value)
    return _nestedwritetracekey!(trace, value, nothing, Limits())
end

function _nestedwritekeyassert!(::Nothing, value,
        ::Union{Nothing,_NestedWriteShape}, ::Limits, trace)
    return
end

function _nestedwritekeyassert!(expected::_NestedWriteKeySnapshot, value,
        shape::Union{Nothing,_NestedWriteShape}, limits::Limits,
        trace::_NestedWriteTrace)
    return _nestedwritekeyassert!(expected, value, shape, limits, trace,
        nothing, nothing)
end

function _nestedwritekeyassert!(expected::_NestedWriteKeySnapshot, value,
        shape::Union{Nothing,_NestedWriteShape}, limits::Limits,
        trace::_NestedWriteTrace, witness, row_witness)
    _nestedwriterowcheck(row_witness)
    _nestedwritekeywitnessroot(witness, value)
    _nestedwritekeydeclared(shape, value)
    _nestedwritekeyequal(expected, value, trace, limits, shape, witness,
        row_witness) || throw(ArgumentError(
        "nested Parquet MAP key changed while it was consumed"))
    return
end

function _nestedwriteoccurrencepayload(value)
    value isa AbstractString && return Int64(ncodeunits(value))
    value isa AbstractVector{UInt8} && return Int64(length(value))
    value isa JSONValue && return Int64(length(value.bytes))
    value isa BSONValue && return Int64(length(value.bytes))
    value isa UUIDs.UUID && return Int64(16)
    value isa Interval && return Int64(12)
    return Int64(0)
end

function _nestedwriteoccurrencelogical(value::Decimal)
    return Int64(value.scale), Int64(_decimaldigits(value.unscaled))
end

function _nestedwriteoccurrencelogical(value::Timestamp)
    adjusted = value.is_adjusted_to_utc ? Int64(1) : Int64(0)
    return adjusted, Int64(0)
end

function _nestedwriteoccurrencelogical(value)
    return Int64(0), Int64(0)
end

function _nestedwritetraceleaf!(trace, value, present::Bool)
    payload = present ? _nestedwriteoccurrencepayload(value) : Int64(0)
    logical, detail = present ? _nestedwriteoccurrencelogical(value) :
        (Int64(0), Int64(0))
    _nestedwritetraceevent!(trace, _NESTED_WRITE_TRACE_LEAF,
        present ? Int64(1) : Int64(0), payload, logical, detail)
    return
end

function _nestedwritetracestruct!(trace, value, present::Bool, count::Int)
    _nestedwritetraceevent!(trace, _NESTED_WRITE_TRACE_STRUCT,
        present ? Int64(1) : Int64(0), Int64(count), Int64(0), Int64(0))
    return
end

function _nestedwritetracecontainer!(trace, kind::UInt8, value,
        present::Bool)
    if !present
        _nestedwritetraceevent!(trace, kind, Int64(0), Int64(0), Int64(0),
            Int64(0))
        return
    end
    value isa ListValue && _validatelistvalue(value)
    value isa MapValue && _validatemapvalue(value)
    count = value isa AbstractVector ?
        Int64(_nestedvectorcount(value, "nested Parquet container")) :
        Int64(_nestedcheckedint(length(value),
            "nested Parquet container length"))
    if value isa AbstractVector
        rawfirst, rawlast = _nestedvectoraxes(value,
            "nested Parquet container")
        first = Int64(rawfirst)
        last = Int64(rawlast)
    else
        first = typemin(Int64)
        last = typemin(Int64)
    end
    _nestedwritetraceevent!(trace, kind, Int64(1), count, first, last)
    return
end

function _nestedwritetracedict!(trace, count::Int)
    _nestedwritetraceevent!(trace, _NESTED_WRITE_TRACE_MAP, Int64(1),
        Int64(count), typemin(Int64), typemin(Int64))
    return
end

function _nestedwritesnapshotarraycharge(values::BitVector)
    return _materializedbitbytes(length(values))
end

function _nestedwritesnapshotarraycharge(values::AbstractVector)
    return _materializedarraybytes(eltype(values), length(values))
end

function _nestedwritevalidatesource(values::AbstractVector, limits::Limits,
        budget::_LiveByteBudget; depth::Int=1)
    Base.@nospecialize values
    stack = _nestedwritepassstackstart(_NestedWriteSourceFrame, budget)
    try
        _nestedwritestackpush!(stack,
            _NestedWriteSourceFrame(values, depth, 0, -1, false), budget)
        while !isempty(stack)
            frame = _nestedwritepassstackpop!(stack)
            try
                if frame.position == 0
                    _checklimit(:metadata_depth, frame.depth,
                        limits.max_metadata_depth)
                    _nestedwritevalidatesourcenode(frame.values)
                    frame = _NestedWriteSourceFrame(frame.values, frame.depth,
                        0, _nestedwritesourcechildcount(frame.values), false)
                end
                _nestedwritesourceschedule!(stack, frame, limits, budget)
            finally
                _nestedwritepassstackprocessed!(stack)
            end
        end
    finally
        _nestedwritepassstackrelease!(stack, budget)
    end
    return
end

function _nestedwritesnapshotmetrics(values::AbstractVector, limits::Limits,
        budget::_LiveByteBudget; depth::Int=1)
    Base.@nospecialize values
    stack = _nestedwritepassstackstart(_NestedWriteSourceFrame, budget)
    count = 0
    charge = Int64(0)
    try
        _nestedwritestackpush!(stack,
            _NestedWriteSourceFrame(values, depth, 0, -1, false), budget)
        while !isempty(stack)
            frame = _nestedwritepassstackpop!(stack)
            try
                if frame.position == 0
                    _checklimit(:metadata_depth, frame.depth,
                        limits.max_metadata_depth)
                    count = Base.checked_add(count, 1)
                    charge = _materializedsum(charge,
                        _nestedwritesnapshotcopycharge(frame.values))
                    frame = _NestedWriteSourceFrame(frame.values, frame.depth,
                        0, _nestedwritesourcechildcount(frame.values), false)
                end
                _nestedwritesourceschedule!(stack, frame, limits, budget)
            finally
                _nestedwritepassstackprocessed!(stack)
            end
        end
    finally
        _nestedwritepassstackrelease!(stack, budget)
    end
    return count, charge
end

function _nestedwritesnapshotnode!(nodes::Vector{_NestedWriteVectorSnapshot},
        values::AbstractVector, limits::Limits, budget::_LiveByteBudget;
        depth::Int=1)
    Base.@nospecialize values
    stack = _nestedwritepassstackstart(_NestedWriteSourceFrame, budget)
    try
        _nestedwritestackpush!(stack,
            _NestedWriteSourceFrame(values, depth, 0, -1, false), budget)
        while !isempty(stack)
            frame = _nestedwritepassstackpop!(stack)
            try
                if frame.position == 0
                    _checklimit(:metadata_depth, frame.depth,
                        limits.max_metadata_depth)
                    _nestedwritesnapshotappend!(nodes, frame.values)
                    frame = _NestedWriteSourceFrame(frame.values, frame.depth,
                        0, _nestedwritesourcechildcount(frame.values), false)
                end
                _nestedwritesourceschedule!(stack, frame, limits, budget)
            finally
                _nestedwritepassstackprocessed!(stack)
            end
        end
    finally
        _nestedwritepassstackrelease!(stack, budget)
    end
    return
end

function _nestedwritesnapshotlookupcapacity(count::Int)
    count >= 0 || throw(ArgumentError(
        "nested Parquet snapshot count must be nonnegative"))
    required = try
        Base.checked_mul(count, 2)
    catch error
        error isa OverflowError || rethrow()
        throw(LimitError(:materialized_bytes, typemax(Int64), typemax(Int64)))
    end
    capacity = 4
    while capacity < required
        capacity = try
            Base.checked_mul(capacity, 2)
        catch error
            error isa OverflowError || rethrow()
            throw(LimitError(:materialized_bytes, typemax(Int64),
                typemax(Int64)))
        end
    end
    return capacity
end

function _nestedwritesnapshotlookuparraycharge(capacity::Int)
    charge = _materializedarraybytes(Union{Nothing,AbstractVector}, capacity)
    return _materializedsum(charge, _materializedarraybytes(
        Union{Nothing,_NestedWriteVectorSnapshot}, capacity))
end

function _nestedwritesnapshotlookupcharge(capacity::Int)
    return _materializedsum(_MATERIALIZED_OBJECT_BYTES,
        _nestedwritesnapshotlookuparraycharge(capacity))
end

function _nestedwritesnapshotlookup(count::Int)
    capacity = _nestedwritesnapshotlookupcapacity(count)
    sources = Vector{Union{Nothing,AbstractVector}}(undef, capacity)
    snapshots = Vector{Union{Nothing,_NestedWriteVectorSnapshot}}(
        undef, capacity)
    fill!(sources, nothing)
    fill!(snapshots, nothing)
    return _NestedWriteSnapshotLookup(sources, snapshots, 0,
        _nestedwritesnapshotlookupcharge(capacity))
end

function _nestedwritesnapshotlookupslot(
        lookup::_NestedWriteSnapshotLookup, source::AbstractVector)
    capacity = length(lookup.sources)
    mask = UInt(capacity - 1)
    index = Int((objectid(source) & mask) + UInt(1))
    while true
        stored = lookup.sources[index]
        (stored === nothing || stored === source) && return index
        index = index == capacity ? 1 : index + 1
    end
end

function _nestedwritesnapshotlookupget(
        lookup::_NestedWriteSnapshotLookup, source::AbstractVector)
    index = _nestedwritesnapshotlookupslot(lookup, source)
    lookup.sources[index] === source || return nothing
    return lookup.snapshots[index]
end

function _nestedwritesnapshotlookupinsertarrays!(
        sources::Vector{Union{Nothing,AbstractVector}},
        snapshots::Vector{Union{Nothing,_NestedWriteVectorSnapshot}},
        source::AbstractVector, snapshot::_NestedWriteVectorSnapshot)
    capacity = length(sources)
    mask = UInt(capacity - 1)
    index = Int((objectid(source) & mask) + UInt(1))
    while sources[index] !== nothing
        sources[index] === source && return false
        index = index == capacity ? 1 : index + 1
    end
    sources[index] = source
    snapshots[index] = snapshot
    return true
end

function _nestedwritesnapshotlookupinsert!(
        lookup::_NestedWriteSnapshotLookup, source::AbstractVector,
        snapshot::_NestedWriteVectorSnapshot)
    _nestedwritesnapshotlookupinsertarrays!(lookup.sources, lookup.snapshots,
        source, snapshot) || return false
    lookup.count = Base.checked_add(lookup.count, 1)
    return true
end

function _nestedwritesnapshotlookupgrow!(
        topology::_NestedWriteTopologySnapshot, budget::_LiveByteBudget)
    lookup = topology.lookup
    newcapacity = _nestedwritesnapshotlookupcapacity(
        Base.checked_add(lookup.count, 1))
    newcapacity <= length(lookup.sources) && return
    newcharge = _nestedwritesnapshotlookuparraycharge(newcapacity)
    _reserve!(budget, newcharge)
    sources = nothing
    snapshots = nothing
    try
        sources = Vector{Union{Nothing,AbstractVector}}(undef, newcapacity)
        snapshots = Vector{Union{Nothing,_NestedWriteVectorSnapshot}}(
            undef, newcapacity)
        fill!(sources, nothing)
        fill!(snapshots, nothing)
    catch
        _release!(budget, newcharge)
        rethrow()
    end
    count = 0
    for index in eachindex(lookup.sources)
        source = lookup.sources[index]
        source === nothing && continue
        _nestedwritesnapshotlookupinsertarrays!(sources, snapshots, source,
            something(lookup.snapshots[index])) || throw(AssertionError(
                "nested Parquet snapshot lookup contains duplicate identities"))
        count = Base.checked_add(count, 1)
    end
    oldcharge = _nestedwritesnapshotlookuparraycharge(length(lookup.sources))
    topologycharge = _materializedsum(topology.charge, newcharge)
    topologycharge = Base.checked_sub(topologycharge, oldcharge)
    lookupcharge = Base.checked_add(Base.checked_sub(lookup.charge,
        oldcharge), newcharge)
    lookup.sources = sources
    lookup.snapshots = snapshots
    lookup.count = count
    lookup.charge = lookupcharge
    topology.charge = topologycharge
    _release!(budget, oldcharge)
    return
end

function _nestedwritesnapshotcopycharge(values::AbstractVector)
    charge = Int64(0)
    if values isa ListVector
        charge = _nestedwritesnapshotarraycharge(values.offsets)
        values.validity === nothing || (charge = _materializedsum(charge,
            _nestedwritesnapshotarraycharge(values.validity)))
    elseif values isa StructVector
        charge = _nestedwritesnapshotarraycharge(values.names)
        charge = _materializedsum(charge,
            _nestedwritesnapshotarraycharge(values.children))
        values.ranks === nothing || (charge = _materializedsum(charge,
            _nestedwritesnapshotarraycharge(values.ranks)))
    elseif values isa MapVector
        charge = _nestedwritesnapshotarraycharge(values.offsets)
        values.validity === nothing || (charge = _materializedsum(charge,
            _nestedwritesnapshotarraycharge(values.validity)))
    end
    return charge
end

function _nestedwritevalidatesourcenode(values::AbstractVector)
    _nestedvectorcount(values, "nested Parquet vector")
    _nestedvectoraxes(values, "nested Parquet vector")
    values isa ListVector && _validatelistvector(values)
    values isa StructVector && _validatestructvector(values)
    values isa MapVector && _validatemapvector(values)
    return
end

function _nestedwritesnapshotappend!(nodes::Vector{_NestedWriteVectorSnapshot},
        values::AbstractVector)
    count = _nestedvectorcount(values, "nested Parquet vector")
    first, last = _nestedvectoraxes(values, "nested Parquet vector")
    if values isa LogicalColumn
        push!(nodes, _NestedWriteVectorSnapshot(values,
            _NESTED_WRITE_VECTOR_LOGICAL, count, first, last, values.values,
            values.spec, nothing, nothing, nothing, nothing, nothing, Int64(0)))
    elseif values isa FixedByteArrayVector
        push!(nodes, _NestedWriteVectorSnapshot(values,
            _NESTED_WRITE_VECTOR_FIXED, count, first, last, values.values,
            nothing, nothing, nothing, nothing, nothing, nothing,
            Int64(values.width)))
    elseif values isa ListVector
        offsets = copy(values.offsets)
        validity = values.validity === nothing ? nothing : copy(values.validity)
        push!(nodes, _NestedWriteVectorSnapshot(values,
            _NESTED_WRITE_VECTOR_LIST, count, first, last, values.offsets,
            values.validity, values.values, nothing, offsets, validity, nothing,
            Int64(0)))
    elseif values isa StructVector
        names = copy(values.names)
        ranks = values.ranks === nothing ? nothing : copy(values.ranks)
        children = copy(values.children)
        push!(nodes, _NestedWriteVectorSnapshot(values,
            _NESTED_WRITE_VECTOR_STRUCT, count, first, last, values.names,
            values.ranks, values.children, nothing, names, ranks, children,
            Int64(values.rows)))
    elseif values isa MapVector
        offsets = copy(values.offsets)
        validity = values.validity === nothing ? nothing : copy(values.validity)
        push!(nodes, _NestedWriteVectorSnapshot(values,
            _NESTED_WRITE_VECTOR_MAP, count, first, last, values.offsets,
            values.validity, values.keys, values.values, offsets, validity,
            nothing, Int64(0)))
    else
        push!(nodes, _NestedWriteVectorSnapshot(values,
            _NESTED_WRITE_VECTOR_GENERIC, count, first, last, nothing, nothing,
            nothing, nothing, nothing, nothing, nothing, Int64(0)))
    end
    return nodes[end]
end

function _nestedwritesnapshotappendlocal!(
        nodes::Vector{_NestedWriteVectorSnapshot}, values::ListVector)
    _validatelistlocal(values)
    count = length(values.offsets) - 1
    offsets = copy(values.offsets)
    validity = values.validity === nothing ? nothing : copy(values.validity)
    snapshot = _NestedWriteVectorSnapshot(values,
        _NESTED_WRITE_VECTOR_LIST, count, 1, count, values.offsets,
        values.validity, values.values, nothing, offsets, validity, nothing,
        Int64(0))
    push!(nodes, snapshot)
    _nestedwritevalidatenodelocal(snapshot)
    return snapshot
end

function _nestedwritesnapshotappendlocal!(
        nodes::Vector{_NestedWriteVectorSnapshot}, values::StructVector)
    _validatestructlocal(values)
    count = values.rows
    names = copy(values.names)
    ranks = values.ranks === nothing ? nothing : copy(values.ranks)
    children = copy(values.children)
    snapshot = _NestedWriteVectorSnapshot(values,
        _NESTED_WRITE_VECTOR_STRUCT, count, 1, count, values.names,
        values.ranks, values.children, nothing, names, ranks, children,
        Int64(values.rows))
    push!(nodes, snapshot)
    _nestedwritevalidatenodelocal(snapshot)
    return snapshot
end

function _nestedwritesnapshotappendlocal!(
        nodes::Vector{_NestedWriteVectorSnapshot}, values::MapVector)
    _validatemaplocal(values)
    count = length(values.offsets) - 1
    offsets = copy(values.offsets)
    validity = values.validity === nothing ? nothing : copy(values.validity)
    snapshot = _NestedWriteVectorSnapshot(values,
        _NESTED_WRITE_VECTOR_MAP, count, 1, count, values.offsets,
        values.validity, values.keys, values.values, offsets, validity,
        nothing, Int64(0))
    push!(nodes, snapshot)
    _nestedwritevalidatenodelocal(snapshot)
    return snapshot
end

function _nestedwriteextendtopologylocal!(
        topology::_NestedWriteTopologySnapshot, values::AbstractVector,
        budget::_LiveByteBudget)
    existing = _nestedwritesnapshotlookupget(topology.lookup, values)
    if existing !== nothing
        _nestedwritevalidatenodelocal(existing)
        return existing
    end
    _nestedwritesnapshotlookupgrow!(topology, budget)
    oldcount = length(topology.nodes)
    newcount = Base.checked_add(oldcount, 1)
    arraycharge = Base.checked_sub(
        _materializedarraybytes(_NestedWriteVectorSnapshot, newcount),
        _materializedarraybytes(_NestedWriteVectorSnapshot, oldcount))
    charge = _materializedsum(arraycharge, _MATERIALIZED_OBJECT_BYTES)
    charge = _materializedsum(charge,
        _nestedwritesnapshotcopycharge(values))
    _reserve!(budget, charge)
    topology.charge = _materializedsum(topology.charge, charge)
    sizehint!(topology.nodes, newcount)
    snapshot = if values isa Union{ListVector,StructVector,MapVector}
        _nestedwritesnapshotappendlocal!(topology.nodes, values)
    else
        _nestedwritesnapshotappend!(topology.nodes, values)
    end
    _nestedwritesnapshotlookupinsert!(topology.lookup, values, snapshot) ||
        throw(AssertionError(
            "nested Parquet topology inserted a duplicate local snapshot"))
    return snapshot
end

function _nestedwriteextendtopology!(topology::_NestedWriteTopologySnapshot,
        values::AbstractVector, limits::Limits, budget::_LiveByteBudget,
        depth::Int=1)
    Base.@nospecialize values
    stack = _nestedwritepassstackstart(_NestedWriteSourceFrame, budget)
    root::Union{Nothing,_NestedWriteVectorSnapshot} = nothing
    try
        _nestedwritestackpush!(stack,
            _NestedWriteSourceFrame(values, depth, 0, -1, false), budget)
        while !isempty(stack)
            frame = _nestedwritepassstackpop!(stack)
            try
                if frame.exit
                    snapshot = _nestedwritesnapshotlookupget(topology.lookup,
                        frame.values)
                    snapshot === nothing && throw(AssertionError(
                        "nested Parquet topology lost a source snapshot"))
                    _nestedwritevalidatenodelocal(snapshot)
                    continue
                end
                if frame.position == 0
                    _checklimit(:metadata_depth, frame.depth,
                        limits.max_metadata_depth)
                    existing = _nestedwritesnapshotlookupget(topology.lookup,
                        frame.values)
                    if existing !== nothing
                        root === nothing && (root = existing)
                        continue
                    end
                    _nestedwritesnapshotlookupgrow!(topology, budget)
                    oldcount = length(topology.nodes)
                    newcount = Base.checked_add(oldcount, 1)
                    arraycharge = Base.checked_sub(
                        _materializedarraybytes(_NestedWriteVectorSnapshot,
                            newcount),
                        _materializedarraybytes(_NestedWriteVectorSnapshot,
                            oldcount))
                    charge = _materializedsum(arraycharge,
                        _MATERIALIZED_OBJECT_BYTES)
                    charge = _materializedsum(charge,
                        _nestedwritesnapshotcopycharge(frame.values))
                    _reserve!(budget, charge)
                    topology.charge = _materializedsum(topology.charge, charge)
                    sizehint!(topology.nodes, newcount)
                    snapshot = _nestedwritesnapshotappend!(topology.nodes,
                        frame.values)
                    _nestedwritesnapshotlookupinsert!(topology.lookup,
                        frame.values, snapshot) || throw(AssertionError(
                            "nested Parquet topology inserted a duplicate source snapshot"))
                    root === nothing && (root = snapshot)
                    _nestedwritevalidatenodelocal(snapshot)
                    _nestedwritevalidatesourcenode(frame.values)
                    _nestedwritevalidatenodelocal(snapshot)
                    frame = _NestedWriteSourceFrame(frame.values, frame.depth,
                        0, _nestedwritesourcechildcount(frame.values), false)
                    if iszero(frame.count)
                        _nestedwritevalidatenodelocal(snapshot)
                        continue
                    end
                end
                _nestedwritesourceschedule!(stack, frame, limits, budget;
                    postorder=true) || throw(AssertionError(
                    "nested Parquet topology source traversal stopped early"))
            finally
                _nestedwritepassstackprocessed!(stack)
            end
        end
    finally
        _nestedwritepassstackrelease!(stack, budget)
    end
    root === nothing && throw(AssertionError(
        "nested Parquet topology did not produce a root snapshot"))
    return root
end

function _nestedwritetraceviewsnapshot!(::Nothing, source::AbstractVector,
        snapshot, ::Limits)
    snapshot === nothing || snapshot.source === source || throw(ArgumentError(
        "nested Parquet view changed its backing source"))
    return snapshot
end

function _nestedwritetraceviewsnapshot!(trace::_NestedWriteTrace,
        source::AbstractVector, snapshot, limits::Limits)
    if trace.capturing
        topology = trace.topology
        topology isa _NestedWriteTopologySnapshot || throw(AssertionError(
            "nested Parquet trace has no topology authority"))
        stored = snapshot === nothing ? _nestedwriteextendtopology!(topology,
            source, limits, trace.budget) : snapshot
        stored.source === source || throw(ArgumentError(
            "nested Parquet view changed its backing source"))
        _nestedwritetracecapture!(trace, _NestedWriteTraceEvent(
            _NESTED_WRITE_TRACE_SOURCE, Int64(0), Int64(0), Int64(0),
            Int64(0), stored))
        return stored
    end
    event = _nestedwritetracenext!(trace)
    event.kind == _NESTED_WRITE_TRACE_SOURCE && iszero(event.a) &&
        iszero(event.b) && iszero(event.c) && iszero(event.d) || throw(
            ArgumentError(
                "nested Parquet input changed its view topology between writer passes"))
    stored = event.value::_NestedWriteVectorSnapshot
    stored.source === source || throw(ArgumentError(
        "nested Parquet view changed its backing source between writer passes"))
    snapshot === nothing || snapshot === stored || throw(ArgumentError(
        "nested Parquet view changed its topology authority"))
    return stored
end

function _nestedwritetopology(input_columns::Union{Nothing,AbstractVector},
        names::Vector{String}, values::Vector{AbstractVector}, limits::Limits,
        budget::_LiveByteBudget; retainedcharge::Int64=Int64(0))
    start = _budgetused(budget)
    try
        nodecount = 0
        copycharge = Int64(0)
        for value in values
            _nestedwritevalidatesource(value, limits, budget)
            count, charge = _nestedwritesnapshotmetrics(value, limits, budget)
            nodecount = Base.checked_add(nodecount, count)
            copycharge = _materializedsum(copycharge, charge)
        end
        localcharge = _materializedsum(
            _materializedarraybytes(_NestedWriteVectorSnapshot, nodecount),
            _materializedproduct(nodecount + 1,
                _MATERIALIZED_OBJECT_BYTES))
        lookupcapacity = _nestedwritesnapshotlookupcapacity(nodecount)
        localcharge = _materializedsum(localcharge,
            _nestedwritesnapshotlookupcharge(lookupcapacity))
        localcharge = _materializedsum(localcharge, copycharge)
        _reserve!(budget, localcharge)
        charge = _materializedsum(localcharge, retainedcharge)
        nodes = _NestedWriteVectorSnapshot[]
        sizehint!(nodes, nodecount)
        for value in values
            _nestedwritesnapshotnode!(nodes, value, limits, budget)
        end
        length(nodes) == nodecount || throw(ArgumentError(
            "nested Parquet vector topology changed while it was copied"))
        lookup = _nestedwritesnapshotlookup(nodecount)
        for snapshot in nodes
            _nestedwritesnapshotlookupinsert!(lookup, snapshot.source,
                snapshot)
        end
        return _NestedWriteTopologySnapshot(input_columns,
            input_columns === nothing ? 0 : length(input_columns),
            input_columns === nothing ? 1 : firstindex(input_columns),
            input_columns === nothing ? 0 : lastindex(input_columns), names,
            values, nodes, lookup, nodecount, charge)
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _nestedwritesnapshotfor(topology::_NestedWriteTopologySnapshot,
        source::AbstractVector)
    snapshot = _nestedwritesnapshotlookupget(topology.lookup, source)
    snapshot === nothing && throw(ArgumentError(
        "nested Parquet source vector is absent from its topology snapshot"))
    return snapshot
end

function _nestedwriterowcheck(witness::Nothing)
    return
end

function _nestedwriterowcheck(witness::_NestedWriteRowWitness)
    snapshot = witness.snapshot
    values = snapshot.source
    index = witness.index
    kind = snapshot.kind
    _nestedvectorcount(values, "nested Parquet row source") == snapshot.count ||
        throw(ArgumentError(
            "nested Parquet vector length changed during row access"))
    firstaxis, lastaxis = _nestedvectoraxes(values,
        "nested Parquet row source")
    firstaxis == snapshot.first && lastaxis == snapshot.last || throw(
        ArgumentError(
            "nested Parquet vector axes changed during row access"))
    if kind == _NESTED_WRITE_VECTOR_LIST
        values isa ListVector || throw(ArgumentError(
            "nested Parquet LIST wrapper changed during row access"))
        values.offsets === snapshot.primary &&
            values.validity === snapshot.secondary &&
            values.values === snapshot.tertiary || throw(ArgumentError(
                "nested Parquet LIST backing topology changed during row access"))
        offsets = values.offsets
        length(offsets) == length(snapshot.copy1) || throw(ArgumentError(
            "nested Parquet LIST offset count changed during row access"))
        offsets[index] == snapshot.copy1[index] &&
            offsets[index + 1] == snapshot.copy1[index + 1] || throw(
                ArgumentError(
                    "nested Parquet LIST offsets changed during row access"))
        validity = values.validity
        validity === nothing || length(validity) == length(snapshot.copy2) ||
            throw(ArgumentError(
                "nested Parquet LIST validity count changed during row access"))
        validity === nothing || validity[index] == snapshot.copy2[index] ||
            throw(ArgumentError(
                "nested Parquet LIST validity changed during row access"))
    elseif kind == _NESTED_WRITE_VECTOR_STRUCT
        values isa StructVector || throw(ArgumentError(
            "nested Parquet struct wrapper changed during row access"))
        values.rows == snapshot.scalar && values.names === snapshot.primary &&
            values.ranks === snapshot.secondary &&
            values.children === snapshot.tertiary || throw(ArgumentError(
                "nested Parquet struct backing topology changed during row access"))
        ranks = values.ranks
        ranks === nothing || length(ranks) == length(snapshot.copy2) || throw(
            ArgumentError(
                "nested Parquet struct rank count changed during row access"))
        ranks === nothing || (ranks[index] == snapshot.copy2[index] &&
            ranks[index + 1] == snapshot.copy2[index + 1]) || throw(
                ArgumentError(
                    "nested Parquet struct ranks changed during row access"))
    elseif kind == _NESTED_WRITE_VECTOR_MAP
        values isa MapVector || throw(ArgumentError(
            "nested Parquet MAP wrapper changed during row access"))
        values.offsets === snapshot.primary &&
            values.validity === snapshot.secondary &&
            values.keys === snapshot.tertiary &&
            values.values === snapshot.quaternary || throw(ArgumentError(
                "nested Parquet MAP backing topology changed during row access"))
        offsets = values.offsets
        length(offsets) == length(snapshot.copy1) || throw(ArgumentError(
            "nested Parquet MAP offset count changed during row access"))
        offsets[index] == snapshot.copy1[index] &&
            offsets[index + 1] == snapshot.copy1[index + 1] || throw(
                ArgumentError(
                    "nested Parquet MAP offsets changed during row access"))
        validity = values.validity
        validity === nothing || length(validity) == length(snapshot.copy2) ||
            throw(ArgumentError(
                "nested Parquet MAP validity count changed during row access"))
        validity === nothing || validity[index] == snapshot.copy2[index] ||
            throw(ArgumentError(
                "nested Parquet MAP validity changed during row access"))
    elseif kind == _NESTED_WRITE_VECTOR_LOGICAL
        values isa LogicalColumn && values.values === snapshot.primary &&
            isequal(values.spec, snapshot.secondary) || throw(ArgumentError(
                "nested Parquet logical wrapper changed during row access"))
    elseif kind == _NESTED_WRITE_VECTOR_FIXED
        values isa FixedByteArrayVector && values.values === snapshot.primary &&
            values.width == snapshot.scalar || throw(ArgumentError(
                "nested Parquet fixed-width wrapper changed during row access"))
    elseif kind == _NESTED_WRITE_VECTOR_GENERIC
        nothing
    else
        throw(AssertionError("row witness has a non-container snapshot"))
    end
    return
end

function _nestedwriterowwitness(snapshot::Nothing, ::Int)
    return nothing
end

function _nestedwriterowwitness(snapshot::_NestedWriteVectorSnapshot,
        index::Int)
    kind = snapshot.kind
    if kind in (_NESTED_WRITE_VECTOR_LIST, _NESTED_WRITE_VECTOR_STRUCT,
            _NESTED_WRITE_VECTOR_MAP)
        1 <= index <= snapshot.count || throw(ArgumentError(
            "nested Parquet row index exceeds its topology snapshot"))
    else
        checkbounds(Bool, snapshot.source, index) || throw(ArgumentError(
            "nested Parquet row index exceeds its topology snapshot"))
    end
    if kind == _NESTED_WRITE_VECTOR_STRUCT
        ranks = snapshot.copy2
        if ranks === nothing
            witness = _NestedWriteRowWitness(snapshot, index, index, index,
                true)
        else
            first = Int(ranks[index])
            last = Int(ranks[index + 1])
            witness = _NestedWriteRowWitness(snapshot, index, first, last,
                first != last)
        end
    elseif kind in (_NESTED_WRITE_VECTOR_LIST, _NESTED_WRITE_VECTOR_MAP)
        offsets = snapshot.copy1
        first, last = _nestedspan(offsets, index)
        validity = snapshot.copy2
        present = validity === nothing || validity[index]
        witness = _NestedWriteRowWitness(snapshot, index, first, last, present)
    else
        witness = _NestedWriteRowWitness(snapshot, index, index, index, true)
    end
    _nestedwriterowcheck(witness)
    return witness
end

function _nestedwriterowvalue(values::AbstractVector, index::Int,
        ::Nothing)
    return values[index]
end

function _nestedwriterowvalue(::AbstractVector, ::Int,
        witness::_NestedWriteRowWitness)
    snapshot = witness.snapshot
    witness.present || return missing
    if snapshot.kind == _NESTED_WRITE_VECTOR_LIST
        return ListValue(snapshot.tertiary, witness.first, witness.last)
    elseif snapshot.kind == _NESTED_WRITE_VECTOR_STRUCT
        return StructValue(snapshot.primary, snapshot.tertiary, witness.last)
    elseif snapshot.kind == _NESTED_WRITE_VECTOR_MAP
        keys = snapshot.tertiary
        values = snapshot.quaternary
        K = eltype(keys)
        hasvalues = values !== nothing
        V = hasvalues ? eltype(values) : Missing
        return MapValue{K,V,hasvalues}(keys, values, witness.first,
            witness.last)
    end
    return snapshot.source[witness.index]
end

function _nestedwriterowaccess(values::AbstractVector, index::Int, snapshot)
    witness = _nestedwriterowwitness(snapshot, index)
    value = _nestedwriterowvalue(values, index, witness)
    _nestedwriterowcheck(witness)
    return value, witness
end

function _nestedwritearrayequal(current::AbstractVector,
        expected::AbstractVector)
    typeof(current) === typeof(expected) || return false
    length(current) == length(expected) || return false
    firstindex(current) == firstindex(expected) &&
        lastindex(current) == lastindex(expected) || return false
    for index in eachindex(current, expected)
        isequal(current[index], expected[index]) || return false
    end
    return true
end

function _nestedwritevectoridentities(current::Vector{AbstractVector},
        expected::Vector{AbstractVector})
    length(current) == length(expected) || return false
    for index in eachindex(current, expected)
        current[index] === expected[index] || return false
    end
    return true
end

function _nestedwritevalidateaxis(snapshot::_NestedWriteVectorSnapshot)
    values = snapshot.source
    _nestedvectorcount(values, "nested Parquet vector") == snapshot.count ||
        throw(ArgumentError(
        "nested Parquet vector length changed between writer phases"))
    first, last = _nestedvectoraxes(values, "nested Parquet vector")
    first == snapshot.first && last == snapshot.last || throw(ArgumentError(
            "nested Parquet vector axes changed between writer phases"))
    return
end

function _nestedwritevalidatenodelocal(snapshot::_NestedWriteVectorSnapshot)
    values = snapshot.source
    kind = snapshot.kind
    if kind == _NESTED_WRITE_VECTOR_LOGICAL
        values isa LogicalColumn || throw(ArgumentError(
            "nested Parquet logical wrapper changed between writer phases"))
        values.values === snapshot.primary || throw(ArgumentError(
            "nested Parquet logical wrapper changed child identity"))
        isequal(values.spec, snapshot.secondary) || throw(ArgumentError(
            "nested Parquet logical metadata changed between writer phases"))
    elseif kind == _NESTED_WRITE_VECTOR_FIXED
        values isa FixedByteArrayVector || throw(ArgumentError(
            "nested Parquet fixed-width wrapper changed between writer phases"))
        values.values === snapshot.primary || throw(ArgumentError(
            "nested Parquet fixed-width wrapper changed child identity"))
        values.width == snapshot.scalar || throw(ArgumentError(
            "nested Parquet fixed width changed between writer phases"))
    elseif kind == _NESTED_WRITE_VECTOR_LIST
        values isa ListVector || throw(ArgumentError(
            "nested Parquet LIST wrapper changed between writer phases"))
        values.offsets === snapshot.primary &&
            _nestedwritearrayequal(values.offsets, snapshot.copy1) ||
            throw(ArgumentError(
                "nested Parquet LIST offsets changed between writer phases"))
        values.validity === snapshot.secondary || throw(ArgumentError(
            "nested Parquet LIST validity identity changed between writer phases"))
        values.validity === nothing ||
            _nestedwritearrayequal(values.validity, snapshot.copy2) ||
            throw(ArgumentError(
                "nested Parquet LIST validity changed between writer phases"))
        values.values === snapshot.tertiary || throw(ArgumentError(
            "nested Parquet LIST child identity changed between writer phases"))
    elseif kind == _NESTED_WRITE_VECTOR_STRUCT
        values isa StructVector || throw(ArgumentError(
            "nested Parquet struct wrapper changed between writer phases"))
        values.rows == snapshot.scalar || throw(ArgumentError(
            "nested Parquet struct row count changed between writer phases"))
        values.names === snapshot.primary &&
            _nestedwritearrayequal(values.names, snapshot.copy1) ||
            throw(ArgumentError(
                "nested Parquet struct names changed between writer phases"))
        values.ranks === snapshot.secondary || throw(ArgumentError(
            "nested Parquet struct rank identity changed between writer phases"))
        values.ranks === nothing ||
            _nestedwritearrayequal(values.ranks, snapshot.copy2) ||
            throw(ArgumentError(
                "nested Parquet struct ranks changed between writer phases"))
        values.children === snapshot.tertiary &&
            _nestedwritevectoridentities(values.children, snapshot.copy3) ||
            throw(ArgumentError(
                "nested Parquet struct child identity or order changed between writer phases"))
    elseif kind == _NESTED_WRITE_VECTOR_MAP
        values isa MapVector || throw(ArgumentError(
            "nested Parquet MAP wrapper changed between writer phases"))
        values.offsets === snapshot.primary &&
            _nestedwritearrayequal(values.offsets, snapshot.copy1) ||
            throw(ArgumentError(
                "nested Parquet MAP offsets changed between writer phases"))
        values.validity === snapshot.secondary || throw(ArgumentError(
            "nested Parquet MAP validity identity changed between writer phases"))
        values.validity === nothing ||
            _nestedwritearrayequal(values.validity, snapshot.copy2) ||
            throw(ArgumentError(
                "nested Parquet MAP validity changed between writer phases"))
        values.keys === snapshot.tertiary || throw(ArgumentError(
            "nested Parquet MAP key identity changed between writer phases"))
        values.values === snapshot.quaternary || throw(ArgumentError(
            "nested Parquet MAP value identity changed between writer phases"))
    elseif kind != _NESTED_WRITE_VECTOR_GENERIC
        throw(AssertionError("unknown nested writer vector snapshot kind"))
    end
    return
end

function _nestedwritevalidatenode(snapshot::_NestedWriteVectorSnapshot)
    _nestedwritevalidatenodelocal(snapshot)
    values = snapshot.source
    kind = snapshot.kind
    kind == _NESTED_WRITE_VECTOR_LIST && _validatelistvector(values)
    kind == _NESTED_WRITE_VECTOR_STRUCT && _validatestructvector(values)
    kind == _NESTED_WRITE_VECTOR_MAP && _validatemapvector(values)
    _nestedwritevalidateaxis(snapshot)
    _nestedwritevalidatenodelocal(snapshot)
    return
end

function _nestedwritedictreserve!(::Nothing,
        materialization::_NestedWriteDictMaterialization, ::Int64)
    return
end

function _nestedwritedictreserve!(trace::_NestedWriteTrace,
        materialization::_NestedWriteDictMaterialization, bytes::Int64)
    _nestedwritetracereserve!(trace, bytes)
    materialization.charge = _materializedsum(materialization.charge, bytes)
    return
end

function _nestedwritedictunreserve!(::Nothing,
        materialization::_NestedWriteDictMaterialization, ::Int64)
    return
end

function _nestedwritedictunreserve!(trace::_NestedWriteTrace,
        materialization::_NestedWriteDictMaterialization, bytes::Int64)
    _nestedwritetraceunreserve!(trace, bytes)
    materialization.charge = Base.checked_sub(materialization.charge, bytes)
    return
end

function _nestedwritedictmaterialization(trace)
    capacity = _nestedwritesnapshotlookupcapacity(0)
    setcharge = _materializedsum(_MATERIALIZED_OBJECT_BYTES,
        _materializedarraybytes(Union{Nothing,AbstractVector}, capacity))
    charge = _materializedsum(_MATERIALIZED_OBJECT_BYTES,
        _materializedproduct(2, setcharge))
    trace isa _NestedWriteTrace && _nestedwritetracereserve!(trace, charge)
    try
        sources = Vector{Union{Nothing,AbstractVector}}(undef, capacity)
        fill!(sources, nothing)
        dependencies = _NestedWriteIdentitySet(sources, 0)
        preflight_sources = Vector{Union{Nothing,AbstractVector}}(undef,
            capacity)
        fill!(preflight_sources, nothing)
        preflight = _NestedWriteIdentitySet(preflight_sources, 0)
        return _NestedWriteDictMaterialization(nothing, nothing, nothing,
            nothing, dependencies, preflight, 0,
            trace isa _NestedWriteTrace ? charge : Int64(0))
    catch
        trace isa _NestedWriteTrace && _nestedwritetraceunreserve!(trace,
            charge)
        rethrow()
    end
end

function _nestedwritedictrelease!(trace,
        materialization::_NestedWriteDictMaterialization)
    entry = materialization.first
    while entry !== nothing
        following = entry.next
        entry.key = nothing
        entry.value = nothing
        entry.next = nothing
        entry = following
    end
    dependency = materialization.dependencies
    while dependency !== nothing
        following = dependency.next
        dependency.next = nothing
        dependency = following
    end
    materialization.first = nothing
    materialization.last = nothing
    materialization.dependencies = nothing
    materialization.dependency_last = nothing
    materialization.dependency_sources = nothing
    materialization.preflight_sources = nothing
    materialization.count = 0
    charge = materialization.charge
    if !iszero(charge) && trace isa _NestedWriteTrace
        _nestedwritetraceunreserve!(trace, charge)
    end
    materialization.charge = Int64(0)
    return
end

function _nestedwritedictentry!(trace,
        materialization::_NestedWriteDictMaterialization, pair::Pair)
    _nestedwritedictreserve!(trace, materialization,
        _MATERIALIZED_OBJECT_BYTES)
    entry = try
        _NestedWriteDictEntry(pair.first, pair.second, nothing)
    catch
        _nestedwritedictunreserve!(trace, materialization,
            _MATERIALIZED_OBJECT_BYTES)
        rethrow()
    end
    if materialization.last === nothing
        materialization.first = entry
    else
        materialization.last.next = entry
    end
    materialization.last = entry
    return
end

function _nestedwritedictidentityslot(
        sources::Vector{Union{Nothing,AbstractVector}},
        source::AbstractVector)
    capacity = length(sources)
    mask = UInt(capacity - 1)
    index = Int((objectid(source) & mask) + UInt(1))
    while true
        stored = sources[index]
        (stored === nothing || stored === source) && return index
        index = index == capacity ? 1 : index + 1
    end
end

function _nestedwritedictidentitygrow!(trace,
        materialization::_NestedWriteDictMaterialization,
        identities::_NestedWriteIdentitySet)
    capacity = length(identities.sources)
    Base.checked_mul(Base.checked_add(identities.count, 1), 2) <= capacity &&
        return
    newcapacity = Base.checked_mul(capacity, 2)
    newcharge = _materializedarraybytes(Union{Nothing,AbstractVector},
        newcapacity)
    _nestedwritedictreserve!(trace, materialization, newcharge)
    sources = try
        output = Vector{Union{Nothing,AbstractVector}}(undef, newcapacity)
        fill!(output, nothing)
        output
    catch
        _nestedwritedictunreserve!(trace, materialization, newcharge)
        rethrow()
    end
    for source in identities.sources
        source === nothing && continue
        index = _nestedwritedictidentityslot(sources, source)
        sources[index] = source
    end
    oldcharge = _materializedarraybytes(Union{Nothing,AbstractVector},
        capacity)
    identities.sources = sources
    _nestedwritedictunreserve!(trace, materialization, oldcharge)
    return
end

function _nestedwritedictidentityinsertset!(trace,
        materialization::_NestedWriteDictMaterialization,
        identities::_NestedWriteIdentitySet, source::AbstractVector)
    index = _nestedwritedictidentityslot(identities.sources, source)
    identities.sources[index] === source && return false
    _nestedwritedictidentitygrow!(trace, materialization, identities)
    index = _nestedwritedictidentityslot(identities.sources, source)
    identities.sources[index] = source
    identities.count = Base.checked_add(identities.count, 1)
    return true
end

function _nestedwritedictidentityinsert!(trace,
        materialization::_NestedWriteDictMaterialization,
        source::AbstractVector)
    identities = materialization.dependency_sources::_NestedWriteIdentitySet
    return _nestedwritedictidentityinsertset!(trace, materialization,
        identities, source)
end

function _nestedwritedictidentitycontains(
        materialization::_NestedWriteDictMaterialization,
        source::AbstractVector)
    identities = materialization.dependency_sources::_NestedWriteIdentitySet
    index = _nestedwritedictidentityslot(identities.sources, source)
    return identities.sources[index] === source
end

function _nestedwritedictpreflightinsert!(trace,
        materialization::_NestedWriteDictMaterialization,
        source::AbstractVector)
    identities = materialization.preflight_sources::_NestedWriteIdentitySet
    return _nestedwritedictidentityinsertset!(trace, materialization,
        identities, source)
end

function _nestedwritedictpreflightrelease!(trace,
        materialization::_NestedWriteDictMaterialization)
    identities = materialization.preflight_sources
    identities isa _NestedWriteIdentitySet || return
    charge = _materializedsum(_MATERIALIZED_OBJECT_BYTES,
        _materializedarraybytes(Union{Nothing,AbstractVector},
            length(identities.sources)))
    materialization.preflight_sources = nothing
    _nestedwritedictunreserve!(trace, materialization, charge)
    return
end

function _nestedwritedictdependencynode!(trace,
        materialization::_NestedWriteDictMaterialization, snapshot)
    _nestedwritedictreserve!(trace, materialization,
        _MATERIALIZED_OBJECT_BYTES)
    dependency = try
        _NestedWriteDictDependency(snapshot, nothing)
    catch
        _nestedwritedictunreserve!(trace, materialization,
            _MATERIALIZED_OBJECT_BYTES)
        rethrow()
    end
    if materialization.dependency_last === nothing
        materialization.dependencies = dependency
    else
        materialization.dependency_last.next = dependency
    end
    materialization.dependency_last = dependency
    return
end

function _nestedwritedictdependency!(trace,
        materialization::_NestedWriteDictMaterialization,
        snapshot::_NestedWriteVectorSnapshot)
    _nestedwritedictidentityinsert!(trace, materialization,
        snapshot.source) || return
    _nestedwritedictdependencynode!(trace, materialization, snapshot)
    return
end

function _nestedwritedictdependency!(trace,
        materialization::_NestedWriteDictMaterialization,
        snapshot::_NestedWriteStructViewSnapshot)
    _nestedwritedictdependencynode!(trace, materialization, snapshot)
    return
end

function _nestedwritedictstructview!(trace,
        materialization::_NestedWriteDictMaterialization, value::StructValue,
        shape)
    names = value.names
    children = value.children
    count = length(children)
    length(names) == count || throw(ArgumentError(
        "nested Parquet struct view changed its field count"))
    if shape isa _NestedWriteStructShape
        names == shape.names && count == length(shape.children) || throw(
            ArgumentError(
                "nested Parquet struct view does not match its declared topology"))
    end
    value.index >= 1 || throw(ArgumentError(
        "nested Parquet struct view has an invalid row index"))
    copycharge = _materializedsum(_MATERIALIZED_OBJECT_BYTES,
        _materializedarraybytes(String, count))
    copycharge = _materializedsum(copycharge,
        _materializedarraybytes(AbstractVector, count))
    _nestedwritedictreserve!(trace, materialization, copycharge)
    snapshot = try
        _NestedWriteStructViewSnapshot(value, names, children, copy(names),
            copy(children), value.index)
    catch
        _nestedwritedictunreserve!(trace, materialization, copycharge)
        rethrow()
    end
    _nestedwritedictdependency!(trace, materialization, snapshot)
    return
end

function _nestedwritedictlistviewlocal(value::ListValue)
    _nestedviewcount(value.first, value.last, "list view")
    return
end

function _nestedwritedictmapviewlocal(
        value::MapValue{K,V,HasValues}) where {K,V,HasValues}
    HasValues isa Bool || throw(ArgumentError(
        "map view has a non-Boolean value-vector discriminator"))
    Missing <: K && throw(ArgumentError(
        "map view key type cannot include Missing"))
    HasValues === false && V !== Missing && throw(ArgumentError(
        "key-only map view must use Missing as its value type"))
    if HasValues === true
        value.values === nothing && throw(ArgumentError(
            "map view lost its value vector"))
    else
        value.values === nothing || throw(ArgumentError(
            "key-only map view gained a value vector"))
    end
    _nestedviewcount(value.first, value.last, "map view")
    return
end

function _nestedwritedictpreflightlocal(value, shape)
    if value isa ListValue
        _nestedwritedictlistviewlocal(value)
    elseif value isa MapValue
        _nestedwritedictmapviewlocal(value)
    elseif value isa StructValue
        length(value.names) == length(value.children) || throw(ArgumentError(
            "nested Parquet struct view changed its field count"))
        value.index >= 1 || throw(ArgumentError(
            "nested Parquet struct view has an invalid row index"))
        if shape isa _NestedWriteStructShape
            value.names == shape.names &&
                length(value.children) == length(shape.children) || throw(
                ArgumentError(
                    "nested Parquet struct view does not match its declared topology"))
        end
    elseif value isa ListVector
        _validatelistlocal(value)
    elseif value isa StructVector
        _validatestructlocal(value)
    elseif value isa MapVector
        _validatemaplocal(value)
    end
    return
end

function _nestedwritedictpreflightgraph!(trace,
        materialization::_NestedWriteDictMaterialization, value, shape,
        limits::Limits, depth::Int)
    trace isa _NestedWriteTrace || return
    work = _nestedwritedictwork(trace, value, shape, depth, false, nothing)
    try
        while work !== nothing
            current = work
            work = current.next
            item = current.value
            itemshape = current.shape
            itemdepth = current.depth
            current.value = nothing
            current.next = nothing
            _nestedwritedictworkfree!(trace)
            _checklimit(:metadata_depth, itemdepth,
                limits.max_metadata_depth)
            if item isa Union{ListValue,MapValue}
                _nestedwritedictpreflightlocal(item, itemshape)
            elseif item isa AbstractVector
                _nestedwritedictpreflightinsert!(trace, materialization,
                    item) || continue
                _nestedwritedictpreflightlocal(item, itemshape)
            else
                _nestedwritedictpreflightlocal(item, itemshape)
            end
            if item isa ListValue
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                childshape = itemshape isa _NestedWriteListShape ?
                    itemshape.element : nothing
                work = _nestedwritedictwork(trace, item.values, childshape,
                    childdepth, true, work)
            elseif item isa StructValue && !isempty(item.children)
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                for index in length(item.children):-1:1
                    childshape = itemshape isa _NestedWriteStructShape &&
                        index <= length(itemshape.children) ?
                        itemshape.children[index] : nothing
                    work = _nestedwritedictwork(trace, item.children[index],
                        childshape, childdepth, true, work)
                end
            elseif item isa MapValue
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                valueshape = itemshape isa _NestedWriteMapShape ?
                    itemshape.value : nothing
                item.values === nothing || (work = _nestedwritedictwork(
                    trace, item.values, valueshape, childdepth, true, work))
                keyshape = itemshape isa _NestedWriteMapShape ?
                    itemshape.key : nothing
                work = _nestedwritedictwork(trace, item.keys, keyshape,
                    childdepth, true, work)
            elseif item isa Union{LogicalColumn,FixedByteArrayVector}
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                work = _nestedwritedictwork(trace, item.values, itemshape,
                    childdepth, true, work)
            elseif item isa ListVector
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                childshape = itemshape isa _NestedWriteListShape ?
                    itemshape.element : nothing
                work = _nestedwritedictwork(trace, item.values, childshape,
                    childdepth, true, work)
            elseif item isa StructVector && !isempty(item.children)
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                for index in length(item.children):-1:1
                    childshape = itemshape isa _NestedWriteStructShape &&
                        index <= length(itemshape.children) ?
                        itemshape.children[index] : nothing
                    work = _nestedwritedictwork(trace, item.children[index],
                        childshape, childdepth, true, work)
                end
            elseif item isa MapVector
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                valueshape = itemshape isa _NestedWriteMapShape ?
                    itemshape.value : nothing
                item.values === nothing || (work = _nestedwritedictwork(
                    trace, item.values, valueshape, childdepth, true, work))
                keyshape = itemshape isa _NestedWriteMapShape ?
                    itemshape.key : nothing
                work = _nestedwritedictwork(trace, item.keys, keyshape,
                    childdepth, true, work)
            elseif item isa Pair && itemshape === nothing
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                work = _nestedwritedictwork(trace, item.second, nothing,
                    childdepth, false, work)
                work = _nestedwritedictwork(trace, item.first, nothing,
                    childdepth, false, work)
            elseif item isa Union{Tuple,NamedTuple} &&
                    itemshape isa Union{Nothing,_NestedWriteStructShape} &&
                    !isempty(item)
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                for index in length(item):-1:1
                    childshape = itemshape isa _NestedWriteStructShape &&
                        index <= length(itemshape.children) ?
                        itemshape.children[index] : nothing
                    work = _nestedwritedictwork(trace, getfield(item, index),
                        childshape, childdepth, false, work)
                end
            elseif itemshape isa _NestedWriteStructShape &&
                    !ismissing(item) && fieldcount(typeof(item)) > 0
                count = min(fieldcount(typeof(item)),
                    length(itemshape.children))
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                for index in count:-1:1
                    work = _nestedwritedictwork(trace, getfield(item, index),
                        itemshape.children[index], childdepth, false, work)
                end
            end
        end
    finally
        _nestedwritedictworkcleanup!(trace, work)
    end
    return
end

function _nestedwritedictsourceauthority(trace::_NestedWriteTrace,
        materialization::_NestedWriteDictMaterialization,
        source::AbstractVector)
    _nestedwritedictidentitycontains(materialization, source) || throw(
        AssertionError(
            "nested Parquet dictionary view has no captured source authority"))
    topology = trace.topology
    topology isa _NestedWriteTopologySnapshot || throw(AssertionError(
        "nested Parquet trace has no topology authority"))
    snapshot = _nestedwritesnapshotlookupget(topology.lookup, source)
    snapshot === nothing && throw(AssertionError(
        "nested Parquet dictionary source authority is absent"))
    return snapshot
end

function _nestedwritedictonebased(snapshot::_NestedWriteVectorSnapshot,
        label::String)
    snapshot.first == 1 && snapshot.last == snapshot.count || throw(
        ArgumentError("$label must use one-based contiguous axes"))
    return snapshot.count
end

function _nestedwritedictlistviewauthority(trace::_NestedWriteTrace,
        materialization::_NestedWriteDictMaterialization, value::ListValue)
    _nestedwritedictlistviewlocal(value)
    snapshot = _nestedwritedictsourceauthority(trace, materialization,
        value.values)
    count = _nestedwritedictonebased(snapshot, "list view backing vector")
    (value.first <= count || (count < typemax(Int) &&
        value.first == count + 1)) || throw(ArgumentError(
            "list view insertion point exceeds its backing vector"))
    if value.last >= value.first
        value.last <= count || throw(ArgumentError(
            "list view child span exceeds its backing vector axes"))
    end
    return
end

function _nestedwritedictmapviewauthority(trace::_NestedWriteTrace,
        materialization::_NestedWriteDictMaterialization, value::MapValue)
    _nestedwritedictmapviewlocal(value)
    keysnapshot = _nestedwritedictsourceauthority(trace, materialization,
        value.keys)
    keycount = _nestedwritedictonebased(keysnapshot, "map view key vector")
    (value.first <= keycount || (keycount < typemax(Int) &&
        value.first == keycount + 1)) || throw(ArgumentError(
            "map view insertion point exceeds its key vector"))
    value.last < value.first || value.last <= keycount || throw(
        ArgumentError("map view entry span exceeds its key-vector axes"))
    if value.values !== nothing
        valuesnapshot = _nestedwritedictsourceauthority(trace,
            materialization, value.values)
        valuecount = _nestedwritedictonebased(valuesnapshot,
            "map view value vector")
        valuecount == keycount || throw(ArgumentError(
            "map view key and value lengths differ"))
        value.last < value.first || value.last <= valuecount || throw(
            ArgumentError("map view entry span exceeds its value-vector axes"))
    end
    return
end

function _nestedwritedictviewlocal(
        snapshot::_NestedWriteStructViewSnapshot)
    value = snapshot.source
    value.names === snapshot.names &&
        value.children === snapshot.children &&
        value.index == snapshot.index || throw(ArgumentError(
            "nested Parquet struct view backing identity changed during dictionary iteration"))
    length(value.names) == length(snapshot.names_copy) &&
        length(value.children) == length(snapshot.children_copy) || throw(
            ArgumentError(
                "nested Parquet struct view field count changed during dictionary iteration"))
    _nestedwritearrayequal(value.names, snapshot.names_copy) || throw(
        ArgumentError(
            "nested Parquet struct view names changed during dictionary iteration"))
    _nestedwritevectoridentities(value.children,
        snapshot.children_copy) || throw(ArgumentError(
            "nested Parquet struct view child identity or order changed during dictionary iteration"))
    return
end

function _nestedwritedictdependencylocal(
        snapshot::_NestedWriteVectorSnapshot)
    _nestedwritevalidatenodelocal(snapshot)
    return
end

function _nestedwritedictdependencylocal(
        snapshot::_NestedWriteStructViewSnapshot)
    _nestedwritedictviewlocal(snapshot)
    return
end

function _nestedwritedictdependencyfull(
        snapshot::_NestedWriteVectorSnapshot)
    _nestedwritevalidatenode(snapshot)
    return
end

function _nestedwritedictdependencyfull(
        snapshot::_NestedWriteStructViewSnapshot)
    _nestedwritedictviewlocal(snapshot)
    _validatestructvalue(snapshot.source)
    _nestedwritedictviewlocal(snapshot)
    return
end

function _nestedwritedictwork(trace, value, shape, depth::Int, source::Bool,
        next)
    trace isa _NestedWriteTrace &&
        _nestedwritetracereserve!(trace, _MATERIALIZED_OBJECT_BYTES)
    try
        return _NestedWriteDictWork(value, shape, depth, source, next)
    catch
        trace isa _NestedWriteTrace &&
            _nestedwritetraceunreserve!(trace, _MATERIALIZED_OBJECT_BYTES)
        rethrow()
    end
end

function _nestedwritedictworkfree!(trace)
    trace isa _NestedWriteTrace &&
        _nestedwritetraceunreserve!(trace, _MATERIALIZED_OBJECT_BYTES)
    return
end

function _nestedwritedictworkcleanup!(trace, work)
    current = work
    while current !== nothing
        following = current.next
        current.value = nothing
        current.next = nothing
        _nestedwritedictworkfree!(trace)
        current = following
    end
    return
end

function _nestedwritedictsnapshot!(trace::_NestedWriteTrace,
        source::AbstractVector, limits::Limits)
    topology = trace.topology
    topology isa _NestedWriteTopologySnapshot || throw(AssertionError(
        "nested Parquet trace has no topology authority"))
    snapshot = if trace.capturing
        _nestedwriteextendtopology!(topology, source, limits, trace.budget)
    else
        stored = _nestedwritesnapshotlookupget(topology.lookup, source)
        stored === nothing && throw(ArgumentError(
            "nested Parquet dictionary changed a source identity between writer passes"))
        stored
    end
    _nestedwritevalidatenodelocal(snapshot)
    return snapshot
end

function _nestedwritedictsnapshotlocal!(trace::_NestedWriteTrace,
        source::AbstractVector)
    topology = trace.topology
    topology isa _NestedWriteTopologySnapshot || throw(AssertionError(
        "nested Parquet trace has no topology authority"))
    snapshot = if trace.capturing
        _nestedwriteextendtopologylocal!(topology, source, trace.budget)
    else
        stored = _nestedwritesnapshotlookupget(topology.lookup, source)
        stored === nothing && throw(ArgumentError(
            "nested Parquet dictionary changed a source identity between writer passes"))
        stored
    end
    _nestedwritevalidatenodelocal(snapshot)
    return snapshot
end

function _nestedwritedictpushsource!(trace,
        materialization::_NestedWriteDictMaterialization, work, value,
        shape, depth::Int, limits::Limits)
    _nestedwritedictidentitycontains(materialization, value) && return work
    snapshot = _nestedwritedictsnapshot!(trace, value, limits)
    _nestedwritedictdependency!(trace, materialization, snapshot)
    if snapshot.kind in (_NESTED_WRITE_VECTOR_LOGICAL,
            _NESTED_WRITE_VECTOR_FIXED)
        childdepth = _nestedwritekeynextdepth(depth, limits)
        work = _nestedwritedictwork(trace, snapshot.primary, shape,
            childdepth, true, work)
    elseif snapshot.kind == _NESTED_WRITE_VECTOR_LIST
        childdepth = _nestedwritekeynextdepth(depth, limits)
        childshape = shape isa _NestedWriteListShape ? shape.element : nothing
        work = _nestedwritedictwork(trace, snapshot.tertiary, childshape,
            childdepth, true, work)
    elseif snapshot.kind == _NESTED_WRITE_VECTOR_STRUCT &&
            !isempty(snapshot.copy3)
        childdepth = _nestedwritekeynextdepth(depth, limits)
        children = snapshot.copy3
        for index in length(children):-1:1
            childshape = shape isa _NestedWriteStructShape &&
                index <= length(shape.children) ? shape.children[index] :
                nothing
            work = _nestedwritedictwork(trace, children[index], childshape,
                childdepth, true, work)
        end
    elseif snapshot.kind == _NESTED_WRITE_VECTOR_MAP
        childdepth = _nestedwritekeynextdepth(depth, limits)
        valueshape = shape isa _NestedWriteMapShape ? shape.value : nothing
        snapshot.quaternary === nothing || (work = _nestedwritedictwork(
            trace, snapshot.quaternary, valueshape, childdepth, true, work))
        keyshape = shape isa _NestedWriteMapShape ? shape.key : nothing
        work = _nestedwritedictwork(trace, snapshot.tertiary, keyshape,
            childdepth, true, work)
    end
    return work
end

function _nestedwritedictpushsourcelocal!(trace,
        materialization::_NestedWriteDictMaterialization, work, value,
        shape, depth::Int, limits::Limits)
    _nestedwritedictidentitycontains(materialization, value) && return work
    snapshot = _nestedwritedictsnapshotlocal!(trace, value)
    snapshot === nothing && return work
    _nestedwritedictdependency!(trace, materialization, snapshot)
    if snapshot.kind in (_NESTED_WRITE_VECTOR_LOGICAL,
            _NESTED_WRITE_VECTOR_FIXED)
        childdepth = _nestedwritekeynextdepth(depth, limits)
        work = _nestedwritedictwork(trace, snapshot.primary, shape,
            childdepth, true, work)
    elseif snapshot.kind == _NESTED_WRITE_VECTOR_LIST
        childdepth = _nestedwritekeynextdepth(depth, limits)
        childshape = shape isa _NestedWriteListShape ? shape.element : nothing
        work = _nestedwritedictwork(trace, snapshot.tertiary, childshape,
            childdepth, true, work)
    elseif snapshot.kind == _NESTED_WRITE_VECTOR_STRUCT &&
            !isempty(snapshot.copy3)
        childdepth = _nestedwritekeynextdepth(depth, limits)
        children = snapshot.copy3
        for index in length(children):-1:1
            childshape = shape isa _NestedWriteStructShape &&
                index <= length(shape.children) ? shape.children[index] :
                nothing
            work = _nestedwritedictwork(trace, children[index], childshape,
                childdepth, true, work)
        end
    elseif snapshot.kind == _NESTED_WRITE_VECTOR_MAP
        childdepth = _nestedwritekeynextdepth(depth, limits)
        valueshape = shape isa _NestedWriteMapShape ? shape.value : nothing
        snapshot.quaternary === nothing || (work = _nestedwritedictwork(
            trace, snapshot.quaternary, valueshape, childdepth, true, work))
        keyshape = shape isa _NestedWriteMapShape ? shape.key : nothing
        work = _nestedwritedictwork(trace, snapshot.tertiary, keyshape,
            childdepth, true, work)
    end
    return work
end

function _nestedwritedictcapturelocal!(trace,
        materialization::_NestedWriteDictMaterialization, value, shape,
        limits::Limits, depth::Int)
    trace isa _NestedWriteTrace || return
    work = _nestedwritedictwork(trace, value, shape, depth, false, nothing)
    try
        while work !== nothing
            current = work
            work = current.next
            item = current.value
            itemshape = current.shape
            itemdepth = current.depth
            source = current.source
            current.value = nothing
            current.next = nothing
            _nestedwritedictworkfree!(trace)
            _checklimit(:metadata_depth, itemdepth,
                limits.max_metadata_depth)
            if item isa ListValue
                _nestedwritedictlistviewlocal(item)
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                childshape = itemshape isa _NestedWriteListShape ?
                    itemshape.element : nothing
                work = _nestedwritedictwork(trace, item.values, childshape,
                    childdepth, true, work)
            elseif item isa StructValue
                _nestedwritedictstructview!(trace, materialization, item,
                    itemshape)
                if !isempty(item.children)
                    childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                    for index in length(item.children):-1:1
                        childshape = itemshape isa _NestedWriteStructShape &&
                            index <= length(itemshape.children) ?
                            itemshape.children[index] : nothing
                        work = _nestedwritedictwork(trace,
                            item.children[index], childshape, childdepth,
                            true, work)
                    end
                end
            elseif item isa MapValue
                _nestedwritedictmapviewlocal(item)
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                valueshape = itemshape isa _NestedWriteMapShape ?
                    itemshape.value : nothing
                item.values === nothing || (work = _nestedwritedictwork(
                    trace, item.values, valueshape, childdepth, true, work))
                keyshape = itemshape isa _NestedWriteMapShape ?
                    itemshape.key : nothing
                work = _nestedwritedictwork(trace, item.keys, keyshape,
                    childdepth, true, work)
            elseif item isa AbstractVector &&
                    (source || _nestedwriteispackagevector(item))
                work = _nestedwritedictpushsourcelocal!(trace,
                    materialization, work, item, itemshape, itemdepth, limits)
            elseif item isa Pair && itemshape === nothing
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                work = _nestedwritedictwork(trace, item.second, nothing,
                    childdepth, false, work)
                work = _nestedwritedictwork(trace, item.first, nothing,
                    childdepth, false, work)
            elseif item isa Union{Tuple,NamedTuple} &&
                    itemshape isa Union{Nothing,_NestedWriteStructShape} &&
                    !isempty(item)
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                for index in length(item):-1:1
                    childshape = itemshape isa _NestedWriteStructShape &&
                        index <= length(itemshape.children) ?
                        itemshape.children[index] : nothing
                    work = _nestedwritedictwork(trace, getfield(item, index),
                        childshape, childdepth, false, work)
                end
            elseif itemshape isa _NestedWriteStructShape &&
                    !ismissing(item) && fieldcount(typeof(item)) > 0
                count = min(fieldcount(typeof(item)),
                    length(itemshape.children))
                childdepth = _nestedwritekeynextdepth(itemdepth, limits)
                for index in count:-1:1
                    work = _nestedwritedictwork(trace, getfield(item, index),
                        itemshape.children[index], childdepth, false, work)
                end
            end
        end
    finally
        _nestedwritedictworkcleanup!(trace, work)
    end
    return
end

function _nestedwritedictdiscover!(trace,
        materialization::_NestedWriteDictMaterialization, value, shape,
        limits::Limits, depth::Int)
    trace isa _NestedWriteTrace || return
    work = _nestedwritedictwork(trace, value, shape, depth, false, nothing)
    try
        while work !== nothing
            current = work
            work = current.next
            item = current.value
            itemshape = current.shape
            depth = current.depth
            source = current.source
            current.value = nothing
            current.next = nothing
            _nestedwritedictworkfree!(trace)
            _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
            if item isa ListValue
                _nestedwritedictlistviewauthority(trace, materialization,
                    item)
            elseif item isa StructValue
                _nestedwritedictpreflightlocal(item, itemshape)
            elseif item isa MapValue
                _nestedwritedictmapviewauthority(trace, materialization,
                    item)
            elseif item isa AbstractVector &&
                    (source || _nestedwriteispackagevector(item))
                work = _nestedwritedictpushsource!(trace, materialization,
                    work, item, itemshape, depth, limits)
            elseif item isa Pair && itemshape === nothing
                childdepth = _nestedwritekeynextdepth(depth, limits)
                work = _nestedwritedictwork(trace, item.second, nothing,
                    childdepth, false, work)
                work = _nestedwritedictwork(trace, item.first, nothing,
                    childdepth, false, work)
            elseif item isa Union{Tuple,NamedTuple} &&
                    itemshape isa Union{Nothing,_NestedWriteStructShape}
                childdepth = _nestedwritekeynextdepth(depth, limits)
                for index in length(item):-1:1
                    childshape = itemshape isa _NestedWriteStructShape &&
                        index <= length(itemshape.children) ?
                        itemshape.children[index] : nothing
                    work = _nestedwritedictwork(trace, getfield(item, index),
                        childshape, childdepth, false, work)
                end
            elseif itemshape isa _NestedWriteStructShape &&
                    !ismissing(item)
                count = min(fieldcount(typeof(item)),
                    length(itemshape.children))
                childdepth = _nestedwritekeynextdepth(depth, limits)
                for index in count:-1:1
                    work = _nestedwritedictwork(trace, getfield(item, index),
                        itemshape.children[index], childdepth, false, work)
                end
            end
        end
    finally
        _nestedwritedictworkcleanup!(trace, work)
    end
    return
end

function _nestedwritedictvalidatelocal!(
        materialization::_NestedWriteDictMaterialization)
    dependency = materialization.dependencies
    while dependency !== nothing
        _nestedwritedictdependencylocal(dependency.snapshot)
        dependency = dependency.next
    end
    return
end

function _nestedwritedictvalidate!(
        materialization::_NestedWriteDictMaterialization)
    _nestedwritedictvalidatelocal!(materialization)
    dependency = materialization.dependencies
    while dependency !== nothing
        _nestedwritedictdependencyfull(dependency.snapshot)
        dependency = dependency.next
    end
    _nestedwritedictvalidatelocal!(materialization)
    return
end

function _nestedwritedictmaterialize(trace, value::AbstractDict, keyshape,
        valueshape, limits::Limits, depth::Int=1)
    materialization = _nestedwritedictmaterialization(trace)
    try
        result = iterate(value)
        while result !== nothing
            result isa Tuple && length(result) == 2 || throw(ArgumentError(
                "Parquet MAP dictionary returned an invalid iteration result"))
            pair = result[1]
            pair isa Pair || throw(ArgumentError(
                "Parquet MAP dictionary must iterate Pair values"))
            ismissing(pair.first) && throw(ArgumentError(
                "Parquet MAP dictionary contains a missing key"))
            _nestedwritekeydeclared(keyshape, pair.first)
            _nestedwritekeydeclared(valueshape, pair.second)
            state = result[2]
            count = Base.checked_add(materialization.count, 1)
            _checklimit(:container_elements, count,
                limits.max_container_elements)
            _nestedwritedictpreflightgraph!(trace, materialization,
                pair.first, keyshape, limits, depth)
            _nestedwritedictpreflightgraph!(trace, materialization,
                pair.second, valueshape, limits, depth)
            _nestedwritedictentry!(trace, materialization, pair)
            materialization.count = count
            _nestedwritedictcapturelocal!(trace, materialization, pair.first,
                keyshape, limits, depth)
            _nestedwritedictcapturelocal!(trace, materialization, pair.second,
                valueshape, limits, depth)
            result = iterate(value, state)
        end
        _nestedwritedictpreflightrelease!(trace, materialization)
        _nestedwritedictvalidatelocal!(materialization)
        entry = materialization.first
        while entry !== nothing
            _nestedwritedictdiscover!(trace, materialization, entry.key,
                keyshape, limits, depth)
            _nestedwritedictdiscover!(trace, materialization, entry.value,
                valueshape, limits, depth)
            entry = entry.next
        end
        _nestedwritedictvalidate!(materialization)
        return materialization
    catch
        _nestedwritedictrelease!(trace, materialization)
        rethrow()
    end
end

function _nestedwriteinputrawname(column::Pair)
    return first(column)
end

function _nestedwriteinputrawname(column)
    hasproperty(column, :name) || throw(ArgumentError(
        "nested writer input columns need name and values fields"))
    return getproperty(column, :name)
end

function _nestedwritevalidateinput(snapshot::_NestedWriteTopologySnapshot,
        budget::_LiveByteBudget)
    snapshot.input_columns === nothing && return
    start = _budgetused(budget)
    try
        columns = snapshot.input_columns
        length(columns) == snapshot.input_count || throw(ArgumentError(
            "nested Parquet top-level column count changed between writer phases"))
        firstindex(columns) == snapshot.input_first &&
            lastindex(columns) == snapshot.input_last || throw(ArgumentError(
                "nested Parquet top-level column axes changed between writer phases"))
        position = 0
        for column in columns
            position += 1
            position <= length(snapshot.names) || throw(ArgumentError(
                "nested Parquet top-level columns changed between writer phases"))
            raw = _nestedwriteinputrawname(column)
            bytes = _writecolumnnamebytes(raw)
            temporary = _materializedsum(_MATERIALIZED_OBJECT_BYTES, bytes)
            _reserve!(budget, temporary)
            matches = _writecolumnnameequal(raw, snapshot.names[position])
            _release!(budget, temporary)
            matches || throw(ArgumentError(
                "nested Parquet top-level column name or order changed between writer phases"))
            _nestedwriteinputvalues(column) === snapshot.values[position] ||
                throw(ArgumentError(
                    "nested Parquet top-level column identity changed between writer phases"))
        end
        position == length(snapshot.names) || throw(ArgumentError(
            "nested Parquet top-level columns changed between writer phases"))
        return
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _nestedwritebarrier!(snapshot::_NestedWriteTopologySnapshot,
        sourcevalidator, budget::_LiveByteBudget)
    _validatewriteinput(sourcevalidator, budget)
    _nestedwritevalidateinput(snapshot, budget)
    for node in snapshot.nodes
        _nestedwritevalidatenode(node)
    end
    return
end

function _nestedwritetopologyrelease!(snapshot::_NestedWriteTopologySnapshot,
        budget::_LiveByteBudget)
    iszero(snapshot.charge) || _release!(budget, snapshot.charge)
    return
end

struct _NestedWriteCountContext
    counts::Vector{_NestedWriteLeafCount}
    limits::Limits
    semantic::Union{Nothing,_NestedSchemaPlan}
    entry_offsets::Union{Nothing,Vector{Vector{Int64}}}
    dense_offsets::Union{Nothing,Vector{Vector{Int64}}}
    payload_offsets::Union{Nothing,Vector{Vector{Int64}}}
    trace::Union{Nothing,_NestedWriteTrace}
end

function _NestedWriteCountContext(counts::Vector{_NestedWriteLeafCount},
        limits::Limits)
    return _NestedWriteCountContext(counts, limits, nothing, nothing, nothing,
        nothing, nothing)
end

struct _NestedWriteEmitContext
    builders::Vector{_NestedWriteLeafBuilder}
    counts::Vector{_NestedWriteLeafCount}
    limits::Limits
    boundaries::Union{Nothing,_NestedWriteCountContext}
    trace::Union{Nothing,_NestedWriteTrace}
end

function _NestedWriteEmitContext(builders::Vector{_NestedWriteLeafBuilder},
        counts::Vector{_NestedWriteLeafCount}, limits::Limits)
    return _NestedWriteEmitContext(builders, counts, limits, nothing, nothing)
end

function _nestedwriteinputname(column::Pair)
    return String(first(column))
end

function _nestedwriteinputname(column)
    hasproperty(column, :name) || throw(ArgumentError(
        "nested writer input columns need name and values fields"))
    return String(getproperty(column, :name))
end

function _nestedwriteinputvalues(column::Pair)
    return last(column)
end

function _nestedwriteinputvalues(column)
    hasproperty(column, :values) || throw(ArgumentError(
        "nested writer input columns need name and values fields"))
    return getproperty(column, :values)
end

function _nestedwritesplittype(declared::Type; key::Bool=false)
    declared === Any && throw(ArgumentError(
        "nested Parquet writer types cannot be Any"))
    declared === Union{} && throw(ArgumentError(
        "nested Parquet writer types cannot be Union{}"))
    members = Base.uniontypes(declared)
    if length(members) == 1
        value_type = only(members)
        if value_type === Missing
            key && throw(ArgumentError("Parquet MAP key types cannot include Missing"))
            return Missing, true
        end
        value_type === Nothing && throw(ArgumentError(
            "nothing is not a Parquet null value; use missing"))
        return value_type, false
    end
    length(members) == 2 && Missing in members || throw(ArgumentError(
        "nested Parquet writer types must be T or Union{Missing,T}, got $declared"))
    key && throw(ArgumentError("Parquet MAP key types cannot include Missing"))
    value_type = members[1] === Missing ? members[2] : members[1]
    value_type === Nothing && throw(ArgumentError(
        "nothing is not a Parquet null value; use missing"))
    return value_type, true
end

function _nestedwriteisfixedtuple(::Type{T}) where {T}
    isconcretetype(T) || return false
    T <: Tuple || return false
    count = fieldcount(T)
    count > 0 || return false
    for field in fieldtypes(T)
        field === UInt8 || return false
    end
    return true
end

function _nestedwriteisscalar(value_type::Type)
    value_type === Missing && return true
    value_type in (Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32,
        UInt64, Float16, Float32, Float64, Dates.Date, Dates.Time,
        Dates.DateTime, Decimal, UUIDs.UUID, JSONValue, BSONValue, Interval) &&
        return true
    value_type <: Timestamp && return true
    value_type <: AbstractString && return true
    value_type <: AbstractVector{UInt8} && return true
    return _nestedwriteisfixedtuple(value_type)
end

function _nestedwriteleafshape(name::String, value_type::Type, optional::Bool,
        budget::_LiveByteBudget; explicit=nothing, fixed_width=nothing,
        source_snapshot=nothing)
    _reserveobjects!(budget, 2)
    aggregate = _NestedWriteLeafAggregate(false, nothing, nothing, Int32(1))
    return _NestedWriteLeafShape(name, value_type, optional, explicit,
        fixed_width, aggregate, source_snapshot)
end

function _nestedwriteshapesnapshot(source,
        topology::Union{Nothing,_NestedWriteTopologySnapshot})
    source isa AbstractVector && topology !== nothing || return nothing
    return _nestedwritesnapshotfor(topology, source)
end

function _nestedwritecheckconcrete(value_type::Type, label::AbstractString)
    isconcretetype(value_type) && return
    throw(ArgumentError("$label must use a concrete declared type, got $value_type"))
end

function _nestedwriteshapestart(name::String, declared::Type, source,
        limits::Limits, budget::_LiveByteBudget, key::Bool, depth::Int,
        topology::Union{Nothing,_NestedWriteTopologySnapshot})
    Base.@nospecialize declared source
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    value_type, optional = _nestedwritesplittype(declared; key=key)
    if source isa LogicalColumn
        key && optional && throw(ArgumentError(
            "Parquet MAP key types cannot include Missing"))
        return _nestedwriteleafshape(name, value_type, optional, budget;
            explicit=source.spec,
            source_snapshot=_nestedwriteshapesnapshot(source, topology))
    elseif source isa FixedByteArrayVector
        source.width > 0 || throw(ArgumentError(
            "fixed byte-array width must be positive"))
        return _nestedwriteleafshape(name, value_type, optional, budget;
            fixed_width=source.width,
            source_snapshot=_nestedwriteshapesnapshot(source, topology))
    elseif source isa StructVector
        isempty(source.names) && throw(ArgumentError(
            "zero-field structs cannot be written to Parquet"))
        packageoptional = source.ranks !== nothing
        packageoptional == optional || throw(ArgumentError(
            "StructVector validity does not match its declared element type"))
        _checklimit(:container_elements, length(source.children),
            limits.max_container_elements)
        _reservearray!(budget, String, length(source.names))
        _reservearray!(budget, _NestedWriteShape, length(source.children))
        _reservearray!(budget, AbstractVector, length(source.children))
        names = copy(source.names)
        sourcechildren = copy(source.children)
        children = _NestedWriteShape[]
        sizehint!(children, length(source.children))
        return _NestedWriteShapeFrame(_NESTED_WRITE_SHAPE_STRUCT, name,
            StructValue, optional, source, depth, names, sourcechildren,
            nothing, nothing, nothing, children, nothing, nothing, 0, true,
            true)
    elseif source isa ListVector
        packageoptional = source.validity !== nothing
        packageoptional == optional || throw(ArgumentError(
            "ListVector validity does not match its declared element type"))
        return _NestedWriteShapeFrame(_NESTED_WRITE_SHAPE_LIST, name,
            value_type, optional, source, depth, nothing, nothing,
            eltype(source.values), source.values, nothing, nothing, nothing,
            nothing, 0, true, true)
    elseif source isa MapVector
        packageoptional = source.validity !== nothing
        packageoptional == optional || throw(ArgumentError(
            "MapVector validity does not match its declared element type"))
        values = source.values
        childtypes = (eltype(source.keys),
            values === nothing ? Missing : eltype(values))
        return _NestedWriteShapeFrame(_NESTED_WRITE_SHAPE_MAP, name,
            value_type, optional, source, depth, nothing, nothing, childtypes,
            source.keys, values, nothing, nothing, nothing, 0, true,
            values !== nothing)
    elseif value_type <: ListValue
        _nestedwritecheckconcrete(value_type, "Parquet list view type")
        elementtype = eltype(value_type)
        elementtype === Any && throw(ArgumentError(
            "Parquet list element types cannot be Any"))
        return _NestedWriteShapeFrame(_NESTED_WRITE_SHAPE_LIST, name,
            value_type, optional, source, depth, nothing, nothing, elementtype,
            nothing, nothing, nothing, nothing, nothing, 0, true, true)
    end
    if _nestedwriteisscalar(value_type)
        value_type === Missing || _nestedwritecheckconcrete(value_type,
            "Parquet scalar type")
        snapshot = _nestedwriteshapesnapshot(source, topology)
        return _nestedwriteleafshape(name, value_type, optional, budget;
            source_snapshot=snapshot)
    elseif value_type <: StructValue
        throw(ArgumentError(
            "StructValue needs its owning StructVector to declare field names and types"))
    elseif value_type <: NamedTuple
        _nestedwritecheckconcrete(value_type, "Parquet struct type")
        count = fieldcount(value_type)
        iszero(count) && throw(ArgumentError(
            "zero-field structs cannot be written to Parquet"))
        _checklimit(:container_elements, count,
            limits.max_container_elements)
        _reservearray!(budget, String, count)
        names = String[String(field) for field in fieldnames(value_type)]
        types = fieldtypes(value_type)
        _reservearray!(budget, _NestedWriteShape, length(names))
        children = _NestedWriteShape[]
        sizehint!(children, length(names))
        return _NestedWriteShapeFrame(_NESTED_WRITE_SHAPE_STRUCT, name,
            value_type, optional, source, depth, names, nothing, types, nothing,
            nothing, children, nothing, nothing, 0, false, true)
    elseif value_type <: MapValue
        _nestedwritecheckconcrete(value_type, "Parquet map view type")
        pairtype = eltype(value_type)
        pairtype <: Pair || throw(ArgumentError(
            "MapValue has no concrete key and value types"))
        mapkeytype = pairtype.parameters[1]
        mapvaluetype = pairtype.parameters[2]
        hasvalues = value_type.parameters[3]
        return _NestedWriteShapeFrame(_NESTED_WRITE_SHAPE_MAP, name,
            value_type, optional, source, depth, nothing, nothing,
            (mapkeytype, mapvaluetype), nothing, nothing, nothing, nothing,
            nothing, 0, true, hasvalues === true)
    elseif value_type <: AbstractDict
        _nestedwritecheckconcrete(value_type, "Parquet map type")
        return _NestedWriteShapeFrame(_NESTED_WRITE_SHAPE_MAP, name,
            value_type, optional, source, depth, nothing, nothing,
            (Base.keytype(value_type), Base.valtype(value_type)), nothing,
            nothing, nothing, nothing, nothing, 0, false, true)
    elseif value_type <: AbstractVector
        _nestedwritecheckconcrete(value_type, "Parquet list type")
        elementtype = eltype(value_type)
        elementtype === Any && throw(ArgumentError(
            "Parquet list element types cannot be Any"))
        return _NestedWriteShapeFrame(_NESTED_WRITE_SHAPE_LIST, name,
            value_type, optional, source, depth, nothing, nothing, elementtype,
            nothing, nothing, nothing, nothing, nothing, 0, false, true)
    end
    throw(ArgumentError("unsupported nested Parquet writer type $value_type"))
end

function _nestedwriteshapeframeaccept(frame::_NestedWriteShapeFrame,
        child::_NestedWriteShape)
    children = frame.children
    first = frame.first
    second = frame.second
    if frame.kind == _NESTED_WRITE_SHAPE_STRUCT
        push!(something(children), child)
    elseif frame.position == 0
        first = child
    else
        second = child
    end
    return _NestedWriteShapeFrame(frame.kind, frame.name, frame.value_type,
        frame.optional, frame.source, frame.depth, frame.names,
        frame.source_children, frame.child_types, frame.first_source,
        frame.second_source, children, first, second, frame.position + 1,
        frame.package_owned, frame.source_has_values)
end

function _nestedwriteshapeframeexpected(frame::_NestedWriteShapeFrame)
    frame.kind == _NESTED_WRITE_SHAPE_STRUCT &&
        return length(something(frame.names))
    frame.kind == _NESTED_WRITE_SHAPE_LIST && return 1
    return 2
end

function _nestedwriteshapeframenext(frame::_NestedWriteShapeFrame,
        limits::Limits)
    position = frame.position + 1
    if frame.kind == _NESTED_WRITE_SHAPE_STRUCT
        names = something(frame.names)
        sourcechildren = frame.source_children
        if sourcechildren === nothing
            return names[position], frame.child_types[position], nothing, false,
                _nestedwritedepthadd(frame.depth, 1, limits)
        end
        child = sourcechildren[position]
        return names[position], eltype(child), child, false,
            _nestedwritedepthadd(frame.depth, 1, limits)
    elseif frame.kind == _NESTED_WRITE_SHAPE_LIST
        return "element", frame.child_types::Type, frame.first_source, false,
            _nestedwritedepthadd(frame.depth, 2, limits)
    end
    types = frame.child_types
    if position == 1
        return "key", types[1], frame.first_source, true,
            _nestedwritedepthadd(frame.depth, 2, limits)
    end
    return "value", types[2], frame.second_source, false,
        _nestedwritedepthadd(frame.depth, 2, limits)
end

function _nestedwriteshapeframefinish(frame::_NestedWriteShapeFrame,
        budget::_LiveByteBudget,
        topology::Union{Nothing,_NestedWriteTopologySnapshot})
    _reserveobjects!(budget)
    snapshot = _nestedwriteshapesnapshot(frame.source, topology)
    if frame.kind == _NESTED_WRITE_SHAPE_STRUCT
        return _NestedWriteStructShape(frame.name, frame.value_type,
            frame.optional, something(frame.names), something(frame.children),
            frame.source_children, snapshot, frame.package_owned)
    elseif frame.kind == _NESTED_WRITE_SHAPE_LIST
        return _NestedWriteListShape(frame.name, frame.value_type,
            frame.optional, something(frame.first), snapshot)
    end
    return _NestedWriteMapShape(frame.name, frame.value_type, frame.optional,
        something(frame.first), something(frame.second), snapshot,
        frame.package_owned, frame.source_has_values)
end

function _nestedwriteshape(name::String, declared::Type, source,
        limits::Limits, budget::_LiveByteBudget; key::Bool=false,
        depth::Int=2,
        topology::Union{Nothing,_NestedWriteTopologySnapshot}=nothing)
    Base.@nospecialize declared source
    stack = _nestedwritepassstackstart(_NestedWriteShapeFrame, budget)
    pending::Union{Nothing,_NestedWriteShape} = nothing
    try
        started = _nestedwriteshapestart(name, declared, source, limits, budget,
            key, depth, topology)
        if started isa _NestedWriteShape
            return started
        end
        _nestedwritestackpush!(stack, started::_NestedWriteShapeFrame, budget)
        while true
            if pending !== nothing
                stack[end] = _nestedwriteshapeframeaccept(stack[end], pending)
                pending = nothing
            end
            frame = stack[end]
            if frame.position == _nestedwriteshapeframeexpected(frame)
                pending = _nestedwriteshapeframefinish(frame, budget, topology)
                _nestedwritestackpop!(stack, budget)
                isempty(stack) && return pending
                continue
            end
            childname, childtype, childsource, childkey, childdepth =
                _nestedwriteshapeframenext(frame, limits)
            started = _nestedwriteshapestart(childname, childtype, childsource,
                limits, budget, childkey, childdepth, topology)
            if started isa _NestedWriteShape
                pending = started
            else
                _nestedwritestackpush!(stack,
                    started::_NestedWriteShapeFrame, budget)
            end
        end
    finally
        _nestedwritepassstackrelease!(stack, budget)
    end
end

function _nestedwritepresent(shape::_NestedWriteShape, value)
    if ismissing(value)
        getfield(shape, :optional) || throw(ArgumentError(
            "required Parquet field $(repr(getfield(shape, :name))) is missing"))
        return false
    end
    value === nothing && throw(ArgumentError(
        "nothing is not a Parquet null value; use missing"))
    return true
end

function _nestedwritecheckvalue(shape::_NestedWriteLeafShape, value)
    shape.value_type === Missing && throw(ArgumentError(
        "UNKNOWN Parquet field $(repr(shape.name)) can contain only missing"))
    value isa shape.value_type || throw(ArgumentError(
        "Parquet field $(repr(shape.name)) contains $(typeof(value)); " *
        "expected $(shape.value_type)"))
    return
end

function _nestedwritecheckcontainer(value::AbstractVector, limits::Limits,
        label::AbstractString)
    _checklimit(:container_elements, _nestedvectorcount(value, label),
        limits.max_container_elements)
    return
end

function _nestedwriteviewaccesscheck(value::AbstractVector, count::Int,
        first::Int, last::Int)
    current = _nestedvectorcount(value, "nested Parquet container")
    currentfirst, currentlast = _nestedvectoraxes(value,
        "nested Parquet container")
    current == count && currentfirst == first && currentlast == last || throw(ArgumentError(
            "nested Parquet container changed length or axes during access"))
    value isa ListValue && _validatelistvalue(value)
    value isa MapValue && _validatemapvalue(value)
    return
end

function _nestedwritelistaccessitem(value::AbstractVector, index::Int)
    return value[index]
end

function _nestedwritelistaccessitem(value::ListValue, index::Int)
    _validatelistvalue(value)
    checkbounds(Bool, value, index) || throw(ArgumentError(
        "nested Parquet list index changed during access"))
    physical = value.first + index - 1
    child = value.values
    checkbounds(Bool, child, physical) || throw(ArgumentError(
        "nested Parquet list child no longer contains its physical index"))
    return child[physical]
end

function _nestedwritelistrowsnapshot!(trace, value,
        shape::_NestedWriteListShape, limits::Limits)
    if value isa ListValue
        return _nestedwriteoccurrencesnapshot!(trace, value.values,
            shape.element, limits)
    elseif _nestedwriteispackagevector(value)
        return _nestedwriteoccurrencesnapshot!(trace, value, shape, limits)
    end
    return nothing
end

function _nestedwritelistrowaccess(value, index::Int, snapshot)
    if value isa ListValue
        physical = value.first + index - 1
        return _nestedwriterowaccess(value.values, physical, snapshot)
    elseif snapshot isa _NestedWriteVectorSnapshot
        return _nestedwriterowaccess(value, index, snapshot)
    end
    return _nestedwritelistaccessitem(value, index), nothing
end

function _nestedwritestructaccesschild(value::StructValue,
        names::Vector{String}, children::Vector{AbstractVector}, count::Int,
        index::Int, expectedname::String,
        expectedchild::Union{Nothing,AbstractVector})
    value.index >= 1 || throw(ArgumentError(
        "nested Parquet struct row index changed during access"))
    value.names === names && length(names) == count || throw(ArgumentError(
        "nested Parquet struct names changed during access"))
    value.children === children && length(children) == count || throw(
        ArgumentError(
            "nested Parquet struct child identity or order changed during access"))
    1 <= index <= count || throw(ArgumentError(
        "nested Parquet struct child index changed during access"))
    names[index] == expectedname || throw(ArgumentError(
        "nested Parquet struct field name or order changed during access"))
    child = children[index]
    expectedchild === nothing || child === expectedchild || throw(ArgumentError(
            "nested Parquet struct child identity or order changed during access"))
    childcount = _nestedvectorcount(child, "nested Parquet struct child")
    childfirst, childlast = _nestedvectoraxes(child,
        "nested Parquet struct child")
    childfirst == 1 && childlast == childcount || throw(ArgumentError(
        "nested Parquet struct child axes changed during access"))
    value.index <= childcount || throw(ArgumentError(
        "nested Parquet struct child no longer contains its row index"))
    return child
end

function _nestedwritescanleaf!(shape::_NestedWriteLeafShape, value, trace,
        row_witness)
    _nestedwriterowcheck(row_witness)
    present = _nestedwritepresent(shape, value)
    _nestedwritetraceleaf!(trace, value, present)
    present || return
    _nestedwritecheckvalue(shape, value)
    shape.explicit === nothing || return
    aggregate = shape.aggregate
    if shape.value_type == Decimal
        decimal = value::Decimal
        if aggregate.scale === nothing
            aggregate.scale = decimal.scale
        elseif aggregate.scale != decimal.scale
            throw(ArgumentError("all values in DECIMAL field $(repr(shape.name)) " *
                "must use the same scale"))
        end
        digits = _decimaldigits(decimal.unscaled)
        digits <= typemax(Int32) || throw(ArgumentError(
            "DECIMAL precision exceeds Int32"))
        aggregate.precision = max(aggregate.precision, Int32(digits),
            decimal.scale)
        aggregate.seen = true
    elseif shape.value_type <: Timestamp
        timestamp = value::Timestamp
        if aggregate.adjusted === nothing
            aggregate.adjusted = timestamp.is_adjusted_to_utc
        elseif aggregate.adjusted != timestamp.is_adjusted_to_utc
            throw(ArgumentError("all values in TIMESTAMP field " *
                "$(repr(shape.name)) must use the same UTC adjustment"))
        end
        aggregate.seen = true
    end
    return
end

function _nestedwritescanenter(shape::_NestedWriteShape, value, row_witness)
    return _NestedWriteScanAction(_NESTED_WRITE_SCAN_ENTER, shape, value,
        row_witness, nothing, nothing, nothing, nothing, nothing, nothing, 0,
        0, 0, 0)
end

function _nestedwritescanpost(shape::_NestedWriteShape, first, second)
    return _NestedWriteScanAction(_NESTED_WRITE_SCAN_POSTCHECK, shape, nothing,
        first, second, nothing, nothing, nothing, nothing, nothing, 0, 0, 0,
        0)
end

function _nestedwritescanstruct!(
        stack::_NestedWritePassStack{_NestedWriteScanAction},
        shape::_NestedWriteStructShape, value, trace, row_witness,
        budget::_LiveByteBudget)
    _nestedwriterowcheck(row_witness)
    present = _nestedwritepresent(shape, value)
    _nestedwritetracestruct!(trace, value, present, length(shape.children))
    present || return
    if shape.package_owned
        value isa StructValue || throw(ArgumentError(
            "Parquet struct field $(repr(shape.name)) requires StructValue rows"))
        value.names == shape.names || throw(ArgumentError(
            "Parquet struct field $(repr(shape.name)) changed its field names"))
        length(value) == length(shape.children) || throw(ArgumentError(
            "Parquet struct field $(repr(shape.name)) changed its field count"))
    else
        value isa shape.value_type || throw(ArgumentError(
            "Parquet struct field $(repr(shape.name)) contains $(typeof(value)); " *
            "expected $(shape.value_type)"))
    end
    _nestedwritestackpush!(stack, _NestedWriteScanAction(
        _NESTED_WRITE_SCAN_STRUCT, shape, value, row_witness, nothing, nothing,
        nothing, nothing, nothing, nothing, 1, length(shape.children), 0, 0),
        budget)
    return
end

function _nestedwritescanlist!(
        stack::_NestedWritePassStack{_NestedWriteScanAction},
        shape::_NestedWriteListShape, value, limits::Limits, trace,
        row_witness, budget::_LiveByteBudget)
    _nestedwriterowcheck(row_witness)
    value isa ListValue && _validatelistvalue(value)
    present = _nestedwritepresent(shape, value)
    _nestedwritetracecontainer!(trace, _NESTED_WRITE_TRACE_LIST, value,
        present)
    present || return
    value isa shape.value_type || throw(ArgumentError(
        "Parquet list field $(repr(shape.name)) contains $(typeof(value)); " *
        "expected $(shape.value_type)"))
    value isa AbstractVector || throw(ArgumentError(
        "Parquet LIST values must be vectors"))
    _nestedwritecheckcontainer(value, limits,
        "Parquet LIST field $(repr(shape.name))")
    count = _nestedvectorcount(value, "Parquet LIST field $(repr(shape.name))")
    first, last = _nestedvectoraxes(value,
        "Parquet LIST field $(repr(shape.name))")
    rowsnapshot = _nestedwritelistrowsnapshot!(trace, value, shape, limits)
    iszero(count) || _nestedwritestackpush!(stack, _NestedWriteScanAction(
        _NESTED_WRITE_SCAN_LIST, shape, value, row_witness, nothing, nothing,
        nothing, nothing, rowsnapshot, nothing, first, count, first, last),
        budget)
    return
end

function _nestedwritemappair(item, name::String)
    item isa Pair || throw(ArgumentError(
        "Parquet MAP field $(repr(name)) must iterate Pair values"))
    return item
end

function _nestedwritescandictrelease!(
        stack::_NestedWritePassStack{_NestedWriteScanAction},
        shape::_NestedWriteMapShape,
        materialization::_NestedWriteDictMaterialization, trace,
        budget::_LiveByteBudget)
    action = _NestedWriteScanAction(_NESTED_WRITE_SCAN_DICT_RELEASE, shape,
        nothing, nothing, nothing, nothing, nothing, materialization, nothing,
        nothing, 0, 0, 0, 0)
    try
        _nestedwritestackpush!(stack, action, budget)
    catch
        _nestedwritedictrelease!(trace, materialization)
        rethrow()
    end
    return
end

function _nestedwritescanmap!(
        stack::_NestedWritePassStack{_NestedWriteScanAction},
        shape::_NestedWriteMapShape, value, limits::Limits, trace, row_witness,
        budget::_LiveByteBudget)
    _nestedwriterowcheck(row_witness)
    value isa MapValue && _validatemapvalue(value)
    present = _nestedwritepresent(shape, value)
    if !present
        _nestedwritetracecontainer!(trace, _NESTED_WRITE_TRACE_MAP, value,
            false)
        return
    end
    value isa shape.value_type || throw(ArgumentError(
        "Parquet map field $(repr(shape.name)) contains $(typeof(value)); " *
        "expected $(shape.value_type)"))
    if value isa AbstractDict
        materialization = _nestedwritedictmaterialize(trace, value,
            shape.key, shape.value, limits)
        _nestedwritescandictrelease!(stack, shape, materialization, trace,
            budget)
        count = materialization.count
        _nestedwritetracedict!(trace, count)
        if !iszero(count)
            _nestedwritestackpush!(stack, _NestedWriteScanAction(
                _NESTED_WRITE_SCAN_MAP_DICT, shape, value, row_witness,
                nothing, materialization.first, nothing, materialization,
                nothing, nothing, 0, count, 0, 0), budget)
        end
        return
    end
    _nestedwritetracecontainer!(trace, _NESTED_WRITE_TRACE_MAP, value, true)
    count = value isa AbstractVector ?
        _nestedvectorcount(value, "Parquet MAP field $(repr(shape.name))") : 0
    _checklimit(:container_elements, count,
        limits.max_container_elements)
    first, last = value isa AbstractVector ?
        _nestedvectoraxes(value, "Parquet MAP field $(repr(shape.name))") :
        (0, 0)
    iszero(count) && return
    if value isa MapValue
        keysnapshot = _nestedwriteoccurrencesnapshot!(trace, value.keys,
            shape.key, limits)
        valuesnapshot = shape.source_has_values ?
            _nestedwriteoccurrencesnapshot!(trace,
                something(value.values), shape.value, limits) : nothing
        _nestedwritestackpush!(stack, _NestedWriteScanAction(
            _NESTED_WRITE_SCAN_MAP_VIEW_KEY, shape, value, row_witness,
            nothing, nothing, nothing, nothing, keysnapshot, valuesnapshot, 1,
            count, first, last), budget)
        return
    end
    result = iterate(value)
    if result !== nothing
        _nestedwritestackpush!(stack, _NestedWriteScanAction(
            _NESTED_WRITE_SCAN_MAP_ITER_KEY, shape, value, row_witness,
            nothing, result, nothing, nothing, nothing, nothing, 1, count,
            first, last), budget)
    end
    return
end

function _nestedwritescanprocess!(
        stack::_NestedWritePassStack{_NestedWriteScanAction},
        action::_NestedWriteScanAction, limits::Limits, trace,
        budget::_LiveByteBudget)
    kind = action.kind
    shape = action.shape
    if kind == _NESTED_WRITE_SCAN_ENTER
        shape isa _NestedWriteLeafShape && return _nestedwritescanleaf!(shape,
            action.value, trace, action.row_witness)
        shape isa _NestedWriteStructShape && return _nestedwritescanstruct!(
            stack, shape, action.value, trace, action.row_witness, budget)
        shape isa _NestedWriteListShape && return _nestedwritescanlist!(stack,
            shape, action.value, limits, trace, action.row_witness, budget)
        return _nestedwritescanmap!(stack, shape::_NestedWriteMapShape,
            action.value, limits, trace, action.row_witness, budget)
    elseif kind == _NESTED_WRITE_SCAN_POSTCHECK
        _nestedwriterowcheck(action.row_witness)
        _nestedwriterowcheck(action.other_witness)
        return
    elseif kind == _NESTED_WRITE_SCAN_KEYASSERT
        _nestedwritekeyassert!(action.expected, action.value, shape, limits,
            trace, nothing, action.row_witness)
        return
    elseif kind == _NESTED_WRITE_SCAN_DICT_RELEASE
        _nestedwritedictrelease!(trace, something(action.materialization))
        return
    elseif kind == _NESTED_WRITE_SCAN_STRUCT
        structshape = shape::_NestedWriteStructShape
        index = action.position
        if structshape.package_owned
            value = action.value::StructValue
            child = _nestedwritestructaccesschild(value, value.names,
                value.children, action.count, index, structshape.names[index],
                something(structshape.source_children)[index])
            snapshot = _nestedwriteoccurrencesnapshot!(trace, child,
                structshape.children[index], limits)
            item, child_witness = _nestedwriterowaccess(child, value.index,
                snapshot)
            if index < action.count
                _nestedwritestackpush!(stack, _NestedWriteScanAction(kind,
                    shape, action.value, action.row_witness, nothing, nothing,
                    nothing, nothing, nothing, nothing, index + 1,
                    action.count, 0, 0), budget)
            end
            _nestedwritestackpush!(stack, _nestedwritescanpost(shape,
                child_witness, action.row_witness), budget)
            _nestedwritestackpush!(stack, _nestedwritescanenter(
                structshape.children[index], item, child_witness), budget)
        else
            item = getfield(action.value, index)
            if index < action.count
                _nestedwritestackpush!(stack, _NestedWriteScanAction(kind,
                    shape, action.value, action.row_witness, nothing, nothing,
                    nothing, nothing, nothing, nothing, index + 1,
                    action.count, 0, 0), budget)
            end
            _nestedwritestackpush!(stack, _nestedwritescanenter(
                structshape.children[index], item, nothing), budget)
        end
        return
    elseif kind == _NESTED_WRITE_SCAN_LIST
        listshape = shape::_NestedWriteListShape
        index = action.position
        _nestedwriterowcheck(action.row_witness)
        _nestedwriteviewaccesscheck(action.value, action.count, action.first,
            action.last)
        item, child_witness = _nestedwritelistrowaccess(action.value, index,
            action.snapshot1)
        _nestedwriteviewaccesscheck(action.value, action.count, action.first,
            action.last)
        index < action.last && _nestedwritestackpush!(stack,
            _NestedWriteScanAction(kind, shape, action.value,
                action.row_witness, nothing, nothing, nothing, nothing,
                action.snapshot1, nothing, index + 1, action.count,
                action.first, action.last), budget)
        _nestedwritestackpush!(stack, _nestedwritescanpost(shape,
            child_witness, action.row_witness), budget)
        _nestedwritestackpush!(stack, _nestedwritescanenter(listshape.element,
            item, child_witness), budget)
        return
    elseif kind == _NESTED_WRITE_SCAN_MAP_DICT
        entry = action.state
        if action.position == 1
            entry = entry.next
            entry === nothing && return
        end
        mapshape = shape::_NestedWriteMapShape
        ismissing(entry.key) && throw(ArgumentError(
            "Parquet MAP field $(repr(mapshape.name)) contains a missing key"))
        expected = _nestedwritetracekey!(trace, entry.key, mapshape.key,
            limits)
        _nestedwritestackpush!(stack, _NestedWriteScanAction(kind, shape,
            action.value, action.row_witness, nothing, entry, nothing,
            action.materialization, nothing, nothing, 1, action.count, 0, 0),
            budget)
        mapvalue = mapshape.source_has_values ? entry.value : missing
        _nestedwritestackpush!(stack, _nestedwritescanenter(mapshape.value,
            mapvalue, nothing), budget)
        _nestedwritestackpush!(stack, _NestedWriteScanAction(
            _NESTED_WRITE_SCAN_KEYASSERT, mapshape.key, entry.key, nothing,
            nothing, nothing, expected, nothing, nothing, nothing, 0, 0, 0,
            0), budget)
        _nestedwritestackpush!(stack, _nestedwritescanenter(mapshape.key,
            entry.key, nothing), budget)
        return
    elseif kind == _NESTED_WRITE_SCAN_MAP_VIEW_KEY
        mapshape = shape::_NestedWriteMapShape
        entry = action.position
        _nestedwriterowcheck(action.row_witness)
        _nestedwriteviewaccesscheck(action.value, action.count, action.first,
            action.last)
        physical = action.value.first + entry - 1
        keyvalue, key_witness = _nestedwriterowaccess(action.value.keys,
            physical, action.snapshot1)
        ismissing(keyvalue) && throw(ArgumentError(
            "Parquet MAP field $(repr(mapshape.name)) contains a missing key"))
        expected = _nestedwritetracekey!(trace, keyvalue, mapshape.key, limits,
            nothing, key_witness)
        _nestedwritestackpush!(stack, _NestedWriteScanAction(
            _NESTED_WRITE_SCAN_MAP_VIEW_VALUE, shape, action.value,
            action.row_witness, key_witness, keyvalue, expected, nothing,
            action.snapshot1, action.snapshot2, physical, action.count,
            action.first, action.last), budget)
        _nestedwritestackpush!(stack, _nestedwritescanenter(mapshape.key,
            keyvalue, key_witness), budget)
        return
    elseif kind == _NESTED_WRITE_SCAN_MAP_VIEW_VALUE
        mapshape = shape::_NestedWriteMapShape
        keyvalue = action.state
        _nestedwritekeyassert!(action.expected, keyvalue, mapshape.key, limits,
            trace, nothing, action.other_witness)
        _nestedwriterowcheck(action.other_witness)
        _nestedwriterowcheck(action.row_witness)
        if mapshape.source_has_values
            mapvalue, value_witness = _nestedwriterowaccess(
                something(action.value.values), action.position,
                action.snapshot2)
        else
            mapvalue = missing
            value_witness = nothing
        end
        entry = action.position - action.value.first + 1
        entry < action.count && _nestedwritestackpush!(stack,
            _NestedWriteScanAction(_NESTED_WRITE_SCAN_MAP_VIEW_KEY, shape,
                action.value, action.row_witness, nothing, nothing, nothing,
                nothing, action.snapshot1, action.snapshot2, entry + 1,
                action.count, action.first, action.last), budget)
        _nestedwritestackpush!(stack, _nestedwritescanpost(shape,
            value_witness, action.row_witness), budget)
        _nestedwritestackpush!(stack, _nestedwritescanenter(mapshape.value,
            mapvalue, value_witness), budget)
        return
    elseif kind == _NESTED_WRITE_SCAN_MAP_ITER_KEY
        mapshape = shape::_NestedWriteMapShape
        _nestedwriteviewaccesscheck(action.value, action.count, action.first,
            action.last)
        result = action.state
        pair = _nestedwritemappair(result[1], mapshape.name)
        ismissing(pair.first) && throw(ArgumentError(
            "Parquet MAP field $(repr(mapshape.name)) contains a missing key"))
        expected = _nestedwritetracekey!(trace, pair.first, mapshape.key,
            limits)
        _nestedwritestackpush!(stack, _NestedWriteScanAction(
            _NESTED_WRITE_SCAN_MAP_ITER_VALUE, shape, action.value,
            action.row_witness, nothing, pair, expected, nothing, result[2],
            nothing, action.position, action.count, action.first, action.last),
            budget)
        _nestedwritestackpush!(stack, _nestedwritescanenter(mapshape.key,
            pair.first, nothing), budget)
        return
    elseif kind == _NESTED_WRITE_SCAN_MAP_ITER_VALUE
        mapshape = shape::_NestedWriteMapShape
        pair = action.state::Pair
        _nestedwritekeyassert!(action.expected, pair.first, mapshape.key,
            limits, trace)
        _nestedwritestackpush!(stack, _NestedWriteScanAction(
            _NESTED_WRITE_SCAN_MAP_ITER_NEXT, shape, action.value,
            action.row_witness, nothing, action.snapshot1, nothing, nothing,
            nothing, nothing, action.position, action.count, action.first,
            action.last), budget)
        mapvalue = mapshape.source_has_values ? pair.second : missing
        _nestedwritestackpush!(stack, _nestedwritescanenter(mapshape.value,
            mapvalue, nothing), budget)
        return
    end
    _nestedwriteviewaccesscheck(action.value, action.count, action.first,
        action.last)
    result = iterate(action.value, action.state)
    result === nothing || _nestedwritestackpush!(stack,
        _NestedWriteScanAction(_NESTED_WRITE_SCAN_MAP_ITER_KEY, shape,
            action.value, action.row_witness, nothing, result, nothing,
            nothing, nothing, nothing, action.position + 1, action.count,
            action.first, action.last), budget)
    return
end

function _nestedwritescancleanup!(
        stack::_NestedWritePassStack{_NestedWriteScanAction}, trace)
    for action in Iterators.reverse(stack.frames)
        if action.kind == _NESTED_WRITE_SCAN_DICT_RELEASE
            _nestedwritedictrelease!(trace,
                something(action.materialization))
        end
    end
    return
end

function _nestedwritescanaggregate!(
        stack::_NestedWritePassStack{_NestedWriteScanAction},
        shape::_NestedWriteShape, value, limits::Limits, trace, row_witness,
        budget::_LiveByteBudget)
    Base.@nospecialize value
    shape isa _NestedWriteLeafShape && return _nestedwritescanleaf!(shape,
        value, trace, row_witness)
    isempty(stack.frames) && !stack.processing || throw(AssertionError(
        "nested writer aggregate scratch stack is not empty"))
    try
        _nestedwritestackpush!(stack,
            _nestedwritescanenter(shape, value, row_witness), budget)
        while !isempty(stack.frames)
            action = _nestedwritepassstackpop!(stack)
            try
                _nestedwritescanprocess!(stack, action, limits, trace, budget)
            finally
                _nestedwritepassstackprocessed!(stack)
            end
        end
    finally
        if !isempty(stack.frames)
            try
                _nestedwritescancleanup!(stack, trace)
            finally
                _nestedwritepassstackclear!(stack)
            end
        end
    end
    isempty(stack.frames) && !stack.processing || throw(AssertionError(
        "nested writer aggregate scratch stack retained actions"))
    return
end

function _nestedwritescanaggregate!(shape::_NestedWriteShape, value,
        limits::Limits, trace, row_witness)
    Base.@nospecialize value
    shape isa _NestedWriteLeafShape && return _nestedwritescanleaf!(shape,
        value, trace, row_witness)
    budget = trace isa _NestedWriteTrace ? trace.budget :
        _LiveByteBudget(limits)
    stack = _nestedwritepassstackstart(_NestedWriteScanAction, budget)
    try
        _nestedwritescanaggregate!(stack, shape, value, limits, trace,
            row_witness, budget)
    finally
        _nestedwritepassstackrelease!(stack, budget)
    end
    return
end

function _nestedwritescanaggregate!(shape::_NestedWriteShape, value,
        limits::Limits, trace)
    return _nestedwritescanaggregate!(shape, value, limits, trace, nothing)
end

function _nestedwritescanaggregate!(shape::_NestedWriteShape, value,
        limits::Limits)
    return _nestedwritescanaggregate!(shape, value, limits, nothing, nothing)
end

function _nestedwriteintegerlogical(name::String, value_type::Type,
        optional::Bool)
    element = _integerwriteelement(name, value_type, optional)
    element === nothing && throw(ArgumentError(
        "unsupported Parquet integer type $value_type"))
    return element
end

function _nestedwritetimestampelement(shape::_NestedWriteLeafShape)
    aggregate = shape.aggregate
    aggregate.seen || throw(ArgumentError(
        "cannot infer TIMESTAMP UTC adjustment for empty or all-null field " *
        "$(repr(shape.name)); use Parquet.LogicalColumn"))
    value_type = shape.value_type
    if value_type == Timestamp{:micros}
        unit = Metadata.TimeUnit(MICROS=Metadata.MicroSeconds())
        converted = Metadata.ConvertedType.TIMESTAMP_MICROS
    elseif value_type == Timestamp{:nanos}
        unit = Metadata.TimeUnit(NANOS=Metadata.NanoSeconds())
        converted = nothing
    else
        throw(ArgumentError("unsupported TIMESTAMP element type $value_type"))
    end
    logical = Metadata.LogicalType(TIMESTAMP=Metadata.TimestampType(
        isAdjustedToUTC=something(aggregate.adjusted), unit=unit))
    return _logicalwriteelement(shape.name, Metadata.Type.INT64, shape.optional;
        logical=logical, converted=converted)
end

function _nestedwritedecimalelement(shape::_NestedWriteLeafShape,
        limits::Limits)
    aggregate = shape.aggregate
    aggregate.seen || throw(ArgumentError(
        "cannot infer DECIMAL precision and scale for empty or all-null field " *
        "$(repr(shape.name)); use Parquet.LogicalColumn"))
    precision = aggregate.precision
    scale = something(aggregate.scale)
    if precision <= 9
        physical = Metadata.Type.INT32
        width = nothing
    elseif precision <= 18
        physical = Metadata.Type.INT64
        width = nothing
    else
        physical = Metadata.Type.FIXED_LEN_BYTE_ARRAY
        width = _decimalwritewidth(precision, limits)
    end
    logical = Metadata.LogicalType(DECIMAL=Metadata.DecimalType(
        scale=scale, precision=precision))
    return _logicalwriteelement(shape.name, physical, shape.optional;
        width=width, logical=logical, converted=Metadata.ConvertedType.DECIMAL,
        scale=scale, precision=precision)
end

function _nestedwriteleafelement(shape::_NestedWriteLeafShape,
        limits::Limits)
    shape.explicit === nothing || return _logicalcolumnwriteelement(shape.name,
        shape.explicit, shape.optional, limits)
    value_type = shape.value_type
    value_type === Missing && return _logicalwriteelement(shape.name,
        Metadata.Type.INT32, true;
        logical=Metadata.LogicalType(UNKNOWN=Metadata.NullType()))
    shape.fixed_width === nothing || return _logicalwriteelement(shape.name,
        Metadata.Type.FIXED_LEN_BYTE_ARRAY, shape.optional;
        width=shape.fixed_width)
    value_type == Dates.Date && return _logicalwriteelement(shape.name,
        Metadata.Type.INT32, shape.optional;
        logical=Metadata.LogicalType(DATE=Metadata.DateType()),
        converted=Metadata.ConvertedType.DATE)
    value_type in (Int8, Int16, UInt8, UInt16, UInt32, UInt64) &&
        return _nestedwriteintegerlogical(shape.name, value_type, shape.optional)
    if value_type == Dates.Time
        unit = Metadata.TimeUnit(NANOS=Metadata.NanoSeconds())
        logical = Metadata.LogicalType(TIME=Metadata.TimeType(
            isAdjustedToUTC=false, unit=unit))
        return _logicalwriteelement(shape.name, Metadata.Type.INT64,
            shape.optional; logical=logical)
    elseif value_type == Dates.DateTime
        unit = Metadata.TimeUnit(MILLIS=Metadata.MilliSeconds())
        logical = Metadata.LogicalType(TIMESTAMP=Metadata.TimestampType(
            isAdjustedToUTC=false, unit=unit))
        return _logicalwriteelement(shape.name, Metadata.Type.INT64,
            shape.optional; logical=logical,
            converted=Metadata.ConvertedType.TIMESTAMP_MILLIS)
    elseif value_type <: Timestamp
        return _nestedwritetimestampelement(shape)
    elseif value_type == Decimal
        return _nestedwritedecimalelement(shape, limits)
    end
    binary = _binarywriteelement(shape.name, value_type, shape.optional)
    binary === nothing || return binary
    physical = _writetype(value_type)
    width = _writetypelength(value_type)
    logical, converted = _stringlogical(value_type)
    return _logicalwriteelement(shape.name, physical, shape.optional;
        width=width, logical=logical, converted=converted)
end

function _nestedwriteschemastart(shape::_NestedWriteShape, limits::Limits,
        budget::_LiveByteBudget)
    if shape isa _NestedWriteLeafShape
        _reservearray!(budget, Metadata.SchemaElement, 1)
        return Metadata.SchemaElement[_nestedwriteleafelement(shape, limits)]
    elseif shape isa _NestedWriteStructShape
        length(shape.children) <= typemax(Int32) || throw(ArgumentError(
            "Parquet struct has more than Int32 children"))
        _reservearray!(budget, Vector{Metadata.SchemaElement},
            length(shape.children))
        fragments = Vector{Vector{Metadata.SchemaElement}}(undef,
            length(shape.children))
        return _NestedWriteSchemaFrame(shape, fragments, nothing, nothing, 0, 1)
    end
    return _NestedWriteSchemaFrame(shape, nothing, nothing, nothing, 0, 0)
end

function _nestedwriteschemaexpected(frame::_NestedWriteSchemaFrame)
    shape = frame.shape
    shape isa _NestedWriteStructShape && return length(shape.children)
    shape isa _NestedWriteListShape && return 1
    return 2
end

function _nestedwriteschemaaccept(frame::_NestedWriteSchemaFrame,
        fragment::Vector{Metadata.SchemaElement}, limits::Limits)
    shape = frame.shape
    first = frame.first
    second = frame.second
    count = frame.count
    if shape isa _NestedWriteStructShape
        something(frame.fragments)[frame.position + 1] = fragment
        count = Base.checked_add(count, length(fragment))
        _checklimit(:container_elements, count,
            limits.max_container_elements)
    elseif frame.position == 0
        first = fragment
        if shape isa _NestedWriteListShape
            count = Base.checked_add(2, length(fragment))
            _checklimit(:container_elements, count,
                limits.max_container_elements)
        end
    else
        second = fragment
        count = Base.checked_add(2,
            Base.checked_add(length(something(first)), length(fragment)))
        _checklimit(:container_elements, count,
            limits.max_container_elements)
    end
    return _NestedWriteSchemaFrame(shape, frame.fragments, first, second,
        frame.position + 1, count)
end

function _nestedwriteschemanext(frame::_NestedWriteSchemaFrame)
    shape = frame.shape
    shape isa _NestedWriteStructShape &&
        return shape.children[frame.position + 1]
    shape isa _NestedWriteListShape && return shape.element
    mapshape = shape::_NestedWriteMapShape
    return frame.position == 0 ? mapshape.key : mapshape.value
end

function _nestedwriteschemafinish(frame::_NestedWriteSchemaFrame,
        budget::_LiveByteBudget)
    shape = frame.shape
    _reservearray!(budget, Metadata.SchemaElement, frame.count)
    repetition = getfield(shape, :optional) ?
        Metadata.FieldRepetitionType.OPTIONAL :
        Metadata.FieldRepetitionType.REQUIRED
    if shape isa _NestedWriteStructShape
        fragments = something(frame.fragments)
        output = Metadata.SchemaElement[Metadata.SchemaElement(
            repetition_type=repetition, name=shape.name,
            num_children=Int32(length(shape.children)))]
        sizehint!(output, frame.count)
        for fragment in fragments
            append!(output, fragment)
            _release!(budget, _materializedarraybytes(Metadata.SchemaElement,
                length(fragment)))
        end
        _release!(budget, _materializedarraybytes(
            Vector{Metadata.SchemaElement}, length(fragments)))
        return output
    elseif shape isa _NestedWriteListShape
        outer = Metadata.SchemaElement(repetition_type=repetition,
            name=shape.name, num_children=Int32(1),
            converted_type=Metadata.ConvertedType.LIST,
            logicalType=Metadata.LogicalType(LIST=Metadata.ListType()))
        repeated = Metadata.SchemaElement(
            repetition_type=Metadata.FieldRepetitionType.REPEATED,
            name="list", num_children=Int32(1))
        element = something(frame.first)
        output = Metadata.SchemaElement[outer, repeated]
        sizehint!(output, frame.count)
        append!(output, element)
        _release!(budget, _materializedarraybytes(Metadata.SchemaElement,
            length(element)))
        return output
    end
    mapshape = shape::_NestedWriteMapShape
    outer = Metadata.SchemaElement(repetition_type=repetition,
        name=mapshape.name, num_children=Int32(1),
        converted_type=Metadata.ConvertedType.MAP,
        logicalType=Metadata.LogicalType(MAP=Metadata.MapType()))
    repeated = Metadata.SchemaElement(
        repetition_type=Metadata.FieldRepetitionType.REPEATED,
        name="key_value", num_children=Int32(2))
    key = something(frame.first)
    value = something(frame.second)
    output = Metadata.SchemaElement[outer, repeated]
    sizehint!(output, frame.count)
    append!(output, key)
    append!(output, value)
    _release!(budget, _materializedarraybytes(Metadata.SchemaElement,
        length(key)))
    _release!(budget, _materializedarraybytes(Metadata.SchemaElement,
        length(value)))
    return output
end

function _nestedwriteschema(shape::_NestedWriteShape, limits::Limits,
        budget::_LiveByteBudget)
    stack = _nestedwritepassstackstart(_NestedWriteSchemaFrame, budget)
    pending::Union{Nothing,Vector{Metadata.SchemaElement}} = nothing
    try
        started = _nestedwriteschemastart(shape, limits, budget)
        started isa Vector{Metadata.SchemaElement} && return started
        _nestedwritestackpush!(stack, started::_NestedWriteSchemaFrame, budget)
        while true
            if pending !== nothing
                stack[end] = _nestedwriteschemaaccept(stack[end], pending,
                    limits)
                pending = nothing
            end
            frame = stack[end]
            if frame.position == _nestedwriteschemaexpected(frame)
                pending = _nestedwriteschemafinish(frame, budget)
                _nestedwritestackpop!(stack, budget)
                isempty(stack) && return pending
                continue
            end
            started = _nestedwriteschemastart(_nestedwriteschemanext(frame),
                limits, budget)
            if started isa Vector{Metadata.SchemaElement}
                pending = started
            else
                _nestedwritestackpush!(stack,
                    started::_NestedWriteSchemaFrame, budget)
            end
        end
    finally
        _nestedwritepassstackrelease!(stack, budget)
    end
end

function _nestedwritebindstart(shape::_NestedWriteShape,
        semantic::_NestedPlan, budget::_LiveByteBudget)
    if shape isa _NestedWriteLeafShape
        semantic isa _NestedLeafPlan || throw(AssertionError(
            "canonical writer leaf did not compile as a semantic leaf"))
        _reserveobjects!(budget)
        return _NestedWriteLeafPlan(semantic, shape)
    elseif shape isa _NestedWriteStructShape
        semantic isa _NestedStructPlan || throw(AssertionError(
            "canonical writer struct did not compile as a semantic struct"))
        length(shape.children) == length(semantic.children) || throw(
            AssertionError(
                "canonical writer struct child count changed during schema compilation"))
        _reservearray!(budget, _NestedWriteNodePlan, length(shape.children))
        children = _NestedWriteNodePlan[]
        sizehint!(children, length(shape.children))
        return _NestedWriteBindFrame(shape, semantic, children, nothing,
            nothing, 0)
    elseif shape isa _NestedWriteListShape
        semantic isa _NestedListPlan || throw(AssertionError(
            "canonical writer LIST did not compile as a semantic list"))
        semantic.annotation == :modern_list || throw(AssertionError(
            "canonical writer LIST lost its modern annotation"))
        semantic.entry.element.name == "list" || throw(AssertionError(
            "canonical writer LIST has a noncanonical repeated wrapper"))
        return _NestedWriteBindFrame(shape, semantic, nothing, nothing,
            nothing, 0)
    end
    mapshape = shape::_NestedWriteMapShape
    semantic isa _NestedMapPlan || throw(AssertionError(
        "canonical writer MAP did not compile as a semantic map"))
    semantic.annotation == :modern_map || throw(AssertionError(
        "canonical writer MAP lost its modern annotation"))
    semantic.entry.element.name == "key_value" || throw(AssertionError(
        "canonical writer MAP has a noncanonical repeated entry"))
    semantic.optional_key && throw(AssertionError(
        "canonical writer MAP compiled an optional key"))
    semantic.value === nothing && throw(AssertionError(
        "canonical writer MAP omitted its value field"))
    return _NestedWriteBindFrame(mapshape, semantic, nothing, nothing,
        nothing, 0)
end

function _nestedwritebindexpected(frame::_NestedWriteBindFrame)
    shape = frame.shape
    shape isa _NestedWriteStructShape && return length(shape.children)
    shape isa _NestedWriteListShape && return 1
    return 2
end

function _nestedwritebindaccept(frame::_NestedWriteBindFrame,
        plan::_NestedWriteNodePlan)
    first = frame.first
    second = frame.second
    if frame.shape isa _NestedWriteStructShape
        push!(something(frame.children), plan)
    elseif frame.position == 0
        first = plan
    else
        second = plan
    end
    return _NestedWriteBindFrame(frame.shape, frame.semantic, frame.children,
        first, second, frame.position + 1)
end

function _nestedwritebindnext(frame::_NestedWriteBindFrame)
    shape = frame.shape
    semantic = frame.semantic
    if shape isa _NestedWriteStructShape
        structsemantic = semantic::_NestedStructPlan
        index = frame.position + 1
        return shape.children[index], structsemantic.children[index]
    elseif shape isa _NestedWriteListShape
        return shape.element, (semantic::_NestedListPlan).element
    end
    mapshape = shape::_NestedWriteMapShape
    mapsemantic = semantic::_NestedMapPlan
    frame.position == 0 && return mapshape.key, mapsemantic.key
    return mapshape.value, something(mapsemantic.value)
end

function _nestedwritebindfinish(frame::_NestedWriteBindFrame,
        budget::_LiveByteBudget)
    _reserveobjects!(budget)
    shape = frame.shape
    semantic = frame.semantic
    shape isa _NestedWriteStructShape && return _NestedWriteStructPlan(
        semantic::_NestedStructPlan, shape, something(frame.children))
    shape isa _NestedWriteListShape && return _NestedWriteListPlan(
        semantic::_NestedListPlan, shape, something(frame.first))
    return _NestedWriteMapPlan(semantic::_NestedMapPlan,
        shape::_NestedWriteMapShape, something(frame.first),
        something(frame.second))
end

function _nestedwritebind(shape::_NestedWriteShape,
        semantic::_NestedPlan, budget::_LiveByteBudget)
    stack = _nestedwritepassstackstart(_NestedWriteBindFrame, budget)
    pending::Union{Nothing,_NestedWriteNodePlan} = nothing
    try
        started = _nestedwritebindstart(shape, semantic, budget)
        started isa _NestedWriteNodePlan && return started
        _nestedwritestackpush!(stack, started::_NestedWriteBindFrame, budget)
        while true
            if pending !== nothing
                stack[end] = _nestedwritebindaccept(stack[end], pending)
                pending = nothing
            end
            frame = stack[end]
            if frame.position == _nestedwritebindexpected(frame)
                pending = _nestedwritebindfinish(frame, budget)
                _nestedwritestackpop!(stack, budget)
                isempty(stack) && return pending
                continue
            end
            childshape, childsemantic = _nestedwritebindnext(frame)
            started = _nestedwritebindstart(childshape, childsemantic, budget)
            if started isa _NestedWriteNodePlan
                pending = started
            else
                _nestedwritestackpush!(stack,
                    started::_NestedWriteBindFrame, budget)
            end
        end
    finally
        _nestedwritepassstackrelease!(stack, budget)
    end
end

function _nestedwritefixedpayload(element::Metadata.SchemaElement, value,
        limits::Limits)
    width = element.type_length
    width === nothing && throw(ArgumentError(
        "fixed byte-array field $(repr(element.name)) has no width"))
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    length(value) == width || throw(ArgumentError(
        "fixed byte-array field $(repr(element.name)) has a value with " *
        "the wrong width"))
    for byte in value
        byte isa UInt8 || throw(ArgumentError(
            "fixed byte-array field $(repr(element.name)) contains a non-byte value"))
    end
    return Int64(width)
end

function _nestedwritedecimalpayload(element::Metadata.SchemaElement, value,
        limits::Limits)
    value isa Decimal || throw(ArgumentError(
        "DECIMAL field $(repr(element.name)) contains a non-Decimal value"))
    precision, scale = something(_decimalparameters(element))
    value.scale == scale || throw(ArgumentError(
        "DECIMAL field $(repr(element.name)) requires scale $scale, got " *
        "$(value.scale)"))
    _checkdecimalvalue(value.unscaled, precision, element.name, ArgumentError)
    physical = element.type_
    if physical == Metadata.Type.INT32
        typemin(Int32) <= value.unscaled <= typemax(Int32) || throw(ArgumentError(
            "DECIMAL value does not fit in INT32"))
        return Int64(0)
    elseif physical == Metadata.Type.INT64
        typemin(Int64) <= value.unscaled <= typemax(Int64) || throw(ArgumentError(
            "DECIMAL value does not fit in INT64"))
        return Int64(0)
    end
    width = physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY ?
        Int(element.type_length) : _twoscomplementwidth(value.unscaled)
    _checklimit(:decimal_bytes, width, limits.max_decimal_bytes)
    _checklimit(:string_bytes, width, limits.max_string_bytes)
    return Int64(width)
end

function _nestedwritebinarypayload(kind::Symbol,
        element::Metadata.SchemaElement, value, limits::Limits)
    if kind === :enum
        value isa AbstractString || throw(ArgumentError(
            "ENUM field $(repr(element.name)) contains a non-string value"))
        bytes = codeunits(value)
        _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
        isvalid(String, bytes) || throw(ArgumentError(
            "ENUM field $(repr(element.name)) contains invalid UTF-8"))
        return Int64(length(bytes))
    elseif kind === :uuid
        value isa UUIDs.UUID || throw(ArgumentError(
            "UUID field $(repr(element.name)) contains a non-UUID value"))
        return Int64(16)
    elseif kind === :float16
        value isa Float16 || throw(ArgumentError(
            "FLOAT16 field $(repr(element.name)) contains a non-Float16 value"))
        return Int64(2)
    elseif kind === :json
        value isa JSONValue || throw(ArgumentError(
            "JSON field $(repr(element.name)) contains an untagged value"))
        _validatejson(value.bytes, limits, ArgumentError)
        return Int64(length(value.bytes))
    elseif kind === :bson
        value isa BSONValue || throw(ArgumentError(
            "BSON field $(repr(element.name)) contains an untagged value"))
        _validatebson(value.bytes, limits, ArgumentError)
        return Int64(length(value.bytes))
    elseif kind === :interval
        value isa Interval || throw(ArgumentError(
            "INTERVAL field $(repr(element.name)) contains a non-Interval value"))
        return Int64(12)
    elseif kind === :unknown
        throw(ArgumentError(
            "UNKNOWN field $(repr(element.name)) can contain only missing"))
    end
    throw(ArgumentError("unsupported binary logical kind $kind"))
end

function _nestedwriteleafpayload(element::Metadata.SchemaElement, value,
        limits::Limits)
    kind = _logicalkind(element)
    if kind isa Union{_TimeLogicalKind,_TimestampLogicalKind,
            _IntegerLogicalKind}
        _temporalphysicalvalue(kind, element, value)
        return Int64(0)
    elseif kind === :string
        value isa AbstractString || throw(ArgumentError(
            "STRING field $(repr(element.name)) contains a non-string value"))
        bytes = codeunits(value)
        _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
        isvalid(String, bytes) || throw(ArgumentError(
            "STRING field $(repr(element.name)) contains invalid UTF-8"))
        return Int64(length(bytes))
    elseif kind === :date
        value isa Dates.Date || throw(ArgumentError(
            "DATE field $(repr(element.name)) contains a non-Date value"))
        _toparquetdate(value)
        return Int64(0)
    elseif kind === :decimal
        return _nestedwritedecimalpayload(element, value, limits)
    elseif kind isa Symbol
        return _nestedwritebinarypayload(kind, element, value, limits)
    end
    physical = element.type_
    expected = _physicaleltype(physical)
    if physical == Metadata.Type.BYTE_ARRAY
        value isa AbstractVector{UInt8} || throw(ArgumentError(
            "BYTE_ARRAY field $(repr(element.name)) contains a non-byte-array value"))
        _checklimit(:string_bytes, length(value), limits.max_string_bytes)
        return Int64(length(value))
    elseif physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY
        value isa Union{Tuple,AbstractVector} || throw(ArgumentError(
            "fixed byte-array field $(repr(element.name)) contains $(typeof(value))"))
        return _nestedwritefixedpayload(element, value, limits)
    end
    value isa expected || throw(ArgumentError(
        "physical field $(repr(element.name)) contains $(typeof(value)); " *
        "expected $expected"))
    return Int64(0)
end

function _nestedwritenormalizephysical(element::Metadata.SchemaElement, value,
        limits::Limits)
    physical = _physicalvalue(element, value; limits=limits)
    if element.type_ in (Metadata.Type.BYTE_ARRAY,
            Metadata.Type.FIXED_LEN_BYTE_ARRAY)
        physical isa Vector{UInt8} && physical !== value && return physical
        bytes = physical isa AbstractString ? codeunits(physical) : physical
        return UInt8[byte for byte in bytes]
    end
    return physical
end

function _nestedwriteincrement(value::Int64, limits::Limits)
    next = try
        Base.checked_add(value, Int64(1))
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64),
            limits.max_container_elements))
    end
    _checklimit(:container_elements, next,
        limits.max_container_elements)
    return next
end

function _nestedwriteaddpayload(value::Int64, bytes::Int64, limits::Limits)
    requested = try
        Base.checked_add(value, bytes)
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:materialized_bytes, typemax(Int64),
            limits.max_materialized_bytes))
    end
    _checklimit(:materialized_bytes, requested,
        limits.max_materialized_bytes)
    return requested
end

function _nestedwriterecord!(context::_NestedWriteCountContext,
        leafindex::Int, repetition::UInt64, definition::UInt64,
        payload::Int64, present::Bool, value)
    count = context.counts[leafindex]
    count.entries = _nestedwriteincrement(count.entries, context.limits)
    if present
        count.dense = _nestedwriteincrement(count.dense, context.limits)
        count.payload_bytes = _nestedwriteaddpayload(count.payload_bytes,
            payload, context.limits)
    end
    return
end

function _nestedwriterecord!(context::_NestedWriteEmitContext,
        leafindex::Int, repetition::UInt64, definition::UInt64,
        payload::Int64, present::Bool, value)
    builder = context.builders[leafindex]
    entry = builder.entry_position + 1
    entry <= length(builder.repetition) || throw(ArgumentError(
        "nested Parquet input changed its level-entry count between passes"))
    builder.entry_position = entry
    builder.repetition[entry] = repetition
    builder.definition[entry] = definition
    if present
        dense = builder.dense_position + 1
        dense <= length(builder.values) || throw(ArgumentError(
            "nested Parquet input changed its dense-value count between passes"))
        builder.dense_position = dense
        builder.payload_position = _nestedwriteaddpayload(
            builder.payload_position, payload, context.limits)
        builder.values[dense] = value
    end
    return
end

function _nestedwritepreflightemit(context::_NestedWriteEmitContext,
        leafindex::Int, payload::Int64)
    builder = context.builders[leafindex]
    count = context.counts[leafindex]
    builder.entry_position < count.entries || throw(ArgumentError(
        "nested Parquet input changed its level-entry count between passes"))
    builder.dense_position < count.dense || throw(ArgumentError(
        "nested Parquet input changed its dense-value count between passes"))
    requested = try
        Base.checked_add(builder.payload_position, payload)
    catch err
        err isa OverflowError || rethrow()
        throw(ArgumentError(
            "nested Parquet input changed its variable-width payload between passes"))
    end
    requested <= count.payload_bytes || throw(ArgumentError(
        "nested Parquet input changed its variable-width payload between passes"))
    return
end

function _nestedwritemarker!(context, range::UnitRange{Int32},
        repetition::UInt64, definition::UInt64)
    for rawindex in range
        _nestedwriterecord!(context, Int(rawindex), repetition, definition,
            Int64(0), false, nothing)
    end
    return
end

function _nestedwriteshred!(context::_NestedWriteCountContext,
        plan::_NestedWriteLeafPlan, value, repetition::UInt64)
    return _nestedwriteshred!(context, plan, value, repetition, nothing)
end

function _nestedwriteshred!(context::_NestedWriteCountContext,
        plan::_NestedWriteLeafPlan, value, repetition::UInt64, row_witness)
    _nestedwriterowcheck(row_witness)
    semantic = plan.semantic
    shape = plan.shape
    present = _nestedwritepresent(shape, value)
    _nestedwritetraceleaf!(context.trace, value, present)
    if !present
        _nestedwriterecord!(context, Int(first(semantic.leaf_range)), repetition,
            UInt64(semantic.parent_definition), Int64(0), false, nothing)
        return
    end
    _nestedwritecheckvalue(shape, value)
    payload = _nestedwriteleafpayload(semantic.source.element, value,
        context.limits)
    _nestedwriterecord!(context, Int(first(semantic.leaf_range)), repetition,
        UInt64(semantic.present_definition), payload, true, value)
    return
end

function _nestedwriteshred!(context::_NestedWriteEmitContext,
        plan::_NestedWriteLeafPlan, value, repetition::UInt64)
    return _nestedwriteshred!(context, plan, value, repetition, nothing)
end

function _nestedwriteshred!(context::_NestedWriteEmitContext,
        plan::_NestedWriteLeafPlan, value, repetition::UInt64, row_witness)
    _nestedwriterowcheck(row_witness)
    semantic = plan.semantic
    shape = plan.shape
    present = _nestedwritepresent(shape, value)
    _nestedwritetraceleaf!(context.trace, value, present)
    if !present
        _nestedwriterecord!(context, Int(first(semantic.leaf_range)), repetition,
            UInt64(semantic.parent_definition), Int64(0), false, nothing)
        return
    end
    _nestedwritecheckvalue(shape, value)
    payload = _nestedwriteleafpayload(semantic.source.element, value,
        context.limits)
    leafindex = Int(first(semantic.leaf_range))
    _nestedwritepreflightemit(context, leafindex, payload)
    physical = _nestedwritenormalizephysical(semantic.source.element, value,
        context.limits)
    _nestedwriterecord!(context, leafindex, repetition,
        UInt64(semantic.present_definition), payload, true, physical)
    return
end

function _nestedwritekeymismatch()
    throw(ArgumentError(
        "nested Parquet MAP key changed while it was physically consumed"))
end

function _nestedwritekeymissing(expected::_NestedWriteKeySnapshot)
    return expected.kind == _NESTED_WRITE_KEY_SCALAR &&
        expected.source_type === Missing && ismissing(expected.value)
end

function _nestedwritekeyleaflogicalequal(expected::_NestedWriteKeySnapshot,
        value)
    if expected.kind == _NESTED_WRITE_KEY_SCALAR
        return typeof(value) === expected.source_type &&
            isequal(value, expected.value)
    elseif expected.kind == _NESTED_WRITE_KEY_STRING
        return value isa AbstractString &&
            typeof(value) === expected.source_type
    elseif expected.kind == _NESTED_WRITE_KEY_BYTES
        if expected.source_type === JSONValue
            return value isa JSONValue
        elseif expected.source_type === BSONValue
            return value isa BSONValue
        end
        return typeof(value) === expected.source_type
    end
    return false
end

function _nestedwritekeyleafstatecheck(expected::_NestedWriteKeySnapshot,
        value)
    expected.kind in (_NESTED_WRITE_KEY_STRING,
        _NESTED_WRITE_KEY_BYTES) || return
    source = if value isa AbstractString
        codeunits(value)
    elseif value isa Union{JSONValue,BSONValue}
        value.bytes
    else
        value
    end
    state = _nestedwritekeyindexedstate(source)
    length(expected.value) == state.count &&
        expected.first == state.first && expected.last == state.last || throw(
            ArgumentError(
                "nested Parquet MAP key changed length or axes while it was consumed"))
    return
end

function _nestedwritekeysnapshotpayload(element::Metadata.SchemaElement,
        expected::_NestedWriteKeySnapshot, limits::Limits)
    expected.kind in (_NESTED_WRITE_KEY_STRING,
        _NESTED_WRITE_KEY_BYTES) || return _nestedwriteleafpayload(element,
            expected.value, limits)
    bytes = expected.value::Vector{UInt8}
    kind = _logicalkind(element)
    if kind in (:string, :enum)
        _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
        isvalid(String, bytes) || throw(ArgumentError(
            "Parquet MAP key contains invalid UTF-8"))
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
        "Parquet MAP-key snapshot has incompatible physical bytes"))
end

function _nestedwritekeyphysicalequal(expected::_NestedWriteKeySnapshot,
        physical, element::Metadata.SchemaElement,
        context::_NestedWriteEmitContext)
    if expected.kind in (_NESTED_WRITE_KEY_STRING, _NESTED_WRITE_KEY_BYTES)
        physical isa AbstractVector{UInt8} || return false
        return _nestedwritekeybytesequal(expected.value, physical)
    end
    expected.source_type === Decimal || return true
    trace = context.trace
    trace === nothing && throw(AssertionError(
        "authoritative MAP-key comparison requires a writer trace"))
    charge = Int64(0)
    if element.type_ in (Metadata.Type.BYTE_ARRAY,
            Metadata.Type.FIXED_LEN_BYTE_ARRAY)
        payload = _nestedwriteleafpayload(element, expected.value,
            context.limits)
        charge = _materializedarraybytes(UInt8, payload)
        _nestedwritetracereserve!(trace, charge)
    end
    try
        canonical = _nestedwritenormalizekeyphysical(element, expected.value,
            context.limits)
        if canonical isa AbstractVector{UInt8}
            physical isa AbstractVector{UInt8} || return false
            return _nestedwritekeybytesequal(canonical, physical)
        end
        return isequal(canonical, physical)
    finally
        iszero(charge) || _nestedwritetraceunreserve!(trace, charge)
    end
end

function _nestedwritekeynormalizedbytes(source, limits::Limits)
    state = _nestedwritekeyindexedstate(source)
    _checklimit(:string_bytes, state.count, limits.max_string_bytes)
    return _nestedwritekeycopybytes(source, state, "normalized")
end

function _nestedwritenormalizekeyphysical(element::Metadata.SchemaElement,
        value, limits::Limits)
    kind = _logicalkind(element)
    if kind in (:string, :enum) && value isa AbstractString
        bytes = _nestedwritekeynormalizedbytes(codeunits(value), limits)
        isvalid(String, bytes) || throw(ArgumentError(
            "STRING value contains invalid UTF-8"))
        return bytes
    end
    physical = _physicalvalue(element, value; limits=limits)
    element.type_ in (Metadata.Type.BYTE_ARRAY,
        Metadata.Type.FIXED_LEN_BYTE_ARRAY) || return physical
    physical isa Vector{UInt8} && physical !== value && return physical
    bytes = physical isa AbstractString ? codeunits(physical) : physical
    return _nestedwritekeynormalizedbytes(bytes, limits)
end

function _nestedwriteshredkey!(context,
        plan::_NestedWriteLeafPlan, value,
        expected::_NestedWriteKeySnapshot, repetition::UInt64)
    return _nestedwriteshredkey!(context, plan, value, expected, repetition,
        nothing)
end

function _nestedwriteshredkey!(context,
        plan::_NestedWriteLeafPlan, value,
        expected::_NestedWriteKeySnapshot, repetition::UInt64, row_witness)
    _nestedwriterowcheck(row_witness)
    semantic = plan.semantic
    shape = plan.shape
    present = _nestedwritepresent(shape, value)
    _nestedwritetraceleaf!(context.trace, value, present)
    _nestedwritekeyleafstatecheck(expected, value)
    if !present
        _nestedwritekeymissing(expected) || _nestedwritekeymismatch()
        _nestedwriterecord!(context, Int(first(semantic.leaf_range)),
            repetition, UInt64(semantic.parent_definition), Int64(0), false,
            nothing)
        return
    end
    _nestedwritecheckvalue(shape, value)
    _nestedwritekeyleaflogicalequal(expected, value) ||
        _nestedwritekeymismatch()
    element = semantic.source.element
    payload = _nestedwritekeysnapshotpayload(element, expected,
        context.limits)
    leafindex = Int(first(semantic.leaf_range))
    physical = value
    if context isa _NestedWriteEmitContext
        _nestedwritepreflightemit(context, leafindex, payload)
        physical = _nestedwritenormalizekeyphysical(element, value,
            context.limits)
        _nestedwritekeyphysicalequal(expected, physical, element, context) ||
            _nestedwritekeymismatch()
    end
    _nestedwriterecord!(context, leafindex, repetition,
        UInt64(semantic.present_definition), payload, true, physical)
    return
end

const _NestedWriteContainerPlan = Union{_NestedWriteStructPlan,
    _NestedWriteListPlan,_NestedWriteMapPlan}

function _nestedwriteshredenter(plan::_NestedWriteNodePlan, value,
        expected::Union{Nothing,_NestedWriteKeySnapshot}, keymode::Bool,
        repetition::UInt64, row_witness)
    return _NestedWriteShredAction(_NESTED_WRITE_SHRED_ENTER, plan, value,
        expected, keymode, repetition, row_witness, nothing, nothing, nothing,
        nothing, nothing, nothing, 0, 0, 0, 0, UInt8(0))
end

function _nestedwriteshredpost(plan::_NestedWriteNodePlan, first, second)
    return _NestedWriteShredAction(_NESTED_WRITE_SHRED_POSTCHECK, plan,
        nothing, nothing, false, UInt64(0), first, second, nothing, nothing,
        nothing, nothing, nothing, 0, 0, 0, 0, UInt8(0))
end

function _nestedwriteshredstructenter!(
        stack::_NestedWritePassStack{_NestedWriteShredAction}, context,
        plan::_NestedWriteStructPlan, value,
        expected::Union{Nothing,_NestedWriteKeySnapshot}, keymode::Bool,
        repetition::UInt64, row_witness, budget::_LiveByteBudget)
    _nestedwriterowcheck(row_witness)
    semantic = plan.semantic
    shape = plan.shape
    present = _nestedwritepresent(shape, value)
    _nestedwritetracestruct!(context.trace, value, present,
        length(plan.children))
    if !present
        keymode && (_nestedwritekeymissing(something(expected)) ||
            _nestedwritekeymismatch())
        _nestedwritemarker!(context, semantic.leaf_range, repetition,
            UInt64(semantic.parent_definition))
        return
    end
    if keymode
        snapshot = something(expected)
        expectedkind = shape.package_owned ? _NESTED_WRITE_KEY_STRUCT :
            _NESTED_WRITE_KEY_NAMED_TUPLE
        snapshot.kind == expectedkind || _nestedwritekeymismatch()
        length(snapshot.children) == length(plan.children) ||
            _nestedwritekeymismatch()
        if shape.package_owned
            value isa StructValue || _nestedwritekeymismatch()
            value.names == shape.names || _nestedwritekeymismatch()
            length(value) == length(plan.children) ||
                _nestedwritekeymismatch()
        else
            value isa shape.value_type || _nestedwritekeymismatch()
        end
    elseif shape.package_owned
        value isa StructValue || throw(ArgumentError(
            "Parquet struct field $(repr(shape.name)) requires StructValue rows"))
        value.names == shape.names || throw(ArgumentError(
            "Parquet struct field $(repr(shape.name)) changed its field names between writer passes"))
        length(value) == length(plan.children) || throw(ArgumentError(
            "Parquet struct field $(repr(shape.name)) changed its field count between writer passes"))
    else
        value isa NamedTuple || throw(ArgumentError(
            "Parquet struct field $(repr(shape.name)) requires NamedTuple rows"))
        value isa shape.value_type || throw(ArgumentError(
            "Parquet struct field $(repr(shape.name)) contains $(typeof(value)); " *
            "expected $(shape.value_type)"))
    end
    _nestedwritestackpush!(stack, _NestedWriteShredAction(
        _NESTED_WRITE_SHRED_STRUCT, plan, value, expected, keymode, repetition,
        row_witness, nothing, nothing, nothing, nothing, nothing, nothing, 1,
        length(plan.children), 0, 0, UInt8(0)), budget)
    return
end

function _nestedwriteshredlistenter!(
        stack::_NestedWritePassStack{_NestedWriteShredAction},
        context, plan::_NestedWriteListPlan, value,
        expected::Union{Nothing,_NestedWriteKeySnapshot}, keymode::Bool,
        repetition::UInt64, row_witness, budget::_LiveByteBudget)
    _nestedwriterowcheck(row_witness)
    value isa ListValue && _validatelistvalue(value)
    semantic = plan.semantic
    shape = plan.shape
    present = _nestedwritepresent(shape, value)
    _nestedwritetracecontainer!(context.trace, _NESTED_WRITE_TRACE_LIST,
        value, present)
    if !present
        keymode && (_nestedwritekeymissing(something(expected)) ||
            _nestedwritekeymismatch())
        _nestedwritemarker!(context, semantic.leaf_range, repetition,
            UInt64(semantic.parent_definition))
        return
    end
    if keymode
        snapshot = something(expected)
        snapshot.kind == _NESTED_WRITE_KEY_LIST || _nestedwritekeymismatch()
        value isa shape.value_type || _nestedwritekeymismatch()
        value isa AbstractVector || _nestedwritekeymismatch()
    else
        value isa shape.value_type || throw(ArgumentError(
            "Parquet list field $(repr(shape.name)) contains $(typeof(value)); " *
            "expected $(shape.value_type)"))
        value isa AbstractVector || throw(ArgumentError(
            "Parquet LIST values must be vectors"))
    end
    _nestedwritecheckcontainer(value, context.limits,
        "Parquet LIST field $(repr(shape.name))")
    label = keymode ? "nested Parquet MAP-key LIST" :
        "Parquet LIST field $(repr(shape.name))"
    count = _nestedvectorcount(value, label)
    first, last = _nestedvectoraxes(value, label)
    rowsnapshot = _nestedwritelistrowsnapshot!(context.trace, value, shape,
        context.limits)
    if keymode
        snapshot = something(expected)
        count == length(snapshot.children) || _nestedwritekeymismatch()
        first == snapshot.first && last == snapshot.last ||
            _nestedwritekeymismatch()
    end
    if iszero(count)
        _nestedwritemarker!(context, semantic.leaf_range, repetition,
            UInt64(semantic.present_definition))
        return
    end
    _nestedwritestackpush!(stack, _NestedWriteShredAction(
        _NESTED_WRITE_SHRED_LIST, plan, value, expected, keymode, repetition,
        row_witness, nothing, nothing, nothing, nothing, rowsnapshot, nothing,
        first, count, first, last, UInt8(0)), budget)
    return
end

function _nestedwriteshreddictrelease!(
        stack::_NestedWritePassStack{_NestedWriteShredAction}, context,
        plan::_NestedWriteMapPlan,
        materialization::_NestedWriteDictMaterialization,
        budget::_LiveByteBudget)
    action = _NestedWriteShredAction(_NESTED_WRITE_SHRED_DICT_RELEASE, plan,
        nothing, nothing, false, UInt64(0), nothing, nothing, nothing, nothing,
        materialization, nothing, nothing, 0, 0, 0, 0, UInt8(0))
    try
        _nestedwritestackpush!(stack, action, budget)
    catch
        _nestedwritedictrelease!(context.trace, materialization)
        rethrow()
    end
    return
end

function _nestedwriteshredmapenter!(
        stack::_NestedWritePassStack{_NestedWriteShredAction},
        context, plan::_NestedWriteMapPlan, value,
        expected::Union{Nothing,_NestedWriteKeySnapshot}, keymode::Bool,
        repetition::UInt64, row_witness, budget::_LiveByteBudget)
    _nestedwriterowcheck(row_witness)
    value isa MapValue && _validatemapvalue(value)
    semantic = plan.semantic
    shape = plan.shape
    present = _nestedwritepresent(shape, value)
    if !present
        _nestedwritetracecontainer!(context.trace, _NESTED_WRITE_TRACE_MAP,
            value, false)
        keymode && (_nestedwritekeymissing(something(expected)) ||
            _nestedwritekeymismatch())
        _nestedwritemarker!(context, semantic.leaf_range, repetition,
            UInt64(semantic.parent_definition))
        return
    end
    if keymode
        snapshot = something(expected)
        snapshot.kind == _NESTED_WRITE_KEY_MAP || _nestedwritekeymismatch()
        value isa shape.value_type || _nestedwritekeymismatch()
    else
        value isa shape.value_type || throw(ArgumentError(
            "Parquet map field $(repr(shape.name)) contains $(typeof(value)); " *
            "expected $(shape.value_type)"))
    end
    if value isa AbstractDict
        materialization = _nestedwritedictmaterialize(context.trace, value,
            shape.key, shape.value, context.limits)
        _nestedwriteshreddictrelease!(stack, context, plan, materialization,
            budget)
        count = materialization.count
        _nestedwritetracedict!(context.trace, count)
        if keymode
            children = something(expected).children
            iseven(length(children)) && count == length(children) ÷ 2 ||
                _nestedwritekeymismatch()
        end
        if iszero(count)
            _nestedwritemarker!(context, semantic.leaf_range, repetition,
                UInt64(semantic.present_definition))
        else
            _nestedwritestackpush!(stack, _NestedWriteShredAction(
                _NESTED_WRITE_SHRED_MAP_DICT, plan, value, expected, keymode,
                repetition, row_witness, nothing, materialization.first,
                nothing, materialization, nothing, nothing, 1, count, 0, 0,
                UInt8(0)), budget)
        end
        return
    end
    _nestedwritetracecontainer!(context.trace, _NESTED_WRITE_TRACE_MAP,
        value, true)
    label = keymode ? "nested Parquet MAP-key MAP" :
        "Parquet MAP field $(repr(shape.name))"
    count = value isa AbstractVector ? _nestedvectorcount(value, label) : 0
    _checklimit(:container_elements, count,
        context.limits.max_container_elements)
    if keymode
        children = something(expected).children
        iseven(length(children)) && count == length(children) ÷ 2 ||
            _nestedwritekeymismatch()
    end
    if iszero(count)
        _nestedwritemarker!(context, semantic.leaf_range, repetition,
            UInt64(semantic.present_definition))
        return
    end
    first, last = value isa AbstractVector ?
        _nestedvectoraxes(value, label) : (0, 0)
    if value isa MapValue
        keysnapshot = _nestedwriteoccurrencesnapshot!(context.trace,
            value.keys, plan.key.shape, context.limits)
        valuesnapshot = shape.source_has_values ?
            _nestedwriteoccurrencesnapshot!(context.trace,
                something(value.values), plan.value.shape, context.limits) :
            nothing
        _nestedwritestackpush!(stack, _NestedWriteShredAction(
            _NESTED_WRITE_SHRED_MAP_VIEW_KEY, plan, value, expected, keymode,
            repetition, row_witness, nothing, nothing, nothing, nothing,
            keysnapshot, valuesnapshot, 1, count, first, last, UInt8(0)),
            budget)
        return
    end
    result = iterate(value)
    result === nothing || _nestedwritestackpush!(stack,
        _NestedWriteShredAction(_NESTED_WRITE_SHRED_MAP_ITER_KEY, plan, value,
            expected, keymode, repetition, row_witness, nothing, result,
            nothing, nothing, nothing, nothing, 1, count, first, last,
            UInt8(0)), budget)
    return
end

function _nestedwriteshredchildexpected(action::_NestedWriteShredAction,
        index::Int)
    action.keymode || return nothing
    return something(action.expected).children[index]
end

function _nestedwriteshredkeymode(action::_NestedWriteShredAction,
        nested::Union{Nothing,_NestedWriteKeySnapshot})
    return action.keymode || nested !== nothing
end

function _nestedwriteshredmapkeyexpected(
        action::_NestedWriteShredAction, entry::Int,
        nested::Union{Nothing,_NestedWriteKeySnapshot})
    action.keymode && return _nestedwriteshredchildexpected(action,
        2 * entry - 1)
    return nested
end

function _nestedwriteshredmapvalueexpected(
        action::_NestedWriteShredAction, entry::Int)
    action.keymode || return nothing
    return _nestedwriteshredchildexpected(action, 2 * entry)
end

function _nestedwriteshredmaprepetition(action::_NestedWriteShredAction,
        entry::Int)
    entry == 1 && return action.repetition
    return UInt64((action.plan::_NestedWriteMapPlan).semantic.repetition_level)
end

function _nestedwriteshredprocessstruct!(
        stack::_NestedWritePassStack{_NestedWriteShredAction}, context,
        action::_NestedWriteShredAction, budget::_LiveByteBudget)
    plan = action.plan::_NestedWriteStructPlan
    shape = plan.shape
    index = action.position
    if shape.package_owned
        value = action.value::StructValue
        child = _nestedwritestructaccesschild(value, value.names,
            value.children, action.count, index, shape.names[index],
            something(shape.source_children)[index])
        snapshot = _nestedwriteoccurrencesnapshot!(context.trace, child,
            plan.children[index].shape, context.limits)
        item, child_witness = _nestedwriterowaccess(child, value.index,
            snapshot)
        if index < action.count
            _nestedwritestackpush!(stack, _NestedWriteShredAction(action.kind,
                plan, value, action.expected, action.keymode,
                action.repetition, action.row_witness, nothing, nothing,
                nothing, nothing, nothing, nothing, index + 1, action.count,
                0, 0, UInt8(0)), budget)
        end
        _nestedwritestackpush!(stack, _nestedwriteshredpost(plan,
            child_witness, action.row_witness), budget)
        _nestedwritestackpush!(stack, _nestedwriteshredenter(
            plan.children[index], item,
            _nestedwriteshredchildexpected(action, index), action.keymode,
            action.repetition, child_witness), budget)
    else
        item = getfield(action.value, index)
        if index < action.count
            _nestedwritestackpush!(stack, _NestedWriteShredAction(action.kind,
                plan, action.value, action.expected, action.keymode,
                action.repetition, action.row_witness, nothing, nothing,
                nothing, nothing, nothing, nothing, index + 1, action.count,
                0, 0, UInt8(0)), budget)
        end
        _nestedwritestackpush!(stack, _nestedwriteshredenter(
            plan.children[index], item,
            _nestedwriteshredchildexpected(action, index), action.keymode,
            action.repetition, nothing), budget)
    end
    return
end

function _nestedwriteshredprocesslist!(
        stack::_NestedWritePassStack{_NestedWriteShredAction}, context,
        action::_NestedWriteShredAction, budget::_LiveByteBudget)
    plan = action.plan::_NestedWriteListPlan
    index = action.position
    _nestedwriterowcheck(action.row_witness)
    _nestedwriteviewaccesscheck(action.value, action.count, action.first,
        action.last)
    ordinal = index - action.first + 1
    expected = _nestedwriteshredchildexpected(action, ordinal)
    repeated = UInt64(plan.semantic.repetition_level)
    itemrepetition = ordinal == 1 ? action.repetition : repeated
    item, child_witness = _nestedwritelistrowaccess(action.value, index,
        action.snapshot1)
    _nestedwriteviewaccesscheck(action.value, action.count, action.first,
        action.last)
    index < action.last && _nestedwritestackpush!(stack,
        _NestedWriteShredAction(action.kind, plan, action.value,
            action.expected, action.keymode, action.repetition,
            action.row_witness, nothing, nothing, nothing, nothing,
            action.snapshot1, nothing, index + 1, action.count, action.first,
            action.last, UInt8(0)), budget)
    _nestedwritestackpush!(stack, _nestedwriteshredpost(plan, child_witness,
        action.row_witness), budget)
    _nestedwritestackpush!(stack, _nestedwriteshredenter(plan.element, item,
        expected, action.keymode, itemrepetition, child_witness), budget)
    return
end

function _nestedwriteshredprocessdict!(
        stack::_NestedWritePassStack{_NestedWriteShredAction}, context,
        action::_NestedWriteShredAction, budget::_LiveByteBudget)
    entry = action.state
    if action.phase == UInt8(1)
        entry = entry.next
        entry === nothing && return
    end
    plan = action.plan::_NestedWriteMapPlan
    shape = plan.shape
    index = action.position
    ismissing(entry.key) && throw(ArgumentError(
        "Parquet MAP field $(repr(shape.name)) contains a missing key"))
    nested = _nestedwritetracekey!(context.trace, entry.key, shape.key,
        context.limits)
    itemrepetition = _nestedwriteshredmaprepetition(action, index)
    _nestedwritestackpush!(stack, _NestedWriteShredAction(action.kind, plan,
        action.value, action.expected, action.keymode, action.repetition,
        action.row_witness, nothing, entry, nothing, action.materialization,
        nothing, nothing, index + 1, action.count, 0, 0, UInt8(1)), budget)
    mapvalue = shape.source_has_values ? entry.value : missing
    _nestedwritestackpush!(stack, _nestedwriteshredenter(plan.value, mapvalue,
        _nestedwriteshredmapvalueexpected(action, index), action.keymode,
        itemrepetition, nothing), budget)
    _nestedwritestackpush!(stack, _NestedWriteShredAction(
        _NESTED_WRITE_SHRED_KEYASSERT, plan.key, entry.key, nothing, false,
        UInt64(0), nothing, nothing, nothing, nested, nothing, nothing, nothing,
        0, 0, 0, 0, UInt8(0)), budget)
    keyexpected = _nestedwriteshredmapkeyexpected(action, index, nested)
    _nestedwritestackpush!(stack, _nestedwriteshredenter(plan.key, entry.key,
        keyexpected, _nestedwriteshredkeymode(action, nested), itemrepetition,
        nothing), budget)
    return
end

function _nestedwriteshredprocessviewkey!(
        stack::_NestedWritePassStack{_NestedWriteShredAction}, context,
        action::_NestedWriteShredAction, budget::_LiveByteBudget)
    plan = action.plan::_NestedWriteMapPlan
    shape = plan.shape
    entry = action.position
    _nestedwriterowcheck(action.row_witness)
    _nestedwriteviewaccesscheck(action.value, action.count, action.first,
        action.last)
    physical = action.value.first + entry - 1
    keyvalue, key_witness = _nestedwriterowaccess(action.value.keys, physical,
        action.snapshot1)
    if !action.keymode
        _nestedwriterowcheck(action.row_witness)
        if shape.source_has_values
            rawvalue, value_witness = _nestedwriterowaccess(
                something(action.value.values), physical, action.snapshot2)
        else
            rawvalue = missing
            value_witness = nothing
        end
    else
        rawvalue = nothing
        value_witness = nothing
    end
    _nestedwriterowcheck(key_witness)
    ismissing(keyvalue) && throw(ArgumentError(
        "Parquet MAP field $(repr(shape.name)) contains a missing key"))
    nested = _nestedwritetracekey!(context.trace, keyvalue, shape.key,
        context.limits, nothing, key_witness)
    itemrepetition = _nestedwriteshredmaprepetition(action, entry)
    _nestedwritestackpush!(stack, _NestedWriteShredAction(
        _NESTED_WRITE_SHRED_MAP_VIEW_VALUE, plan, action.value,
        action.expected, action.keymode, action.repetition, action.row_witness,
        key_witness, keyvalue, nested, nothing, action.snapshot1,
        action.snapshot2, physical, action.count, action.first, action.last,
        rawvalue, value_witness, UInt8(0)), budget)
    keyexpected = _nestedwriteshredmapkeyexpected(action, entry, nested)
    _nestedwritestackpush!(stack, _nestedwriteshredenter(plan.key, keyvalue,
        keyexpected, _nestedwriteshredkeymode(action, nested), itemrepetition,
        key_witness), budget)
    return
end

function _nestedwriteshredprocessviewvalue!(
        stack::_NestedWritePassStack{_NestedWriteShredAction}, context,
        action::_NestedWriteShredAction, budget::_LiveByteBudget)
    plan = action.plan::_NestedWriteMapPlan
    shape = plan.shape
    keyvalue = action.state
    _nestedwritekeyassert!(action.nested, keyvalue, shape.key,
        context.limits, context.trace, nothing, action.other_witness)
    _nestedwriterowcheck(action.other_witness)
    entry = action.position - action.value.first + 1
    itemrepetition = _nestedwriteshredmaprepetition(action, entry)
    if action.keymode
        _nestedwriterowcheck(action.row_witness)
        if shape.source_has_values
            mapvalue, value_witness = _nestedwriterowaccess(
                something(action.value.values), action.position,
                action.snapshot2)
        else
            mapvalue = missing
            value_witness = nothing
        end
    else
        mapvalue = action.rawvalue
        value_witness = action.value_witness
    end
    entry < action.count && _nestedwritestackpush!(stack,
        _NestedWriteShredAction(_NESTED_WRITE_SHRED_MAP_VIEW_KEY, plan,
            action.value, action.expected, action.keymode, action.repetition,
            action.row_witness, nothing, nothing, nothing, nothing,
            action.snapshot1, action.snapshot2, entry + 1,
            action.count, action.first, action.last, UInt8(0)), budget)
    _nestedwritestackpush!(stack, _nestedwriteshredpost(plan, value_witness,
        action.row_witness), budget)
    _nestedwritestackpush!(stack, _nestedwriteshredenter(plan.value, mapvalue,
        _nestedwriteshredmapvalueexpected(action, entry), action.keymode,
        itemrepetition, value_witness), budget)
    return
end

function _nestedwriteshredprocessiterkey!(
        stack::_NestedWritePassStack{_NestedWriteShredAction}, context,
        action::_NestedWriteShredAction, budget::_LiveByteBudget)
    plan = action.plan::_NestedWriteMapPlan
    shape = plan.shape
    _nestedwriteviewaccesscheck(action.value, action.count, action.first,
        action.last)
    result = action.state
    pair = _nestedwritemappair(result[1], shape.name)
    ismissing(pair.first) && throw(ArgumentError(
        "Parquet MAP field $(repr(shape.name)) contains a missing key"))
    nested = _nestedwritetracekey!(context.trace, pair.first, shape.key,
        context.limits)
    entry = action.position
    itemrepetition = _nestedwriteshredmaprepetition(action, entry)
    _nestedwritestackpush!(stack, _NestedWriteShredAction(
        _NESTED_WRITE_SHRED_MAP_ITER_VALUE, plan, action.value,
        action.expected, action.keymode, action.repetition, action.row_witness,
        nothing, pair, nested, nothing, result[2], nothing, entry,
        action.count, action.first, action.last, UInt8(0)), budget)
    keyexpected = _nestedwriteshredmapkeyexpected(action, entry, nested)
    _nestedwritestackpush!(stack, _nestedwriteshredenter(plan.key, pair.first,
        keyexpected, _nestedwriteshredkeymode(action, nested), itemrepetition,
        nothing), budget)
    return
end

function _nestedwriteshredprocessitervalue!(
        stack::_NestedWritePassStack{_NestedWriteShredAction}, context,
        action::_NestedWriteShredAction, budget::_LiveByteBudget)
    plan = action.plan::_NestedWriteMapPlan
    shape = plan.shape
    pair = action.state::Pair
    _nestedwritekeyassert!(action.nested, pair.first, shape.key,
        context.limits, context.trace)
    entry = action.position
    itemrepetition = _nestedwriteshredmaprepetition(action, entry)
    _nestedwritestackpush!(stack, _NestedWriteShredAction(
        _NESTED_WRITE_SHRED_MAP_ITER_NEXT, plan, action.value,
        action.expected, action.keymode, action.repetition, action.row_witness,
        nothing, action.snapshot1, nothing, nothing, nothing, nothing, entry,
        action.count, action.first, action.last, UInt8(0)), budget)
    mapvalue = shape.source_has_values ? pair.second : missing
    _nestedwritestackpush!(stack, _nestedwriteshredenter(plan.value, mapvalue,
        _nestedwriteshredmapvalueexpected(action, entry), action.keymode,
        itemrepetition, nothing), budget)
    return
end

function _nestedwriteshredprocess!(
        stack::_NestedWritePassStack{_NestedWriteShredAction},
        context, action::_NestedWriteShredAction, budget::_LiveByteBudget)
    kind = action.kind
    plan = action.plan
    if kind == _NESTED_WRITE_SHRED_ENTER
        if plan isa _NestedWriteLeafPlan
            if action.keymode
                return _nestedwriteshredkey!(context, plan, action.value,
                    something(action.expected), action.repetition,
                    action.row_witness)
            end
            return _nestedwriteshred!(context, plan, action.value,
                action.repetition, action.row_witness)
        elseif plan isa _NestedWriteStructPlan
            return _nestedwriteshredstructenter!(stack, context, plan,
                action.value, action.expected, action.keymode,
                action.repetition, action.row_witness, budget)
        elseif plan isa _NestedWriteListPlan
            return _nestedwriteshredlistenter!(stack, context, plan,
                action.value, action.expected, action.keymode,
                action.repetition, action.row_witness, budget)
        end
        return _nestedwriteshredmapenter!(stack, context,
            plan::_NestedWriteMapPlan, action.value, action.expected,
            action.keymode, action.repetition, action.row_witness, budget)
    elseif kind == _NESTED_WRITE_SHRED_POSTCHECK
        _nestedwriterowcheck(action.row_witness)
        _nestedwriterowcheck(action.other_witness)
        return
    elseif kind == _NESTED_WRITE_SHRED_KEYASSERT
        _nestedwritekeyassert!(action.nested, action.value, plan.shape,
            context.limits, context.trace, nothing, action.row_witness)
        return
    elseif kind == _NESTED_WRITE_SHRED_DICT_RELEASE
        _nestedwritedictrelease!(context.trace,
            something(action.materialization))
        return
    elseif kind == _NESTED_WRITE_SHRED_STRUCT
        return _nestedwriteshredprocessstruct!(stack, context, action, budget)
    elseif kind == _NESTED_WRITE_SHRED_LIST
        return _nestedwriteshredprocesslist!(stack, context, action, budget)
    elseif kind == _NESTED_WRITE_SHRED_MAP_DICT
        return _nestedwriteshredprocessdict!(stack, context, action, budget)
    elseif kind == _NESTED_WRITE_SHRED_MAP_VIEW_KEY
        return _nestedwriteshredprocessviewkey!(stack, context, action, budget)
    elseif kind == _NESTED_WRITE_SHRED_MAP_VIEW_VALUE
        return _nestedwriteshredprocessviewvalue!(stack, context, action,
            budget)
    elseif kind == _NESTED_WRITE_SHRED_MAP_ITER_KEY
        return _nestedwriteshredprocessiterkey!(stack, context, action, budget)
    elseif kind == _NESTED_WRITE_SHRED_MAP_ITER_VALUE
        return _nestedwriteshredprocessitervalue!(stack, context, action,
            budget)
    end
    _nestedwriteviewaccesscheck(action.value, action.count, action.first,
        action.last)
    result = iterate(action.value, action.state)
    result === nothing || _nestedwritestackpush!(stack,
        _NestedWriteShredAction(_NESTED_WRITE_SHRED_MAP_ITER_KEY, plan,
            action.value, action.expected, action.keymode, action.repetition,
            action.row_witness, nothing, result, nothing, nothing, nothing,
            nothing, action.position + 1, action.count, action.first,
            action.last, UInt8(0)), budget)
    return
end

function _nestedwriteshredcleanup!(
        stack::_NestedWritePassStack{_NestedWriteShredAction},
        context)
    for action in Iterators.reverse(stack.frames)
        action.kind == _NESTED_WRITE_SHRED_DICT_RELEASE || continue
        _nestedwritedictrelease!(context.trace,
            something(action.materialization))
    end
    return
end

function _nestedwriteshrediterative!(
        stack::_NestedWritePassStack{_NestedWriteShredAction},
        context, plan::_NestedWriteNodePlan, value,
        expected::Union{Nothing,_NestedWriteKeySnapshot}, keymode::Bool,
        repetition::UInt64, row_witness, budget::_LiveByteBudget)
    Base.@nospecialize value
    isempty(stack.frames) && !stack.processing || throw(AssertionError(
        "nested writer shred scratch stack is not empty"))
    try
        _nestedwritestackpush!(stack, _nestedwriteshredenter(plan, value,
            expected, keymode, repetition, row_witness), budget)
        while !isempty(stack.frames)
            action = _nestedwritepassstackpop!(stack)
            try
                _nestedwriteshredprocess!(stack, context, action, budget)
            finally
                _nestedwritepassstackprocessed!(stack)
            end
        end
    finally
        if !isempty(stack.frames)
            try
                _nestedwriteshredcleanup!(stack, context)
            finally
                _nestedwritepassstackclear!(stack)
            end
        end
    end
    isempty(stack.frames) && !stack.processing || throw(AssertionError(
        "nested writer shred scratch stack retained actions"))
    return
end

function _nestedwriteshrediterative!(context, plan::_NestedWriteNodePlan,
        value, expected::Union{Nothing,_NestedWriteKeySnapshot}, keymode::Bool,
        repetition::UInt64, row_witness)
    Base.@nospecialize value
    trace = context.trace
    budget = trace isa _NestedWriteTrace ? trace.budget :
        _LiveByteBudget(context.limits)
    stack = _nestedwritepassstackstart(_NestedWriteShredAction, budget)
    try
        _nestedwriteshrediterative!(stack, context, plan, value, expected,
            keymode, repetition, row_witness, budget)
    finally
        _nestedwritepassstackrelease!(stack, budget)
    end
    return
end

function _nestedwriteshred!(context, plan::_NestedWriteContainerPlan, value,
        repetition::UInt64, row_witness)
    return _nestedwriteshrediterative!(context, plan, value, nothing, false,
        repetition, row_witness)
end

function _nestedwriteshred!(context, plan::_NestedWriteContainerPlan, value,
        repetition::UInt64)
    return _nestedwriteshred!(context, plan, value, repetition, nothing)
end

function _nestedwriteshredkey!(context, plan::_NestedWriteContainerPlan, value,
        expected::_NestedWriteKeySnapshot, repetition::UInt64, row_witness)
    return _nestedwriteshrediterative!(context, plan, value, expected, true,
        repetition, row_witness)
end

function _nestedwriteshredkey!(context, plan::_NestedWriteContainerPlan, value,
        expected::_NestedWriteKeySnapshot, repetition::UInt64)
    return _nestedwriteshredkey!(context, plan, value, expected, repetition,
        nothing)
end

function _nestedwritebuilderbytes(count::_NestedWriteLeafCount,
        physical::Type)
    bytes = _materializedarraybytes(UInt64, count.entries)
    bytes = _materializedsum(bytes,
        _materializedarraybytes(UInt64, count.entries))
    bytes = _materializedsum(bytes,
        _materializedarraybytes(physical, count.dense))
    if physical === Vector{UInt8}
        bytes = _materializedsum(bytes, _materializedproduct(count.dense,
            _MATERIALIZED_ARRAY_HEADER_BYTES))
        bytes = _materializedsum(bytes, count.payload_bytes)
    end
    return _materializedsum(bytes, _MATERIALIZED_OBJECT_BYTES)
end

function _nestedwritebuilders(counts::Vector{_NestedWriteLeafCount},
        semantic::_NestedSchemaPlan, budget::_LiveByteBudget)
    length(counts) == length(semantic.leaves) || throw(AssertionError(
        "nested writer count and leaf totals differ"))
    charge = _materializedarraybytes(_NestedWriteLeafBuilder, length(counts))
    for (count, leaf) in zip(counts, semantic.leaves)
        physical = _physicaleltype(leaf.source.element.type_)
        charge = _materializedsum(charge,
            _nestedwritebuilderbytes(count, physical))
    end
    _reserve!(budget, charge)
    builders = _NestedWriteLeafBuilder[]
    sizehint!(builders, length(counts))
    for (count, leaf) in zip(counts, semantic.leaves)
        physical = _physicaleltype(leaf.source.element.type_)
        entries = Int(count.entries)
        dense = Int(count.dense)
        push!(builders, _NestedWriteLeafBuilder(
            Vector{UInt64}(undef, entries), Vector{UInt64}(undef, entries),
            Vector{physical}(undef, dense), 0, 0, Int64(0)))
    end
    return builders
end

function _nestedwritevalidatebuilders(builders::Vector{_NestedWriteLeafBuilder},
        counts::Vector{_NestedWriteLeafCount}, semantic::_NestedSchemaPlan,
        rows::Int)
    for index in eachindex(builders, counts, semantic.leaves)
        builder = builders[index]
        count = counts[index]
        leaf = semantic.leaves[index]
        builder.entry_position == count.entries || throw(ArgumentError(
            "nested Parquet input changed its level-entry count between passes"))
        builder.dense_position == count.dense || throw(ArgumentError(
            "nested Parquet input changed its dense-value count between passes"))
        builder.payload_position == count.payload_bytes || throw(ArgumentError(
            "nested Parquet input changed its variable-width payload between passes"))
        LeafStream(builder.repetition, builder.definition, builder.values,
            leaf.source.max_repetition_level, leaf.source.max_definition_level;
            expected_rows=rows)
    end
    return
end


function _nestedwritecolumn(builder::_NestedWriteLeafBuilder,
        leaf::_NestedLeafPlan, rows::Int, budget::_LiveByteBudget)
    node = leaf.source
    element = node.element
    pathcharge = _reservearray!(budget, String, length(node.path))
    pathcharge >= 0 || throw(AssertionError(
        "nested writer path charge is negative"))
    _reserveobjects!(budget)
    optional = element.repetition_type == Metadata.FieldRepetitionType.OPTIONAL
    return WriteColumn(element.name, builder.values, element.type_,
        element.type_length, optional, element.logicalType,
        element.converted_type, copy(node.path), builder.repetition,
        builder.definition, node.max_repetition_level,
        node.max_definition_level, rows, Metadata.SchemaElement[])
end

function _nestedwritefinishfields(fragments::Vector{Vector{Metadata.SchemaElement}},
        plans::Vector{_NestedWriteNodePlan}, semantic::_NestedSchemaPlan,
        builders::Vector{_NestedWriteLeafBuilder}, rows::Int,
        budget::_LiveByteBudget)
    length(fragments) == length(plans) || throw(AssertionError(
        "nested writer field schema and plan counts differ"))
    _reservearray!(budget, WriteFieldPlan, length(plans))
    fields = WriteFieldPlan[]
    sizehint!(fields, length(plans))
    for (fragment, plan) in zip(fragments, plans)
        range = getfield(getfield(plan, :semantic), :leaf_range)
        isempty(range) && throw(ArgumentError(
            "zero-leaf fields cannot be written to Parquet"))
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

function _nestedwritecompile(shapes::Vector{_NestedWriteShape},
        fragments::Vector{Vector{Metadata.SchemaElement}}, limits::Limits,
        budget::_LiveByteBudget)
    length(shapes) == length(fragments) || throw(AssertionError(
        "nested writer shape and schema fragment counts differ"))
    length(shapes) <= typemax(Int32) || throw(ArgumentError(
        "Parquet schema has more than Int32 top-level fields"))
    total = 1
    for fragment in fragments
        total = try
            Base.checked_add(total, length(fragment))
        catch err
            err isa OverflowError || rethrow()
            throw(LimitError(:container_elements, typemax(Int64),
                limits.max_container_elements))
        end
        _checklimit(:container_elements, total,
            limits.max_container_elements)
    end
    _reservearray!(budget, Metadata.SchemaElement, total)
    elements = Metadata.SchemaElement[Metadata.SchemaElement(
        name="schema", num_children=Int32(length(shapes)))]
    sizehint!(elements, total)
    for fragment in fragments
        append!(elements, fragment)
    end
    schema = Schema(elements; limits=limits, budget=budget)
    semantic = _nestedplan(schema; limits=limits, budget=budget)
    length(semantic.root.children) == length(shapes) || throw(AssertionError(
        "nested writer top-level schema changed during semantic compilation"))
    _reservearray!(budget, _NestedWriteNodePlan, length(shapes))
    plans = _NestedWriteNodePlan[]
    sizehint!(plans, length(shapes))
    for (shape, child) in zip(shapes, semantic.root.children)
        push!(plans, _nestedwritebind(shape, child, budget))
    end
    return semantic, plans
end

function _nestedwritecounts(leaves::Int, budget::_LiveByteBudget)
    _reservearray!(budget, _NestedWriteLeafCount, leaves)
    _reserveobjects!(budget, leaves)
    counts = _NestedWriteLeafCount[]
    sizehint!(counts, leaves)
    for _ in 1:leaves
        push!(counts, _NestedWriteLeafCount(Int64(0), Int64(0), Int64(0)))
    end
    return counts
end

function _nestedwriteboundarybytes(leaves::Int, rows::Int)
    bytes = _materializedproduct(3,
        _materializedarraybytes(Vector{Int64}, leaves))
    inner = _materializedarraybytes(Int64, rows + 1)
    bytes = _materializedsum(bytes,
        _materializedproduct(3 * leaves, inner))
    return _materializedsum(bytes, _MATERIALIZED_OBJECT_BYTES)
end

function _nestedwritecountcontext(counts::Vector{_NestedWriteLeafCount},
        semantic::_NestedSchemaPlan, rows::Int, limits::Limits,
        budget::_LiveByteBudget, trace::Union{Nothing,_NestedWriteTrace}=nothing)
    leaves = length(semantic.leaves)
    length(counts) == leaves || throw(AssertionError(
        "nested writer count and semantic leaf totals differ"))
    charge = _nestedwriteboundarybytes(leaves, rows)
    _reserve!(budget, charge)
    entries = Vector{Vector{Int64}}(undef, leaves)
    dense = Vector{Vector{Int64}}(undef, leaves)
    payload = Vector{Vector{Int64}}(undef, leaves)
    for index in 1:leaves
        entries[index] = zeros(Int64, rows + 1)
        dense[index] = zeros(Int64, rows + 1)
        payload[index] = zeros(Int64, rows + 1)
    end
    return _NestedWriteCountContext(counts, limits, semantic, entries, dense,
        payload, trace), charge
end

function _nestedwriterawcumulative(element::Metadata.SchemaElement,
        dense::Int64, payload::Int64)
    physical = element.type_
    physical == Metadata.Type.BOOLEAN && return cld(dense, Int64(8))
    physical in (Metadata.Type.INT32, Metadata.Type.FLOAT) &&
        return Base.checked_mul(dense, Int64(4))
    physical in (Metadata.Type.INT64, Metadata.Type.DOUBLE) &&
        return Base.checked_mul(dense, Int64(8))
    physical == Metadata.Type.BYTE_ARRAY && return Base.checked_add(payload,
        Base.checked_mul(dense, Int64(4)))
    physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY && return payload
    throw(ArgumentError("unsupported writer physical type $physical"))
end

function _nestedwritefinishrow!(context::_NestedWriteCountContext,
        range, row::Int)
    context.entry_offsets === nothing && return
    semantic = something(context.semantic)
    for rawindex in range
        index = Int(rawindex)
        count = context.counts[index]
        context.entry_offsets[index][row + 1] = count.entries
        context.dense_offsets[index][row + 1] = count.dense
        element = semantic.leaves[index].source.element
        context.payload_offsets[index][row + 1] =
            _nestedwriterawcumulative(element, count.dense,
                count.payload_bytes)
    end
    return
end

function _nestedwritefinishrow!(context::_NestedWriteEmitContext,
        range, row::Int)
    context.boundaries === nothing && return
    boundaries = context.boundaries
    semantic = something(boundaries.semantic)
    for rawindex in range
        index = Int(rawindex)
        builder = context.builders[index]
        builder.entry_position == boundaries.entry_offsets[index][row + 1] ||
            throw(ArgumentError(
                "nested Parquet input changed its per-row level-entry count between passes"))
        builder.dense_position == boundaries.dense_offsets[index][row + 1] ||
            throw(ArgumentError(
                "nested Parquet input changed its per-row dense-value count between passes"))
        element = semantic.leaves[index].source.element
        payload = _nestedwriterawcumulative(element,
            Int64(builder.dense_position), builder.payload_position)
        payload == boundaries.payload_offsets[index][row + 1] ||
            throw(ArgumentError(
                "nested Parquet input changed its per-row physical payload between passes"))
    end
    return
end

function _nestedwritevalidateboundaries(context::_NestedWriteCountContext,
        rows::Int)
    context.entry_offsets === nothing && return
    semantic = something(context.semantic)
    for index in eachindex(context.counts, semantic.leaves)
        entries = context.entry_offsets[index]
        dense = context.dense_offsets[index]
        payload = context.payload_offsets[index]
        length(entries) == rows + 1 == length(dense) == length(payload) ||
            throw(AssertionError("nested writer count-prefix lengths differ"))
        first(entries) == first(dense) == first(payload) == 0 ||
            throw(AssertionError("nested writer count prefixes do not start at zero"))
        for row in 1:rows
            entries[row + 1] > entries[row] || throw(ArgumentError(
                "every top-level row must add a level entry to every leaf"))
            dense[row + 1] >= dense[row] && payload[row + 1] >= payload[row] ||
                throw(AssertionError("nested writer count prefixes are not monotonic"))
        end
        count = context.counts[index]
        last(entries) == count.entries && last(dense) == count.dense ||
            throw(AssertionError("nested writer count prefixes have wrong terminals"))
        element = semantic.leaves[index].source.element
        last(payload) == _nestedwriterawcumulative(element, count.dense,
            count.payload_bytes) || throw(AssertionError(
                "nested writer payload prefix has the wrong terminal"))
    end
    return
end

function _nestedwriterowlowerbound(element::Metadata.SchemaElement,
        choice::Union{Nothing,WriteEncodingChoice}, raw::Int64)
    choice === nothing && return raw
    choice.dictionary && return Int64(0)
    encoding = choice.encoding
    encoding === nothing && return raw
    encoding in (Metadata.Encoding.PLAIN,
        Metadata.Encoding.BYTE_STREAM_SPLIT) && return raw
    return Int64(0)
end

function _nestedwritepreflightrows(context::_NestedWriteCountContext,
        choices::Union{Nothing,Vector{WriteEncodingChoice}})
    context.entry_offsets === nothing && return
    semantic = something(context.semantic)
    choices === nothing || length(choices) == length(semantic.leaves) ||
        throw(AssertionError("writer preflight choice and leaf counts differ"))
    rows = length(first(context.entry_offsets)) - 1
    for index in eachindex(semantic.leaves)
        entries = context.entry_offsets[index]
        dense = context.dense_offsets[index]
        payload = context.payload_offsets[index]
        element = semantic.leaves[index].source.element
        choice = choices === nothing ? nothing : choices[index]
        for row in 1:rows
            entrycount = entries[row + 1] - entries[row]
            entrycount <= typemax(Int32) || throw(LimitError(:page_values,
                entrycount, Int64(typemax(Int32))))
            raw = if element.type_ == Metadata.Type.BOOLEAN
                cld(dense[row + 1] - dense[row], Int64(8))
            else
                payload[row + 1] - payload[row]
            end
            lower = _nestedwriterowlowerbound(element, choice, raw)
            lower <= context.limits.max_page_bytes || throw(LimitError(
                :page_bytes, lower, context.limits.max_page_bytes))
        end
    end
    return
end

function _nestedwritepackageaccesscheck(::AbstractVector)
    return
end

function _nestedwritepackageaccesscheck(values::ListVector)
    _validatelistvector(values)
    return
end

function _nestedwritepackageaccesscheck(values::StructVector)
    _validatestructvector(values)
    return
end

function _nestedwritepackageaccesscheck(values::MapVector)
    _validatemapvector(values)
    return
end

function _nestedwritecolumnaccesscheck(values::AbstractVector, count::Int,
        first::Int, last::Int)
    _nestedvectorcount(values, "nested Parquet input") == count || throw(ArgumentError(
        "nested Parquet input changed its vector length during a writer pass"))
    currentfirst, currentlast = _nestedvectoraxes(values,
        "nested Parquet input")
    currentfirst == first && currentlast == last || throw(
        ArgumentError(
            "nested Parquet input changed its vector axes during a writer pass"))
    return
end

function _nestedwritescanrows!(context, plans::Vector{_NestedWriteNodePlan},
        values::Vector{AbstractVector}, rows::Int)
    length(plans) == length(values) || throw(AssertionError(
        "nested writer plan and input column counts differ"))
    trace = context.trace
    budget = trace === nothing ? nothing : trace.budget
    stack::Union{Nothing,_NestedWritePassStack{_NestedWriteShredAction}} =
        nothing
    try
        for index in eachindex(plans, values)
            plan = plans[index]
            column = values[index]
            expected = _nestedvectorcount(column, "nested Parquet column")
            first, last = _nestedvectoraxes(column, "nested Parquet column")
            shape = getfield(plan, :shape)
            snapshot = _nestedwritesourcesnapshot(shape)
            count = 0
            for rowindex in eachindex(column)
                _nestedwritecolumnaccesscheck(column, expected, first, last)
                count += 1
                value, row_witness = _nestedwriterowaccess(column, rowindex,
                    snapshot)
                if plan isa _NestedWriteLeafPlan
                    _nestedwriteshred!(context, plan, value, UInt64(0),
                        row_witness)
                else
                    if stack === nothing
                        budget === nothing &&
                            (budget = _LiveByteBudget(context.limits))
                        stack = _nestedwritepassstackstart(
                            _NestedWriteShredAction, something(budget))
                    end
                    _nestedwriteshrediterative!(something(stack), context,
                        plan, value, nothing, false, UInt64(0), row_witness,
                        something(budget))
                end
                _nestedwriterowcheck(row_witness)
                range = getfield(getfield(plan, :semantic), :leaf_range)
                _nestedwritefinishrow!(context, range, count)
            end
            count == rows || throw(ArgumentError(
                "nested Parquet input changed its row count between passes"))
        end
    finally
        stack === nothing || _nestedwritepassstackrelease!(stack,
            something(budget))
    end
    return
end

function _nestedwritevalidatedinputs(input_columns::AbstractVector, rows::Int,
        limits::Limits, budget::_LiveByteBudget)
    isempty(input_columns) && throw(ArgumentError(
        "a Parquet table must have at least one column"))
    _checklimit(:container_elements, length(input_columns),
        limits.max_container_elements)
    _reservearray!(budget, String, length(input_columns))
    _reservearray!(budget, AbstractVector, length(input_columns))
    _reserveobjects!(budget)
    names = String[]
    values = AbstractVector[]
    seen = Set{String}()
    sizehint!(names, length(input_columns))
    sizehint!(values, length(input_columns))
    for column in input_columns
        name = _nestedwriteinputname(column)
        occursin('\0', name) && throw(ArgumentError(
            "Parquet top-level field names cannot contain NUL"))
        name in seen && throw(ArgumentError(
            "Parquet column names must be unique"))
        _reserveobjects!(budget)
        push!(seen, name)
        columnvalues = _nestedwriteinputvalues(column)
        columnvalues isa AbstractVector || throw(ArgumentError(
            "Parquet columns must be vectors"))
        _nestedwritecheckcontainer(columnvalues, limits,
            "Parquet column $(repr(name))")
        count = _nestedvectorcount(columnvalues,
            "Parquet column $(repr(name))")
        count == rows || throw(ArgumentError(
            "Parquet column $(repr(name)) has $count rows; " *
            "expected $rows"))
        push!(names, name)
        push!(values, columnvalues)
    end
    return names, values
end

function _nestedwriteshapes(names::Vector{String},
        values::Vector{AbstractVector}, limits::Limits,
        budget::_LiveByteBudget,
        topology::_NestedWriteTopologySnapshot)
    _reservearray!(budget, _NestedWriteShape, length(values))
    shapes = _NestedWriteShape[]
    sizehint!(shapes, length(values))
    for index in eachindex(names, values)
        push!(shapes, _nestedwriteshape(names[index], eltype(values[index]),
            values[index], limits, budget; topology=topology))
    end
    return shapes
end

function _nestedwritescanaggregates!(shapes::Vector{_NestedWriteShape},
        values::Vector{AbstractVector}, rows::Int, limits::Limits,
        trace::Union{Nothing,_NestedWriteTrace}=nothing)
    budget = trace === nothing ? nothing : trace.budget
    stack::Union{Nothing,_NestedWritePassStack{_NestedWriteScanAction}} =
        nothing
    try
        for index in eachindex(shapes, values)
            shape = shapes[index]
            column = values[index]
            expected = _nestedvectorcount(column, "nested Parquet column")
            first, last = _nestedvectoraxes(column, "nested Parquet column")
            snapshot = _nestedwritesourcesnapshot(shape)
            count = 0
            for rowindex in eachindex(column)
                _nestedwritecolumnaccesscheck(column, expected, first, last)
                count += 1
                value, row_witness = _nestedwriterowaccess(column, rowindex,
                    snapshot)
                if shape isa _NestedWriteLeafShape
                    _nestedwritescanleaf!(shape, value, trace, row_witness)
                else
                    if stack === nothing
                        budget === nothing &&
                            (budget = _LiveByteBudget(limits))
                        stack = _nestedwritepassstackstart(
                            _NestedWriteScanAction, something(budget))
                    end
                    _nestedwritescanaggregate!(something(stack), shape, value,
                        limits, trace, row_witness, something(budget))
                end
                _nestedwriterowcheck(row_witness)
            end
            count == rows || throw(ArgumentError(
                "nested Parquet input changed its row count during schema inference"))
        end
    finally
        stack === nothing || _nestedwritepassstackrelease!(stack,
            something(budget))
    end
    return
end

function _nestedwriteschemafragments(shapes::Vector{_NestedWriteShape},
        limits::Limits, budget::_LiveByteBudget)
    _reservearray!(budget, Vector{Metadata.SchemaElement}, length(shapes))
    fragments = Vector{Vector{Metadata.SchemaElement}}(undef, length(shapes))
    for index in eachindex(shapes)
        fragments[index] = _nestedwriteschema(shapes[index], limits, budget)
    end
    return fragments
end

function _nestedwritefields(input_columns::AbstractVector, rows::Integer,
        limits::Limits, budget::_LiveByteBudget; preflight=nothing,
        sourcevalidator=nothing)
    rows >= 0 || throw(ArgumentError("Parquet row count must be nonnegative"))
    rows <= typemax(Int) || throw(ArgumentError(
        "Parquet row count exceeds the Julia index range"))
    rowcount = Int(rows)
    _checklimit(:container_elements, rowcount,
        limits.max_container_elements)
    start = _budgetused(budget)
    try
        names, values = _nestedwritevalidatedinputs(input_columns, rowcount,
            limits, budget)
        topology = _nestedwritetopology(input_columns, names, values, limits,
            budget)
        _nestedwritebarrier!(topology, sourcevalidator, budget)
        shapes = _nestedwriteshapes(names, values, limits, budget, topology)
        _nestedwritebarrier!(topology, sourcevalidator, budget)
        trace = _nestedwritetrace(budget, topology)
        _nestedwritescanaggregates!(shapes, values, rowcount, limits, trace)
        _nestedwritebarrier!(topology, sourcevalidator, budget)
        fragments = _nestedwriteschemafragments(shapes, limits, budget)
        semantic, plans = _nestedwritecompile(shapes, fragments, limits, budget)
        _nestedwritebarrier!(topology, sourcevalidator, budget)
        choices, choicecharge = preflight === nothing ?
            (nothing, Int64(0)) : preflight(semantic)
        _nestedwritebarrier!(topology, sourcevalidator, budget)
        counts = _nestedwritecounts(length(semantic.leaves), budget)
        countcontext, boundarycharge = _nestedwritecountcontext(counts,
            semantic, rowcount, limits, budget, trace)
        _nestedwritetracecompare!(trace)
        _nestedwritescanrows!(countcontext, plans, values, rowcount)
        _nestedwritetracefinishcompare!(trace)
        _nestedwritebarrier!(topology, sourcevalidator, budget)
        _nestedwritevalidateboundaries(countcontext, rowcount)
        _nestedwritepreflightrows(countcontext, choices)
        iszero(choicecharge) || _release!(budget, choicecharge)
        choices = nothing
        builders = _nestedwritebuilders(counts, semantic, budget)
        _nestedwritebarrier!(topology, sourcevalidator, budget)
        emitcontext = _NestedWriteEmitContext(builders, counts, limits,
            countcontext, trace)
        _nestedwritetracecompare!(trace)
        _nestedwritescanrows!(emitcontext, plans, values, rowcount)
        _nestedwritetracefinishcompare!(trace)
        _nestedwritebarrier!(topology, sourcevalidator, budget)
        _nestedwritevalidatebuilders(builders, counts, semantic, rowcount)
        fields = _nestedwritefinishfields(fragments, plans, semantic, builders,
            rowcount, budget)
        _nestedwritebarrier!(topology, sourcevalidator, budget)
        _release!(budget, boundarycharge)
        countcontext = nothing
        emitcontext = nothing
        _nestedwritetracerelease!(trace)
        trace = nothing
        _nestedwritetopologyrelease!(topology, budget)
        topology = nothing
        return fields
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end
