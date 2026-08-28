import Tables
import Dates

mutable struct N5HActionVector{T,F} <: AbstractVector{T}
    values::Vector{T}
    calls::Int
    trigger::Int
    action::F
end

function N5HActionVector(values::Vector{T}, trigger::Int, action::F) where {T,F}
    return N5HActionVector{T,F}(values, 0, trigger, action)
end

function N5HActionVector(action::F, values::Vector{T}, trigger::Int) where {T,F}
    return N5HActionVector(values, trigger, action)
end

function Base.IndexStyle(::Type{<:N5HActionVector})
    return IndexLinear()
end

function Base.size(values::N5HActionVector)
    return size(values.values)
end

function Base.getindex(values::N5HActionVector, index::Int)
    values.calls += 1
    values.calls == values.trigger && values.action()
    return values.values[index]
end

mutable struct N5HSequenceVector{T} <: AbstractVector{T}
    values::Vector{T}
    calls::Int
end

function Base.IndexStyle(::Type{<:N5HSequenceVector})
    return IndexLinear()
end

function Base.size(::N5HSequenceVector)
    return (1,)
end

function Base.getindex(values::N5HSequenceVector, ::Int)
    values.calls += 1
    return values.values[min(values.calls, length(values.values))]
end

mutable struct N5HAxisVector{T} <: AbstractVector{T}
    values::Vector{T}
    shifted::Bool
end

mutable struct N5HBackingAxisVector{T} <: AbstractVector{T}
    values::Vector{T}
    calls::Int
    trigger::Int
    shifted::Bool
end

function Base.IndexStyle(::Type{<:N5HAxisVector})
    return IndexCartesian()
end

function Base.size(values::N5HAxisVector)
    return size(values.values)
end

function Base.axes(values::N5HAxisVector)
    count = length(values.values)
    return values.shifted ? (0:(count - 1),) : (Base.OneTo(count),)
end

function Base.getindex(values::N5HAxisVector, index::Int)
    values.shifted = true
    return values.values[index]
end

function Base.IndexStyle(::Type{<:N5HBackingAxisVector})
    return IndexCartesian()
end

function Base.size(values::N5HBackingAxisVector)
    return size(values.values)
end

function Base.axes(values::N5HBackingAxisVector)
    count = length(values.values)
    return values.shifted ? (0:(count - 1),) : (Base.OneTo(count),)
end

function Base.getindex(values::N5HBackingAxisVector, index::Int)
    values.calls += 1
    values.calls == values.trigger && (values.shifted = true)
    return values.values[index]
end

mutable struct N5HAlternatingDict <: AbstractDict{Int32,Int32}
    passes::Int
end

struct N5HCountDict <: AbstractDict{Int32,Int32}
    count::Int
end

struct N5HLyingCycleDict <: AbstractDict{Int32,Int32} end

mutable struct N5HFinalMutKeyDict{K,V} <: AbstractDict{K,V}
    item::Pair{K,V}
    mutated::Bool
    attacks::Int
end

mutable struct N5HFinalMutMap{K,V} <: AbstractDict{K,V}
    item::Pair{K,V}
    mutated::Bool
    attacks::Int
end

mutable struct N5HNothingStateDict{K,V} <: AbstractDict{K,V}
    item::Pair{K,V}
    calls::Int
    length_calls::Int
end

mutable struct N5HRestoringChild <: AbstractVector{Int32}
    owner::Base.RefValue{Any}
    armed::Bool
    restores::Int
end

mutable struct N5HCountingVector <: AbstractVector{Int32}
    calls::Int
end

mutable struct N5HParentAfterChildDict <: AbstractDict{Int32,Any}
    parent::Parquet.ListVector
    attacks::Int
end

mutable struct N5HChangingSourceDict{V,E} <: AbstractDict{Int32,V}
    old::V
    new::V
    starts::Int
    terminals::Int
    error::E
end

struct N5HSecondTouchDict{V} <: AbstractDict{Int32,Any}
    second::V
end

struct N5HRestoreVector{T,D} <: AbstractVector{T}
    value::T
    target::D
    key::Bool
end

mutable struct N5HLengthMutationVector{T,F} <: AbstractVector{T}
    values::Vector{T}
    action::F
    armed::Bool
    calls::Int
end

mutable struct N5HSequencedByteKey <: AbstractVector{UInt8}
    passes::Int
    attack::Int
end

struct N5HBadByteKey <: AbstractVector{UInt8} end

struct N5HThrowByteKey{E} <: AbstractVector{UInt8}
    error::E
end

mutable struct N5HAxisShiftByteKey <: AbstractVector{UInt8}
    shifted::Bool
end

mutable struct N5HShiftBadByteKey <: AbstractVector{UInt8}
    shifted::Bool
end

mutable struct N5HSequencedShiftBadByteKey <: AbstractVector{UInt8}
    calls::Int
    attack::Int
    shifted::Bool
end

mutable struct N5HSequencedLengthByteKey <: AbstractVector{UInt8}
    calls::Int
end

struct N5HThrowLengthKey{E} <: AbstractVector{Int32}
    error::E
end

mutable struct N5HChain <: AbstractVector{Any}
    child::Any
end

mutable struct N5HLyingInt32Vector <: AbstractVector{Int32}
    child::Any
    calls::Int
end

mutable struct N5HMutableSentinel <: Exception
    value::Int
end

struct N5HThrowAnyVector{E} <: AbstractVector{Any}
    error::E
end

mutable struct N5HHiddenNestedChild <: AbstractVector{Int32}
    values::Vector{Int32}
    calls::Int
    owner::Base.RefValue{Any}
end

struct N5HThrowMetricChild{E} <: AbstractVector{Int32}
    error::E
end

struct N5HFalseBoundsChild <: AbstractVector{Int32} end

struct N5HThrowBoundsChild{E} <: AbstractVector{Int32}
    error::E
end

mutable struct N5HShiftOuter{T} <: AbstractVector{T}
    value::T
    shifted::Bool
end

mutable struct N5HSequencedString <: AbstractString
    calls::Int
end

mutable struct N5HSequencedCodeunitVector <: AbstractVector{UInt8}
    reads::Int
end

struct N5HSameSourceString <: AbstractString
    bytes::N5HSequencedCodeunitVector
end

struct N5HShiftBadString <: AbstractString
    bytes::N5HSequencedShiftBadByteKey
end

mutable struct N5HNCodeunitAxisBytes <: AbstractVector{UInt8}
    shifted::Bool
end

mutable struct N5HNCodeunitAxisString <: AbstractString
    bytes::N5HNCodeunitAxisBytes
    calls::Int
    attack::Int
end

function Base.length(values::N5HCountDict)
    return values.count
end

function Base.getindex(::N5HCountDict, key::Int32)
    return Int32(10) * key
end

function Base.iterate(values::N5HCountDict, state::Int=1)
    state > values.count && return nothing
    key = Int32(state)
    return key => values[key], state + 1
end

function Base.length(::N5HLyingCycleDict)
    return 1
end

function Base.getindex(::N5HLyingCycleDict, ::Int32)
    return Int32(1)
end

function Base.iterate(values::N5HLyingCycleDict, state::Int=1)
    state == 1 || return nothing
    return values => Int32(1), 2
end

function N5HFinalMutKeyDict(item::Pair{K,V}) where {K,V}
    return N5HFinalMutKeyDict{K,V}(item, false, 0)
end

function N5HFinalMutMap(item::Pair{K,V}) where {K,V}
    return N5HFinalMutMap{K,V}(item, false, 0)
end

function Base.length(values::N5HFinalMutKeyDict)
    if values.mutated
        values.item.first.offsets[2] = 1
        values.mutated = false
    end
    return 1
end

function Base.length(values::N5HFinalMutMap)
    if values.mutated
        values.item.second.offsets[2] = 1
        values.mutated = false
    end
    return 1
end

function Base.getindex(values::N5HFinalMutKeyDict{K,V}, key::K) where {K,V}
    isequal(key, values.item.first) || throw(KeyError(key))
    return values.item.second
end

function Base.getindex(values::N5HFinalMutMap{K,V}, key::K) where {K,V}
    isequal(key, values.item.first) || throw(KeyError(key))
    return values.item.second
end

function Base.iterate(values::N5HFinalMutKeyDict, state::Int=1)
    state == 1 && return values.item, 2
    values.item.first.offsets[2] = 0
    values.mutated = true
    values.attacks += 1
    return nothing
end

function Base.iterate(values::N5HFinalMutMap, state::Int=1)
    state == 1 && return values.item, 2
    values.item.second.offsets[2] = 0
    values.mutated = true
    values.attacks += 1
    return nothing
end

function Base.length(values::N5HNothingStateDict)
    values.length_calls += 1
    throw(AssertionError("dictionary length must not be called"))
end

function Base.getindex(values::N5HNothingStateDict{K,V}, key::K) where {K,V}
    isequal(key, values.item.first) || throw(KeyError(key))
    return values.item.second
end

function Base.iterate(values::N5HNothingStateDict)
    values.calls += 1
    return values.item, nothing
end

function Base.iterate(values::N5HNothingStateDict, ::Nothing)
    values.calls += 1
    return nothing
end

function Base.IndexStyle(::Type{N5HRestoringChild})
    return IndexLinear()
end

function Base.size(::N5HRestoringChild)
    return (1,)
end

function Base.axes(::N5HRestoringChild)
    return (Base.OneTo(1),)
end

function Base.length(values::N5HRestoringChild)
    owner = values.owner[]
    if values.armed && owner !== nothing && owner.offsets[2] == 0
        owner.offsets[2] = 1
        values.restores += 1
    end
    return 1
end

function Base.getindex(::N5HRestoringChild, index::Int)
    index == 1 || throw(BoundsError(index))
    return Int32(11)
end

function Base.IndexStyle(::Type{N5HCountingVector})
    return IndexLinear()
end

function Base.size(values::N5HCountingVector)
    values.calls += 1
    return (1,)
end

function Base.getindex(::N5HCountingVector, index::Int)
    index == 1 || throw(BoundsError(index))
    return Int32(11)
end

function Base.length(::N5HParentAfterChildDict)
    throw(AssertionError("dictionary length must not be called"))
end

function Base.getindex(values::N5HParentAfterChildDict, key::Int32)
    key == 1 && return Parquet.ListValue(values.parent.values, 1, 1)
    key == 2 && return values.parent
    throw(KeyError(key))
end

function Base.iterate(values::N5HParentAfterChildDict, state::Int=1)
    state == 1 && return Int32(1) => values[Int32(1)], 2
    state == 2 && return Int32(2) => values[Int32(2)], 3
    values.parent.offsets[2] = 0
    values.attacks += 1
    return nothing
end

function Base.length(::N5HChangingSourceDict)
    throw(AssertionError("dictionary length must not be called"))
end

function Base.getindex(values::N5HChangingSourceDict, key::Int32)
    key == 1 || throw(KeyError(key))
    return isone(values.starts) ? values.old : values.new
end

function Base.iterate(values::N5HChangingSourceDict)
    values.starts += 1
    source = isone(values.starts) ? values.old : values.new
    return Int32(1) => source, 2
end

function Base.iterate(values::N5HChangingSourceDict, state::Int)
    state == 2 || throw(ArgumentError("invalid dictionary iterator state"))
    values.terminals += 1
    values.starts >= 2 && throw(values.error)
    return nothing
end

function Base.length(::N5HSecondTouchDict)
    throw(AssertionError("dictionary length must not be called"))
end

function Base.getindex(values::N5HSecondTouchDict, key::Int32)
    key == 1 && return Int32(1)
    key == 2 && return values.second
    throw(KeyError(key))
end

function Base.iterate(values::N5HSecondTouchDict, state::Int=1)
    state == 1 && return Int32(1) => values[Int32(1)], 2
    state == 2 && return Int32(2) => values[Int32(2)], 3
    return nothing
end

function Base.IndexStyle(::Type{<:N5HRestoreVector})
    return IndexLinear()
end

function Base.size(::N5HRestoreVector)
    return (1,)
end

function Base.axes(::N5HRestoreVector)
    return (Base.OneTo(1),)
end

function Base.length(values::N5HRestoreVector)
    target = values.target
    if target.mutated
        nested = values.key ? target.item.first : target.item.second
        nested.offsets[2] = 1
        target.mutated = false
    end
    return 1
end

function Base.getindex(values::N5HRestoreVector, index::Int)
    index == 1 || throw(BoundsError(values, index))
    return values.value
end

function Base.IndexStyle(::Type{<:N5HLengthMutationVector})
    return IndexLinear()
end

function Base.size(values::N5HLengthMutationVector)
    return size(values.values)
end

function Base.axes(values::N5HLengthMutationVector)
    return axes(values.values)
end

function Base.length(values::N5HLengthMutationVector)
    if values.armed
        values.calls += 1
        values.armed = false
        values.action()
    end
    return length(values.values)
end

function Base.getindex(values::N5HLengthMutationVector, index::Int)
    return values.values[index]
end

function Base.IndexStyle(::Type{N5HSequencedByteKey})
    return IndexLinear()
end

function Base.size(::N5HSequencedByteKey)
    return (1,)
end

function Base.iterate(key::N5HSequencedByteKey, state::Int=1)
    state == 1 || return nothing
    key.passes += 1
    byte = key.passes == key.attack ? UInt8(0x62) : UInt8(0x61)
    return byte, 2
end

function Base.getindex(key::N5HSequencedByteKey, ::Int)
    item = iterate(key)
    item === nothing && throw(BoundsError(key, 1))
    return first(item)
end

function Base.IndexStyle(::Type{N5HBadByteKey})
    return IndexLinear()
end

function Base.size(::N5HBadByteKey)
    return (1,)
end

function Base.getindex(::N5HBadByteKey, ::Int)
    return 300
end

function Base.IndexStyle(::Type{<:N5HThrowByteKey})
    return IndexLinear()
end

function Base.size(::N5HThrowByteKey)
    return (1,)
end

function Base.getindex(key::N5HThrowByteKey, ::Int)
    throw(key.error)
end

function Base.IndexStyle(::Type{N5HAxisShiftByteKey})
    return IndexCartesian()
end

function Base.size(::N5HAxisShiftByteKey)
    return (1,)
end

function Base.axes(key::N5HAxisShiftByteKey)
    return key.shifted ? (0:0,) : (Base.OneTo(1),)
end

function Base.getindex(key::N5HAxisShiftByteKey, ::Int)
    key.shifted = true
    return UInt8(0x61)
end

function Base.IndexStyle(::Type{N5HShiftBadByteKey})
    return IndexCartesian()
end

function Base.size(::N5HShiftBadByteKey)
    return (1,)
end

function Base.axes(key::N5HShiftBadByteKey)
    return key.shifted ? (0:0,) : (Base.OneTo(1),)
end

function Base.getindex(key::N5HShiftBadByteKey, ::Int)
    key.shifted = true
    return 300
end

function Base.IndexStyle(::Type{N5HSequencedShiftBadByteKey})
    return IndexCartesian()
end

function Base.size(::N5HSequencedShiftBadByteKey)
    return (1,)
end

function Base.axes(key::N5HSequencedShiftBadByteKey)
    return key.shifted ? (0:0,) : (Base.OneTo(1),)
end

function Base.getindex(key::N5HSequencedShiftBadByteKey, ::Int)
    key.calls += 1
    if key.calls == key.attack
        key.shifted = true
        return 300
    end
    return UInt8(0x61)
end

function Base.IndexStyle(::Type{N5HSequencedLengthByteKey})
    return IndexLinear()
end

function Base.size(::N5HSequencedLengthByteKey)
    return (2,)
end

function Base.length(key::N5HSequencedLengthByteKey)
    key.calls += 1
    return key.calls == 1 ? 1 : 2
end

function Base.getindex(::N5HSequencedLengthByteKey, ::Int)
    return UInt8(0x61)
end

function Base.IndexStyle(::Type{<:N5HThrowLengthKey})
    return IndexLinear()
end

function Base.size(::N5HThrowLengthKey)
    return (1,)
end

function Base.length(key::N5HThrowLengthKey)
    throw(key.error)
end

function Base.getindex(::N5HThrowLengthKey, ::Int)
    return Int32(1)
end

function Base.IndexStyle(::Type{N5HChain})
    return IndexLinear()
end

function Base.size(::N5HChain)
    return (1,)
end

function Base.getindex(value::N5HChain, index::Int)
    @boundscheck checkbounds(value, index)
    return value.child
end

function Base.IndexStyle(::Type{N5HLyingInt32Vector})
    return IndexLinear()
end

function Base.size(::N5HLyingInt32Vector)
    return (1,)
end

function Base.getindex(value::N5HLyingInt32Vector, index::Int)
    @boundscheck checkbounds(value, index)
    value.calls += 1
    return value.child
end

function Base.IndexStyle(::Type{<:N5HThrowAnyVector})
    return IndexLinear()
end

function Base.size(::N5HThrowAnyVector)
    return (1,)
end

function Base.getindex(value::N5HThrowAnyVector, ::Int)
    throw(value.error)
end

function Base.IndexStyle(::Type{N5HHiddenNestedChild})
    return IndexLinear()
end

function Base.size(value::N5HHiddenNestedChild)
    return size(value.values)
end

function Base.getindex(value::N5HHiddenNestedChild, index::Int)
    owner = value.owner[]
    if owner !== nothing
        value.calls += 1
        phase = mod1(value.calls, 3)
        phase == 1 && (owner.offsets[2] = 0)
        phase == 3 && (owner.offsets[2] = 1)
    end
    return value.values[index]
end

function Base.IndexStyle(::Type{<:N5HThrowMetricChild})
    return IndexLinear()
end

function Base.size(value::N5HThrowMetricChild)
    throw(value.error)
end

function Base.getindex(::N5HThrowMetricChild, ::Int)
    return Int32(0)
end

function Base.IndexStyle(::Type{N5HFalseBoundsChild})
    return IndexLinear()
end

function Base.size(::N5HFalseBoundsChild)
    return (1,)
end

function Base.getindex(value::N5HFalseBoundsChild, index::Int)
    throw(BoundsError(value, index))
end

function Base.checkbounds(::Type{Bool}, ::N5HFalseBoundsChild, ::Int)
    return false
end

function Base.IndexStyle(::Type{<:N5HThrowBoundsChild})
    return IndexLinear()
end

function Base.size(::N5HThrowBoundsChild)
    return (1,)
end

function Base.getindex(value::N5HThrowBoundsChild, index::Int)
    throw(BoundsError(value, index))
end

function Base.checkbounds(::Type{Bool}, value::N5HThrowBoundsChild, ::Int)
    throw(value.error)
end

function Base.IndexStyle(::Type{<:N5HShiftOuter})
    return IndexCartesian()
end

function Base.size(::N5HShiftOuter)
    return (1,)
end

function Base.axes(value::N5HShiftOuter)
    return value.shifted ? (0:0,) : (Base.OneTo(1),)
end

function Base.getindex(value::N5HShiftOuter, ::Int)
    value.shifted = true
    return value.value
end

function Base.codeunit(::Type{N5HSequencedString})
    return UInt8
end

function Base.ncodeunits(::N5HSequencedString)
    return 1
end

function Base.codeunit(::N5HSequencedString, index::Integer)
    index == 1 || throw(BoundsError(index))
    return UInt8(0x61)
end

function Base.codeunits(value::N5HSequencedString)
    value.calls += 1
    return UInt8[value.calls == 1 ? 0x61 : 0x62]
end

function Base.IndexStyle(::Type{N5HSequencedCodeunitVector})
    return IndexLinear()
end

function Base.size(::N5HSequencedCodeunitVector)
    return (1,)
end

function Base.getindex(value::N5HSequencedCodeunitVector, ::Int)
    value.reads += 1
    return UInt8(0xff)
end

function Base.codeunit(::Type{N5HSameSourceString})
    return UInt8
end

function Base.ncodeunits(::N5HSameSourceString)
    return 1
end

function Base.codeunit(value::N5HSameSourceString, index::Integer)
    return value.bytes[index]
end

function Base.codeunits(value::N5HSameSourceString)
    return value.bytes
end

function Base.codeunit(::Type{N5HShiftBadString})
    return UInt8
end

function Base.ncodeunits(::N5HShiftBadString)
    return 1
end

function Base.codeunit(value::N5HShiftBadString, index::Integer)
    return value.bytes[Int(index)]
end

function Base.codeunits(value::N5HShiftBadString)
    return value.bytes
end

function Base.IndexStyle(::Type{N5HNCodeunitAxisBytes})
    return IndexCartesian()
end

function Base.size(::N5HNCodeunitAxisBytes)
    return (1,)
end

function Base.axes(value::N5HNCodeunitAxisBytes)
    return value.shifted ? (0:0,) : (Base.OneTo(1),)
end

function Base.getindex(::N5HNCodeunitAxisBytes, ::Int)
    return UInt8(0x61)
end

function Base.codeunit(::Type{N5HNCodeunitAxisString})
    return UInt8
end

function Base.ncodeunits(value::N5HNCodeunitAxisString)
    value.calls += 1
    value.calls == value.attack && (value.bytes.shifted = true)
    return 1
end

function Base.codeunit(value::N5HNCodeunitAxisString, index::Integer)
    return value.bytes[Int(index)]
end

function Base.codeunits(value::N5HNCodeunitAxisString)
    return value.bytes
end

function Base.length(::N5HAlternatingDict)
    return 2
end

function Base.getindex(::N5HAlternatingDict, key::Int32)
    key == 1 && return Int32(10)
    key == 2 && return Int32(20)
    throw(KeyError(key))
end

function Base.iterate(values::N5HAlternatingDict)
    values.passes += 1
    reversed = iseven(values.passes)
    key = reversed ? Int32(2) : Int32(1)
    return key => values[key], (reversed, 2)
end

function Base.iterate(values::N5HAlternatingDict,
        state::Tuple{Bool,Int})
    reversed, position = state
    position > 2 && return nothing
    key = reversed ? Int32(1) : Int32(2)
    return key => values[key], (reversed, position + 1)
end

mutable struct N5HLiveTable
    names::Vector{Symbol}
    values::Vector{AbstractVector}
    replacement::AbstractVector
    calls::Int
    mutateat::Int
end

function Tables.istable(::Type{N5HLiveTable})
    return true
end

function Tables.columnaccess(::Type{N5HLiveTable})
    return true
end

function Tables.columns(table::N5HLiveTable)
    return table
end

function Tables.columnnames(table::N5HLiveTable)
    table.calls += 1
    table.calls == table.mutateat && (table.values[1] = table.replacement)
    return table.names
end

function Tables.getcolumn(table::N5HLiveTable, name::Symbol)
    index = findfirst(isequal(name), table.names)
    index === nothing && throw(KeyError(name))
    return table.values[index]
end

mutable struct N5HNameOrderTable
    names::Vector{Symbol}
    values::Vector{AbstractVector}
    calls::Int
end

function Tables.istable(::Type{N5HNameOrderTable})
    return true
end

function Tables.columnaccess(::Type{N5HNameOrderTable})
    return true
end

function Tables.columns(table::N5HNameOrderTable)
    return table
end

function Tables.columnnames(table::N5HNameOrderTable)
    table.calls += 1
    table.calls == 2 && reverse!(table.names)
    return table.names
end

function Tables.getcolumn(table::N5HNameOrderTable, name::Symbol)
    index = findfirst(isequal(name), table.names)
    index === nothing && throw(KeyError(name))
    return table.values[index]
end

mutable struct N5HRenameTable
    name::Symbol
    value::Vector{Int32}
    calls::Int
end

function Tables.istable(::Type{N5HRenameTable})
    return true
end

function Tables.columnaccess(::Type{N5HRenameTable})
    return true
end

function Tables.columns(table::N5HRenameTable)
    return table
end

function Tables.columnnames(table::N5HRenameTable)
    table.calls += 1
    table.calls == 2 && (table.name = :renamed)
    return (table.name,)
end

function Tables.getcolumn(table::N5HRenameTable, ::Symbol)
    return table.value
end

struct N5HExtraNames end

function Base.length(::N5HExtraNames)
    return 1
end

function Base.iterate(::N5HExtraNames, state::Int=1)
    state == 1 && return :a, 2
    state == 2 && return :b, 3
    return nothing
end

mutable struct N5HBadNamesTable
    calls::Int
    a::Vector{Int32}
    b::Vector{Int32}
end

function Tables.istable(::Type{N5HBadNamesTable})
    return true
end

function Tables.columnaccess(::Type{N5HBadNamesTable})
    return true
end

function Tables.columns(table::N5HBadNamesTable)
    return table
end

function Tables.columnnames(table::N5HBadNamesTable)
    table.calls += 1
    return table.calls == 1 ? (:a,) : N5HExtraNames()
end

function Tables.getcolumn(table::N5HBadNamesTable, name::Symbol)
    name === :a && return table.a
    name === :b && return table.b
    throw(KeyError(name))
end

struct N5HSentinelError <: Exception end

struct N5HThrowVector{T} <: AbstractVector{T}
    error::N5HSentinelError
end

function Base.IndexStyle(::Type{<:N5HThrowVector})
    return IndexLinear()
end

function Base.size(::N5HThrowVector)
    return (1,)
end

function Base.getindex(values::N5HThrowVector, ::Int)
    throw(values.error)
end

struct N5HZeroAxisVector{T} <: AbstractVector{T}
    values::Vector{T}
end

struct N5HNonIntLengthVector <: AbstractVector{Int32} end

struct N5HUIntSizeVector <: AbstractVector{Int32} end

struct N5HUIntAxisVector <: AbstractVector{Int32} end

struct N5HHugeVector <: AbstractVector{Int32} end

struct N5HHugeUIntDict <: AbstractDict{Int32,Int32} end

struct N5HThrowSizeVector{E} <: AbstractVector{Int32}
    error::E
end

function Base.IndexStyle(::Type{<:N5HZeroAxisVector})
    return IndexCartesian()
end

function Base.size(values::N5HZeroAxisVector)
    return size(values.values)
end

function Base.axes(values::N5HZeroAxisVector)
    return (0:(length(values.values) - 1),)
end

function Base.getindex(values::N5HZeroAxisVector, index::Int)
    return values.values[index + 1]
end

function Base.IndexStyle(::Type{N5HNonIntLengthVector})
    return IndexLinear()
end

function Base.size(::N5HNonIntLengthVector)
    return (1,)
end

function Base.length(::N5HNonIntLengthVector)
    return Int32(1)
end

function Base.getindex(::N5HNonIntLengthVector, ::Int)
    return Int32(1)
end

function Base.IndexStyle(::Type{N5HUIntSizeVector})
    return IndexLinear()
end

function Base.size(::N5HUIntSizeVector)
    return (UInt(1),)
end

function Base.length(::N5HUIntSizeVector)
    return UInt(1)
end

function Base.getindex(::N5HUIntSizeVector, ::Int)
    return Int32(1)
end

function Base.IndexStyle(::Type{N5HUIntAxisVector})
    return IndexCartesian()
end

function Base.size(::N5HUIntAxisVector)
    return (1,)
end

function Base.axes(::N5HUIntAxisVector)
    return (UInt(1):UInt(1),)
end

function Base.getindex(::N5HUIntAxisVector, ::Int)
    return Int32(1)
end

function Base.IndexStyle(::Type{N5HHugeVector})
    return IndexLinear()
end

function Base.size(::N5HHugeVector)
    return (typemax(Int),)
end

function Base.getindex(::N5HHugeVector, ::Int)
    return Int32(1)
end

function Base.length(::N5HHugeUIntDict)
    return typemax(UInt)
end

function Base.getindex(::N5HHugeUIntDict, key::Int32)
    return key
end

function Base.iterate(::N5HHugeUIntDict, state::Int=1)
    state == 1 || return nothing
    return Int32(1) => Int32(1), 2
end

function Base.IndexStyle(::Type{<:N5HThrowSizeVector})
    return IndexLinear()
end

function Base.size(values::N5HThrowSizeVector)
    throw(values.error)
end

function Base.getindex(::N5HThrowSizeVector, ::Int)
    return Int32(1)
end

function n5herror(f)
    try
        f()
        return nothing
    catch err
        return err
    end
end

function n5hchain(depth::Int, leaf=Int32(1))
    depth >= 0 || throw(ArgumentError("chain depth must be nonnegative"))
    value = leaf
    for _ in 1:depth
        value = N5HChain(value)
    end
    return value
end

function n5hcycle(depth::Int)
    depth >= 1 || throw(ArgumentError("cycle depth must be positive"))
    root = N5HChain(nothing)
    current = root
    for _ in 2:depth
        child = N5HChain(nothing)
        current.child = child
        current = child
    end
    current.child = root
    return root
end

function n5hlistphasemutation(trigger::Int)
    owner = Ref{Any}(nothing)
    child = N5HActionVector(Int32[1, 2], trigger) do
        pop!(owner[].offsets)
        return
    end
    column = Parquet.ListVector(Int32[0, 1, 2], child)
    owner[] = column
    return (items=column,)
end

function n5hlistaxismutation(trigger::Int)
    child = N5HBackingAxisVector(Int32[1, 2], 0, trigger, false)
    column = Parquet.ListVector(Int32[0, 1, 2], child)
    return (items=column,)
end

function n5hstructphasemutation(trigger::Int)
    owner = Ref{Any}(nothing)
    firstchild = N5HActionVector(Int32[1], trigger) do
        pop!(owner[].children)
        return
    end
    column = Parquet.StructVector(["x", "y"],
        [firstchild, Int32[2]])
    owner[] = column
    return (s=column,)
end

function n5hstructchildsubstitution(trigger::Int)
    owner = Ref{Any}(nothing)
    firstchild = N5HActionVector(Int32[1], trigger) do
        owner[].children[2] = Int32[]
        return
    end
    column = Parquet.StructVector(["x", "y"],
        [firstchild, Int32[2]])
    owner[] = column
    return (s=column,)
end

function n5hstructchildidentity(trigger::Int)
    owner = Ref{Any}(nothing)
    replacement = N5HThrowVector{Int32}(N5HSentinelError())
    firstchild = N5HActionVector(Int32[1], trigger) do
        owner[].children[2] = replacement
        return
    end
    column = Parquet.StructVector(["x", "y"],
        [firstchild, Int32[2]])
    owner[] = column
    return (s=column,)
end

function n5hstructchildshrink(trigger::Int)
    later = Int32[2]
    firstchild = N5HActionVector(Int32[1], trigger) do
        empty!(later)
        return
    end
    column = Parquet.StructVector(["x", "y"], [firstchild, later])
    return (s=column,)
end

function n5hnestedlistmutation(trigger::Int)
    nested = Parquet.ListVector(Int32[0, 1], Int32[2])
    firstchild = N5HActionVector(Int32[1], trigger) do
        nested.offsets[2] = 0
        return
    end
    column = Parquet.StructVector(["x", "items"], [firstchild, nested])
    return (s=column,), firstchild
end

function n5hhiddenlistattack()
    owner = Ref{Any}(nothing)
    child = N5HHiddenNestedChild(Int32[10, 20], 0, owner)
    inner = Parquet.ListVector(Int32[0, 1, 2], child)
    owner[] = inner
    outer = Parquet.ListValue(inner, 1, 2)
    return (x=[outer],), child, inner
end

function n5hhiddenmapattack()
    owner = Ref{Any}(nothing)
    keys = N5HHiddenNestedChild(Int32[1, 2], 0, owner)
    inner = Parquet.MapVector(Int32[0, 1, 2], keys, Int32[10, 20])
    owner[] = inner
    outer = Parquet.ListValue(inner, 1, 2)
    return (x=[outer],), keys, inner
end

function n5hhiddendirectlistattack()
    owner = Ref{Any}(nothing)
    child = N5HHiddenNestedChild(Int32[10, 20], 0, owner)
    inner = Parquet.ListVector(Int32[0, 1, 2], child)
    owner[] = inner
    return (x=[inner],), child, inner
end

function n5hhiddendirectmapattack()
    owner = Ref{Any}(nothing)
    keys = N5HHiddenNestedChild(Int32[1, 2], 0, owner)
    inner = Parquet.MapVector(Int32[0, 1, 2], keys, Int32[10, 20])
    owner[] = inner
    return (x=[inner],), keys, inner
end

function n5hhiddendirectlistkeyattack()
    owner = Ref{Any}(nothing)
    child = N5HHiddenNestedChild(Int32[10, 20], 0, owner)
    inner = Parquet.ListVector(Int32[0, 1, 2], child)
    owner[] = inner
    column = Parquet.MapVector(Int32[0, 1], [inner], Int32[7])
    return (m=column,), child, inner
end

function n5hhiddendirectmapkeyattack()
    owner = Ref{Any}(nothing)
    keys = N5HHiddenNestedChild(Int32[1, 2], 0, owner)
    inner = Parquet.MapVector(Int32[0, 1, 2], keys, Int32[10, 20])
    owner[] = inner
    column = Parquet.MapVector(Int32[0, 1], [inner], Int32[7])
    return (m=column,), keys, inner
end

function n5hmapphasemutation(trigger::Int)
    owner = Ref{Any}(nothing)
    keys = N5HActionVector(Int32[1], typemax(Int)) do
        pop!(something(owner[].values))
        return
    end
    column = Parquet.MapVector(Int32[0, 1], keys, Int32[10])
    owner[] = column
    keys.calls = 0
    keys.trigger = trigger
    return (m=column,)
end

function n5hmapaxismutation(trigger::Int)
    values = N5HBackingAxisVector(Int32[10], 0, typemax(Int), false)
    keys = N5HActionVector(Int32[1], typemax(Int)) do
        values.shifted = true
        return
    end
    column = Parquet.MapVector(Int32[0, 1], keys, values)
    keys.calls = 0
    keys.trigger = trigger
    return (m=column,)
end

function n5hstructkeymutation(trigger::Int)
    owner = Ref{Any}(nothing)
    child = N5HActionVector(Int32[1], trigger) do
        pop!(owner[].children)
        return
    end
    keys = Parquet.StructVector(["x", "y"], [child, Int32[2]])
    owner[] = keys
    maps = Parquet.MapVector(Int32[0, 1], keys, Int32[10])
    return (m=maps,)
end

function n5hstructkeysubstitution(trigger::Int)
    owner = Ref{Any}(nothing)
    child = N5HActionVector(Int32[1], trigger) do
        owner[].children[2] = Int32[]
        return
    end
    keys = Parquet.StructVector(["x", "y"], [child, Int32[2]])
    owner[] = keys
    maps = Parquet.MapVector(Int32[0, 1], keys, Int32[10])
    return (m=maps,)
end

function n5hstructkeyidentity(trigger::Int)
    owner = Ref{Any}(nothing)
    replacement = N5HThrowVector{Int32}(N5HSentinelError())
    child = N5HActionVector(Int32[1], trigger) do
        owner[].children[2] = replacement
        return
    end
    keys = Parquet.StructVector(["x", "y"], [child, Int32[2]])
    owner[] = keys
    maps = Parquet.MapVector(Int32[0, 1], keys, Int32[10])
    return (m=maps,)
end

function n5hstructkeyshrink(trigger::Int)
    later = Int32[2]
    child = N5HActionVector(Int32[1], trigger) do
        empty!(later)
        return
    end
    keys = Parquet.StructVector(["x", "y"], [child, later])
    maps = Parquet.MapVector(Int32[0, 1], keys, Int32[10])
    return (m=maps,)
end

function n5hstructchildthrow(error)
    child = N5HActionVector(Int32[1], 1) do
        throw(error)
    end
    return (s=Parquet.StructVector(["x", "y"],
        [child, Int32[2]]),), child
end

function n5hpublicatomic(table)
    sink = IOBuffer()
    Base.write(sink, UInt8[0xa5, 0x5a])
    err = n5herror() do
        Parquet.write(sink, table; checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test !(err isa BoundsError)
    @test take!(sink) == UInt8[0xa5, 0x5a]
    return err
end

function n5hprovenancedeclaredkeyattack(value=n5hchain(64))
    bytes = Parquet._encodefile((m=Dict{Int32,Int32}[
        Dict(Int32(1) => Int32(2))],);
        checksum=false, pageindex=false)
    originaltable = Parquet.Table(bytes)
    original = originaltable.columns.m
    keys = N5HLyingInt32Vector(value, 0)
    replacement = Parquet.MapVector(copy(original.offsets), keys,
        original.values)
    keys.calls = 0
    table = Parquet.Table(originaltable.file, originaltable.metadata,
        originaltable.schema, (m=replacement,), originaltable.rows, false)
    @atomic originaltable.closed = true
    return table
end

function n5hprovenancekeysequence(values::Vector{Int32})
    bytes = Parquet._encodefile((m=Dict{Int32,Int32}[
        Dict(Int32(1) => Int32(2))],);
        checksum=false, pageindex=false)
    originaltable = Parquet.Table(bytes)
    original = originaltable.columns.m
    keys = N5HSequenceVector(values, 0)
    replacement = Parquet.MapVector(copy(original.offsets), keys,
        collect(something(original.values)); validity=original.validity)
    keys.calls = 0
    table = Parquet.Table(originaltable.file, originaltable.metadata,
        originaltable.schema, (m=replacement,), originaltable.rows, false)
    @atomic originaltable.closed = true
    return table, keys
end

function n5hprovenancethrowkey(error)
    bytes = Parquet._encodefile((m=Dict{Int32,Int32}[
        Dict(Int32(1) => Int32(2))],);
        checksum=false, pageindex=false)
    originaltable = Parquet.Table(bytes)
    original = originaltable.columns.m
    keys = N5HActionVector(Int32[1], typemax(Int)) do
        throw(error)
    end
    replacement = Parquet.MapVector(copy(original.offsets), keys,
        original.values)
    keys.calls = 0
    keys.trigger = 1
    table = Parquet.Table(originaltable.file, originaltable.metadata,
        originaltable.schema, (m=replacement,), originaltable.rows, false)
    @atomic originaltable.closed = true
    return table, keys
end

function n5hprovenancelistphase(trigger::Int)
    bytes = Parquet._encodefile((items=[Int32[1], Int32[2]],);
        checksum=false, pageindex=false)
    originaltable = Parquet.Table(bytes)
    original = originaltable.columns.items
    owner = Ref{Any}(nothing)
    child = N5HActionVector(collect(original.values), trigger) do
        pop!(owner[].offsets)
        return
    end
    replacement = Parquet.ListVector(copy(original.offsets), child;
        validity=original.validity)
    owner[] = replacement
    table = Parquet.Table(originaltable.file, originaltable.metadata,
        originaltable.schema, (items=replacement,), originaltable.rows, false)
    @atomic originaltable.closed = true
    return table, child
end

function n5hprovenancestructphase(trigger::Int)
    bytes = Parquet._encodefile(
        (s=[(x=Int32(1), y=Int32(2))],);
        checksum=false, pageindex=false)
    table = Parquet.Table(bytes)
    column = table.columns.s
    firstchild = N5HActionVector(collect(column.children[1]), trigger) do
        pop!(column.children)
        return
    end
    column.children[1] = firstchild
    return table, firstchild
end

function n5hprovenancestructsubstitution(trigger::Int)
    bytes = Parquet._encodefile(
        (s=[(x=Int32(1), y=Int32(2))],);
        checksum=false, pageindex=false)
    table = Parquet.Table(bytes)
    column = table.columns.s
    firstchild = N5HActionVector(collect(column.children[1]), trigger) do
        column.children[2] = Int32[]
        return
    end
    column.children[1] = firstchild
    return table, firstchild
end

function n5hprovenancestructidentity(trigger::Int)
    bytes = Parquet._encodefile(
        (s=[(x=Int32(1), y=Int32(2))],);
        checksum=false, pageindex=false)
    table = Parquet.Table(bytes)
    column = table.columns.s
    replacement = N5HThrowVector{Int32}(N5HSentinelError())
    firstchild = N5HActionVector(collect(column.children[1]), trigger) do
        column.children[2] = replacement
        return
    end
    column.children[1] = firstchild
    return table, firstchild
end

function n5hprovenancestructshrink(trigger::Int)
    bytes = Parquet._encodefile(
        (s=[(x=Int32(1), y=Int32(2))],);
        checksum=false, pageindex=false)
    table = Parquet.Table(bytes)
    column = table.columns.s
    later = column.children[2]
    firstchild = N5HActionVector(collect(column.children[1]), trigger) do
        empty!(later)
        return
    end
    column.children[1] = firstchild
    return table, firstchild
end

function n5hprovenancenestedlist(trigger::Int)
    bytes = Parquet._encodefile(
        (s=[(x=Int32(1), items=Int32[2])],);
        checksum=false, pageindex=false)
    table = Parquet.Table(bytes)
    column = table.columns.s
    nested = column.children[2]
    firstchild = N5HActionVector(collect(column.children[1]), trigger) do
        nested.offsets[2] = 0
        return
    end
    column.children[1] = firstchild
    return table, firstchild
end

function n5hprovenancestructmapkeyidentity()
    rows = [Dict((x=Int32(1), y=Int32(2)) => Int32(3))]
    bytes = Parquet._encodefile((m=rows,);
        checksum=false, pageindex=false)
    table = Parquet.Table(bytes)
    keys = table.columns.m.keys
    replacement = N5HThrowVector{Int32}(N5HSentinelError())
    firstchild = N5HActionVector(collect(keys.children[1]), 1) do
        keys.children[2] = replacement
        return
    end
    keys.children[1] = firstchild
    return table, firstchild
end

function n5hprovenancemapphase(trigger::Int)
    bytes = Parquet._encodefile((m=Dict{Int32,Int32}[
        Dict(Int32(1) => Int32(2))],);
        checksum=false, pageindex=false)
    originaltable = Parquet.Table(bytes)
    original = originaltable.columns.m
    owner = Ref{Any}(nothing)
    keys = N5HActionVector(collect(original.keys), typemax(Int)) do
        pop!(something(owner[].values))
        return
    end
    replacement = Parquet.MapVector(copy(original.offsets), keys,
        collect(something(original.values)); validity=original.validity)
    owner[] = replacement
    keys.calls = 0
    keys.trigger = trigger
    table = Parquet.Table(originaltable.file, originaltable.metadata,
        originaltable.schema, (m=replacement,), originaltable.rows, false)
    @atomic originaltable.closed = true
    return table, keys
end

function n5hprovenancescalaraxis(trigger::Int)
    bytes = Parquet._encodefile((x=Int32[1, 2],);
        checksum=false, pageindex=false)
    originaltable = Parquet.Table(bytes)
    values = N5HBackingAxisVector(collect(originaltable.columns.x), 0,
        trigger, false)
    table = Parquet.Table(originaltable.file, originaltable.metadata,
        originaltable.schema, (x=values,), originaltable.rows, false)
    @atomic originaltable.closed = true
    return table, values
end

function n5hprovenancemapaxis(trigger::Int)
    bytes = Parquet._encodefile((m=Dict{Int32,Int32}[
        Dict(Int32(1) => Int32(2))],);
        checksum=false, pageindex=false)
    originaltable = Parquet.Table(bytes)
    original = originaltable.columns.m
    values = N5HBackingAxisVector(collect(something(original.values)), 0,
        typemax(Int), false)
    keys = N5HActionVector(collect(original.keys), typemax(Int)) do
        values.shifted = true
        return
    end
    replacement = Parquet.MapVector(copy(original.offsets), keys, values;
        validity=original.validity)
    keys.calls = 0
    keys.trigger = trigger
    table = Parquet.Table(originaltable.file, originaltable.metadata,
        originaltable.schema, (m=replacement,), originaltable.rows, false)
    @atomic originaltable.closed = true
    return table, keys
end

function n5hchildsubstitution()
    owner = Ref{Any}(nothing)
    replacement = Int32[99]
    child = N5HActionVector(Int32[7], 1) do
        owner[].children[1] = replacement
        return
    end
    column = Parquet.StructVector(["x"], [child])
    owner[] = column
    return (s=column,)
end

function n5hoffsetmutation()
    owner = Ref{Any}(nothing)
    child = N5HActionVector(Int32[1, 2], 1) do
        owner[].offsets[2] = 0
        return
    end
    column = Parquet.ListVector(Int32[0, 1, 2], child)
    owner[] = column
    return (items=column,)
end

function n5hvaliditymutation()
    owner = Ref{Any}(nothing)
    child = N5HActionVector(Int32[1], 1) do
        owner[].validity[2] = true
        return
    end
    column = Parquet.ListVector(Int32[0, 1, 1], child;
        validity=Bool[true, false])
    owner[] = column
    return (items=column,)
end

function n5hrankmutation()
    owner = Ref{Any}(nothing)
    child = N5HActionVector(Int32[1, 2], 1) do
        owner[].ranks[2] = 0
        owner[].ranks[3] = 1
        return
    end
    column = Parquet.StructVector(["x"], [child];
        ranks=Int32[0, 1, 1, 2])
    owner[] = column
    return (s=column,)
end

function n5hlengthmutation()
    owner = Ref{Any}(nothing)
    values = N5HActionVector(Int32[1], 1) do
        push!(owner[].values, Int32(2))
        return
    end
    owner[] = values
    return (x=values,)
end

function n5hstructnamemutation()
    owner = Ref{Any}(nothing)
    child = N5HActionVector(Int32[1], 1) do
        owner[].names[1] = "renamed"
        return
    end
    column = Parquet.StructVector(["x"], [child])
    owner[] = column
    return (s=column,)
end

function n5hstructordermutation()
    owner = Ref{Any}(nothing)
    firstchild = N5HActionVector(Int32[1], 1) do
        reverse!(owner[].children)
        return
    end
    column = Parquet.StructVector(["x", "y"],
        [firstchild, Int32[2]])
    owner[] = column
    return (s=column,)
end

function n5hstructcountmutation()
    owner = Ref{Any}(nothing)
    secondchild = N5HActionVector(Int32[2], 1) do
        pop!(owner[].children)
        return
    end
    column = Parquet.StructVector(["x", "y"],
        [Int32[1], secondchild])
    owner[] = column
    return (s=column,)
end

function n5hterminaloffsetmutation()
    owner = Ref{Any}(nothing)
    child = N5HActionVector(Int32[1, 2], 2) do
        owner[].offsets[end] = 1
        return
    end
    column = Parquet.ListVector(Int32[0, 2], child)
    owner[] = column
    return (items=column,)
end

function n5hassertprivatefailure(table; limits::Parquet.Limits=Parquet.Limits())
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    err = n5herror() do
        Parquet._writefields(table, limits, budget)
    end
    @test err isa ArgumentError
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)
    return err
end

function n5hprovenanceattack()
    bytes = Parquet._encodefile((s=[(x=Int32(7),)],); checksum=false,
        pageindex=false)
    table = Parquet.Table(bytes)
    column = table.columns.s
    original = column.children[1]
    replacement = Int32[99]
    attack = N5HActionVector(collect(original), 2) do
        column.children[1] = replacement
        return
    end
    column.children[1] = attack
    return table
end

function n5hschemaelement(element::Parquet.Metadata.SchemaElement;
        type_=element.type_, type_length=element.type_length,
        repetition_type=element.repetition_type, name=element.name,
        num_children=element.num_children,
        converted_type=element.converted_type, scale=element.scale,
        precision=element.precision, field_id=element.field_id,
        logicalType=element.logicalType,
        unknown_fields=element.unknown_fields)
    return Parquet.Metadata.SchemaElement(; type_, type_length,
        repetition_type, name, num_children, converted_type, scale, precision,
        field_id, logicalType, unknown_fields)
end

function n5hprovenanceannotationattack()
    date = Dates.Date(2024, 1, 2)
    bytes = Parquet._encodefile((s=[(d=date,)],); checksum=false,
        pageindex=false)
    table = Parquet.Table(bytes)
    column = table.columns.s
    leafindex = something(findfirst(element -> element.name == "d",
        table.metadata.schema))
    original = column.children[1]
    attack = N5HActionVector(collect(original), 2) do
        element = table.metadata.schema[leafindex]
        table.metadata.schema[leafindex] = n5hschemaelement(element;
            logicalType=nothing)
        return
    end
    column.children[1] = attack
    return table
end

function n5hprovenancerowattack()
    bytes = Parquet._encodefile((s=[(x=Int32(7),)],); checksum=false,
        pageindex=false)
    table = Parquet.Table(bytes)
    column = table.columns.s
    original = column.children[1]
    attack = N5HActionVector(collect(original), 2) do
        table.rows += 1
        return
    end
    column.children[1] = attack
    return table
end

function n5hprovenancetopologyattack()
    bytes = Parquet._encodefile((s=[(x=Int32(7), y=Int32(8))],);
        checksum=false, pageindex=false)
    table = Parquet.Table(bytes)
    column = table.columns.s
    original = column.children[2]
    attack = N5HActionVector(collect(original), 2) do
        pop!(column.children)
        return
    end
    column.children[2] = attack
    return table
end

@testset "N5-C ordinary source mutation barriers" begin
    @test n5hassertprivatefailure(n5hchildsubstitution()) isa ArgumentError
    @test n5hassertprivatefailure(n5hoffsetmutation()) isa ArgumentError
    @test n5hassertprivatefailure(n5hvaliditymutation()) isa ArgumentError
    @test n5hassertprivatefailure(n5hrankmutation()) isa ArgumentError
    lengtherror = n5hassertprivatefailure(n5hlengthmutation())
    @test occursin("vector length", sprint(showerror, lengtherror))
    nameerror = n5hassertprivatefailure(n5hstructnamemutation())
    @test occursin("struct names", sprint(showerror, nameerror))
    childordererror = n5hassertprivatefailure(n5hstructordermutation())
    @test occursin("child identity or order", sprint(showerror, childordererror))
    childcounterror = n5hassertprivatefailure(n5hstructcountmutation())
    @test occursin("child identity or order", sprint(showerror, childcounterror))
    terminalerror = n5hassertprivatefailure(n5hterminaloffsetmutation())
    @test occursin("LIST offsets", sprint(showerror, terminalerror))

    axeserror = n5hassertprivatefailure((x=N5HAxisVector(Int32[1], false),))
    @test occursin("axes", sprint(showerror, axeserror))

    ordererror = n5hassertprivatefailure(
        (m=N5HAlternatingDict[N5HAlternatingDict(0)],))
    @test occursin("MAP keys", sprint(showerror, ordererror))

    lists = N5HSequenceVector(
        [Int32[1], Int32[1, 2], Int32[1, 2]], 0)
    listerror = n5hassertprivatefailure((items=lists,))
    @test occursin("occurrence", sprint(showerror, listerror))

    maps = N5HSequenceVector(
        N5HCountDict[N5HCountDict(1), N5HCountDict(2), N5HCountDict(2)], 0)
    maperror = n5hassertprivatefailure((items=maps,))
    @test occursin("occurrence", sprint(showerror, maperror))

    payload = N5HSequenceVector(["x", "x", "longer"], 0)
    payloaderror = n5hassertprivatefailure((x=payload,))
    @test occursin("occurrence", sprint(showerror, payloaderror))

    decimals = N5HSequenceVector([
        Parquet.Decimal(9, 1), Parquet.Decimal(9, 1),
        Parquet.Decimal(99, 2)], 0)
    decimalerror = n5hassertprivatefailure((x=decimals,))
    @test occursin("occurrence", sprint(showerror, decimalerror))

    timestamps = N5HSequenceVector([
        Parquet.Timestamp(Int64(1), :micros, false),
        Parquet.Timestamp(Int64(2), :micros, false),
        Parquet.Timestamp(Int64(3), :micros, true)], 0)
    timestamperror = n5hassertprivatefailure((x=timestamps,))
    @test occursin("occurrence", sprint(showerror, timestamperror))

    lowpage = Parquet.Limits(max_page_bytes=0)
    precedence = n5hassertprivatefailure(n5hchildsubstitution();
        limits=lowpage)
    @test precedence isa ArgumentError
end

@testset "N5-C live Tables column barriers" begin
    source = Int32[1]
    replacement = Int32[2]
    table = N5HLiveTable([:x], AbstractVector[source], replacement, 0, 2)
    err = n5herror() do
        Parquet._encodefile(table; checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("identity", sprint(showerror, err))

    order = N5HNameOrderTable([:a, :b],
        AbstractVector[Int32[1], Int32[2]], 0)
    err = n5herror() do
        Parquet._encodefile(order; checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("name or order", sprint(showerror, err))

    rename = N5HRenameTable(:a, Int32[1], 0)
    err = n5herror() do
        Parquet._encodefile(rename; checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("name or order", sprint(showerror, err))

    badnames = N5HBadNamesTable(0, Int32[1], Int32[2])
    err = n5herror() do
        Parquet._encodefile(badnames; checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test !(err isa BoundsError)
end

@testset "N5-C provenance late mutation barrier" begin
    attacks = (
        (n5hprovenanceattack, "child identity"),
        (n5hprovenanceannotationattack, "SchemaElement"),
        (n5hprovenancerowattack, "row count"),
        (n5hprovenancetopologyattack, "child count"),
    )
    for (factory, message) in attacks
        table = factory()
        try
            budget = Parquet._LiveByteBudget(Parquet.Limits())
            Parquet._reserve!(budget, 64)
            err = n5herror() do
                Parquet._writefields(table, Parquet.Limits(), budget)
            end
            @test err isa ArgumentError
            @test occursin(message, sprint(showerror, err))
            @test Parquet._budgetused(budget) == 64
            Parquet._release!(budget, 64)
        finally
            close(table)
        end
    end
end

@testset "N5-C controls and recursive key copies" begin
    changing = N5HSequenceVector(Int32[1, 2, 3], 0)
    fields, rows = Parquet._writefields((x=changing,), Parquet.Limits())
    @test rows == 1
    @test only(only(fields).leaves).values == Int32[3]
    @test changing.calls == 3

    zeroaxis = N5HZeroAxisVector(Int32[4, 5])
    bytes = Parquet._encodefile((x=zeroaxis,); checksum=false,
        pageindex=false)
    table = Parquet.Table(bytes)
    try
        @test collect(table.columns.x) == Int32[4, 5]
    finally
        close(table)
    end

    keys = Parquet.StructVector(["optional"],
        [Union{Missing,Int32}[missing, Int32(2)]])
    maps = Parquet.MapVector(Int32[0, 2], keys, Int32[10, 20])
    for version in (:v1, :v2)
        bytes = Parquet._encodefile((m=maps,); checksum=false,
            pageversion=version, pageindex=false)
        @test bytes == Parquet._encodefile((m=maps,); checksum=false,
            pageversion=version, pageindex=false)
        table = Parquet.Table(bytes)
        try
            pairs = collect(table.columns.m[1])
            @test length(pairs) == 2
            @test pairs[1].first[1] === missing
            @test pairs[2].first[1] == Int32(2)
        finally
            close(table)
        end
    end

    duplicate = Parquet.MapVector(Int32[0, 2], Int32[1, 1],
        Int32[10, 11])
    bytes = Parquet._encodefile((m=duplicate,); checksum=false,
        pageindex=false)
    table = Parquet.Table(bytes)
    try
        @test collect(table.columns.m[1]) ==
            [Int32(1) => Int32(10), Int32(1) => Int32(11)]
    finally
        close(table)
    end

end

@testset "N5-C bounded authoritative MAP-key snapshots" begin
    limits = Parquet.Limits(max_metadata_depth=4)
    lying = (m=N5HLyingCycleDict[N5HLyingCycleDict()],)
    err = n5herror() do
        Parquet._encodefile(lying; checksum=false, pageindex=false,
            limits=limits)
    end
    @test err isa ArgumentError
    @test !(err isa StackOverflowError)
    @test occursin("expected Int32", sprint(showerror, err))

    sink = IOBuffer()
    Base.write(sink, UInt8[0xa5, 0x5a])
    err = n5herror() do
        Parquet.write(sink, lying; checksum=false, pageindex=false,
            limits=limits)
    end
    @test err isa ArgumentError
    @test take!(sink) == UInt8[0xa5, 0x5a]

    budget = Parquet._LiveByteBudget(Parquet.Limits(
        max_materialized_bytes=100_000, max_metadata_depth=4))
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace,
        [[[Int32(1)]]], nothing, Parquet.Limits(max_metadata_depth=4))
    @test snapshot isa Parquet._NestedWriteKeySnapshot
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_metadata_depth=3)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, [[[Int32(1)]]], nothing,
            limits)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :metadata_depth
    @test err.requested == 4
    @test err.maximum == 3
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=192,
        max_metadata_depth=1)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, Int32[1], nothing, limits)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :metadata_depth
    @test err.requested == 2
    @test err.maximum == 1
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    value = N5HSameSourceString(N5HSequencedCodeunitVector(0))
    limits = Parquet.Limits(max_materialized_bytes=192,
        max_string_bytes=0)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, value, nothing, limits)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :string_bytes
    @test value.bytes.reads == 0
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=100_000)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, value, nothing, limits)
    end
    @test err isa ArgumentError
    @test occursin("invalid UTF-8", sprint(showerror, err))
    @test value.bytes.reads == 1
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_metadata_depth=1)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace, Int32[], nothing, limits)
    @test isempty(snapshot.children)
    Parquet._nestedwritetracecompare!(trace)
    @test Parquet._nestedwritetracekey!(trace, Int32[], nothing, limits) ===
        snapshot
    Parquet._nestedwritetracefinishcompare!(trace)
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    capturelimits = Parquet.Limits(max_materialized_bytes=100_000,
        max_metadata_depth=2)
    budget = Parquet._LiveByteBudget(capturelimits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    Parquet._nestedwritetracekey!(trace, Int32[1], nothing, capturelimits)
    Parquet._nestedwritetracecompare!(trace)
    before = Parquet._budgetused(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, Int32[1], nothing,
            Parquet.Limits(max_metadata_depth=1))
    end
    @test err isa Parquet.LimitError
    @test err.resource == :metadata_depth
    @test Parquet._budgetused(budget) == before
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    cycle = []
    push!(cycle, cycle)
    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_metadata_depth=4)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, cycle, nothing, limits)
    end
    @test err isa ArgumentError
    @test occursin("cyclic", sprint(showerror, err))
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    value = N5HSequencedString(0)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace, value, nothing, limits)
    @test snapshot.value == UInt8[0x61]
    @test value.calls == 1
    Parquet._nestedwritetracecompare!(trace)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, value, nothing, limits)
    end
    @test err isa ArgumentError
    @test value.calls == 2
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_metadata_depth=1)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, cycle, nothing, limits)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :metadata_depth
    @test err.requested == 2
    @test err.maximum == 1
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=8_192,
        max_metadata_depth=1_000_000)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace, Int32(1), nothing,
        limits)
    @test snapshot.value == Int32(1)
    @test Parquet._budgetused(budget) <= limits.max_materialized_bytes
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_container_elements=2)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace, Int32[1, 2], nothing,
        limits)
    @test length(snapshot.children) == 2
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=192,
        max_container_elements=1)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, Int32[1, 2], nothing, limits)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :container_elements
    @test err.requested == 2
    @test err.maximum == 1
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_container_elements=0, max_string_bytes=2)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace,
        (UInt8(0x61), UInt8(0x62)), nothing, limits)
    @test snapshot.value == UInt8[0x61, 0x62]
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_string_bytes=2)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace, UInt8[0x61, 0x62],
        nothing, limits)
    @test snapshot.value == UInt8[0x61, 0x62]
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=192,
        max_string_bytes=1)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, UInt8[0x61, 0x62], nothing,
            limits)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :string_bytes
    @test err.requested == 2
    @test err.maximum == 1
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    aggregate = Parquet._NestedWriteLeafAggregate(false, nothing, nothing,
        Int32(1))
    shape = Parquet._NestedWriteLeafShape("key", Vector{UInt8}, false,
        nothing, Int32(2), aggregate)
    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_container_elements=0, max_string_bytes=2)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace, UInt8[0x61, 0x62],
        shape, limits)
    @test snapshot.value == UInt8[0x61, 0x62]
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=192,
        max_container_elements=0, max_string_bytes=2)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, UInt8[0x61], shape, limits)
    end
    @test err isa ArgumentError
    @test occursin("wrong width", sprint(showerror, err))
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    decimal = Parquet.Decimal(1234567890123456789, 0)
    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_decimal_bytes=9)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace, decimal, nothing, limits)
    @test snapshot.value == decimal
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=192,
        max_decimal_bytes=8)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, decimal, nothing, limits)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :decimal_bytes
    @test err.requested == 9
    @test err.maximum == 8
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_decimal_bytes=0)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace,
        Parquet.Decimal(12, 0), nothing, limits)
    @test snapshot.value == Parquet.Decimal(12, 0)
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)
end

@testset "N5-C MAP-key conversion and consume gap" begin
    limits = Parquet.Limits(max_materialized_bytes=100_000)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, N5HBadByteKey(), nothing,
            limits)
    end
    @test err isa ArgumentError
    @test !(err isa InexactError)
    @test occursin("not a UInt8", sprint(showerror, err))
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, N5HAxisShiftByteKey(false),
            nothing, limits)
    end
    @test err isa ArgumentError
    @test occursin("axes", sprint(showerror, err))
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    key = N5HShiftBadByteKey(false)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, key, nothing, limits)
    end
    @test err isa ArgumentError
    @test occursin("axes", sprint(showerror, err))
    @test !occursin("not a UInt8", sprint(showerror, err))
    @test key.shifted
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    key = N5HSequencedShiftBadByteKey(0, 2, false)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    Parquet._nestedwritetracekey!(trace, key, nothing, limits)
    Parquet._nestedwritetracecompare!(trace)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, key, nothing, limits)
    end
    @test err isa ArgumentError
    @test occursin("axes", sprint(showerror, err))
    @test !occursin("not a UInt8", sprint(showerror, err))
    @test key.calls == 2
    @test key.shifted
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    sequenced = N5HSequencedLengthByteKey(0)
    limits = Parquet.Limits(max_materialized_bytes=100_000,
        max_string_bytes=1)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, sequenced, nothing, limits)
    end
    @test err isa ArgumentError
    @test !(err isa BoundsError)
    @test occursin("length", sprint(showerror, err))
    @test sequenced.calls == 1
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    sentinel = N5HSentinelError()
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, N5HThrowLengthKey(sentinel),
            nothing, limits)
    end
    @test err === sentinel
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    sentinel = N5HSentinelError()
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, N5HThrowByteKey(sentinel),
            nothing, limits)
    end
    @test err === sentinel
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    key = N5HSequencedByteKey(0, 6)
    maps = Parquet.MapVector(Int32[0, 1], N5HSequencedByteKey[key],
        Int32[7])
    err = n5herror() do
        Parquet._encodefile((m=maps,); checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("physically consumed", sprint(showerror, err))
    @test key.passes == 6

    key = N5HSequencedByteKey(0, 6)
    maps = Parquet.MapVector(Int32[0, 1], N5HSequencedByteKey[key],
        Int32[7])
    sink = IOBuffer()
    Base.write(sink, UInt8[0xde, 0xad])
    err = n5herror() do
        Parquet.write(sink, (m=maps,); checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test take!(sink) == UInt8[0xde, 0xad]

    key = N5HSequencedShiftBadByteKey(0, 6, false)
    maps = Parquet.MapVector(Int32[0, 1],
        N5HSequencedShiftBadByteKey[key], Int32[7])
    err = n5herror() do
        Parquet._encodefile((m=maps,); checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("axes", sprint(showerror, err))
    @test !occursin("not a UInt8", sprint(showerror, err))
    @test key.calls == 6
    @test key.shifted

    key = N5HSequencedShiftBadByteKey(0, 4, false)
    maps = Parquet.MapVector(Int32[0, 1],
        N5HShiftBadString[N5HShiftBadString(key)], Int32[7])
    err = n5herror() do
        Parquet._encodefile((m=maps,); checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("axes", sprint(showerror, err))
    @test !occursin("not a UInt8", sprint(showerror, err))
    @test !occursin("UTF-8", sprint(showerror, err))
    @test key.calls == 4
    @test key.shifted

    key = N5HSequencedShiftBadByteKey(0, 6, false)
    maps = Parquet.MapVector(Int32[0, 1],
        N5HShiftBadString[N5HShiftBadString(key)], Int32[7])
    err = n5herror() do
        Parquet._encodefile((m=maps,); checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("axes", sprint(showerror, err))
    @test !occursin("not a UInt8", sprint(showerror, err))
    @test !occursin("UTF-8", sprint(showerror, err))
    @test key.calls == 6
    @test key.shifted

    for attack in 1:3
        value = N5HNCodeunitAxisString(N5HNCodeunitAxisBytes(false), 0,
            attack)
        maps = Parquet.MapVector(Int32[0, 1],
            N5HNCodeunitAxisString[value], Int32[7])
        err = n5herror() do
            Parquet._encodefile((m=maps,); checksum=false, pageindex=false)
        end
        @test err isa ArgumentError
        @test occursin("axes", sprint(showerror, err))
        @test value.calls == attack
        @test value.bytes.shifted
    end

    key = N5HSequencedByteKey(0, typemax(Int))
    maps = Parquet.MapVector(Int32[0, 1], N5HSequencedByteKey[key],
        Int32[7])
    bytes = Parquet._encodefile((m=maps,); checksum=false, pageindex=false)
    @test key.passes == 7
    table = Parquet.Table(bytes)
    try
        pair = only(collect(table.columns.m[1]))
        @test pair.first == UInt8[0x61]
        @test pair.second == Int32(7)
    finally
        close(table)
    end

    decimal = Parquet.Decimal(12, 0)
    decimalmap = Parquet.MapVector(Int32[0, 1],
        Parquet.Decimal[decimal], Int32[1])
    fields, rows = Parquet._writefields((m=decimalmap,), Parquet.Limits())
    @test rows == 1
    element = fields[1].schema[3]
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    expected = Parquet._nestedwritetracekey!(trace, decimal, nothing, limits)
    physical = try
        Base.GMP.MPZ.set!(decimal.unscaled, BigInt(13))
        Parquet._nestedwritenormalizekeyphysical(element, decimal, limits)
    finally
        Base.GMP.MPZ.set!(decimal.unscaled, BigInt(12))
    end
    context = Parquet._NestedWriteEmitContext(
        Parquet._NestedWriteLeafBuilder[], Parquet._NestedWriteLeafCount[],
        limits, nothing, trace)
    @test !Parquet._nestedwritekeyphysicalequal(expected, physical, element,
        context)
    @test decimal == Parquet.Decimal(12, 0)
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)
end

@testset "N5-C budget ownership and exception identity" begin
    limits = Parquet.Limits(max_materialized_bytes=65)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    names = ["x"]
    values = AbstractVector[Int32[1]]
    columns = Pair{String,AbstractVector}["x" => values[1]]
    err = n5herror() do
        Parquet._nestedwritetopology(columns, names, values, limits, budget)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :materialized_bytes
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=100_000)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    topology = Parquet._nestedwritetopology(columns, names, values, limits,
        budget)
    @test Parquet._budgetused(budget) > 64
    Parquet._nestedwritetopologyrelease!(topology, budget)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=400)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, repeat("x", 1024))
    end
    @test err isa Parquet.LimitError
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    limits = Parquet.Limits(max_materialized_bytes=1_024,
        max_container_elements=typemax(Int64))
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, N5HCountDict(typemax(Int)),
            nothing, limits)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :materialized_bytes
    @test err.requested > limits.max_materialized_bytes
    @test err.maximum == limits.max_materialized_bytes
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    sentinel = N5HSentinelError()
    budget = Parquet._LiveByteBudget(Parquet.Limits())
    Parquet._reserve!(budget, 64)
    err = n5herror() do
        Parquet._writefields((x=N5HThrowVector{Int32}(sentinel),),
            Parquet.Limits(), budget)
    end
    @test err === sentinel
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)
end

@testset "N5-C public sink and path atomicity" begin
    for version in (:v1, :v2)
        sink = IOBuffer()
        Base.write(sink, UInt8[0xa5, 0x5a])
        err = n5herror() do
            Parquet.write(sink, n5hchildsubstitution(); pageversion=version,
                checksum=false, pageindex=false)
        end
        @test err isa ArgumentError
        @test take!(sink) == UInt8[0xa5, 0x5a]
    end

    mktempdir() do directory
        path = joinpath(directory, "atomic.parquet")
        sentinel = UInt8[0xde, 0xad, 0xbe, 0xef]
        Base.write(path, sentinel)
        err = n5herror() do
            Parquet.write(path, n5hchildsubstitution(); checksum=false,
                pageindex=false)
        end
        @test err isa ArgumentError
        @test Base.read(path) == sentinel
        return
    end
end

@testset "N5-C iterative deep MAP-key frames" begin
    depth = 20_000
    value = n5hchain(depth)
    limits = Parquet.Limits(max_materialized_bytes=32_000_000,
        max_metadata_depth=depth + 1, max_container_elements=1)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace, value, nothing, limits)
    @test snapshot isa Parquet._NestedWriteKeySnapshot
    Parquet._nestedwritetracecompare!(trace)
    @test Parquet._nestedwritetracekey!(trace, value, nothing, limits) ===
        snapshot
    Parquet._nestedwritetracefinishcompare!(trace)
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    boundary = n5hchain(64)
    exact = Parquet.Limits(max_materialized_bytes=1_000_000,
        max_metadata_depth=65, max_container_elements=1)
    budget = Parquet._LiveByteBudget(exact)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    Parquet._nestedwritetracekey!(trace, boundary, nothing, exact)
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    oneover = Parquet.Limits(max_materialized_bytes=1_000_000,
        max_metadata_depth=64, max_container_elements=1)
    budget = Parquet._LiveByteBudget(oneover)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, boundary, nothing, oneover)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :metadata_depth
    @test err.requested == 65
    @test err.maximum == 64
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    cycle = n5hcycle(64)
    cyclelimits = Parquet.Limits(max_materialized_bytes=1_000_000,
        max_metadata_depth=66, max_container_elements=1)
    budget = Parquet._LiveByteBudget(cyclelimits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, cycle, nothing, cyclelimits)
    end
    @test err isa ArgumentError
    @test !(err isa StackOverflowError)
    @test occursin("cyclic", sprint(showerror, err))
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    cycleoneover = Parquet.Limits(max_materialized_bytes=1_000_000,
        max_metadata_depth=65, max_container_elements=1)
    budget = Parquet._LiveByteBudget(cycleoneover)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, cycle, nothing, cycleoneover)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :metadata_depth
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    mismatch = n5hchain(64, Int32(2))
    budget = Parquet._LiveByteBudget(exact)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    Parquet._nestedwritetracekey!(trace, boundary, nothing, exact)
    Parquet._nestedwritetracecompare!(trace)
    before = Parquet._budgetused(budget)
    err = n5herror() do
        Parquet._nestedwritetracekey!(trace, mismatch, nothing, exact)
    end
    @test err isa ArgumentError
    @test Parquet._budgetused(budget) == before
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    shallow = Parquet.Limits(max_materialized_bytes=8_192,
        max_metadata_depth=1_000_000, max_container_elements=1)
    budget = Parquet._LiveByteBudget(shallow)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    snapshot = Parquet._nestedwritetracekey!(trace, N5HChain(Int32(1)),
        nothing, shallow)
    @test only(snapshot.children).value == Int32(1)
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    for sentinel in (N5HMutableSentinel(1), BoundsError(:user, 7))
        budget = Parquet._LiveByteBudget(exact)
        Parquet._reserve!(budget, 64)
        trace = Parquet._nestedwritetrace(budget)
        hostile = N5HChain(N5HThrowAnyVector(sentinel))
        err = n5herror() do
            Parquet._nestedwritetracekey!(trace, hostile, nothing, exact)
        end
        @test err === sentinel
        Parquet._nestedwritetracerelease!(trace)
        @test Parquet._budgetused(budget) == 64
        Parquet._release!(budget, 64)
    end
end

@testset "N5-C stable package validators" begin
    for (factory, needle) in (
            (() -> begin
                owner = Ref{Any}(nothing)
                child = N5HLengthMutationVector(Int32[11], () -> begin
                    owner[].offsets[2] = 0
                    return
                end, false, 0)
                column = Parquet.ListVector(Int32[0, 1], child)
                owner[] = column
                return column, child
            end, "LIST offsets"),
            (() -> begin
                owner = Ref{Any}(nothing)
                keys = N5HLengthMutationVector(Int32[1], () -> begin
                    owner[].offsets[2] = 0
                    return
                end, false, 0)
                column = Parquet.MapVector(Int32[0, 1], keys, Int32[11])
                owner[] = column
                return column, keys
            end, "MAP offsets"),
            (() -> begin
                owner = Ref{Any}(nothing)
                child = N5HLengthMutationVector(Int32[11], () -> begin
                    owner[].ranks[2] = 0
                    return
                end, false, 0)
                column = Parquet.StructVector(["x"], [child];
                    ranks=Int32[0, 1])
                owner[] = column
                return column, child
            end, "struct ranks"),
        )
        column, child = factory()
        limits = Parquet.Limits()
        budget = Parquet._LiveByteBudget(limits)
        Parquet._reserve!(budget, 64)
        topology = Parquet._nestedwritetopology(nothing, ["x"],
            AbstractVector[column], limits, budget)
        try
            snapshot = Parquet._nestedwritesnapshotfor(topology, column)
            child.calls = 0
            child.armed = true
            err = n5herror() do
                Parquet._nestedwritevalidatenode(snapshot)
            end
            @test err isa ArgumentError
            @test occursin(needle, sprint(showerror, err))
            @test child.calls == 1
        finally
            Parquet._nestedwritetopologyrelease!(topology, budget)
        end
        @test Parquet._budgetused(budget) == 64
        Parquet._release!(budget, 64)
        @test Parquet._budgetused(budget) == 0
    end

    for (factory, validate, needle) in (
            (() -> begin
                owner = Ref{Any}(nothing)
                child = N5HLengthMutationVector(Int32[11], () -> begin
                    owner[].offsets[1] = 1
                    return
                end, false, 0)
                column = Parquet.ListVector(Int32[0, 1], child)
                owner[] = column
                return column, child
            end, Parquet._validatelistvector, "list offsets"),
            (() -> begin
                owner = Ref{Any}(nothing)
                keys = N5HLengthMutationVector(Int32[1], () -> begin
                    owner[].offsets[1] = 1
                    return
                end, false, 0)
                column = Parquet.MapVector(Int32[0, 1], keys, Int32[11])
                owner[] = column
                return column, keys
            end, Parquet._validatemapvector, "map offsets"),
            (() -> begin
                owner = Ref{Any}(nothing)
                child = N5HLengthMutationVector(Int32[11], () -> begin
                    owner[].ranks[1] = 1
                    return
                end, false, 0)
                column = Parquet.StructVector(["x"], [child];
                    ranks=Int32[0, 1])
                owner[] = column
                return column, child
            end, Parquet._validatestructvector, "struct ranks"),
        )
        column, child = factory()
        child.armed = true
        err = n5herror() do
            validate(column)
        end
        @test err isa ArgumentError
        @test occursin(needle, sprint(showerror, err))
        @test child.calls == 1
    end

    names = ["x"]
    child = N5HLengthMutationVector(Int32[11], () -> begin
        empty!(names)
        return
    end, false, 0)
    value = Parquet.StructValue(names, AbstractVector[child], 1)
    child.armed = true
    err = n5herror() do
        Parquet._validatestructvalue(value)
    end
    @test err isa ArgumentError
    @test occursin("struct child identity", sprint(showerror, err))
    @test child.calls == 1

    keys = Int32[1]
    values = N5HLengthMutationVector(Int32[11], () -> begin
        empty!(keys)
        return
    end, false, 0)
    value = Parquet.MapValue{Int32,Int32,true}(keys, values, 1, 1)
    values.armed = true
    err = n5herror() do
        Parquet._validatemapvalue(value)
    end
    @test err isa ArgumentError
    @test occursin("map view", sprint(showerror, err))
    @test values.calls == 1
end

@testset "N5-C within-pass package mutation" begin
    stable = N5HNothingStateDict(Int32(1) => Int32(2), 0, 0)
    materialization = Parquet._nestedwritedictmaterialize(nothing, stable,
        nothing, nothing, Parquet.Limits())
    @test materialization.count == 1
    @test materialization.first.key == Int32(1)
    @test materialization.first.value == Int32(2)
    @test materialization.first.next === nothing
    @test stable.calls == 2
    @test stable.length_calls == 0
    Parquet._nestedwritedictrelease!(nothing, materialization)

    for invalid in (
            Parquet.ListValue(N5HThrowMetricChild(
                N5HMutableSentinel(81)), 0, -1),
            Parquet.MapValue{Int32,Int32,:bad}(
                N5HThrowMetricChild(N5HMutableSentinel(82)), nothing, 1, 0),
        )
        dictionary = Dict{Int32,Any}(Int32(1) => invalid)
        limits = Parquet.Limits()
        budget = Parquet._LiveByteBudget(limits)
        Parquet._reserve!(budget, 64)
        trace = Parquet._nestedwritetrace(budget)
        err = n5herror() do
            Parquet._nestedwritedictmaterialize(trace, dictionary, nothing,
                nothing, limits)
        end
        @test err isa ArgumentError
        @test !(err isa N5HMutableSentinel)
        Parquet._nestedwritetracerelease!(trace)
        @test Parquet._budgetused(budget) == 64
        Parquet._release!(budget, 64)
    end

    for reverse in (false, true)
        sentinel = N5HMutableSentinel(reverse ? 84 : 83)
        callback = Parquet.ListValue(N5HThrowMetricChild(sentinel), 1, 0)
        malformed = Parquet.ListVector(Int32[0, 1], Int32[11])
        malformed.offsets[1] = 1
        nested = Parquet.ListValue(malformed, 1, 1)
        pair = reverse ? nested => callback : callback => nested
        dictionary = N5HNothingStateDict(pair, 0, 0)
        limits = Parquet.Limits()
        budget = Parquet._LiveByteBudget(limits)
        Parquet._reserve!(budget, 64)
        trace = Parquet._nestedwritetrace(budget)
        err = n5herror() do
            Parquet._nestedwritedictmaterialize(trace, dictionary, nothing,
                nothing, limits)
        end
        @test err isa ArgumentError
        @test err !== sentinel
        @test dictionary.calls == 1
        @test dictionary.length_calls == 0
        Parquet._nestedwritetracerelease!(trace)
        @test Parquet._budgetused(budget) == 64
        Parquet._release!(budget, 64)
    end

    sentinel = N5HMutableSentinel(86)
    dictionary = N5HSecondTouchDict(N5HThrowMetricChild(sentinel))
    limits = Parquet.Limits(max_container_elements=1)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    trace = Parquet._nestedwritetrace(budget)
    err = n5herror() do
        Parquet._nestedwritedictmaterialize(trace, dictionary, nothing,
            nothing, limits)
    end
    @test err isa Parquet.LimitError
    @test err.resource == :container_elements
    @test err !== sentinel
    Parquet._nestedwritetracerelease!(trace)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    owner = Ref{Any}(nothing)
    child = N5HRestoringChild(owner, true, 0)
    parent = Parquet.ListVector(Int32[0, 1], child)
    owner[] = parent
    dictionary = N5HParentAfterChildDict(parent, 0)
    limits = Parquet.Limits()
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    topology = Parquet._nestedwritetopology(nothing, ["x"],
        AbstractVector[Int32[1]], limits, budget)
    trace = Parquet._nestedwritetrace(budget, topology)
    err = n5herror() do
        Parquet._nestedwritedictmaterialize(trace, dictionary, nothing,
            nothing, limits)
    end
    @test err isa ArgumentError
    @test occursin("LIST offsets", sprint(showerror, err))
    @test dictionary.attacks == 1
    @test child.restores == 0
    @test parent.offsets == Int32[0, 0]
    Parquet._nestedwritetracerelease!(trace)
    Parquet._nestedwritetopologyrelease!(topology, budget)
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    old = Parquet.ListVector(Int32[0, 1], Int32[11])
    new = Parquet.ListVector(Int32[0, 1], Int32[11])
    sentinel = N5HMutableSentinel(85)
    changing = N5HChangingSourceDict(old, new, 0, 0, sentinel)
    limits = Parquet.Limits()
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    err = n5herror() do
        Parquet._writefields((m=[changing],), limits, budget)
    end
    @test err isa ArgumentError
    @test err !== sentinel
    @test changing.starts == 2
    @test changing.terminals == 1
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)

    function n5haliascalls(entries::Int)
        child = N5HCountingVector(0)
        parent = Parquet.ListVector(Int32[0, 1], child)
        child.calls = 0
        dictionary = Dict{Int32,Any}(Int32(index) => parent for index in
            1:entries)
        limits = Parquet.Limits()
        budget = Parquet._LiveByteBudget(limits)
        Parquet._reserve!(budget, 64)
        topology = Parquet._nestedwritetopology(nothing, ["x"],
            AbstractVector[Int32[1]], limits, budget)
        trace = Parquet._nestedwritetrace(budget, topology)
        materialization = Parquet._nestedwritedictmaterialize(trace,
            dictionary, nothing, nothing, limits)
        calls = child.calls
        @test materialization.count == entries
        dependencies = 0
        dependency = materialization.dependencies
        while dependency !== nothing
            dependencies += 1
            dependency = dependency.next
        end
        @test dependencies == 2
        @test materialization.dependency_sources.count == 2
        Parquet._nestedwritedictrelease!(trace, materialization)
        Parquet._nestedwritetracerelease!(trace)
        Parquet._nestedwritetopologyrelease!(topology, budget)
        @test Parquet._budgetused(budget) == 64
        Parquet._release!(budget, 64)
        return calls
    end
    @test n5haliascalls(32) == n5haliascalls(1)

    inner = Parquet.ListVector(Int32[0, 1], Int32[11])
    dictionary = N5HFinalMutKeyDict(inner => Int32(22))
    keys = N5HRestoreVector(dictionary, dictionary, true)
    outer = Parquet.MapVector(Int32[0, 1], keys, Int32[7])
    err = n5herror() do
        Parquet._encodefile((m=outer,); checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("LIST offsets", sprint(showerror, err))
    @test dictionary.attacks == 1
    @test inner.offsets == Int32[0, 0]

    inner = Parquet.ListVector(Int32[0, 1], Int32[11])
    dictionary = N5HFinalMutMap(Int32(22) => inner)
    rows = N5HRestoreVector(dictionary, dictionary, false)
    err = n5herror() do
        Parquet._encodefile((m=rows,); checksum=false, pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("LIST offsets", sprint(showerror, err))
    @test dictionary.attacks == 1
    @test inner.offsets == Int32[0, 0]

    inner = Parquet.ListVector(Int32[0, 1], Int32[11])
    dictionary = N5HFinalMutMap(Int32(22) => inner)
    rows = N5HRestoreVector(dictionary, dictionary, false)
    err = n5herror() do
        Parquet._encodefile((a=inner, m=rows); checksum=false,
            pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("LIST offsets", sprint(showerror, err))
    @test dictionary.attacks == 1
    @test inner.offsets == Int32[0, 0]

    owner = Ref{Any}(nothing)
    child = N5HRestoringChild(owner, true, 0)
    inner = Parquet.ListVector(Int32[0, 1], child)
    owner[] = inner
    dictionary = N5HFinalMutMap(Int32(22) => inner)
    err = n5herror() do
        Parquet._encodefile((m=[dictionary],); checksum=false,
            pageindex=false)
    end
    @test err isa ArgumentError
    @test occursin("LIST offsets", sprint(showerror, err))
    @test dictionary.attacks == 1
    @test child.restores == 0
    @test inner.offsets == Int32[0, 0]

    cases = (
        (n5hlistphasemutation, (1, 3, 5),
            table -> table.items.values.calls),
        (n5hlistaxismutation, (1, 3, 5),
            table -> table.items.values.calls),
        (n5hstructphasemutation, (1, 2, 3),
            table -> table.s.children[1].calls),
        (n5hstructchildsubstitution, (1, 2, 3),
            table -> table.s.children[1].calls),
        (n5hstructchildidentity, (1, 2, 3),
            table -> table.s.children[1].calls),
        (n5hstructchildshrink, (1, 2, 3),
            table -> table.s.children[1].calls),
        (n5hmapphasemutation, (1, 2, 3),
            table -> table.m.keys.calls),
        (n5hmapaxismutation, (1, 2, 3),
            table -> table.m.keys.calls),
        (n5hstructkeymutation, (1, 4, 7),
            table -> table.m.keys.children[1].calls),
        (n5hstructkeysubstitution, (1, 4, 7),
            table -> table.m.keys.children[1].calls),
        (n5hstructkeyidentity, (1, 4, 7),
            table -> table.m.keys.children[1].calls),
        (n5hstructkeyshrink, (1, 4, 7),
            table -> table.m.keys.children[1].calls),
    )
    for (factory, triggers, calls) in cases
        for trigger in triggers
            table = factory(trigger)
            err = n5hassertprivatefailure(table)
            @test !(err isa BoundsError)
            @test calls(table) == trigger
        end
    end
    for (factory, trigger, calls) in (
            (n5hlistphasemutation, 5, table -> table.items.values.calls),
            (n5hlistaxismutation, 5, table -> table.items.values.calls),
            (n5hstructphasemutation, 3,
                table -> table.s.children[1].calls),
            (n5hstructchildidentity, 3,
                table -> table.s.children[1].calls),
            (n5hstructchildshrink, 3,
                table -> table.s.children[1].calls),
            (n5hstructkeyidentity, 7,
                table -> table.m.keys.children[1].calls),
            (n5hstructkeyshrink, 7,
                table -> table.m.keys.children[1].calls),
            (n5hmapaxismutation, 3, table -> table.m.keys.calls),
            (n5hmapphasemutation, 3, table -> table.m.keys.calls))
        table = factory(trigger)
        n5hpublicatomic(table)
        @test calls(table) == trigger
    end
end

@testset "N5-C provenance declared MAP-key root" begin
    table = n5hprovenancedeclaredkeyattack()
    try
        sink = IOBuffer()
        Base.write(sink, UInt8[0xa5, 0x5a])
        err = n5herror() do
            Parquet.write(sink, table; checksum=false, pageindex=false,
                limits=Parquet.Limits(max_metadata_depth=4))
        end
        @test err isa ArgumentError
        @test !(err isa StackOverflowError)
        @test occursin("expected Int32", sprint(showerror, err))
        @test table.columns.m.keys.calls == 1
        @test take!(sink) == UInt8[0xa5, 0x5a]
    finally
        close(table)
    end
end

@testset "N5-C provenance within-pass package mutation" begin
    cases = (
        (n5hprovenancelistphase, (1, 3)),
        (n5hprovenancescalaraxis, (1, 3)),
        (n5hprovenancestructphase, (1, 2)),
        (n5hprovenancestructsubstitution, (1, 2)),
        (n5hprovenancestructidentity, (1, 2)),
        (n5hprovenancestructshrink, (1, 2)),
        (n5hprovenancemapphase, (1, 2)),
        (n5hprovenancemapaxis, (1, 2)),
    )
    for (factory, triggers) in cases
        for trigger in triggers
            table, callback = factory(trigger)
            try
                limits = Parquet.Limits()
                budget = Parquet._LiveByteBudget(limits)
                Parquet._reserve!(budget, 64)
                err = n5herror() do
                    Parquet._writefields(table, limits, budget)
                end
                @test err isa ArgumentError
                @test !(err isa BoundsError)
                @test callback.calls == trigger
                @test Parquet._budgetused(budget) == 64
                Parquet._release!(budget, 64)
            finally
                close(table)
            end
        end
    end

    table, callback = n5hprovenancestructmapkeyidentity()
    try
        limits = Parquet.Limits()
        budget = Parquet._LiveByteBudget(limits)
        Parquet._reserve!(budget, 64)
        err = n5herror() do
            Parquet._writefields(table, limits, budget)
        end
        @test err isa ArgumentError
        @test !(err isa BoundsError)
        @test callback.calls == 1
        @test Parquet._budgetused(budget) == 64
        Parquet._release!(budget, 64)
    finally
        close(table)
    end

    table, callback = n5hprovenancestructmapkeyidentity()
    try
        n5hpublicatomic(table)
        @test callback.calls == 1
    finally
        close(table)
    end
end

@testset "N5-C wrapper and provenance exception identity" begin
    for sentinel in (N5HMutableSentinel(2), BoundsError(:wrapper, 9))
        keys = N5HActionVector(Int32[1], typemax(Int)) do
            throw(sentinel)
        end
        map = Parquet.MapVector(Int32[0, 1], keys, Int32[10])
        keys.calls = 0
        keys.trigger = 1
        limits = Parquet.Limits()
        budget = Parquet._LiveByteBudget(limits)
        Parquet._reserve!(budget, 64)
        err = n5herror() do
            Parquet._writefields((m=map,), limits, budget)
        end
        @test err === sentinel
        @test keys.calls == 1
        @test Parquet._budgetused(budget) == 64
        Parquet._release!(budget, 64)

        structtable, structchild = n5hstructchildthrow(sentinel)
        budget = Parquet._LiveByteBudget(limits)
        Parquet._reserve!(budget, 64)
        err = n5herror() do
            Parquet._writefields(structtable, limits, budget)
        end
        @test err === sentinel
        @test structchild.calls == 1
        @test Parquet._budgetused(budget) == 64
        Parquet._release!(budget, 64)

        table, provenancekeys = n5hprovenancethrowkey(sentinel)
        try
            sink = IOBuffer()
            Base.write(sink, UInt8[0xa5, 0x5a])
            err = n5herror() do
                Parquet.write(sink, table; checksum=false, pageindex=false)
            end
            @test err === sentinel
            @test provenancekeys.calls == 1
            @test take!(sink) == UInt8[0xa5, 0x5a]
        finally
            close(table)
        end
    end
end


@testset "N5-C stable package invariants and hostile metrics" begin
    list = Parquet.ListVector(Int32[0, 0], Int32[];
        validity=Bool[false])
    list.offsets[2] = 1
    @test n5hassertprivatefailure((items=list,)) isa ArgumentError

    map = Parquet.MapVector(Int32[0, 0], Int32[], Int32[];
        validity=Bool[false])
    map.offsets[2] = 1
    @test n5hassertprivatefailure((m=map,)) isa ArgumentError

    interior = Parquet.ListVector(Int32[0, 0, 0], Int32[])
    interior.offsets[2] = 100
    limits = Parquet.Limits(max_container_elements=10)
    err = n5hassertprivatefailure((items=interior,); limits=limits)
    @test err isa ArgumentError
    @test !(err isa Parquet.LimitError)

    required = Parquet.StructVector(["x"], [Int32[1]])
    push!(required.children[1], Int32(2))
    @test n5hassertprivatefailure((s=required,)) isa ArgumentError

    optional = Parquet.StructVector(["x"], [Int32[1, 2]];
        ranks=Int32[0, 1, 2])
    optional.ranks[2] = 2
    @test n5hassertprivatefailure((s=optional,)) isa ArgumentError

    rawoptional = Parquet.StructVector{
        Union{Missing,Parquet.StructValue},Vector{Int32}}(
            String[], Int32[0], AbstractVector[], 0)
    @test_throws ArgumentError Parquet._validatestructvector(rawoptional)

    @test_throws ArgumentError Parquet._validatelistvalue(
        Parquet.ListValue(Int32[], typemin(Int), typemax(Int)))
    @test_throws ArgumentError Parquet._validatelistvalue(
        Parquet.ListValue(Int32[], 2, 1))
    @test_throws ArgumentError Parquet._validatemapvalue(
        Parquet.MapValue{Int32,Missing,false}(
            Int32[], Missing[], 1, 0))
    @test_throws ArgumentError Parquet._validatemapvalue(
        Parquet.MapValue{Int32,Int32,true}(Int32[], nothing, 1, 0))
    @test_throws ArgumentError Parquet._validatemapvalue(
        Parquet.MapValue{Int32,Int32,:invalid}(
            Int32[], Int32[], 1, 0))
    @test_throws ArgumentError Parquet._validatemapvalue(
        Parquet.MapValue{Int32,Int32,false}(Int32[], nothing, 1, 0))
    @test_throws ArgumentError Parquet._validatemapvalue(
        Parquet.MapValue{Union{Missing,Int32},Int32,true}(
            Union{Missing,Int32}[], Int32[], 1, 0))

    @test_throws ArgumentError Parquet.ListVector(Int32[0],
        N5HZeroAxisVector(Int32[]))
    @test_throws ArgumentError Parquet.MapVector(Int32[0],
        N5HZeroAxisVector(Int32[]), N5HZeroAxisVector(Int32[]))

    rawmap = Parquet.MapVector{
        Parquet.MapValue{Int32,Int32,:invalid},Int32,Int32,:invalid,
        Int32,Nothing}(Int32[0], nothing, Int32[], Int32[])
    @test_throws ArgumentError Parquet._validatemapvector(rawmap)

    for hostile in (N5HNonIntLengthVector(), N5HUIntSizeVector(),
            N5HUIntAxisVector())
        err = n5herror() do
            Parquet._nestedvectorcount(hostile, "hostile vector")
            Parquet._nestedvectoraxes(hostile, "hostile vector")
        end
        @test err isa ArgumentError
        @test !(err isa Union{MethodError,InexactError})
    end
    err = n5herror() do
        Parquet._nestedwritekeycount(N5HHugeUIntDict())
    end
    @test err isa ArgumentError
    @test !(err isa Union{MethodError,InexactError})

    huge = N5HHugeVector()
    @test length(Parquet.ListValue(huge, typemax(Int),
        typemax(Int) - 1)) == 0
    @test_throws ArgumentError Parquet._nestedspan(
        Int64[typemax(Int), typemax(Int)], 1)

    for sentinel in (N5HMutableSentinel(71), BoundsError(:metric, 3))
        @test n5herror(() -> Parquet._nestedvectorcount(
            N5HThrowSizeVector(sentinel), "hostile vector")) === sentinel
        @test n5herror(() -> Parquet._nestedvectorcount(
            N5HThrowLengthKey(sentinel), "hostile vector")) === sentinel
    end

    column = Parquet.StructVector(["x", "y"],
        AbstractVector[Int32[1], Int32[2]])
    sentinel = N5HMutableSentinel(72)
    preflight = _ -> begin
        column.children[2] = N5HThrowMetricChild(sentinel)
        return nothing, Int64(0)
    end
    limits = Parquet.Limits()
    budget = Parquet._LiveByteBudget(limits)
    err = n5herror() do
        Parquet._nestedwritefields(
            Pair{String,AbstractVector}["s" => column], 1, limits, budget;
            preflight=preflight)
    end
    @test err isa ArgumentError
    @test err !== sentinel
    @test Parquet._budgetused(budget) == 0

    view = Parquet.StructValue(["x"],
        AbstractVector[N5HFalseBoundsChild()], 1)
    @test_throws ArgumentError Parquet._validatestructvalue(view)
    sentinel = N5HMutableSentinel(73)
    view = Parquet.StructValue(["x"],
        AbstractVector[N5HThrowBoundsChild(sentinel)], 1)
    @test n5herror(() -> Parquet._validatestructvalue(view)) === sentinel

    sentinel = N5HMutableSentinel(74)
    outer = N5HShiftOuter(N5HThrowMetricChild(sentinel), false)
    budget = Parquet._LiveByteBudget(limits)
    err = n5herror() do
        Parquet._writefields((x=outer,), limits, budget)
    end
    @test err isa ArgumentError
    @test err !== sentinel
    @test outer.shifted
    @test Parquet._budgetused(budget) == 0

    sentinel = N5HMutableSentinel(75)
    outer = N5HShiftOuter(N5HThrowMetricChild(sentinel), false)
    budget = Parquet._LiveByteBudget(limits)
    err = n5herror() do
        Parquet._writefields((x=[outer],), limits, budget)
    end
    @test err isa ArgumentError
    @test err !== sentinel
    @test outer.shifted
    @test Parquet._budgetused(budget) == 0
end


@testset "N5-C hidden package view topology" begin
    factories = (n5hhiddenlistattack, n5hhiddenmapattack,
        n5hhiddendirectlistattack, n5hhiddendirectmapattack,
        n5hhiddendirectlistkeyattack, n5hhiddendirectmapkeyattack)
    for factory in factories
        table, callback, owner = factory()
        err = n5hassertprivatefailure(table)
        @test !(err isa BoundsError)
        @test callback.calls == 1
        @test owner.offsets[2] == 0

        table, callback, owner = factory()
        n5hpublicatomic(table)
        @test callback.calls == 1
        @test owner.offsets[2] == 0
    end

    mktempdir() do directory
        path = joinpath(directory, "hidden-view.parquet")
        sentinel = UInt8[0xde, 0xad, 0xbe, 0xef]
        for factory in factories
            Base.write(path, sentinel)
            table, callback, owner = factory()
            err = n5herror() do
                Parquet.write(path, table; checksum=false, pageindex=false)
            end
            @test err isa ArgumentError
            @test !(err isa BoundsError)
            @test callback.calls == 1
            @test owner.offsets[2] == 0
            @test Base.read(path) == sentinel
        end
        return
    end

    inner = Parquet.ListVector(Int32[0, 1, 2], Int32[10, 20])
    outer = Parquet.ListValue(inner, 1, 2)
    bytes = Parquet._encodefile((x=[outer],); checksum=false,
        pageindex=false)
    reread = Parquet.Table(bytes)
    try
        observed = [collect(item) for item in reread.columns.x[1]]
        @test observed == [Int32[10], Int32[20]]
    finally
        close(reread)
    end

    inner = Parquet.MapVector(Int32[0, 1, 2], Int32[1, 2],
        Int32[10, 20])
    outer = Parquet.ListValue(inner, 1, 2)
    bytes = Parquet._encodefile((x=[outer],); checksum=false,
        pageindex=false)
    reread = Parquet.Table(bytes)
    try
        observed = [collect(item) for item in reread.columns.x[1]]
        @test observed == [[Int32(1) => Int32(10)],
            [Int32(2) => Int32(20)]]
    finally
        close(reread)
    end

    inner = Parquet.ListVector(Int32[0, 1, 2], Int32[10, 20])
    bytes = Parquet._encodefile((x=[inner],); checksum=false,
        pageindex=false)
    reread = Parquet.Table(bytes)
    try
        observed = [collect(item) for item in reread.columns.x[1]]
        @test observed == [Int32[10], Int32[20]]
    finally
        close(reread)
    end

    inner = Parquet.MapVector(Int32[0, 1, 2], Int32[1, 2],
        Int32[10, 20])
    bytes = Parquet._encodefile((x=[inner],); checksum=false,
        pageindex=false)
    reread = Parquet.Table(bytes)
    try
        observed = [collect(item) for item in reread.columns.x[1]]
        @test observed == [[Int32(1) => Int32(10)],
            [Int32(2) => Int32(20)]]
    finally
        close(reread)
    end

    for (inner, expected) in (
            (Parquet.ListVector(Int32[0, 1, 2], Int32[10, 20]),
                [Int32[10], Int32[20]]),
            (Parquet.MapVector(Int32[0, 1, 2], Int32[1, 2],
                Int32[10, 20]),
                [[Int32(1) => Int32(10)], [Int32(2) => Int32(20)]]))
        column = Parquet.MapVector(Int32[0, 1], [inner], Int32[7])
        bytes = Parquet._encodefile((m=column,); checksum=false,
            pageindex=false)
        reread = Parquet.Table(bytes)
        try
            pair = only(reread.columns.m[1])
            observed = [collect(item) for item in pair.first]
            @test observed == expected
            @test pair.second == Int32(7)
        finally
            close(reread)
        end
    end

    logical = Parquet.LogicalColumn(String["alpha", "beta"], :enum)
    fixed = Parquet.FixedByteArrayVector{Vector{UInt8}}(
        [UInt8[0x01, 0x02], UInt8[0x03, 0x04]], Int32(2))
    for (inner, expected) in ((logical, ["alpha", "beta"]),
            (fixed, [UInt8[0x01, 0x02], UInt8[0x03, 0x04]]))
        bytes = Parquet._encodefile((x=[inner],); checksum=false,
            pageindex=false)
        reread = Parquet.Table(bytes)
        try
            @test collect(reread.columns.x[1]) == expected
        finally
            close(reread)
        end
    end

    backing = N5HShiftOuter("alpha", false)
    logical = Parquet.LogicalColumn(backing, :enum)
    err = n5hassertprivatefailure((x=[logical],))
    @test err isa ArgumentError
    @test backing.shifted

    inner = Parquet.StructVector(["x"], [Int32[1]])
    limits = Parquet.Limits()
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    err = n5herror() do
        Parquet._writefields((x=[inner],), limits, budget)
    end
    @test err isa ArgumentError
    @test occursin("StructValue needs its owning StructVector",
        sprint(showerror, err))
    @test Parquet._budgetused(budget) == 64
    Parquet._release!(budget, 64)
end


@testset "N5-C empty MAP source trace alignment" begin
    values = Parquet.MapVector(Int32[0, 0, 0, 2, 3],
        String["a", "b", "c"],
        Union{Missing,Int32}[missing, 2, 3];
        validity=Bool[false, true, true, true])
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile((attrs=values,);
            pageversion=pageversion, checksum=false, pageindex=false)
        reread = Parquet.Table(bytes)
        try
            @test reread.columns.attrs[1] === missing
            @test isempty(reread.columns.attrs[2])
            @test isequal(collect(reread.columns.attrs[3]),
                Pair{String,Union{Missing,Int32}}[
                    "a" => missing, "b" => Int32(2)])
            @test collect(reread.columns.attrs[4]) ==
                ["c" => Int32(3)]
        finally
            close(reread)
        end
    end
end


@testset "N5-C nested row witnesses" begin
    for trigger in (1, 2, 3)
        table, callback = n5hnestedlistmutation(trigger)
        err = n5hassertprivatefailure(table)
        @test err isa ArgumentError
        @test !(err isa BoundsError)
        @test callback.calls == trigger
    end
    for trigger in (1, 2)
        table, callback = n5hprovenancenestedlist(trigger)
        try
            limits = Parquet.Limits()
            budget = Parquet._LiveByteBudget(limits)
            Parquet._reserve!(budget, 64)
            err = n5herror() do
                Parquet._writefields(table, limits, budget)
            end
            @test err isa ArgumentError
            @test !(err isa BoundsError)
            @test callback.calls == trigger
            @test Parquet._budgetused(budget) == 64
            Parquet._release!(budget, 64)
        finally
            close(table)
        end
    end
end


@testset "N5-C provenance one-read MAP-key authority" begin
    sequence = Int32[1, 2, 1, 1, 3, 1]
    table, keys = n5hprovenancekeysequence(sequence)
    try
        limits = Parquet.Limits()
        budget = Parquet._LiveByteBudget(limits)
        Parquet._reserve!(budget, 64)
        err = n5herror() do
            Parquet._writefields(table, limits, budget)
        end
        @test err isa ArgumentError
        @test !(err isa BoundsError)
        @test keys.calls == 2
        @test Parquet._budgetused(budget) == 64
        Parquet._release!(budget, 64)
    finally
        close(table)
    end

    table, keys = n5hprovenancekeysequence(sequence)
    try
        sink = IOBuffer()
        Base.write(sink, UInt8[0xa5, 0x5a])
        err = n5herror() do
            Parquet.write(sink, table; checksum=false, pageindex=false)
        end
        @test err isa ArgumentError
        @test keys.calls == 2
        @test take!(sink) == UInt8[0xa5, 0x5a]
    finally
        close(table)
    end

    for factory in (n5hprovenancemapphase, n5hprovenancemapaxis)
        for trigger in (3, 4)
            table, callback = factory(trigger)
            try
                bytes = Parquet._encodefile(table; checksum=false,
                    pageindex=false)
                @test callback.calls == 2
                reread = Parquet.Table(bytes)
                try
                    @test reread.rows == 1
                    @test length(reread.columns.m) == 1
                    @test only(reread.columns.m)[1] ==
                        (Int32(1) => Int32(2))
                finally
                    close(reread)
                end
            finally
                close(table)
            end
        end
    end
end


@testset "N5-C provenance binding-kind precedence" begin
    recursive = Any[
        Int32(1) => Int32(2),
        (Int32(1),),
        (x=Int32(1),),
        Parquet.ListValue(Int32[], 1, 0),
        Parquet.MapValue{Int32,Int32,true}(Int32[], Int32[], 1, 0),
        Dict(Int32(1) => Int32(2)),
        Parquet.StructValue(["x"], AbstractVector[Int32[1]], 1),
    ]
    for value in recursive
        table = n5hprovenancedeclaredkeyattack(value)
        try
            sink = IOBuffer()
            Base.write(sink, UInt8[0xa5, 0x5a])
            err = n5herror() do
                Parquet.write(sink, table; checksum=false, pageindex=false)
            end
            @test err isa ArgumentError
            @test !(err isa Union{MethodError,StackOverflowError})
            @test table.columns.m.keys.calls == 1
            @test take!(sink) == UInt8[0xa5, 0x5a]
        finally
            close(table)
        end
    end
end
