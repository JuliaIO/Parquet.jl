function _checkfixedvalue(value, width::Int32)
    ismissing(value) && return
    length(value) == width ||
        throw(ArgumentError("fixed byte-array value length $(length(value)) differs from width $width"))
    return
end

struct FixedByteArrayVector{T} <: AbstractVector{T}
    values::Vector{T}
    width::Int32

    function FixedByteArrayVector{T}(values::Vector{T}, width::Int32) where {T}
        T in (Vector{UInt8}, Union{Missing,Vector{UInt8}}) ||
            throw(ArgumentError("fixed byte-array vector has unsupported element type $T"))
        width > 0 || throw(ArgumentError("fixed byte-array width must be positive"))
        foreach(value -> _checkfixedvalue(value, width), values)
        return new{T}(values, width)
    end
end

function FixedByteArrayVector(::Type{T}, width::Int32) where {T}
    return FixedByteArrayVector{T}(T[], width)
end

function Base.IndexStyle(::Type{<:FixedByteArrayVector})
    return IndexLinear()
end

function Base.size(column::FixedByteArrayVector)
    return size(column.values)
end

function Base.getindex(column::FixedByteArrayVector, index::Int)
    return column.values[index]
end

function Base.setindex!(column::FixedByteArrayVector, value, index::Int)
    _checkfixedvalue(value, column.width)
    column.values[index] = value
    return value
end

function Base.push!(column::FixedByteArrayVector, value)
    _checkfixedvalue(value, column.width)
    push!(column.values, value)
    return column
end

function Base.append!(column::FixedByteArrayVector, values)
    foreach(value -> _checkfixedvalue(value, column.width), values)
    append!(column.values, values)
    return column
end

function Base.sizehint!(column::FixedByteArrayVector, size::Integer)
    sizehint!(column.values, size)
    return column
end

const _NestedIndex = Union{Int32,Int64}

function _nestedindextype(terminal::Integer)
    terminal <= typemax(Int32) && return Int32
    return Int64
end

function _nestedindexvalue(value::Integer, label::AbstractString)
    value >= 0 || throw(ArgumentError("$label must be nonnegative"))
    value <= typemax(Int) || throw(ArgumentError("$label exceeds the Julia index range"))
    return Int(value)
end

function _nestedcheckedint(value, label::AbstractString)
    value isa Int || throw(ArgumentError("$label is not an Int"))
    value >= 0 || throw(ArgumentError("$label must be nonnegative"))
    return value
end

function _nestedvectorcount(values::AbstractVector, label::AbstractString)
    Base.@nospecialize values
    sizecount = size(values, 1)
    sizecount isa Int || throw(ArgumentError(
        "$label size is not an Int"))
    sizecount >= 0 || throw(ArgumentError(
        "$label size must be nonnegative"))
    count = length(values)
    count isa Int || throw(ArgumentError("$label length is not an Int"))
    count == sizecount || throw(ArgumentError(
        "$label length differs from its size"))
    return sizecount
end

function _nestedvectoraxisvalue(value, label::AbstractString)
    value isa Int || throw(ArgumentError("$label is not an Int"))
    return value
end

function _nestedvectoraxes(values::AbstractVector, label::AbstractString)
    Base.@nospecialize values
    first = _nestedvectoraxisvalue(firstindex(values), "$label first index")
    last = _nestedvectoraxisvalue(lastindex(values), "$label last index")
    return first, last
end

function _nestedviewcount(first::Int, last::Int, label::AbstractString)
    first >= 1 || throw(ArgumentError("$label has an invalid first index"))
    emptylast = try
        Base.checked_sub(first, 1)
    catch error
        error isa OverflowError || rethrow()
        throw(ArgumentError("$label has an invalid entry span"))
    end
    last >= emptylast || throw(ArgumentError("$label has an invalid entry span"))
    last == emptylast && return 0
    return try
        Base.checked_add(Base.checked_sub(last, first), 1)
    catch error
        error isa OverflowError || rethrow()
        throw(ArgumentError("$label entry span exceeds the Julia index range"))
    end
end

function _nestedindices(indices::AbstractVector{<:Integer}, label::AbstractString)
    isempty(indices) && throw(ArgumentError("$label must contain an initial zero"))
    firstvalue = _nestedindexvalue(first(indices), "$label entry")
    iszero(firstvalue) || throw(ArgumentError("$label must start at zero"))
    previous = firstvalue
    for value in Iterators.drop(indices, 1)
        current = _nestedindexvalue(value, "$label entry")
        current >= previous || throw(ArgumentError("$label must be nondecreasing"))
        previous = current
    end
    T = _nestedindextype(previous)
    output = Vector{T}(undef, length(indices))
    for (index, value) in enumerate(indices)
        output[index] = T(value)
    end
    return output
end

function _nestedvalidity(validity::Nothing, rows::Int, offsets::Vector{<:_NestedIndex})
    return nothing
end

function _nestedvalidity(validity::AbstractVector{Bool}, rows::Int,
        offsets::Vector{<:_NestedIndex})
    length(validity) == rows || throw(ArgumentError(
        "nested validity has $(length(validity)) entries for $rows rows"))
    output = BitVector(validity)
    for row in 1:rows
        output[row] || offsets[row] == offsets[row + 1] || throw(ArgumentError(
            "null nested row $row has a nonempty child span"))
    end
    return output
end

function _nestedspan(offsets::Vector{<:_NestedIndex}, row::Int)
    firstindex = try
        Base.checked_add(Int(offsets[row]), 1)
    catch error
        error isa OverflowError || rethrow()
        throw(ArgumentError("nested offset exceeds the Julia index range"))
    end
    lastindex = Int(offsets[row + 1])
    return firstindex, lastindex
end

function _validatenestedphysicalshape(values::AbstractVector, terminal::Int,
        label::AbstractString)
    Base.@nospecialize values
    count = _nestedvectorcount(values, label)
    count == terminal || throw(ArgumentError(
        "$label length differs from its terminal offset"))
    first, last = _nestedvectoraxes(values, label)
    first == 1 && last == count || throw(ArgumentError(
        "$label must use one-based contiguous axes"))
    return
end

function _validatenestedphysicalspan(values::AbstractVector, terminal::Int,
        label::AbstractString)
    Base.@nospecialize values
    _validatenestedphysicalshape(values, terminal, label)
    iszero(terminal) && return
    checkbounds(Bool, values, 1) && checkbounds(Bool, values, terminal) ||
        throw(ArgumentError("$label axes do not contain its physical span"))
    return
end

struct ListValue{T} <: AbstractVector{T}
    values::AbstractVector{T}
    first::Int
    last::Int
end

function _validatelistvalue(value::ListValue)
    count = _nestedviewcount(value.first, value.last, "list view")
    backing = _nestedvectorcount(value.values, "list view backing vector")
    first, last = _nestedvectoraxes(value.values,
        "list view backing vector")
    first == 1 && last == backing || throw(ArgumentError(
        "list view backing vector must use one-based contiguous axes"))
    (value.first <= backing || (backing < typemax(Int) &&
        value.first == backing + 1)) || throw(ArgumentError(
        "list view insertion point exceeds its backing vector"))
    if !iszero(count)
        checkbounds(Bool, value.values, value.first) &&
            checkbounds(Bool, value.values, value.last) || throw(
                ArgumentError(
                    "list view child span exceeds its backing vector axes"))
    end
    return
end

function Base.IndexStyle(::Type{<:ListValue})
    return IndexLinear()
end

function Base.size(value::ListValue)
    return (_nestedviewcount(value.first, value.last, "list view"),)
end

function Base.getindex(value::ListValue, index::Int)
    _validatelistvalue(value)
    @boundscheck checkbounds(value, index)
    item = value.values[value.first + index - 1]
    _validatelistvalue(value)
    return item
end

function _collectnestedview(value::AbstractVector{T}) where {T}
    output = Vector{T}(undef, length(value))
    for index in eachindex(value)
        output[index] = value[index]
    end
    return output
end

function Base.collect(value::ListValue)
    return _collectnestedview(value)
end

function Base.copy(value::ListValue)
    return collect(value)
end

struct ListVector{T,E,O<:_NestedIndex,V<:Union{Nothing,BitVector}} <:
        AbstractVector{T}
    offsets::Vector{O}
    validity::V
    values::AbstractVector{E}
end

function ListVector(offsets::AbstractVector{<:Integer}, values::AbstractVector;
        validity::Union{Nothing,AbstractVector{Bool}}=nothing)
    Base.@nospecialize values
    normalized = _nestedindices(offsets, "list offsets")
    rows = length(normalized) - 1
    _validatenestedphysicalshape(values, Int(last(normalized)), "list child")
    valid = _nestedvalidity(validity, rows, normalized)
    E = eltype(values)
    V = ListValue{E}
    T = valid === nothing ? V : Union{Missing,V}
    return ListVector{T,E,eltype(normalized),typeof(valid)}(
        normalized, valid, values)
end

function ListVector(offsets::AbstractVector{<:Integer}, validity::AbstractVector{Bool},
        values::AbstractVector)
    return ListVector(offsets, values; validity=validity)
end

function Base.IndexStyle(::Type{<:ListVector})
    return IndexLinear()
end

function Base.size(column::ListVector)
    return (length(column.offsets) - 1,)
end

function _validatelistlocal(column::ListVector)
    offsets = column.offsets
    isempty(offsets) && throw(ArgumentError(
        "list offsets lost their initial zero"))
    iszero(first(offsets)) || throw(ArgumentError(
        "list offsets no longer start at zero"))
    rows = length(offsets) - 1
    column.validity === nothing || length(column.validity) == rows || throw(
        ArgumentError("list validity length differs from its row count"))
    previous = Int(first(offsets))
    for row in 1:rows
        current = Int(offsets[row + 1])
        current >= previous || throw(ArgumentError(
            "LIST offsets are no longer nondecreasing"))
        column.validity === nothing || column.validity[row] ||
            current == previous || throw(ArgumentError(
                "null LIST row has a nonempty child span"))
        previous = current
    end
    return previous
end

function _validatelistvector(column::ListVector)
    terminal = _validatelistlocal(column)
    _validatenestedphysicalspan(column.values, terminal, "LIST child")
    _validatelistlocal(column)
    return
end

function Base.getindex(column::ListVector, index::Int)
    @boundscheck checkbounds(column, index)
    column.validity === nothing || column.validity[index] || return missing
    firstindex, lastindex = _nestedspan(column.offsets, index)
    return ListValue(column.values, firstindex, lastindex)
end

struct StructValue
    names::Vector{String}
    children::Vector{AbstractVector}
    index::Int
end

function _validatestructvaluelocal(value::StructValue, names::Vector{String},
        children::Vector{AbstractVector}, count::Int)
    value.names === names && value.children === children || throw(
        ArgumentError("struct backing identity changed during access"))
    length(children) == count || throw(ArgumentError(
        "struct child identity or order changed during access"))
    length(names) == count || throw(ArgumentError(
        "struct child identity or order changed during access"))
    value.index >= 1 || throw(ArgumentError(
        "struct view has an invalid row index"))
    return
end

function _validatestructvalue(value::StructValue)
    names = value.names
    children = value.children
    count = length(children)
    _validatestructvaluelocal(value, names, children, count)
    for (index, child) in enumerate(children)
        childcount = _nestedvectorcount(child, "struct view child $index")
        first, last = _nestedvectoraxes(child, "struct view child $index")
        first == 1 && last == childcount || throw(ArgumentError(
            "struct view child $index must use one-based contiguous axes"))
        value.index <= childcount || throw(ArgumentError(
            "struct view child $index does not contain its row index"))
        checkbounds(Bool, child, value.index) || throw(ArgumentError(
            "struct view child $index axes do not contain its row index"))
    end
    _validatestructvaluelocal(value, names, children, count)
    return
end

function Base.length(value::StructValue)
    return length(value.children)
end

function Base.eltype(::Type{<:StructValue})
    return Pair{String,Any}
end

function _structvaluechild(value::StructValue, index::Int)
    names = value.names
    children = value.children
    count = length(children)
    _validatestructvaluelocal(value, names, children, count)
    child = children[index]
    childcount = _nestedvectorcount(child, "struct view child $index")
    first, last = _nestedvectoraxes(child, "struct view child $index")
    first == 1 && last == childcount || throw(ArgumentError(
        "struct view child $index must use one-based contiguous axes"))
    value.index <= childcount || throw(ArgumentError(
        "struct view child $index does not contain its row index"))
    checkbounds(Bool, child, value.index) || throw(ArgumentError(
        "struct view child $index axes do not contain its row index"))
    _validatestructvaluelocal(value, names, children, count)
    children[index] === child || throw(ArgumentError(
        "struct child identity or order changed during access"))
    return child
end

function Base.getindex(value::StructValue, index::Int)
    @boundscheck 1 <= index <= length(value) || throw(BoundsError(value, index))
    child = _structvaluechild(value, index)
    item = child[value.index]
    _structvaluechild(value, index) === child || throw(ArgumentError(
        "struct child identity or order changed during access"))
    return item
end

function _structfieldindex(value::StructValue, name::AbstractString)
    found = 0
    for index in eachindex(value.names)
        value.names[index] == name || continue
        iszero(found) || throw(ArgumentError("struct field name $(repr(name)) is ambiguous"))
        found = index
    end
    iszero(found) && throw(KeyError(name))
    return found
end

function Base.getindex(value::StructValue, name::AbstractString)
    return value[_structfieldindex(value, name)]
end

function Base.getindex(value::StructValue, name::Symbol)
    return value[String(name)]
end

function Base.iterate(value::StructValue, index::Int=1)
    index > length(value) && return nothing
    return value.names[index] => value[index], index + 1
end

function Base.collect(value::StructValue)
    output = Vector{Pair{String,Any}}(undef, length(value))
    for index in eachindex(value.names)
        output[index] = value.names[index] => value[index]
    end
    return output
end

function Base.copy(value::StructValue)
    return collect(value)
end

function Base.:(==)(left::StructValue, right::StructValue)
    length(left) == length(right) || return false
    result = true
    for index in 1:length(left)
        left.names[index] == right.names[index] || return false
        equal = left[index] == right[index]
        equal === false && return false
        equal === missing && (result = missing)
    end
    return result
end

function Base.isequal(left::StructValue, right::StructValue)
    length(left) == length(right) || return false
    for index in 1:length(left)
        isequal(left.names[index], right.names[index]) || return false
        isequal(left[index], right[index]) || return false
    end
    return true
end

function Base.hash(value::StructValue, seed::UInt)
    output = hash(:ParquetStructValue, seed)
    output = hash(length(value), output)
    for index in 1:length(value)
        output = hash(value.names[index], output)
        output = hash(value[index], output)
    end
    return output
end

function (::Type{NamedTuple})(value::StructValue)
    length(unique(value.names)) == length(value.names) || throw(ArgumentError(
        "cannot convert a struct with duplicate field names to NamedTuple"))
    names = try
        Tuple(Symbol(name) for name in value.names)
    catch err
        err isa ArgumentError || rethrow()
        throw(ArgumentError("cannot convert a struct with an invalid field name to NamedTuple"))
    end
    values = ntuple(index -> value[index], length(value))
    return NamedTuple{names}(values)
end

struct StructVector{T,R<:Union{Nothing,Vector{Int32},Vector{Int64}}} <: AbstractVector{T}
    names::Vector{String}
    ranks::R
    children::Vector{AbstractVector}
    rows::Int
end

function _structchildren(children::Union{Tuple,AbstractVector})
    output = Vector{AbstractVector}(undef, length(children))
    for (index, child) in enumerate(children)
        child isa AbstractVector || throw(ArgumentError("struct child $index is not a vector"))
        output[index] = child
    end
    return output
end

function _structrows(children::Vector{AbstractVector}, rows)
    if isempty(children)
        rows === nothing && throw(ArgumentError(
            "a zero-field required struct needs an explicit row count"))
        return _nestedindexvalue(rows, "struct row count")
    end
    expected = _nestedvectorcount(first(children), "struct child 1")
    _validatenestedphysicalshape(first(children), expected, "struct child 1")
    rows === nothing || _nestedindexvalue(rows, "struct row count") == expected ||
        throw(ArgumentError("struct row count differs from its child length"))
    return expected
end

function _validatechildren(names::Vector{String}, children::Vector{AbstractVector}, expected::Int)
    length(names) == length(children) || throw(ArgumentError(
        "struct has $(length(names)) names for $(length(children)) children"))
    for (index, child) in enumerate(children)
        _validatenestedphysicalshape(child, expected, "struct child $index")
    end
    return
end

function _structranks(ranks::AbstractVector{<:Integer})
    normalized = _nestedindices(ranks, "struct ranks")
    for index in 1:(length(normalized) - 1)
        difference = normalized[index + 1] - normalized[index]
        difference <= 1 || throw(ArgumentError(
            "struct rank difference at row $index exceeds one"))
    end
    return normalized
end

function StructVector(names::AbstractVector{<:AbstractString},
        children::Union{Tuple,AbstractVector};
        ranks::Union{Nothing,AbstractVector{<:Integer}}=nothing, rows=nothing)
    normalizednames = String[String(name) for name in names]
    normalizedchildren = _structchildren(children)
    if ranks === nothing
        rowcount = _structrows(normalizedchildren, rows)
        _validatechildren(normalizednames, normalizedchildren, rowcount)
        return StructVector{StructValue,Nothing}(
            normalizednames, nothing, normalizedchildren, rowcount)
    end
    isempty(normalizedchildren) && throw(ArgumentError(
        "an optional zero-field struct has unobservable presence"))
    normalizedranks = _structranks(ranks)
    rowcount = length(normalizedranks) - 1
    rows === nothing || _nestedindexvalue(rows, "struct row count") == rowcount ||
        throw(ArgumentError("struct row count differs from its rank length"))
    _validatechildren(normalizednames, normalizedchildren, Int(last(normalizedranks)))
    T = Union{Missing,StructValue}
    return StructVector{T,typeof(normalizedranks)}(
        normalizednames, normalizedranks, normalizedchildren, rowcount)
end

function StructVector(names::AbstractVector{<:AbstractString},
        ranks::AbstractVector{<:Integer}, children::Union{Tuple,AbstractVector}; rows=nothing)
    return StructVector(names, children; ranks=ranks, rows=rows)
end

function Base.IndexStyle(::Type{<:StructVector})
    return IndexLinear()
end

function Base.size(column::StructVector)
    return (column.rows,)
end

function _validatestructlocal(column::StructVector)
    names = column.names
    children = column.children
    count = length(children)
    length(names) == count || throw(ArgumentError(
        "struct child identity or order changed: vector has a different " *
        "number of names and children"))
    column.rows >= 0 || throw(ArgumentError(
        "struct vector has a negative row count"))
    if column.ranks === nothing
        return column.rows
    end
    isempty(children) && throw(ArgumentError(
        "optional zero-field struct has unobservable presence"))
    ranks = column.ranks
    length(ranks) == column.rows + 1 || throw(ArgumentError(
        "struct rank length differs from its row count"))
    isempty(ranks) && throw(ArgumentError(
        "struct ranks lost their initial zero"))
    iszero(first(ranks)) || throw(ArgumentError(
        "struct ranks no longer start at zero"))
    previous = Int(first(ranks))
    for row in 1:column.rows
        current = Int(ranks[row + 1])
        current >= previous || throw(ArgumentError(
            "struct ranks are no longer nondecreasing"))
        current - previous <= 1 || throw(ArgumentError(
            "struct rank difference exceeds one"))
        previous = current
    end
    return previous
end

function _validatestructvector(column::StructVector)
    terminal = _validatestructlocal(column)
    label = column.ranks === nothing ? "required" : "optional"
    for (index, child) in enumerate(column.children)
        _nestedvectorcount(child, "struct child $index") == terminal ||
            throw(ArgumentError(
                "$label struct child $index length differs from its " *
                (label == "required" ? "row count" : "terminal rank")))
        _validatenestedphysicalspan(child, terminal,
            "$label struct child $index")
    end
    _validatestructlocal(column)
    return
end

function Base.getindex(column::StructVector, index::Int)
    @boundscheck checkbounds(column, index)
    if column.ranks === nothing
        return StructValue(column.names, column.children, index)
    end
    firstindex = Int(column.ranks[index])
    lastindex = Int(column.ranks[index + 1])
    firstindex == lastindex && return missing
    return StructValue(column.names, column.children, lastindex)
end

struct MapValue{K,V,HasValues} <: AbstractVector{Pair{K,V}}
    keys::AbstractVector{K}
    values::Union{Nothing,AbstractVector{V}}
    first::Int
    last::Int
end

function _validatemapvaluekeys(value::MapValue, count::Int)
    keycount = _nestedvectorcount(value.keys, "map view key vector")
    keyfirst, keylast = _nestedvectoraxes(value.keys, "map view key vector")
    keyfirst == 1 && keylast == keycount || throw(ArgumentError(
        "map view key vector must use one-based contiguous axes"))
    (value.first <= keycount || (keycount < typemax(Int) &&
        value.first == keycount + 1)) || throw(ArgumentError(
        "map view insertion point exceeds its key vector"))
    if !iszero(count)
        checkbounds(Bool, value.keys, value.first) &&
            checkbounds(Bool, value.keys, value.last) || throw(ArgumentError(
                "map view entry span exceeds its key-vector axes"))
    end
    return keycount
end

function _validatemapvaluevalues(value::MapValue{K,V,HasValues}, count::Int,
        keycount::Int) where {K,V,HasValues}
    if HasValues === true
        values = value.values
        values === nothing && throw(ArgumentError(
            "map view lost its value vector"))
        valuecount = _nestedvectorcount(values, "map view value vector")
        valuefirst, valuelast = _nestedvectoraxes(values,
            "map view value vector")
        valuefirst == 1 && valuelast == valuecount || throw(ArgumentError(
            "map view value vector must use one-based contiguous axes"))
        valuecount == keycount || throw(ArgumentError(
            "map view key and value lengths differ"))
        (value.first <= valuecount || (valuecount < typemax(Int) &&
            value.first == valuecount + 1)) || throw(ArgumentError(
            "map view insertion point exceeds its value vector"))
        if !iszero(count)
            checkbounds(Bool, values, value.first) &&
                checkbounds(Bool, values, value.last) || throw(ArgumentError(
                    "map view entry span exceeds its value-vector axes"))
        end
    else
        value.values === nothing || throw(ArgumentError(
            "key-only map view gained a value vector"))
    end
    return
end

function _validatemapvalue(value::MapValue{K,V,HasValues}) where {K,V,HasValues}
    HasValues isa Bool || throw(ArgumentError(
        "map view has a non-Boolean value-vector discriminator"))
    Missing <: K && throw(ArgumentError(
        "map view key type cannot include Missing"))
    HasValues === false && V !== Missing && throw(ArgumentError(
        "key-only map view must use Missing as its value type"))
    count = _nestedviewcount(value.first, value.last, "map view")
    keycount = _validatemapvaluekeys(value, count)
    _validatemapvaluevalues(value, count, keycount)
    keycount = _validatemapvaluekeys(value, count)
    _validatemapvaluevalues(value, count, keycount)
    return
end

function Base.IndexStyle(::Type{<:MapValue})
    return IndexLinear()
end

function Base.size(value::MapValue)
    return (_nestedviewcount(value.first, value.last, "map view"),)
end

function Base.getindex(value::MapValue{K,V,true}, index::Int) where {K,V}
    _validatemapvalue(value)
    @boundscheck checkbounds(value, index)
    physical = value.first + index - 1
    keys = value.keys
    values = something(value.values)
    key = keys[physical]
    value.keys === keys && value.values === values || throw(ArgumentError(
        "map view changed its backing vectors during key access"))
    _validatemapvalue(value)
    item = values[physical]
    _validatemapvalue(value)
    return Pair{K,V}(key, item)
end

function Base.getindex(value::MapValue{K,Missing,false}, index::Int) where {K}
    _validatemapvalue(value)
    @boundscheck checkbounds(value, index)
    physical = value.first + index - 1
    key = value.keys[physical]
    _validatemapvalue(value)
    return Pair{K,Missing}(key, missing)
end

function Base.getindex(value::MapValue{K,V,HasValues}, index::Int) where
        {K,V,HasValues}
    _validatemapvalue(value)
    throw(ArgumentError("map view has invalid type parameters"))
end

function Base.collect(value::MapValue)
    return _collectnestedview(value)
end

function Base.copy(value::MapValue)
    return collect(value)
end

struct MapVector{T,K,V,HasValues,O<:_NestedIndex,
        Validity<:Union{Nothing,BitVector}} <: AbstractVector{T}
    offsets::Vector{O}
    validity::Validity
    keys::AbstractVector{K}
    values::Union{Nothing,AbstractVector{V}}
end

function _validatemapvalues(keys::AbstractVector, values::Nothing)
    Base.@nospecialize keys
    count = _nestedvectorcount(keys, "map key")
    _validatenestedphysicalshape(keys, count, "map key")
    return count
end

function _validatemapvalues(keys::AbstractVector, values::AbstractVector)
    Base.@nospecialize keys values
    count = _nestedvectorcount(keys, "map key")
    _validatenestedphysicalshape(keys, count, "map key")
    _validatenestedphysicalshape(values, count, "map value")
    return count
end

function MapVector(offsets::AbstractVector{<:Integer}, keys::AbstractVector,
        values::Union{Nothing,AbstractVector}=nothing;
        validity::Union{Nothing,AbstractVector{Bool}}=nothing)
    Base.@nospecialize keys values
    Missing <: eltype(keys) && throw(ArgumentError("map key type cannot include Missing"))
    count = _validatemapvalues(keys, values)
    any(ismissing, keys) && throw(ArgumentError("map keys cannot be missing"))
    _validatemapvalues(keys, values) == count || throw(ArgumentError(
        "map key count changed during validation"))
    normalized = _nestedindices(offsets, "map offsets")
    rows = length(normalized) - 1
    Int(last(normalized)) == count || throw(ArgumentError(
        "map terminal offset $(last(normalized)) differs from key length $count"))
    valid = _nestedvalidity(validity, rows, normalized)
    K = eltype(keys)
    hasvalues = values !== nothing
    V = hasvalues ? eltype(values) : Missing
    R = MapValue{K,V,hasvalues}
    T = valid === nothing ? R : Union{Missing,R}
    return MapVector{T,K,V,hasvalues,eltype(normalized),typeof(valid)}(
        normalized, valid, keys, values)
end

function MapVector(offsets::AbstractVector{<:Integer}, validity::AbstractVector{Bool},
        keys::AbstractVector, values::Union{Nothing,AbstractVector})
    return MapVector(offsets, keys, values; validity=validity)
end

function Base.IndexStyle(::Type{<:MapVector})
    return IndexLinear()
end

function Base.size(column::MapVector)
    return (length(column.offsets) - 1,)
end

function _validatemaplocal(column::MapVector{T,K,V,HasValues}) where
        {T,K,V,HasValues}
    HasValues isa Bool || throw(ArgumentError(
        "map vector has a non-Boolean value-vector discriminator"))
    Missing <: K && throw(ArgumentError(
        "map vector key type cannot include Missing"))
    HasValues === false && V !== Missing && throw(ArgumentError(
        "key-only map vector must use Missing as its value type"))
    offsets = column.offsets
    isempty(offsets) && throw(ArgumentError(
        "map offsets lost their initial zero"))
    iszero(first(offsets)) || throw(ArgumentError(
        "map offsets no longer start at zero"))
    rows = length(offsets) - 1
    column.validity === nothing || length(column.validity) == rows || throw(
        ArgumentError("map validity length differs from its row count"))
    previous = Int(first(offsets))
    for row in 1:rows
        current = Int(offsets[row + 1])
        current >= previous || throw(ArgumentError(
            "MAP offsets are no longer nondecreasing"))
        column.validity === nothing || column.validity[row] ||
            current == previous || throw(ArgumentError(
                "null MAP row has a nonempty entry span"))
        previous = current
    end
    return previous
end

function _validatemapvector(column::MapVector{T,K,V,HasValues}) where
        {T,K,V,HasValues}
    terminal = _validatemaplocal(column)
    _validatenestedphysicalspan(column.keys, terminal, "MAP key")
    if HasValues === true
        values = column.values
        values === nothing && throw(ArgumentError(
            "map vector lost its values"))
        _validatenestedphysicalspan(values, terminal, "MAP value")
    else
        column.values === nothing || throw(ArgumentError(
            "key-only map vector gained values"))
    end
    _validatemaplocal(column)
    return
end

function Base.getindex(column::MapVector{T,K,V,HasValues}, index::Int) where
        {T,K,V,HasValues}
    HasValues isa Bool || throw(ArgumentError(
        "map vector has a non-Boolean value-vector discriminator"))
    Missing <: K && throw(ArgumentError(
        "map vector key type cannot include Missing"))
    HasValues === false && V !== Missing && throw(ArgumentError(
        "key-only map vector must use Missing as its value type"))
    @boundscheck checkbounds(column, index)
    column.validity === nothing || column.validity[index] || return missing
    firstindex, lastindex = _nestedspan(column.offsets, index)
    return MapValue{K,V,HasValues}(
        column.keys, column.values, firstindex, lastindex)
end

function maplookup(value::MapValue, key)
    for index in length(value):-1:1
        pair = value[index]
        isequal(pair.first, key) && return pair.second
    end
    throw(KeyError(key))
end

function maplookup(value::MapValue, key, default)
    for index in length(value):-1:1
        pair = value[index]
        isequal(pair.first, key) && return pair.second
    end
    return default
end

struct _StableByteKey <: AbstractVector{UInt8}
    bytes::String
end

function Base.IndexStyle(::Type{_StableByteKey})
    return IndexLinear()
end

function Base.size(value::_StableByteKey)
    return (ncodeunits(value.bytes),)
end

function Base.getindex(value::_StableByteKey, index::Int)
    @boundscheck checkbounds(value, index)
    return codeunit(value.bytes, index)
end

struct _StableListKey <: AbstractVector{Any}
    values::Core.SimpleVector
end

function Base.IndexStyle(::Type{_StableListKey})
    return IndexLinear()
end

function Base.size(value::_StableListKey)
    return (length(value.values),)
end

function Base.getindex(value::_StableListKey, index::Int)
    @boundscheck checkbounds(value, index)
    return value.values[index]
end

struct _StableStructKey
    names::Core.SimpleVector
    values::Core.SimpleVector
end

function _structkeylength(value::StructValue)
    return length(value)
end

function _structkeylength(value::_StableStructKey)
    return length(value.names)
end

function _structkeyname(value::StructValue, index::Int)
    return value.names[index]
end

function _structkeyname(value::_StableStructKey, index::Int)
    return value.names[index]
end

function _structkeyvalue(value::StructValue, index::Int)
    return value[index]
end

function _structkeyvalue(value::_StableStructKey, index::Int)
    return value.values[index]
end

function _structkeyisequal(left, right)
    length = _structkeylength(left)
    length == _structkeylength(right) || return false
    for index in 1:length
        isequal(_structkeyname(left, index), _structkeyname(right, index)) || return false
        isequal(_structkeyvalue(left, index), _structkeyvalue(right, index)) || return false
    end
    return true
end

function Base.isequal(left::_StableStructKey, right::_StableStructKey)
    return _structkeyisequal(left, right)
end

function Base.isequal(left::_StableStructKey, right::StructValue)
    return _structkeyisequal(left, right)
end

function Base.isequal(left::StructValue, right::_StableStructKey)
    return _structkeyisequal(left, right)
end

function Base.hash(value::_StableStructKey, seed::UInt)
    output = hash(:ParquetStructValue, seed)
    output = hash(_structkeylength(value), output)
    for index in 1:_structkeylength(value)
        output = hash(_structkeyname(value, index), output)
        output = hash(_structkeyvalue(value, index), output)
    end
    return output
end

function _stablekeyerror(value)
    throw(ArgumentError(
        "map key type $(typeof(value)) does not have stable content hash semantics"))
end

function _stablekeysimplevector(value)
    output = []
    sizehint!(output, length(value))
    for item in value
        push!(output, _snapshotmapkey(item))
    end
    return Core.svec(output...)
end

function _snapshotmapkey(value::ListValue)
    return _StableListKey(_stablekeysimplevector(value))
end

function _snapshotmapkey(value::StructValue)
    names = Core.svec(value.names...)
    output = []
    sizehint!(output, length(value))
    for index in 1:length(value)
        push!(output, _snapshotmapkey(value[index]))
    end
    return _StableStructKey(names, Core.svec(output...))
end

function _snapshotmapkey(value::MapValue)
    return _stablekeyerror(value)
end

function _snapshotmapkey(value::Tuple)
    return map(_snapshotmapkey, value)
end

function _snapshotmapkey(value::NamedTuple)
    mapped = map(_snapshotmapkey, values(value))
    return NamedTuple{keys(value)}(mapped)
end

function _snapshotmapkey(value::AbstractString)
    return String(value)
end

function _snapshotmapkey(value::Symbol)
    return value
end

function _snapshotmapkey(value::AbstractVector{UInt8})
    return _StableByteKey(String(collect(value)))
end

function _snapshotmapkey(value::AbstractArray)
    return _stablekeyerror(value)
end

function _snapshotmapkey(value::Missing)
    return missing
end

function _snapshotmapkey(value::Decimal)
    return copy(value)
end

function _snapshotmapkey(value::JSONValue)
    return copy(value)
end

function _snapshotmapkey(value::BSONValue)
    return copy(value)
end

function _snapshotmapkey(value::_StableByteKey)
    return value
end

function _snapshotmapkey(value::_StableListKey)
    return value
end

function _snapshotmapkey(value::_StableStructKey)
    return value
end

function _snapshotmapkey(value)
    value === nothing && return _stablekeyerror(value)
    type = typeof(value)
    Base.ismutabletype(type) && return _stablekeyerror(value)
    (isbitstype(type) || fieldcount(type) == 0) && return value
    return _stablekeyerror(value)
end

function (::Type{Dict})(value::MapValue{K,V}) where {K,V}
    keys = []
    values = Vector{V}(undef, length(value))
    sizehint!(keys, length(value))
    Key = Union{}
    for (index, pair) in enumerate(value)
        ismissing(pair.first) && throw(ArgumentError("map keys cannot be missing"))
        key = _snapshotmapkey(pair.first)
        push!(keys, key)
        values[index] = pair.second
        Key = typejoin(Key, typeof(key))
    end
    isempty(keys) && (Key = Any)
    output = Dict{Key,V}()
    sizehint!(output, length(value))
    for index in eachindex(keys, values)
        output[keys[index]] = values[index]
    end
    return output
end
