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

# Nested columns. A value type alone fixes the vector that stores such values
# (`_columntype`), so nested columns keep their child vectors untyped and read them back
# through that type: column types take only their element type as a parameter, however
# deep the schema.

# A row of a list column: a read-only view of `values[first:last]`, where `values` is the
# column's element vector (a `_columntype(T)`). `copy(row)` gives a `Vector{T}`.
struct ListValue{T} <: AbstractVector{T}
    values::Any
    first::Int
    last::Int
end

# A row of a map column: its entries as ordered `key => value` pairs (duplicate keys are
# kept; `Dict(row)` keeps the last). `keys` and `values` are the column's entry vectors.
struct MapValue{K,V} <: AbstractVector{Pair{K,V}}
    keys::Any
    values::Any
    first::Int
    last::Int
end

# A row of a struct column whose field names repeat or number more than
# `_MAX_NAMEDTUPLE_FIELDS`, or that holds more than `_MAX_ROW_DEPTH` levels of nested
# values: its fields as `name => value` pairs. `row[name]` looks a field up by name.
# Other struct rows are `NamedTuple`s.
struct StructValue <: AbstractVector{Pair{String,Any}}
    names::Vector{String}
    children::Vector{AbstractVector}
    index::Int
end

const _MAX_NAMEDTUPLE_FIELDS = 64
# A struct's rows are NamedTuples only up to this many levels of nested values (counting
# itself), so a deep schema yields a bounded number of row types for code to compile.
const _MAX_ROW_DEPTH = 32

# Row `i` is `missing` where `validity[i]` is false, else the `ListValue` of
# `values[(offsets[i] + 1):offsets[i + 1]]`. `validity` is empty when `T` excludes
# `missing`.
struct ListVector{T} <: AbstractVector{T}
    offsets::Vector{Int}
    validity::BitVector
    values::Any
end

# Row `i` is `missing` where `validity[i]` is false, else the row built from element `i`
# of each child (one vector per field, `length` long).
struct StructVector{T} <: AbstractVector{T}
    names::Vector{String}
    children::Vector{AbstractVector}
    validity::BitVector
    length::Int
end

# Row `i` is `missing` where `validity[i]` is false, else the `MapValue` of entries
# `(offsets[i] + 1):offsets[i + 1]` of `keys` and `values`.
struct MapVector{T} <: AbstractVector{T}
    offsets::Vector{Int}
    validity::BitVector
    keys::Any
    values::Any
end

_columntype(::Type{T}) where {T} = _columntype(T, Base.nonmissingtype(T))
_columntype(::Type{Missing}) = Vector{Missing}
_columntype(::Type{T}, ::Type) where {T} = Vector{T}
_columntype(::Type{T}, ::Type{DataStrings.DataString}) where {T} = DataStrings.StringVector{T}
_columntype(::Type{T}, ::Type{DataStrings.DataBytes}) where {T} = DataStrings.BytesVector{T}
_columntype(::Type{T}, ::Type{<:ListValue}) where {T} = ListVector{T}
_columntype(::Type{T}, ::Type{<:MapValue}) where {T} = MapVector{T}
_columntype(::Type{T}, ::Type{<:NamedTuple}) where {T} = StructVector{T}
_columntype(::Type{T}, ::Type{StructValue}) where {T} = StructVector{T}

# The row type of a struct with these field names and field types, `depth` levels of
# nested values deep.
function _structrowtype(names::AbstractVector{String}, types, depth::Int)
    length(names) <= _MAX_NAMEDTUPLE_FIELDS && allunique(names) &&
        depth <= _MAX_ROW_DEPTH || return StructValue
    return NamedTuple{Tuple(Symbol(name) for name in names),Tuple{types...}}
end

# Levels of nested lists, maps, and structs in a column's values, counted up to `limit`.
_nesteddepth(::AbstractVector, ::Int) = 0
_nesteddepth(column::ListVector, limit::Int) =
    limit <= 0 ? 1 : 1 + _nesteddepth(column.values, limit - 1)
_nesteddepth(column::MapVector, limit::Int) = limit <= 0 ? 1 :
    1 + max(_nesteddepth(column.keys, limit - 1), _nesteddepth(column.values, limit - 1))
_nesteddepth(column::StructVector, limit::Int) = limit <= 0 ? 1 :
    1 + maximum(child -> _nesteddepth(child, limit - 1), column.children; init=0)

Base.IndexStyle(::Type{<:Union{ListValue,MapValue,StructValue}}) = IndexLinear()
Base.IndexStyle(::Type{<:Union{ListVector,StructVector,MapVector}}) = IndexLinear()

Base.size(row::Union{ListValue,MapValue}) = (row.last - row.first + 1,)
Base.size(row::StructValue) = (length(row.names),)
Base.size(column::Union{ListVector,MapVector}) = (length(column.offsets) - 1,)
Base.size(column::StructVector) = (column.length,)

# Element `index` of `values`, a vector of `T` values: read through its `_columntype` (as
# every read column is), or dynamically for another vector (hand-built writer inputs).
Base.@propagate_inbounds function _childvalue(::Type{T}, values, index::Int) where {T}
    C = _columntype(T)
    values isa C && return (values::C)[index]
    return values[index]::T
end

Base.@propagate_inbounds function Base.getindex(row::ListValue{T}, index::Int) where {T}
    @boundscheck checkbounds(row, index)
    return @inbounds _childvalue(T, row.values, row.first + index - 1)
end

Base.@propagate_inbounds function Base.getindex(row::MapValue{K,V}, index::Int) where {K,V}
    @boundscheck checkbounds(row, index)
    entry = row.first + index - 1
    return @inbounds Pair{K,V}(_childvalue(K, row.keys, entry), _childvalue(V, row.values, entry))
end

Base.@propagate_inbounds function Base.getindex(row::StructValue, index::Int)
    @boundscheck checkbounds(row, index)
    return row.names[index] => row.children[index][row.index]
end

function Base.getindex(row::StructValue, name::Union{AbstractString,Symbol})
    key = String(name)
    found = findfirst(==(key), row.names)
    found === nothing && throw(KeyError(name))
    findnext(==(key), row.names, found + 1) === nothing || throw(ArgumentError(
        "struct field name $(repr(key)) is ambiguous"))
    return row.children[found][row.index]
end

Base.@propagate_inbounds function Base.getindex(column::ListVector{T}, index::Int) where {T}
    @boundscheck checkbounds(column, index)
    R = Base.nonmissingtype(T)
    T === R || @inbounds(column.validity[index]) || return missing
    @inbounds return R(column.values, column.offsets[index] + 1, column.offsets[index + 1])
end

Base.@propagate_inbounds function Base.getindex(column::MapVector{T}, index::Int) where {T}
    @boundscheck checkbounds(column, index)
    R = Base.nonmissingtype(T)
    T === R || @inbounds(column.validity[index]) || return missing
    @inbounds return R(column.keys, column.values, column.offsets[index] + 1,
        column.offsets[index + 1])
end

Base.@propagate_inbounds function Base.getindex(column::StructVector{T}, index::Int) where {T}
    @boundscheck checkbounds(column, index)
    R = Base.nonmissingtype(T)
    T === R || @inbounds(column.validity[index]) || return missing
    return _structrow(R, column.names, column.children, index)
end

_structrow(::Type{StructValue}, names, children, index::Int) =
    StructValue(names, children, index)

# Generated so each field reads through its own type (an `ntuple` closure that is not
# inlined looks field types up at run time, allocating per row).
@generated function _structrow(::Type{R}, names, children::Vector{AbstractVector},
        index::Int) where {R<:NamedTuple}
    fields = [:(@inbounds _childvalue($(fieldtype(R, field)), children[$field], index))
        for field in 1:fieldcount(R)]
    return :(R(($(fields...),)))
end

# Converting plain values to rows (as `push!` on a column of rows does) copies them into
# vectors of their own.
Base.convert(::Type{ListValue{T}}, row::ListValue{T}) where {T} = row
Base.convert(::Type{MapValue{K,V}}, row::MapValue{K,V}) where {K,V} = row

function Base.convert(::Type{ListValue{T}}, items::AbstractVector) where {T}
    values = _fromrows(_columntype(T), items)
    return ListValue{T}(values, 1, length(values))
end

function Base.convert(::Type{MapValue{K,V}},
        entries::Union{AbstractDict,AbstractVector{<:Pair}}) where {K,V}
    keys = _fromrows(_columntype(K), [entry.first for entry in entries])
    values = _fromrows(_columntype(V), [entry.second for entry in entries])
    return MapValue{K,V}(keys, values, 1, length(keys))
end

# The vector of type `C` (a `_columntype`) holding `rows`.
_fromrows(::Type{Vector{T}}, rows::AbstractVector) where {T} = Vector{T}(rows)
_fromrows(::Type{DataStrings.StringVector{T}}, rows::AbstractVector) where {T} =
    DataStrings.StringVector{T}(rows)

function _fromrows(::Type{DataStrings.BytesVector{T}}, rows::AbstractVector) where {T}
    payloads = Vector{DataStrings.StringPayload}(undef, length(rows))
    buffers = Vector{UInt8}[]
    for (index, row) in enumerate(rows)
        if row === missing
            Missing <: T || throw(ArgumentError("a $T column cannot hold missing"))
            payloads[index] = DataStrings.PAYLOAD_MISSING
            continue
        end
        bytes = collect(UInt8, row)
        count = length(bytes)
        payloads[index] = if count <= DataStrings.INLINE_MAX
            DataStrings.inline_payload(bytes, 1, count)
        else
            push!(buffers, bytes)
            DataStrings.view_payload(bytes, 1, count, length(buffers) - 1, 0)
        end
    end
    return DataStrings.BytesVector{T}(payloads, buffers, Val(:trusted))
end

_rowvalidity(::Type{T}, count::Int) where {T} =
    Missing <: T ? trues(count) : BitVector()

function _missingrow(::Type{T}, validity::BitVector, index::Int) where {T}
    Missing <: T || throw(ArgumentError("a column of $T rows cannot hold missing"))
    validity[index] = false
    return
end

function _fromrows(::Type{ListVector{T}}, rows::AbstractVector) where {T}
    E = eltype(Base.nonmissingtype(T))
    offsets = zeros(Int, length(rows) + 1)
    validity = _rowvalidity(T, length(rows))
    items = E[]
    for (index, row) in enumerate(rows)
        row === missing ? _missingrow(T, validity, index) : append!(items, row)
        offsets[index + 1] = length(items)
    end
    return ListVector{T}(offsets, validity, _fromrows(_columntype(E), items))
end

function _fromrows(::Type{MapVector{T}}, rows::AbstractVector) where {T}
    K, V = fieldtypes(eltype(Base.nonmissingtype(T)))
    offsets = zeros(Int, length(rows) + 1)
    validity = _rowvalidity(T, length(rows))
    keys, values = K[], V[]
    for (index, row) in enumerate(rows)
        if row === missing
            _missingrow(T, validity, index)
        else
            for entry in row
                push!(keys, entry.first)
                push!(values, entry.second)
            end
        end
        offsets[index + 1] = length(keys)
    end
    return MapVector{T}(offsets, validity, _fromrows(_columntype(K), keys),
        _fromrows(_columntype(V), values))
end

function _fromrows(::Type{StructVector{T}}, rows::AbstractVector) where {T}
    R = Base.nonmissingtype(T)
    R <: NamedTuple || throw(ArgumentError("cannot build a column of $R rows"))
    validity = _rowvalidity(T, length(rows))
    for (index, row) in enumerate(rows)
        row === missing && _missingrow(T, validity, index)
    end
    children = AbstractVector[_fromrows(_columntype(F),
        F[row === missing ? _filler(F) : getfield(convert(R, row), field) for row in rows])
        for (field, F) in enumerate(fieldtypes(R))]
    return StructVector{T}(String[String(name) for name in fieldnames(R)], children,
        validity, length(rows))
end

# A value of type `T` for a slot no row can observe (a required field of a null struct).
function _filler(::Type{T}) where {T}
    Missing <: T && return missing
    isbitstype(T) && return _zeroed(T)
    return _nonbitsfiller(T)
end

function _zeroed(::Type{T}) where {T}
    bytes = zeros(UInt8, max(sizeof(T), 1))
    return GC.@preserve bytes unsafe_load(Ptr{T}(pointer(bytes)))
end

_nonbitsfiller(::Type{DataStrings.DataString}) = DataStrings.DataString("")
_nonbitsfiller(::Type{DataStrings.DataBytes}) = DataStrings.DataBytes(UInt8[])
_nonbitsfiller(::Type{ListValue{T}}) where {T} = convert(ListValue{T}, T[])
_nonbitsfiller(::Type{MapValue{K,V}}) where {K,V} = convert(MapValue{K,V}, Pair{K,V}[])
_nonbitsfiller(::Type{R}) where {R<:NamedTuple} = R(map(_filler, fieldtypes(R)))
_nonbitsfiller(::Type{JSONValue}) = JSONValue(UInt8[], Val(:validated))
_nonbitsfiller(::Type{BSONValue}) = BSONValue(UInt8[], Val(:validated))
_nonbitsfiller(::Type{Decimal}) = Decimal(0, 0)
_nonbitsfiller(::Type{T}) where {T} = throw(ArgumentError(
    "no placeholder value of type $T for a null struct's required field"))

# Validated constructors from plain vectors (tests and hand-built writer inputs). Null
# rows of a list or map must be empty.

function _checkoffsets(offsets::AbstractVector{<:Integer}, count::Integer, label::String)
    !isempty(offsets) && first(offsets) == 0 || throw(ArgumentError(
        "$label offsets must start with zero"))
    issorted(offsets) || throw(ArgumentError("$label offsets must be nondecreasing"))
    last(offsets) == count || throw(ArgumentError(
        "$label offsets end at $(last(offsets)) for $count elements"))
    return Vector{Int}(offsets)
end

function _checkvalidity(validity, offsets::Vector{Int}, label::String)
    validity === nothing && return BitVector()
    length(validity) == length(offsets) - 1 || throw(ArgumentError(
        "$label validity has $(length(validity)) entries for $(length(offsets) - 1) rows"))
    for (row, valid) in enumerate(validity)
        valid || offsets[row] == offsets[row + 1] || throw(ArgumentError(
            "null $label row $row has entries"))
    end
    return BitVector(validity)
end

function ListVector(offsets::AbstractVector{<:Integer}, values::AbstractVector;
        validity::Union{Nothing,AbstractVector{Bool}}=nothing)
    normalized = _checkoffsets(offsets, length(values), "list")
    R = ListValue{eltype(values)}
    T = validity === nothing ? R : Union{Missing,R}
    return ListVector{T}(normalized, _checkvalidity(validity, normalized, "list"), values)
end

function MapVector(offsets::AbstractVector{<:Integer}, keys::AbstractVector,
        values::Union{Nothing,AbstractVector}=nothing;
        validity::Union{Nothing,AbstractVector{Bool}}=nothing)
    Missing <: eltype(keys) && throw(ArgumentError("map keys cannot be missing"))
    values === nothing && (values = Vector{Missing}(undef, length(keys)))
    length(values) == length(keys) || throw(ArgumentError(
        "map has $(length(keys)) keys and $(length(values)) values"))
    normalized = _checkoffsets(offsets, length(keys), "map")
    R = MapValue{eltype(keys),eltype(values)}
    T = validity === nothing ? R : Union{Missing,R}
    return MapVector{T}(normalized, _checkvalidity(validity, normalized, "map"), keys,
        values)
end

function StructVector(names::AbstractVector{<:AbstractString},
        children::Union{Tuple,AbstractVector};
        validity::Union{Nothing,AbstractVector{Bool}}=nothing, rows=nothing)
    length(names) == length(children) || throw(ArgumentError(
        "struct has $(length(names)) names for $(length(children)) children"))
    count = rows !== nothing ? Int(rows) : validity !== nothing ? length(validity) :
        isempty(children) ? throw(ArgumentError(
            "a struct with no fields needs an explicit row count")) : length(first(children))
    columns = AbstractVector[child for child in children]
    for (index, child) in enumerate(columns)
        length(child) == count || throw(ArgumentError(
            "struct child $index has $(length(child)) values for $count rows"))
    end
    validity === nothing || length(validity) == count || throw(ArgumentError(
        "struct validity has $(length(validity)) entries for $count rows"))
    depth = 1 + maximum(child -> _nesteddepth(child, _MAX_ROW_DEPTH), columns; init=0)
    R = _structrowtype(String[String(name) for name in names],
        Type[eltype(child) for child in columns], depth)
    T = validity === nothing ? R : Union{Missing,R}
    return StructVector{T}(String[String(name) for name in names], columns,
        validity === nothing ? BitVector() : BitVector(validity), count)
end
