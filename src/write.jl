import Tables

struct WriteEncodingChoice
    encoding::Union{Nothing,Metadata.Encoding.T}
    dictionary::Bool
end

function _writevalueerror(path::AbstractString, err)
    message = err isa ArgumentError ? err.msg : sprint(showerror, err)
    return ArgumentError("cannot write a value of field $(repr(String(path))): $message")
end

function _writecolumnname(name::Symbol)
    return String(name)
end

function _writecolumnname(name::AbstractString)
    return String(name)
end

function _writecolumnname(name)
    throw(ArgumentError(
        "Parquet column names must be Symbols or strings, got $(typeof(name))"))
end

function _validatewritecolumnnames(names)
    normalized = String[]
    sizehint!(normalized, length(names)::Int)
    seen = Set{String}()
    for raw in names
        name = _writecolumnname(raw)::String
        name in seen && throw(ArgumentError(
            "Parquet column names must be unique"))
        push!(normalized, name)
        push!(seen, name)
    end
    return normalized
end

Base.@noinline function _writerownamedtupleschema(T::Type)
    Base.@nospecialize T
    return fieldnames(T), fieldtypes(T)
end

Base.@noinline function _preflightwriteroweltype(rows, limits::Limits)
    Base.@nospecialize rows
    Base.IteratorEltype(typeof(rows)) isa Base.HasEltype || return nothing
    T = eltype(rows)
    isconcretetype(T) || return nothing
    if T <: NamedTuple
        _checklimit(:container_elements, fieldcount(T),
            limits.max_container_elements)
        return T
    end
    return nothing
end

Base.@noinline function _writerowschema(rows, limits::Limits)
    Base.@nospecialize rows
    declared = _preflightwriteroweltype(rows, limits)
    schema = Tables.schema(rows)
    if schema === nothing
        # The caller collects such rows into columns instead.
        declared === nothing && return nothing
        return _writerownamedtupleschema(declared)
    end
    names = schema.names
    types = schema.types
    names === nothing && throw(ArgumentError(
        "row-oriented Tables sources must declare column names"))
    types === nothing && throw(ArgumentError(
        "row-oriented Tables sources must declare column types"))
    _checklimit(:container_elements, length(names),
        limits.max_container_elements)
    length(names) == length(types) || throw(ArgumentError(
        "row-oriented Tables schema has different name and type counts"))
    return names, types
end

function _writerowcount(rows, limits::Limits)
    Base.haslength(typeof(rows)) || return nothing
    count = length(rows)
    _checklimit(:container_elements, count,
        limits.max_container_elements)
    return count
end

function _preflightwriterowtypes(names::Vector{String}, types,
        limits::Limits)
    for (name, T) in zip(names, types)
        T isa Type || throw(ArgumentError(
            "row-oriented Tables schemas must contain Julia types"))
        # Abstract and Union column types are narrowed from the collected values.
        _writedeclared(T) && _nestedwriteshape(name, T, nothing, limits)
    end
    return
end

function _writerowarrays(types, count::Int)
    columns = AbstractVector[]
    sizehint!(columns, length(types))
    for T in types
        push!(columns, Vector{T}(undef, count))
    end
    return columns
end

# Checked as rows are collected, so a value over a limit fails before later rows are read.
function _checkwriterowvalue(value, limits::Limits, depth::Int=1)
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    (ismissing(value) || isbits(value)) && return
    if value isa Union{JSONValue,BSONValue}
        _checklimit(:string_bytes, length(value.bytes), limits.max_string_bytes)
    elseif value isa Decimal
        _checklimit(:decimal_bytes, _twoscomplementwidth(value.unscaled),
            limits.max_decimal_bytes)
    elseif value isa AbstractString
        _checklimit(:string_bytes, ncodeunits(value), limits.max_string_bytes)
    elseif value isa AbstractVector
        _checklimit(:container_elements, length(value),
            limits.max_container_elements)
        for item in value
            _checkwriterowvalue(item, limits, depth + 1)
        end
    elseif value isa NamedTuple
        for item in values(value)
            _checkwriterowvalue(item, limits, depth + 1)
        end
    elseif value isa Pair
        _checkwriterowvalue(first(value), limits, depth + 1)
        _checkwriterowvalue(last(value), limits, depth + 1)
    elseif value isa AbstractDict
        _checklimit(:container_elements, length(value),
            limits.max_container_elements)
        for (key, item) in value
            _checkwriterowvalue(key, limits, depth + 1)
            _checkwriterowvalue(item, limits, depth + 1)
        end
    end
    return
end

function _fillwriterowarrays!(columns::Vector{AbstractVector}, rows,
        count::Int, limits::Limits)
    position = 0
    for row in rows
        position += 1
        position <= count || throw(ArgumentError(
            "row-oriented Tables source yielded more rows than declared"))
        for index in eachindex(columns)
            value = Tables.getcolumn(row, index)
            _checkwriterowvalue(value, limits)
            columns[index][position] = value
        end
    end
    position == count || throw(ArgumentError(
        "row-oriented Tables source yielded $position rows but declared $count"))
    return columns
end

function _knownwriterowcolumns(rows, types, count::Int, limits::Limits)
    columns = _writerowarrays(types, count)
    _fillwriterowarrays!(columns, rows, count, limits)
    return columns, count
end

function _pushwriterow!(columns::Vector{AbstractVector}, row, limits::Limits)
    for index in eachindex(columns)
        value = Tables.getcolumn(row, index)
        _checkwriterowvalue(value, limits)
        push!(columns[index], value)
    end
    return
end

function _unknownwriterowcolumns(rows, types, limits::Limits)
    columns = _writerowarrays(types, 0)
    count = 0
    for row in rows
        requested = try
            Base.checked_add(count, 1)
        catch err
            err isa OverflowError || rethrow()
            throw(LimitError(:container_elements, typemax(Int64),
                limits.max_container_elements))
        end
        _checklimit(:container_elements, requested,
            limits.max_container_elements)
        _pushwriterow!(columns, row, limits)
        count = requested
    end
    return columns, count
end

Base.@nospecializeinfer function _writeinputrowcolumns(table, limits::Limits)
    Base.@nospecialize table
    rowsource = Tables.rows(table)
    schema = _writerowschema(rowsource, limits)
    schema === nothing && return _writeinputcolumns(_collectrows(table), limits)
    names, types = schema
    normalized = _validatewritecolumnnames(names)
    _preflightwriterowtypes(normalized, types, limits)
    count = _writerowcount(rowsource, limits)
    columns, rowcount = count === nothing ?
        _unknownwriterowcolumns(rowsource, types, limits) :
        _knownwriterowcolumns(rowsource, types, count, limits)
    output = Pair{String,AbstractVector}[]
    sizehint!(output, length(columns))
    for index in eachindex(normalized)
        push!(output, Pair{String,AbstractVector}(normalized[index],
            _barrier(_writeinputcolumn, normalized[index], columns[index])))
    end
    return output, rowcount
end

# Rows without a declared schema (a generator of NamedTuples, a vector of dictionaries) are
# collected into columns as Tables.columns collects them, widening types as values come.
function _collectrows(table)
    Base.@nospecialize table
    message = "Parquet.write needs a Tables.jl table, such as a DataFrame, a NamedTuple " *
        "of vectors, or a vector of NamedTuples; got $(typeof(table))"
    # A vector of DataFrames, for example, holds tables rather than rows.
    Base.IteratorEltype(typeof(table)) isa Base.HasEltype && eltype(table) !== Any &&
        Tables.istable(eltype(table)) && throw(ArgumentError(message * ", which holds " *
            "tables; pass Tables.partitioner(tables) to write them into one file"))
    try
        return Tables.columns(table)
    catch err
        err isa ArgumentError && occursin("AbstractRow", err.msg) || rethrow()
        throw(ArgumentError(message))
    end
end

Base.@nospecializeinfer function _writeinputcolumns(table, limits::Limits)
    Base.@nospecialize table
    Tables.columnaccess(table) || return _barrier(_writeinputrowcolumns, table,
        limits)::Tuple{Vector{Pair{String,AbstractVector}},Int}
    output, rows = _writecolumnvectors(Tables.columns(table), limits)
    for index in eachindex(output)
        name, values = output[index]
        output[index] = name => _barrier(_writeinputcolumn, name, values)
    end
    return output, rows
end

# Validate the complete column table before either a file or a partitioned write begins.
Base.@nospecializeinfer function _writecolumnvectors(columns, limits::Limits)
    Base.@nospecialize columns
    names = collect(Tables.columnnames(columns))
    normalized = _validatewritecolumnnames(names)
    output = Pair{String,AbstractVector}[]
    sizehint!(output, length(names)::Int)
    rows = nothing
    for (raw, name) in zip(names, normalized)
        values = Tables.getcolumn(columns, raw)
        values isa AbstractVector || throw(ArgumentError(
            "Parquet columns must be vectors"))
        if rows === nothing
            rows = length(values)
            _checklimit(:container_elements, rows,
                limits.max_container_elements)
        end
        length(values) == rows || throw(ArgumentError(
            "Parquet columns have different lengths"))
        push!(output, name => values)
    end
    return output, something(rows, 0)
end

# A column whose element type is abstract, a Union of several types, or a wrapper such
# as CategoricalValue is narrowed from its values to one concrete element type. Pooled
# columns (PooledArray, CategoricalArray) stay pooled so their pool becomes the
# dictionary.
function _writeinputcolumn(name::String, values::AbstractVector)
    pooled = _pooledcolumn(values)
    pooled === nothing || return pooled
    # Compiled only for the columns that need it, not with every column type's write.
    _writedeclared(eltype(values)) || (values = _barrier(_narrowwritecolumn, name, values))
    # Extensions add methods for their packages' column types.
    return _barrier(_writeextensioncolumn, name, values)::AbstractVector
end

# Extensions convert columns of their packages' types to types this package writes
# (ParquetTimeZonesExt: ZonedDateTime to Durations.ZonedTimestamp).
_writeextensioncolumn(name::String, values::AbstractVector) = values

"""
    _PooledColumn{T}

A pooled column (`DataAPI.refpool` is not `nothing`) seen through `DataAPI.unwrap`, with
element type `T`: the unwrapped values, and `Missing` when the pool holds `missing`.
"""
struct _PooledColumn{T,V<:AbstractVector,R<:AbstractVector,P} <: AbstractVector{T}
    values::V
    refs::R
    pool::P
end

Base.size(column::_PooledColumn) = size(column.values)
Base.IndexStyle(::Type{<:_PooledColumn}) = IndexLinear()
Base.@propagate_inbounds Base.getindex(column::_PooledColumn{T}, index::Int) where {T} =
    DataAPI.unwrap(column.pool[column.refs[index]])::T

function _pooledcolumn(values::AbstractVector)
    values isa _PooledColumn && return values
    pool = DataAPI.refpool(values)
    pool === nothing && return nothing
    refs = DataAPI.refarray(values)
    refs === values && return nothing
    T = Union{}
    nulls = false
    for ref in axes(pool, 1)
        value = DataAPI.unwrap(pool[ref])
        value === missing ? (nulls = true) : (T = typejoin(T, typeof(value)))
    end
    T === Union{} && return nothing
    (isconcretetype(T) && _writemap(T) !== nothing) || return nothing
    nulls |= Missing <: eltype(values)
    E = nulls ? Union{Missing,T} : T
    return _PooledColumn{E,typeof(values),typeof(refs),typeof(pool)}(values, refs, pool)
end

function _writedeclared(T::Type)
    T === Missing && return true
    S = Base.nonmissingtype(T)
    # A tuple type is abstract when a field may be missing; it still writes as a list.
    S isa DataType && S <: Tuple && !isempty(S.parameters) &&
        !Base.isvarargtype(S.parameters[end]) &&
        return all(F -> F === Missing || _writedeclared(F), fieldtypes(S))
    S !== Union{} && isconcretetype(S) || return false
    _writemap(S) === nothing || return true
    _writevaluescanned(S) && return true
    # Containers are checked field by field when their schema is inferred; one that
    # leaves element types open is narrowed from its values first.
    return S <: Union{AbstractVector,NamedTuple,AbstractDict,ListValue,StructValue,
        MapValue} && _writesettled(S)
end

function _narrowwritecolumn(name::String, values::AbstractVector)
    T = Union{}
    nulls = false
    seen = Union{}  # a value type whose values are all of that type, already in T
    for value in values
        if ismissing(value)
            nulls = true
        else
            item = DataAPI.unwrap(value)
            typeof(item) === seen && continue
            V = _writevaluetype(item, name)
            V === typeof(item) && (seen = V)
            T = _writepromote(T, V, name)
        end
    end
    T === Union{} && return Vector{Missing}(missing, length(values))
    _writesettled(T) || throw(ArgumentError("column $(repr(name)) holds only empty " *
        "containers in some field, so their element type is unknown; declare it, for " *
        "example Vector{Int}[] for empty lists of integers"))
    output = Vector{nulls ? Union{Missing,T} : T}(undef, length(values))
    for (index, value) in enumerate(values)
        output[index] = ismissing(value) ? missing :
            _writenarrowed(T, DataAPI.unwrap(value), name)
    end
    return output
end

# The type `value` writes as. A container whose type leaves its element types open (an
# empty `[]` is a `Vector{Any}`) takes the type its elements share, Union{} if it has none.
# Compiled and inferred once: values of every type pass through here.
Base.@nospecializeinfer function _writevaluetype(@nospecialize(value), name::String)
    T = typeof(value)
    _writesettled(T) && return T
    # A tuple writes as a list, so its elements take the one type they share.
    value isa Tuple && !isempty(value) &&
        return NTuple{length(value),_writeelementtype(value, name)}
    value isa AbstractVector && return Vector{_writeelementtype(value, name)}
    # A map row keeps its entries' order and duplicate keys, which a `Dict` would not.
    value isa MapValue && return Vector{Pair{_writeelementtype(keys(value), name),
        _writeelementtype(values(value), name)}}
    value isa AbstractDict && return Dict{_writeelementtype(keys(value), name),
        _writeelementtype(values(value), name)}
    value isa Pair && return Pair{_writevaluetype(value.first, name),
        _writevaluetype(value.second, name)}
    value isa NamedTuple && return NamedTuple{keys(value),
        Tuple{(_writevaluetype(field, name) for field in value)...}}
    value isa Tuple && return Tuple{(_writevaluetype(field, name) for field in value)...}
    return T
end

function _writeelementtype(items, name::String)
    T = Union{}
    for item in items
        T = _writepromote(T, _writevaluetype(item, name), name)
    end
    return T
end

# Whether type T fixes the write type of all of its values, element types included.
# Inferred once, not for each column type it is asked about.
Base.@nospecializeinfer function _writesettled(@nospecialize(T::Type))
    T === Missing && return true
    S = Base.nonmissingtype(T)
    # A tuple type is abstract when a field may be missing. It writes as a list, so it is
    # settled when its fields have one type.
    S isa DataType && S <: Tuple && !any(Base.isvarargtype, S.parameters) &&
        return allequal(S.parameters) && all(_writesettled, S.parameters)
    isconcretetype(S) || return false
    S <: StructValue && return true
    # Packages define these for their own types, so they are looked up at run time.
    S <: AbstractVector && return _writesettled(_barrier(eltype, S))
    S <: AbstractDict &&
        return _writesettled(_barrier(keytype, S)) && _writesettled(_barrier(valtype, S))
    S <: Union{NamedTuple,Pair} && return all(_writesettled, fieldtypes(S))
    return true
end

# The one element type that holds values of types A and B exactly.
function _writepromote(A::Type, B::Type, name::String)
    A === Union{} && return B
    B === Union{} && return A
    A === B && return A
    Nothing in (A, B) && throw(ArgumentError("column $(repr(name)) contains " *
        "nothing; use missing for null values"))
    A === Missing && return Union{Missing,B}
    B === Missing && return Union{Missing,A}
    # Removing Missing from an abstract type such as Any leaves it unchanged.
    S, U = Base.nonmissingtype(A), Base.nonmissingtype(B)
    (S, U) == (A, B) || return Union{Missing,_writepromote(S, U, name)}
    A <: AbstractString && B <: AbstractString && return String
    if A <: NamedTuple && B <: NamedTuple && fieldnames(A) == fieldnames(B)
        types = map((a, b) -> _writepromote(a, b, name), fieldtypes(A), fieldtypes(B))
        return NamedTuple{fieldnames(A),Tuple{types...}}
    end
    if A <: AbstractVector && B <: AbstractVector
        return Vector{_writepromote(eltype(A), eltype(B), name)}
    end
    if A <: Tuple && B <: Tuple && fieldcount(A) == fieldcount(B)
        return Tuple{map((a, b) -> _writepromote(a, b, name), fieldtypes(A),
            fieldtypes(B))...}
    end
    if A <: Pair && B <: Pair
        return Pair{_writepromote(fieldtype(A, 1), fieldtype(B, 1), name),
            _writepromote(fieldtype(A, 2), fieldtype(B, 2), name)}
    end
    if A <: Dict && B <: Dict
        return Dict{_writepromote(keytype(A), keytype(B), name),
            _writepromote(valtype(A), valtype(B), name)}
    end
    promoted = promote_type(A, B)
    numeric = A <: Real && B <: Real && !(A <: Bool) && !(B <: Bool)
    (numeric || promoted <: Durations.Timestamp) && isconcretetype(promoted) &&
        return promoted
    throw(ArgumentError("column $(repr(name)) mixes values of types $A and $B; " *
        "convert it to one element type"))
end

function _writenarrowed(::Type{T}, value, name::String) where {T}
    converted = try
        convert(T, value)
    catch err
        err isa InexactError || rethrow()
        _notexact(T, value, name)
    end
    T <: Union{Real,Durations.Timestamp} && !isequal(converted, value) &&
        _notexact(T, value, name)
    return converted
end

@noinline _notexact(T, value, name) = throw(ArgumentError("column $(repr(name)) value " *
    "$value does not convert to $T exactly, the one type its values promote to, so its " *
    "values have no common type; convert the column to one element type first"))

"""
    _bloomleaves(leaves, bloomfilter) -> Vector{Bool}

Which leaves get bloom filters: all but BOOLEAN leaves for `true`, none for `false`, or
the leaves a selector or collection of selectors names (column names, leaf path tuples,
or leaf ordinals, as in encoding overrides).
"""
function _bloomleaves(leaves::Vector{SchemaNode}, bloomfilter::Bool)
    return Bool[bloomfilter && leaf.element.type_ != Metadata.Type.BOOLEAN
        for leaf in leaves]
end

_bloomleaves(leaves::Vector{SchemaNode}, selector::Union{Symbol,AbstractString,Integer}) =
    _bloomleaves(leaves, (selector,))

function _bloomleaves(leaves::Vector{SchemaNode},
        selectors::Union{AbstractVector,AbstractSet,Tuple})
    flags = fill(false, length(leaves))
    for selector in selectors
        index = _writeencodingleafindex(leaves, _writeencodingselector(selector);
            policy="bloomfilter")
        leaves[index].element.type_ == Metadata.Type.BOOLEAN && throw(ArgumentError(
            "bloom filters are not written for BOOLEAN columns; $(repr(selector)) is one"))
        flags[index] = true
    end
    return flags
end

_bloomleaves(::Vector{SchemaNode}, bloomfilter) = throw(ArgumentError(
    "bloomfilter must be a Bool, a column selector, or a collection of selectors, got " *
    "$(typeof(bloomfilter))"))

# Scalar element types the flat column writer maps exactly as the nested writer does.
function _flatwritetype(::Type{T}) where {T}
    T === Missing && return true
    S = Base.nonmissingtype(T)
    (S === Union{} || !isconcretetype(S)) && return false
    _writemap(S) === nothing || return true
    return _writevaluescanned(S)
end

function _flatwritable(values::AbstractVector)
    values isa Union{LogicalColumn,FixedByteArrayVector} && return true
    return _flatwritetype(eltype(values))
end

function _writeencoding(::Nothing)
    return nothing
end

function _writeencoding(encoding::Metadata.Encoding.T)
    encoding == Metadata.Encoding.PLAIN && return encoding
    encoding == Metadata.Encoding.DELTA_BINARY_PACKED && return encoding
    encoding == Metadata.Encoding.DELTA_LENGTH_BYTE_ARRAY && return encoding
    encoding == Metadata.Encoding.DELTA_BYTE_ARRAY && return encoding
    encoding == Metadata.Encoding.BYTE_STREAM_SPLIT && return encoding
    encoding == Metadata.Encoding.RLE && return encoding
    encoding in (Metadata.Encoding.PLAIN_DICTIONARY, Metadata.Encoding.RLE_DICTIONARY) &&
        throw(ArgumentError(
            "dictionary encodings cannot be selected directly; use :dictionary"))
    encoding == Metadata.Encoding.BIT_PACKED &&
        throw(ArgumentError("deprecated BIT_PACKED encoding is read-only"))
    throw(ArgumentError("unsupported explicit Parquet value encoding $encoding"))
end

function _writeencoding(encoding::Symbol)
    name = Symbol(lowercase(String(encoding)))
    name === :plain && return Metadata.Encoding.PLAIN
    name === :delta_binary_packed && return Metadata.Encoding.DELTA_BINARY_PACKED
    name === :delta_length_byte_array && return Metadata.Encoding.DELTA_LENGTH_BYTE_ARRAY
    name === :delta_byte_array && return Metadata.Encoding.DELTA_BYTE_ARRAY
    name === :byte_stream_split && return Metadata.Encoding.BYTE_STREAM_SPLIT
    name === :rle && return Metadata.Encoding.RLE
    name in (:plain_dictionary, :rle_dictionary) &&
        throw(ArgumentError(
            "dictionary encodings cannot be selected directly; use :dictionary"))
    name === :bit_packed &&
        throw(ArgumentError("deprecated BIT_PACKED encoding is read-only"))
    throw(ArgumentError("unknown explicit Parquet value encoding $(repr(encoding))"))
end

function _writeencoding(encoding::AbstractString)
    return _writeencoding(Symbol(encoding))
end

function _writeencoding(encoding)
    throw(ArgumentError(
        "explicit Parquet value encoding must be a Symbol, string, Encoding value, or nothing"))
end

function _writeencodingchoice(encoding::Metadata.Encoding.T)
    return WriteEncodingChoice(_writeencoding(encoding), false)
end

function _writeencodingchoice(encoding::Symbol)
    Symbol(lowercase(String(encoding))) === :dictionary &&
        return WriteEncodingChoice(nothing, true)
    return WriteEncodingChoice(_writeencoding(encoding), false)
end

function _writeencodingchoice(encoding::AbstractString)
    return _writeencodingchoice(Symbol(encoding))
end

function _writeencodingchoice(::Nothing)
    throw(ArgumentError(
        "a per-column Parquet encoding cannot be nothing; omit the column override"))
end

function _writeencodingchoice(encoding)
    throw(ArgumentError(
        "a Parquet encoding must be a Symbol, string, or Encoding value, got $(typeof(encoding))"))
end

function _validatewriteencodingchoice(column::Metadata.SchemaElement, choice::WriteEncodingChoice)
    encoding = choice.encoding
    encoding === nothing || _validatewriteencoding(column, encoding)
    return
end

function _writeencodingselector(selector::Integer)
    selector isa Bool && throw(ArgumentError(
        "a Parquet physical leaf ordinal must be a positive integer"))
    selector > 0 || throw(ArgumentError(
        "a Parquet physical leaf ordinal must be positive, got $selector"))
    return (:ordinal, selector)
end

function _writeencodingselector(selector::Tuple)
    isempty(selector) && throw(ArgumentError(
        "a Parquet physical leaf path selector cannot be empty"))
    path = String[]
    sizehint!(path, length(selector))
    for segment in selector
        segment isa Union{Symbol,AbstractString} || throw(ArgumentError(
            "Parquet physical leaf path segments must be Symbols or strings, got $(typeof(segment))"))
        push!(path, String(segment))
    end
    return (:path, Tuple(path))
end

function _writeencodingselector(selector::Symbol)
    return (:name, String(selector))
end

function _writeencodingselector(selector::AbstractString)
    return (:name, String(selector))
end

function _writeencodingselector(selector)
    throw(ArgumentError(
        "a Parquet leaf encoding selector must be a positive integer, exact path tuple, Symbol, or string, got $(typeof(selector))"))
end

function _writepathmatches(path::Vector{String}, selector::Tuple)
    length(path) == length(selector) || return false
    for (pathsegment, selectorsegment) in zip(path, selector)
        pathsegment == selectorsegment || return false
    end
    return true
end

function _singlewriteencodingmatch(matches::Vector{Int}, selector; kind::String,
        policy::String)
    isempty(matches) && throw(ArgumentError(
        "unknown Parquet writer $kind $(repr(selector)) in $policy"))
    length(matches) == 1 || throw(ArgumentError(
        "ambiguous Parquet writer $kind $(repr(selector)) selects multiple physical leaves; use a positive leaf ordinal"))
    return only(matches)
end

function _writeencodingleafindex(leaves::Vector{SchemaNode},
    selector::Tuple{Symbol,<:Integer}; policy::String="encoding policy")
    ordinal = selector[2]
    matches = Int[]
    for (index, leaf) in enumerate(leaves)
        leaf.column_index == ordinal && push!(matches, index)
    end
    return _singlewriteencodingmatch(matches, ordinal;
        kind="physical leaf ordinal", policy)
end

function _writeencodingleafindex(leaves::Vector{SchemaNode},
    selector::Tuple{Symbol,<:Tuple}; policy::String="encoding policy")
    path = selector[2]
    matches = Int[]
    for (index, leaf) in enumerate(leaves)
        _writepathmatches(leaf.path, path) && push!(matches, index)
    end
    return _singlewriteencodingmatch(matches, path;
        kind="physical leaf path", policy)
end

function _writeencodingleafindex(leaves::Vector{SchemaNode},
    selector::Tuple{Symbol,String}; policy::String="encoding policy")
    name = selector[2]
    flat = Int[]
    for (index, leaf) in enumerate(leaves)
        length(leaf.path) == 1 && only(leaf.path) == name && push!(flat, index)
    end
    isempty(flat) || return _singlewriteencodingmatch(flat, name;
        kind="column", policy)
    group = Int[]
    for (index, leaf) in enumerate(leaves)
        !isempty(leaf.path) && first(leaf.path) == name && push!(group, index)
    end
    return _singlewriteencodingmatch(group, name;
        kind="top-level field", policy)
end

function _mappedwriteencodingchoices(leaves::Vector{SchemaNode}, entries,
    dictionary::Bool)
    selectors = Set{Any}()
    assigned = Set{Int}()
    overrides = Dict{Int,WriteEncodingChoice}()
    for (rawselector, encoding) in entries
        selector = _writeencodingselector(rawselector)
        selector in selectors && throw(ArgumentError(
            "duplicate Parquet encoding policy selector $(repr(rawselector))"))
        push!(selectors, selector)
        index = _writeencodingleafindex(leaves, selector)
        index in assigned && throw(ArgumentError(
            "multiple Parquet encoding policy selectors assign physical leaf ordinal $(leaves[index].column_index)"))
        choice = _writeencodingchoice(encoding)
        _validatewriteencodingchoice(leaves[index].element, choice)
        push!(assigned, index)
        overrides[index] = choice
    end
    default = WriteEncodingChoice(nothing, dictionary)
    choices = WriteEncodingChoice[]
    sizehint!(choices, length(leaves))
    for index in eachindex(leaves)
        push!(choices, get(overrides, index, default))
    end
    return choices
end

function _writeencodingchoices(leaves::Vector{SchemaNode}, ::Nothing,
    dictionary::Bool)
    return fill(WriteEncodingChoice(nothing, dictionary), length(leaves))
end

function _writeencodingchoices(leaves::Vector{SchemaNode}, encoding::Union{
        Symbol,AbstractString,Metadata.Encoding.T}, dictionary::Bool)
    # An explicit encoding takes precedence over the dictionary default.
    choice = _writeencodingchoice(encoding)
    foreach(leaf -> _validatewriteencodingchoice(leaf.element, choice), leaves)
    return fill(choice, length(leaves))
end

function _writeencodingchoices(leaves::Vector{SchemaNode}, encoding::Pair,
    dictionary::Bool)
    return _mappedwriteencodingchoices(leaves, (encoding,), dictionary)
end

function _writeencodingchoices(leaves::Vector{SchemaNode}, encoding::NamedTuple,
    dictionary::Bool)
    return _mappedwriteencodingchoices(leaves, pairs(encoding), dictionary)
end

function _writeencodingchoices(leaves::Vector{SchemaNode}, encoding::AbstractDict,
    dictionary::Bool)
    return _mappedwriteencodingchoices(leaves, pairs(encoding), dictionary)
end

function _writeencodingchoices(::Vector{SchemaNode}, encoding, ::Bool)
    throw(ArgumentError(
        "Parquet encoding must be nothing, a Symbol, string, Pair, NamedTuple, or AbstractDict, got $(typeof(encoding))"))
end

function _validatewriteencoding(column::Metadata.SchemaElement, encoding::Metadata.Encoding.T)
    physical = column.type_
    encoding == Metadata.Encoding.PLAIN && return
    encoding == Metadata.Encoding.DELTA_BINARY_PACKED &&
        physical in (Metadata.Type.INT32, Metadata.Type.INT64) && return
    encoding in (Metadata.Encoding.DELTA_LENGTH_BYTE_ARRAY, Metadata.Encoding.DELTA_BYTE_ARRAY) &&
        physical == Metadata.Type.BYTE_ARRAY && return
    encoding == Metadata.Encoding.DELTA_BYTE_ARRAY &&
        physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY && return
    encoding == Metadata.Encoding.BYTE_STREAM_SPLIT &&
        physical in (Metadata.Type.INT32, Metadata.Type.INT64, Metadata.Type.FLOAT,
            Metadata.Type.DOUBLE, Metadata.Type.FIXED_LEN_BYTE_ARRAY) && return
    encoding == Metadata.Encoding.RLE && physical == Metadata.Type.BOOLEAN && return
    throw(ArgumentError(
        "Parquet value encoding $encoding is not valid for column $(repr(column.name)) " *
        "with physical type $physical"))
end

function _writecodec(codec::Metadata.CompressionCodec.T)
    codecwritable(codec) || _unwritablecodec(codec)
    return codec
end

function _writecodec(codec::Symbol)
    name = Symbol(lowercase(String(codec)))
    name in (:uncompressed, :none) && return Metadata.CompressionCodec.UNCOMPRESSED
    name === :snappy && return Metadata.CompressionCodec.SNAPPY
    name === :gzip && return Metadata.CompressionCodec.GZIP
    name === :brotli && return Metadata.CompressionCodec.BROTLI
    name === :zstd && return Metadata.CompressionCodec.ZSTD
    name === :lz4_raw && return Metadata.CompressionCodec.LZ4_RAW
    name === :lz4 && return Metadata.CompressionCodec.LZ4_RAW
    name === :lzo && return _writecodec(Metadata.CompressionCodec.LZO)
    throw(ArgumentError("unknown Parquet compression codec :$codec; use :snappy, " *
        ":zstd, :gzip, :lz4, :brotli, or nothing"))
end

function _writecodec(codec::AbstractString)
    return _writecodec(Symbol(codec))
end

function _writecodec(codec)
    throw(ArgumentError("Parquet compression codec must be a Symbol, string, or CompressionCodec value"))
end

function _writepageversion(pageversion::Symbol)
    version = Symbol(lowercase(String(pageversion)))
    version === :v1 && return version
    version === :v2 && return version
    throw(ArgumentError("Parquet page version must be :v1 or :v2"))
end

function _writepageversion(pageversion::AbstractString)
    return _writepageversion(Symbol(pageversion))
end

function _writepageversion(pageversion)
    throw(ArgumentError("Parquet page version must be :v1 or :v2"))
end

function _writeschemaleafcount(node::SchemaNode)
    node.element.type_ !== nothing && return 1
    total = 0
    for child in node.children
        total = Base.checked_add(total, _writeschemaleafcount(child))
    end
    return total
end

# Counts the encoded footer first so max_footer_bytes is checked before it is allocated.
function _writeencodefooter(value, limits::Limits)
    try
        exact = Thrift._encodedsize(value)
        exact <= typemax(UInt32) || throw(ArgumentError(
            "Parquet footer exceeds UInt32 bytes"))
        _checklimit(:footer_bytes, exact, limits.max_footer_bytes)
        bytes = Vector{UInt8}(undef, Int(exact))
        Thrift._encodefixed!(bytes, value)
        return bytes
    catch err
        err isa Thrift._WriteCountOverflow && throw(ArgumentError(
            "Parquet footer exceeds UInt32 bytes"))
        rethrow()
    end
end

function _writekeyvaluemetadata(table, ::Limits)
    Base.@nospecialize table
    return nothing
end

# Key-value entries that describe the source file's schema. A rewrite of a selected or
# renamed table would carry them stale (Polars then reads the wrong columns), so they
# are never copied from the input.
const _SOURCE_SCHEMA_KEYS = ("ARROW:schema", "pandas",
    "org.apache.spark.sql.parquet.row.metadata")

function _rewritekeyvaluemetadata(entries::Vector{Metadata.KeyValue}, limits::Limits)
    kept = Metadata.KeyValue[item for item in entries
        if !(item.key in _SOURCE_SCHEMA_KEYS)]
    _checklimit(:container_elements, length(kept), limits.max_container_elements)
    for item in kept
        _checklimit(:string_bytes, ncodeunits(item.key), limits.max_string_bytes)
        item.value === nothing || _checklimit(:string_bytes,
            ncodeunits(item.value), limits.max_string_bytes)
    end
    return isempty(kept) ? nothing : kept
end

function _createdby()
    return "Parquet.jl version $(pkgversion(@__MODULE__))"
end

# The flattened schema elements of each top-level field.
function _writetopfields(elements::Vector{Metadata.SchemaElement})
    fields = Vector{Metadata.SchemaElement}[]
    index = 2
    while index <= length(elements)
        start = index
        pending = 1
        while pending > 0
            pending += something(elements[index].num_children, Int32(0)) - 1
            index += 1
        end
        push!(fields, elements[start:(index - 1)])
    end
    return fields
end
