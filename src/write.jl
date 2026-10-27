import Tables

struct WriteColumn{V}
    name::String
    values::V
    physical::Metadata.Type.T
    type_length::Union{Nothing,Int32}
    optional::Bool
    logical::Union{Nothing, Metadata.LogicalType}
    converted::Union{Nothing, Metadata.ConvertedType.T}
    path::Vector{String}
    repetitions::Union{Nothing,AbstractVector{<:Unsigned}}
    definitions::Union{Nothing,AbstractVector{<:Unsigned}}
    max_repetition_level::Int16
    max_definition_level::Int16
    rows::Int
    schema::Vector{Metadata.SchemaElement}
end

struct WriteFieldPlan
    schema::Vector{Metadata.SchemaElement}
    leaves::Vector{WriteColumn}
end

struct WriteLeafPlan
    ordinal::Int32
    path::Vector{String}
    column::WriteColumn
    entry_offsets::Vector{Int64}
    dense_offsets::Vector{Int64}
    payload_offsets::Vector{Int64}
end

function WriteLeafPlan(ordinal::Int32, path::Vector{String}, column::WriteColumn)
    return WriteLeafPlan(ordinal, path, column, Int64[], Int64[], Int64[])
end

struct WriteEncodingChoice
    encoding::Union{Nothing,Metadata.Encoding.T}
    dictionary::Bool
end

function _writeleafschema(name::String, physical::Metadata.Type.T,
    type_length::Union{Nothing,Int32}, optional::Bool,
    logical::Union{Nothing,Metadata.LogicalType},
    converted::Union{Nothing,Metadata.ConvertedType.T})
    repetition = optional ? Metadata.FieldRepetitionType.OPTIONAL :
        Metadata.FieldRepetitionType.REQUIRED
    return Metadata.SchemaElement(
        type_=physical,
        type_length=type_length,
        repetition_type=repetition,
        name=name,
        converted_type=converted,
        logicalType=logical,
    )
end

function WriteColumn(name::String, values, physical::Metadata.Type.T,
    type_length::Union{Nothing,Int32}, optional::Bool,
    logical::Union{Nothing,Metadata.LogicalType},
    converted::Union{Nothing,Metadata.ConvertedType.T})
    definition = Int16(optional ? 1 : 0)
    schema = Metadata.SchemaElement[
        _writeleafschema(name, physical, type_length, optional, logical, converted),
    ]
    return WriteColumn(name, values, physical, type_length, optional, logical, converted,
        String[name], nothing, nothing, Int16(0), definition, length(values), schema)
end

function _writecolumn(name, values::AbstractVector, limits::Limits)
    type = eltype(values)
    type == Missing && return _unknowncolumn(name, values, limits)
    value_type = Base.nonmissingtype(type)
    map = _writemap(value_type)
    map === nothing || return _mappedcolumn(String(name), values, map)
    logical = _logicalwritecolumn(name, values, value_type, limits)
    logical === nothing || return logical
    throw(_writeunsupported(value_type, String(name)))
end

function _writecolumn(name, values::FixedByteArrayVector, ::Limits)
    optional = Missing <: eltype(values)
    return WriteColumn(String(name), values, Metadata.Type.FIXED_LEN_BYTE_ARRAY,
        values.width, optional, nothing, nothing)
end

function _writecolumn(name, values::AbstractVector)
    return _writecolumn(name, values, Limits())
end

function _writecolumn(name, values::FixedByteArrayVector)
    return _writecolumn(name, values, Limits())
end

function _mappedcolumn(name::String, values::AbstractVector, map::_WriteMap)
    optional = Missing <: eltype(values)
    element = _writeelement(map, name, optional)
    physical = _mappedvalues(name, values, map)
    return WriteColumn(name, physical, element.type_, element.type_length, optional,
        element.logicalType, element.converted_type, String[name], nothing, nothing,
        Int16(0), Int16(optional), length(values), Metadata.SchemaElement[element])
end

function _mappedvalues(name::String, values::AbstractVector,
        map::_WriteMap{typeof(identity)})
    return values
end

function _mappedvalues(name::String, values::AbstractVector, map::_WriteMap{F,P}) where {F,P}
    output = Vector{Missing <: eltype(values) ? Union{Missing,P} : P}(undef, length(values))
    try
        for (index, value) in enumerate(values)
            output[index] = ismissing(value) ? missing : map.convert(value)
        end
    catch err
        err isa Union{ArgumentError,InexactError,OverflowError} || rethrow()
        throw(_writevalueerror(name, err))
    end
    return output
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
    sizehint!(normalized, length(names))
    seen = Set{String}()
    for raw in names
        name = _writecolumnname(raw)
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
        declared === nothing && throw(ArgumentError(
            "row-oriented Tables sources must declare a schema or a concrete NamedTuple element type"))
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
    isempty(names) && throw(ArgumentError(
        "a Parquet table must have at least one column"))
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

function _writeinputrowcolumns(table, limits::Limits)
    Base.@nospecialize table
    rowsource = Tables.rows(table)
    names, types = _writerowschema(rowsource, limits)
    normalized = _validatewritecolumnnames(names)
    _preflightwriterowtypes(normalized, types, limits)
    count = _writerowcount(rowsource, limits)
    columns, rowcount = count === nothing ?
        _unknownwriterowcolumns(rowsource, types, limits) :
        _knownwriterowcolumns(rowsource, types, count, limits)
    output = Pair{String,AbstractVector}[]
    sizehint!(output, length(columns))
    for index in eachindex(columns, normalized)
        push!(output, Pair{String,AbstractVector}(normalized[index],
            _writeinputcolumn(normalized[index], columns[index])))
    end
    return output, rowcount
end

function _writeinputcolumns(table, limits::Limits)
    Base.@nospecialize table
    Tables.columnaccess(table) || return _writeinputrowcolumns(table, limits)
    columns = Tables.columns(table)
    names = collect(Tables.columnnames(columns))
    isempty(names) && throw(ArgumentError(
        "a Parquet table must have at least one column"))
    normalized = _validatewritecolumnnames(names)
    output = Pair{String,AbstractVector}[]
    sizehint!(output, length(names))
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
        push!(output, Pair{String,AbstractVector}(name, _writeinputcolumn(name, values)))
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
    _writedeclared(eltype(values)) && return values
    return _narrowwritecolumn(name, values)
end

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
    S !== Union{} && isconcretetype(S) || return false
    _writemap(S) === nothing || return true
    _writevaluescanned(S) && return true
    # Containers are checked field by field when their schema is inferred.
    return S <: Union{AbstractVector,NamedTuple,AbstractDict,Tuple,ListValue,
        StructValue,MapValue}
end

function _narrowwritecolumn(name::String, values::AbstractVector)
    T = Union{}
    nulls = false
    for value in values
        if ismissing(value)
            nulls = true
        else
            T = _writepromote(T, typeof(DataAPI.unwrap(value)), name)
        end
    end
    T === Union{} && return Vector{Missing}(missing, length(values))
    output = Vector{nulls ? Union{Missing,T} : T}(undef, length(values))
    for (index, value) in enumerate(values)
        output[index] = ismissing(value) ? missing :
            _writenarrowed(T, DataAPI.unwrap(value), name)
    end
    return output
end

# The one element type that holds values of types A and B exactly.
function _writepromote(A::Type, B::Type, name::String)
    A === Union{} && return B
    A === B && return A
    Nothing in (A, B) && throw(ArgumentError("column $(repr(name)) contains " *
        "nothing; use missing for null values"))
    A === Missing && return Union{Missing,B}
    B === Missing && return Union{Missing,A}
    if Missing <: A || Missing <: B
        return Union{Missing,_writepromote(Base.nonmissingtype(A),
            Base.nonmissingtype(B), name)}
    end
    A <: AbstractString && B <: AbstractString && return String
    if A <: NamedTuple && B <: NamedTuple && fieldnames(A) == fieldnames(B)
        types = map((a, b) -> _writepromote(a, b, name), fieldtypes(A), fieldtypes(B))
        return NamedTuple{fieldnames(A),Tuple{types...}}
    end
    if A <: Vector && B <: Vector
        return Vector{_writepromote(eltype(A), eltype(B), name)}
    end
    promoted = promote_type(A, B)
    numeric = A <: Real && B <: Real && !(A <: Bool) && !(B <: Bool)
    (numeric || promoted <: Durations.Timestamp) && isconcretetype(promoted) &&
        return promoted
    throw(ArgumentError("column $(repr(name)) mixes values of types $A and $B; " *
        "convert it to one element type"))
end

function _writenarrowed(::Type{T}, value, name::String) where {T}
    converted = convert(T, value)
    T <: Union{Real,Durations.Timestamp} && !isequal(converted, value) &&
        throw(ArgumentError("column $(repr(name)) value $value does not convert to " *
            "$T exactly"))
    return converted
end

# Resolves the encoding policy against the schema leaves before any value is read.
function _preflightwriteencoding(semantic::_NestedSchemaPlan, encoding,
        dictionary::Bool)
    return _writeencodingchoices(_writeleafplans(semantic), encoding, dictionary)
end

function _writeleafplans(semantic::_NestedSchemaPlan)
    emptyschema = Metadata.SchemaElement[]
    leaves = WriteLeafPlan[]
    sizehint!(leaves, length(semantic.leaves))
    for leaf in semantic.leaves
        node = leaf.source
        element = node.element
        optional = element.repetition_type ==
            Metadata.FieldRepetitionType.OPTIONAL
        column = WriteColumn(element.name, nothing, element.type_,
            element.type_length, optional, element.logicalType,
            element.converted_type, node.path, nothing, nothing,
            node.max_repetition_level, node.max_definition_level, 0,
            emptyschema)
        push!(leaves, WriteLeafPlan(node.column_index, node.path, column))
    end
    return leaves
end

"""
    _bloomleaves(leaves, bloomfilter) -> Vector{Bool}

Which leaves get bloom filters: all but BOOLEAN leaves for `true`, none for `false`, or
the leaves a selector or collection of selectors names (column names, leaf path tuples,
or leaf ordinals, as in encoding overrides).
"""
function _bloomleaves(leaves::Vector{WriteLeafPlan}, bloomfilter::Bool)
    return Bool[bloomfilter && leaf.column.physical != Metadata.Type.BOOLEAN
        for leaf in leaves]
end

_bloomleaves(leaves::Vector{WriteLeafPlan}, selector::Union{Symbol,AbstractString,Integer}) =
    _bloomleaves(leaves, (selector,))

function _bloomleaves(leaves::Vector{WriteLeafPlan},
        selectors::Union{AbstractVector,AbstractSet,Tuple})
    flags = fill(false, length(leaves))
    for selector in selectors
        index = _writeencodingleafindex(leaves, _writeencodingselector(selector);
            policy="bloomfilter")
        leaves[index].column.physical == Metadata.Type.BOOLEAN && throw(ArgumentError(
            "bloom filters are not written for BOOLEAN columns; $(repr(selector)) is one"))
        flags[index] = true
    end
    return flags
end

_bloomleaves(::Vector{WriteLeafPlan}, bloomfilter) = throw(ArgumentError(
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

# The flat writer reads a column several times (limits, statistics, pages), so a
# source that is not a plain Vector is read once into one; every pass then sees the
# same values.
function _flatsnapshot(values::Vector)
    return values
end

function _flatsnapshot(values::FixedByteArrayVector)
    return values
end

function _flatsnapshot(values::AbstractVector)
    return collect(values)
end

# Columns of scalars skip the nested shredder: every value is one PLAIN leaf entry.
function _flatwritefields(columns::Vector{Pair{String,AbstractVector}},
        limits::Limits)
    fields = WriteFieldPlan[]
    sizehint!(fields, length(columns))
    for (name, source) in columns
        occursin('\0', name) && throw(ArgumentError(
            "Parquet top-level field names cannot contain NUL"))
        values = _flatsnapshot(source)
        push!(fields, _writefieldplan(_writecolumn(name, values, limits)))
    end
    return fields
end

function _writefieldsencoded(table, limits::Limits, encoding, dictionary::Bool)
    Base.@nospecialize table
    columns, rows = _writeinputcolumns(table, limits)
    fields = if all(column -> _flatwritable(last(column)), columns)
        _flatwritefields(columns, limits)
    else
        preflight = semantic -> _preflightwriteencoding(semantic, encoding,
            dictionary)
        _nestedwritefields(columns, rows, limits; preflight=preflight)
    end
    return fields, rows
end

function _writefields(table, limits::Limits)
    Base.@nospecialize table
    return _writefieldsencoded(table, limits, nothing, false)
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

function _writeencodingcolumn(name::Symbol)
    return String(name)
end

function _writeencodingcolumn(name::AbstractString)
    return String(name)
end

function _writeencodingcolumn(name)
    throw(ArgumentError(
        "Parquet encoding policy keys must be Symbols or strings, got $(typeof(name))"))
end

function _writeencodingoverrides(entries)
    overrides = Dict{String,WriteEncodingChoice}()
    for (rawname, encoding) in entries
        name = _writeencodingcolumn(rawname)
        haskey(overrides, name) &&
            throw(ArgumentError("duplicate Parquet encoding policy for column $(repr(name))"))
        overrides[name] = _writeencodingchoice(encoding)
    end
    return overrides
end

function _validatewriteencodingchoice(column::WriteColumn, choice::WriteEncodingChoice)
    encoding = choice.encoding
    encoding === nothing || _validatewriteencoding(column, encoding)
    return
end

function _mappedwriteencodingchoices(columns::Vector{WriteColumn}, entries, dictionary::Bool)
    overrides = _writeencodingoverrides(entries)
    names = Set(column.name for column in columns)
    unknown = sort!(String[name for name in keys(overrides) if !(name in names)])
    isempty(unknown) || throw(ArgumentError(
        "unknown Parquet writer column" * (length(unknown) == 1 ? " " : "s ") *
        join(repr.(unknown), ", ") * " in encoding policy"))
    default = WriteEncodingChoice(nothing, dictionary)
    choices = WriteEncodingChoice[]
    sizehint!(choices, length(columns))
    for column in columns
        choice = get(overrides, column.name, default)
        _validatewriteencodingchoice(column, choice)
        push!(choices, choice)
    end
    return choices
end

function _writeencodingchoices(columns::Vector{WriteColumn}, ::Nothing, dictionary::Bool)
    return fill(WriteEncodingChoice(nothing, dictionary), length(columns))
end

function _writeencodingchoices(columns::Vector{WriteColumn}, encoding::Union{
        Symbol,AbstractString,Metadata.Encoding.T}, dictionary::Bool)
    # An explicit encoding takes precedence over the dictionary default.
    choice = _writeencodingchoice(encoding)
    foreach(column -> _validatewriteencodingchoice(column, choice), columns)
    return fill(choice, length(columns))
end

function _writeencodingchoices(columns::Vector{WriteColumn}, encoding::Pair, dictionary::Bool)
    return _mappedwriteencodingchoices(columns, (encoding,), dictionary)
end

function _writeencodingchoices(columns::Vector{WriteColumn}, encoding::NamedTuple,
    dictionary::Bool)
    return _mappedwriteencodingchoices(columns, pairs(encoding), dictionary)
end

function _writeencodingchoices(columns::Vector{WriteColumn}, encoding::AbstractDict,
    dictionary::Bool)
    return _mappedwriteencodingchoices(columns, pairs(encoding), dictionary)
end

function _writeencodingchoices(::Vector{WriteColumn}, encoding, ::Bool)
    throw(ArgumentError(
        "Parquet encoding must be nothing, a Symbol, string, Pair, NamedTuple, or AbstractDict, got $(typeof(encoding))"))
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

function _writeencodingleafindex(leaves::Vector{WriteLeafPlan},
    selector::Tuple{Symbol,<:Integer}; policy::String="encoding policy")
    ordinal = selector[2]
    matches = Int[]
    for (index, leaf) in enumerate(leaves)
        leaf.ordinal == ordinal && push!(matches, index)
    end
    return _singlewriteencodingmatch(matches, ordinal;
        kind="physical leaf ordinal", policy)
end

function _writeencodingleafindex(leaves::Vector{WriteLeafPlan},
    selector::Tuple{Symbol,<:Tuple}; policy::String="encoding policy")
    path = selector[2]
    matches = Int[]
    for (index, leaf) in enumerate(leaves)
        _writepathmatches(leaf.path, path) && push!(matches, index)
    end
    return _singlewriteencodingmatch(matches, path;
        kind="physical leaf path", policy)
end

function _writeencodingleafindex(leaves::Vector{WriteLeafPlan},
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

function _mappedwriteencodingchoices(leaves::Vector{WriteLeafPlan}, entries,
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
            "multiple Parquet encoding policy selectors assign physical leaf ordinal $(leaves[index].ordinal)"))
        choice = _writeencodingchoice(encoding)
        _validatewriteencodingchoice(leaves[index].column, choice)
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

function _writeencodingchoices(leaves::Vector{WriteLeafPlan}, ::Nothing,
    dictionary::Bool)
    return fill(WriteEncodingChoice(nothing, dictionary), length(leaves))
end

function _writeencodingchoices(leaves::Vector{WriteLeafPlan}, encoding::Union{
        Symbol,AbstractString,Metadata.Encoding.T}, dictionary::Bool)
    # An explicit encoding takes precedence over the dictionary default.
    choice = _writeencodingchoice(encoding)
    foreach(leaf -> _validatewriteencodingchoice(leaf.column, choice), leaves)
    return fill(choice, length(leaves))
end

function _writeencodingchoices(leaves::Vector{WriteLeafPlan}, encoding::Pair,
    dictionary::Bool)
    return _mappedwriteencodingchoices(leaves, (encoding,), dictionary)
end

function _writeencodingchoices(leaves::Vector{WriteLeafPlan}, encoding::NamedTuple,
    dictionary::Bool)
    return _mappedwriteencodingchoices(leaves, pairs(encoding), dictionary)
end

function _writeencodingchoices(leaves::Vector{WriteLeafPlan}, encoding::AbstractDict,
    dictionary::Bool)
    return _mappedwriteencodingchoices(leaves, pairs(encoding), dictionary)
end

function _writeencodingchoices(::Vector{WriteLeafPlan}, encoding, ::Bool)
    throw(ArgumentError(
        "Parquet encoding must be nothing, a Symbol, string, Pair, NamedTuple, or AbstractDict, got $(typeof(encoding))"))
end

function _validatewritechoicecount(leaves::Vector{WriteLeafPlan},
        choices::Vector{WriteEncodingChoice})
    length(choices) == length(leaves) || throw(UnsupportedFeatureError(
        "nested fields with multiple physical leaves need per-leaf encoding selection"))
    return
end

function _validatewriteencoding(column::WriteColumn, encoding::Metadata.Encoding.T)
    physical = column.physical
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
    name === :uncompressed && return Metadata.CompressionCodec.UNCOMPRESSED
    name === :snappy && return Metadata.CompressionCodec.SNAPPY
    name === :gzip && return Metadata.CompressionCodec.GZIP
    name === :brotli && return Metadata.CompressionCodec.BROTLI
    name === :zstd && return Metadata.CompressionCodec.ZSTD
    name === :lz4_raw && return Metadata.CompressionCodec.LZ4_RAW
    name === :lz4 && return Metadata.CompressionCodec.LZ4_RAW
    name === :lzo && return _writecodec(Metadata.CompressionCodec.LZO)
    throw(ArgumentError("unknown Parquet compression codec $(repr(codec))"))
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

function _withoutcolumnschema(column::WriteColumn, path::Vector{String}=column.path)
    return WriteColumn(column.name, column.values, column.physical, column.type_length,
        column.optional, column.logical, column.converted, copy(path), column.repetitions,
        column.definitions, column.max_repetition_level, column.max_definition_level,
        column.rows, Metadata.SchemaElement[])
end

function _writefieldplan(column::WriteColumn)
    schema = column.schema
    isempty(schema) && throw(ArgumentError(
        "writer column $(repr(column.name)) has no top-level field schema"))
    return WriteFieldPlan(copy(schema), WriteColumn[_withoutcolumnschema(column)])
end

function _writeschemaelementcount(fields::Vector{WriteFieldPlan}, limits::Limits)
    total = 1
    for field in fields
        isempty(field.schema) && throw(ArgumentError("writer field has an empty schema"))
        isempty(field.leaves) && throw(ArgumentError("writer field has no physical leaves"))
        total = try
            Base.checked_add(total, length(field.schema))
        catch err
            err isa OverflowError || rethrow()
            throw(LimitError(:container_elements, typemax(Int64),
                limits.max_container_elements))
        end
        _checklimit(:container_elements, total, limits.max_container_elements)
    end
    return total
end

function _flattenwriteschema(fields::Vector{WriteFieldPlan}, limits::Limits)
    isempty(fields) && throw(ArgumentError("a Parquet table must have at least one column"))
    length(fields) <= typemax(Int32) ||
        throw(ArgumentError("Parquet schema has more than Int32 top-level fields"))
    count = _writeschemaelementcount(fields, limits)
    elements = Metadata.SchemaElement[
        Metadata.SchemaElement(name="schema", num_children=Int32(length(fields))),
    ]
    sizehint!(elements, count)
    for field in fields
        append!(elements, field.schema)
    end
    return elements
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

function _writepartitionmismatch(expected::Vector{Metadata.SchemaElement},
        actual::Vector{Metadata.SchemaElement}, index::Int)
    for (left, right) in zip(_writetopfields(expected), _writetopfields(actual))
        name = first(left).name
        first(right).name == name || throw(ArgumentError(
            "partition $index has column $(repr(first(right).name)) where the " *
            "first partition has $(repr(name))"))
        left == right || throw(ArgumentError("column $(repr(name)) of partition " *
            "$index does not have the type or nullability of the first partition"))
    end
    throw(ArgumentError("partition $index has $(length(_writetopfields(actual))) " *
        "columns; the first partition has $(length(_writetopfields(expected)))"))
end
