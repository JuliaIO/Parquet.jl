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
    repetitions::Union{Nothing,AbstractVector{UInt64}}
    definitions::Union{Nothing,AbstractVector{UInt64}}
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

struct WriteRowGroupPlan
    rows::Int
    leaves::Vector{WriteLeafPlan}
end

struct WritePlan
    elements::Vector{Metadata.SchemaElement}
    schema::Schema
    rows::Int
    rowgroups::Vector{WriteRowGroupPlan}
end

struct ColumnPages
    bytes::Vector{UInt8}
    uncompressed_size::Int64
    data_offset::Int64
    dictionary_offset::Union{Nothing,Int64}
    encodings::Vector{Metadata.Encoding.T}
    encoding_stats::Vector{Metadata.PageEncodingStats}
    page_locations::Vector{Metadata.PageLocation}
end

function ColumnPages(bytes::Vector{UInt8}, uncompressed_size::Int64,
        data_offset::Int64, dictionary_offset::Union{Nothing,Int64},
        encodings::Vector{Metadata.Encoding.T},
        encoding_stats::Vector{Metadata.PageEncodingStats})
    return ColumnPages(bytes, uncompressed_size, data_offset,
        dictionary_offset, encodings, encoding_stats, Metadata.PageLocation[])
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

function _writetype(::Type{Bool})
    return Metadata.Type.BOOLEAN
end

function _writetype(::Type{Int32})
    return Metadata.Type.INT32
end

function _writetype(::Type{Int64})
    return Metadata.Type.INT64
end

function _writetype(::Type{Float32})
    return Metadata.Type.FLOAT
end

function _writetype(::Type{Float64})
    return Metadata.Type.DOUBLE
end

function _writetype(::Type{T}) where {T<:AbstractString}
    return Metadata.Type.BYTE_ARRAY
end

function _writetype(::Type{T}) where {T<:AbstractVector{UInt8}}
    return Metadata.Type.BYTE_ARRAY
end

function _writetype(::Type{NTuple{N,UInt8}}) where {N}
    return Metadata.Type.FIXED_LEN_BYTE_ARRAY
end

function _writetype(::Type{T}) where {T}
    throw(ArgumentError("unsupported PLAIN writer element type $T"))
end

function _writetypelength(::Type{T}) where {T}
    return nothing
end

function _writetypelength(::Type{NTuple{N,UInt8}}) where {N}
    N > 0 || throw(ArgumentError("fixed byte-array width must be positive"))
    N <= typemax(Int32) || throw(ArgumentError("fixed byte-array width exceeds Int32"))
    return Int32(N)
end

function _stringlogical(::Type{T}) where {T}
    T <: AbstractString || return nothing, nothing
    logical = Metadata.LogicalType(STRING=Metadata.StringType())
    return logical, Metadata.ConvertedType.UTF8
end

function _addlistentries(total::Int, count::Int, limits::Limits)
    requested = try
        Base.checked_add(total, count)
    catch err
        err isa OverflowError || rethrow()
        throw(LimitError(:container_elements, typemax(Int64),
            limits.max_container_elements))
    end
    _checklimit(:container_elements, requested, limits.max_container_elements)
    return requested
end

function _listdateentrycount(values::AbstractVector, limits::Limits)
    entries = 0
    for row in values
        if ismissing(row)
            entries = _addlistentries(entries, 1, limits)
            continue
        end
        row isa AbstractVector || throw(ArgumentError(
            "Parquet LIST rows must be vectors or missing"))
        entries = _addlistentries(entries, max(1, length(row)), limits)
        for value in row
            ismissing(value) || value isa Dates.Date || throw(ArgumentError(
                "Parquet DATE list elements must be Date values or missing"))
        end
    end
    return entries
end

function _listdatecolumn(name, values::AbstractVector, container_type::Type,
    limits::Limits)
    element_type = Base.nonmissingtype(eltype(container_type))
    element_type == Dates.Date || throw(ArgumentError(
        "unsupported nested Parquet writer element type $element_type"))
    entries = _listdateentrycount(values, limits)
    repetition = UInt64[]
    definition = UInt64[]
    dense = Int32[]
    sizehint!(repetition, entries)
    sizehint!(definition, entries)
    for row in values
        if ismissing(row)
            push!(repetition, 0)
            push!(definition, 0)
            continue
        end
        row isa AbstractVector || throw(ArgumentError("Parquet LIST rows must be vectors or missing"))
        if isempty(row)
            push!(repetition, 0)
            push!(definition, 1)
            continue
        end
        for (index, value) in enumerate(row)
            push!(repetition, isone(index) ? 0 : 1)
            if ismissing(value)
                push!(definition, 2)
            elseif value isa Dates.Date
                push!(definition, 3)
                push!(dense, _toparquetdate(value))
            else
                throw(ArgumentError("Parquet DATE list elements must be Date values or missing"))
            end
        end
    end
    length(repetition) == entries || throw(ArgumentError(
        "Parquet LIST input changed while it was encoded"))
    logical = Metadata.LogicalType(DATE=Metadata.DateType())
    converted = Metadata.ConvertedType.DATE
    outer = Metadata.SchemaElement(
        repetition_type=Metadata.FieldRepetitionType.OPTIONAL,
        name=String(name),
        num_children=Int32(1),
        converted_type=Metadata.ConvertedType.LIST,
        logicalType=Metadata.LogicalType(LIST=Metadata.ListType()),
    )
    repeated = Metadata.SchemaElement(
        repetition_type=Metadata.FieldRepetitionType.REPEATED,
        name="list",
        num_children=Int32(1),
    )
    element = _writeleafschema("element", Metadata.Type.INT32, nothing, true,
        logical, converted)
    path = String[String(name), "list", "element"]
    schema = Metadata.SchemaElement[outer, repeated, element]
    return WriteColumn(String(name), dense, Metadata.Type.INT32, nothing, true,
        logical, converted, path, repetition, definition, Int16(1), Int16(3),
        length(values), schema)
end

function _datecolumn(name, values::AbstractVector, limits::Limits)
    logical = Metadata.LogicalType(DATE=Metadata.DateType())
    converted = Metadata.ConvertedType.DATE
    optional = Missing <: eltype(values)
    element = _writeleafschema(String(name), Metadata.Type.INT32, nothing, optional,
        logical, converted)
    physical = _physicalvalues(element, values; limits=limits)
    return WriteColumn(String(name), physical, Metadata.Type.INT32, nothing, optional,
        logical, converted)
end

function _writecolumn(name, values::AbstractVector, limits::Limits)
    type = eltype(values)
    optional = Missing <: type
    type == Missing && return _unknowncolumn(name, values, limits)
    value_type = Base.nonmissingtype(type)
    if value_type <: AbstractVector && !(value_type <: AbstractVector{UInt8})
        return _listdatecolumn(name, values, value_type, limits)
    end
    value_type == Dates.Date && return _datecolumn(name, values, limits)
    logical = _logicalwritecolumn(name, values, value_type, limits)
    logical === nothing || return logical
    physical = _writetype(value_type)
    type_length = _writetypelength(value_type)
    logical, converted = _stringlogical(value_type)
    return WriteColumn(String(name), values, physical, type_length, optional, logical, converted)
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

function _writervaluepayload(value)
    value isa AbstractString && return Int64(ncodeunits(value))
    value isa AbstractVector{UInt8} && return Int64(length(value))
    value isa JSONValue && return Int64(length(value.bytes))
    value isa BSONValue && return Int64(length(value.bytes))
    value isa UUIDs.UUID && return Int64(16)
    value isa Interval && return Int64(12)
    return Int64(0)
end

function _reservewritenormalization!(budget::_LiveByteBudget,
    values::AbstractVector, limits::Limits)
    count = length(values)
    _reserveobjects!(budget, 4)
    _reservearray!(budget, UInt64, count)
    value_type = Base.nonmissingtype(eltype(values))
    if value_type <: AbstractVector && !(value_type <: AbstractVector{UInt8})
        entries = _listdateentrycount(values, limits)
        _reservearray!(budget, UInt64, entries)
        _reservearray!(budget, UInt64, entries)
        _reservearray!(budget, Int32, entries)
        return
    end
    objects = Int64(0)
    payload = Int64(0)
    for value in values
        ismissing(value) && continue
        bytes = _writervaluepayload(value)
        iszero(bytes) && continue
        objects = _materializedsum(objects, _MATERIALIZED_OBJECT_BYTES)
        payload = _materializedsum(payload, bytes)
    end
    _reserve!(budget, _materializedsum(objects, payload))
    if values isa LogicalColumn && values.spec isa _DecimalLogicalColumnSpec &&
            values.spec.precision > 18
        width = _decimalwritewidth(values.spec.precision, limits)
        _reserve!(budget, _materializedproduct(count,
            _materializedsum(_MATERIALIZED_ARRAY_HEADER_BYTES, width)))
    end
    return
end

function _writecolumnnamebytes(name::Symbol)
    return Int64(sizeof(name))
end

function _writecolumnnamebytes(name::AbstractString)
    return Int64(ncodeunits(name))
end

function _writecolumnnamebytes(name)
    throw(ArgumentError(
        "Parquet column names must be Symbols or strings, got $(typeof(name))"))
end

function _validatewritecolumnnames(names, budget::_LiveByteBudget)
    temporary = Int64(0)
    retained = Int64(0)
    try
        arraycharge = _reservearray!(budget, String, length(names))
        temporary = _materializedsum(temporary, arraycharge)
        temporary = _materializedsum(temporary, _reserveobjects!(budget))
        normalized = String[]
        sizehint!(normalized, length(names))
        seen = Set{String}()
        for raw in names
            bytes = _writecolumnnamebytes(raw)
            payloadcharge = _materializedproduct(bytes, 2)
            objectcharge = _materializedproduct(2, _MATERIALIZED_OBJECT_BYTES)
            itemcharge = _materializedsum(objectcharge, payloadcharge)
            _reserve!(budget, itemcharge)
            temporary = _materializedsum(temporary, itemcharge)
            name = String(raw)
            name in seen && throw(ArgumentError(
                "Parquet column names must be unique"))
            push!(normalized, name)
            push!(seen, name)
            retained = _materializedsum(retained,
                _materializedsum(_MATERIALIZED_OBJECT_BYTES, bytes))
        end
        _release!(budget, temporary - arraycharge - retained)
        return normalized, arraycharge
    catch
        iszero(temporary) || _release!(budget, temporary)
        rethrow()
    end
end

function _writecolumns(table, limits::Limits, budget::_LiveByteBudget)
    columns = Tables.columns(table)
    raw_names = Tables.columnnames(columns)
    rawcharge = _reservearray!(budget, Any, length(raw_names))
    try
        names = collect(raw_names)
        isempty(names) && throw(ArgumentError(
            "a Parquet table must have at least one column"))
        normalized, namearraycharge = _validatewritecolumnnames(names, budget)
        try
            _reservearray!(budget, WriteColumn, length(names))
            output = WriteColumn[]
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
                _reservewritenormalization!(budget, values, limits)
                push!(output, _writecolumn(name, values, limits))
            end
            return output, something(rows, 0)
        finally
            _release!(budget, namearraycharge)
        end
    finally
        _release!(budget, rawcharge)
    end
end

function _writecolumns(table, limits::Limits)
    return _writecolumns(table, limits, _LiveByteBudget(limits))
end

function _preflightwriterowcolumncount(count::Int, limits::Limits,
        budget::_LiveByteBudget)
    _checklimit(:container_elements, count,
        limits.max_container_elements)
    minimum = _materializedsum(
        _materializedarraybytes(String, count),
        _materializedarraybytes(Pair{String,AbstractVector}, count))
    minimum = _materializedsum(minimum,
        _materializedarraybytes(AbstractVector, count))
    _reserve!(budget, minimum)
    _release!(budget, minimum)
    return
end

Base.@noinline function _writerownamedtupleschema(T::Type)
    Base.@nospecialize T
    return fieldnames(T), fieldtypes(T)
end

Base.@noinline function _preflightwriteroweltype(rows, limits::Limits,
        budget::_LiveByteBudget)
    Base.@nospecialize rows
    Base.IteratorEltype(typeof(rows)) isa Base.HasEltype || return nothing
    T = eltype(rows)
    isconcretetype(T) || return nothing
    if T <: NamedTuple
        _preflightwriterowcolumncount(fieldcount(T), limits, budget)
        return T
    end
    return nothing
end

Base.@noinline function _writerowschema(rows, limits::Limits,
        budget::_LiveByteBudget)
    Base.@nospecialize rows
    declared = _preflightwriteroweltype(rows, limits, budget)
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
    _preflightwriterowcolumncount(length(names), limits, budget)
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

function _writerowarraycharge(types, count::Int)
    bytes = Int64(0)
    for T in types
        T isa Type || throw(ArgumentError(
            "row-oriented Tables schemas must contain Julia types"))
        bytes = _materializedsum(bytes,
            _materializedarraybytes(T, count))
    end
    return bytes
end

function _preflightwriterowtypes(names::Vector{String}, types,
        limits::Limits, budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        for (name, T) in zip(names, types)
            T isa Type || throw(ArgumentError(
                "row-oriented Tables schemas must contain Julia types"))
            _nestedwriteshape(name, T, nothing, limits, budget)
        end
        return
    finally
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
    end
end

function _writerowarrays(types, count::Int)
    columns = AbstractVector[]
    sizehint!(columns, length(types))
    for T in types
        push!(columns, Vector{T}(undef, count))
    end
    return columns
end

function _writerowretain!(budget::_LiveByteBudget, value, limits::Limits,
        depth::Int=1)
    _checklimit(:metadata_depth, depth, limits.max_metadata_depth)
    (ismissing(value) || isbits(value)) && return Int64(0)
    if value isa Union{JSONValue,BSONValue}
        bytes = Int64(length(value.bytes))
        _checklimit(:string_bytes, bytes, limits.max_string_bytes)
        charge = _materializedsum(
            _materializedproduct(3, _MATERIALIZED_OBJECT_BYTES), bytes)
        _reserve!(budget, charge)
        return charge
    elseif value isa Decimal
        bytes = Int64(_twoscomplementwidth(value.unscaled))
        _checklimit(:decimal_bytes, bytes, limits.max_decimal_bytes)
        charge = _materializedsum(
            _materializedproduct(2, _MATERIALIZED_OBJECT_BYTES), bytes)
        _reserve!(budget, charge)
        return charge
    elseif value isa AbstractString
        bytes = Int64(ncodeunits(value))
        _checklimit(:string_bytes, bytes, limits.max_string_bytes)
        charge = _materializedsum(_MATERIALIZED_OBJECT_BYTES, bytes)
        _reserve!(budget, charge)
        return charge
    elseif value isa AbstractVector
        count = length(value)
        _checklimit(:container_elements, count,
            limits.max_container_elements)
        charge = _materializedarraybytes(eltype(value), count)
        _reserve!(budget, charge)
        for item in value
            charge = _materializedsum(charge,
                _writerowretain!(budget, item, limits, depth + 1))
        end
        return charge
    elseif value isa NamedTuple
        charge = _reserveobjects!(budget)
        for item in values(value)
            charge = _materializedsum(charge,
                _writerowretain!(budget, item, limits, depth + 1))
        end
        return charge
    elseif value isa Pair
        charge = _reserveobjects!(budget)
        charge = _materializedsum(charge,
            _writerowretain!(budget, first(value), limits, depth + 1))
        return _materializedsum(charge,
            _writerowretain!(budget, last(value), limits, depth + 1))
    elseif value isa AbstractDict
        count = length(value)
        _checklimit(:container_elements, count,
            limits.max_container_elements)
        charge = _materializedsum(_MATERIALIZED_OBJECT_BYTES,
            _materializedproduct(count, 4 * _MATERIALIZED_OBJECT_BYTES))
        _reserve!(budget, charge)
        for (key, item) in value
            charge = _materializedsum(charge,
                _writerowretain!(budget, key, limits, depth + 1))
            charge = _materializedsum(charge,
                _writerowretain!(budget, item, limits, depth + 1))
        end
        return charge
    end
    _reserveobjects!(budget)
    return _MATERIALIZED_OBJECT_BYTES
end

function _fillwriterowarrays!(columns::Vector{AbstractVector}, rows,
        count::Int, limits::Limits, budget::_LiveByteBudget)
    position = 0
    retained = Int64(0)
    for row in rows
        position += 1
        position <= count || throw(ArgumentError(
            "row-oriented Tables source yielded more rows than declared"))
        for index in eachindex(columns)
            value = Tables.getcolumn(row, index)
            retained = _materializedsum(retained,
                _writerowretain!(budget, value, limits))
            columns[index][position] = value
        end
    end
    position == count || throw(ArgumentError(
        "row-oriented Tables source yielded $position rows but declared $count"))
    return retained
end

function _knownwriterowcolumns(rows, types, count::Int, limits::Limits,
        budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        charge = _materializedsum(
            _materializedarraybytes(AbstractVector, length(types)),
            _writerowarraycharge(types, count))
        _reserve!(budget, charge)
        columns = _writerowarrays(types, count)
        retained = _fillwriterowarrays!(columns, rows, count, limits, budget)
        return columns, count, _materializedsum(charge, retained)
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writerowgrowthcharge(types)
    bytes = Int64(0)
    for T in types
        item = _materializedarraybytes(T, 1; header=false)
        bytes = _materializedsum(bytes,
            _materializedproduct(4, max(item, Int64(1))))
    end
    return bytes
end

function _pushwriterow!(columns::Vector{AbstractVector}, row,
        limits::Limits, budget::_LiveByteBudget)
    retained = Int64(0)
    for index in eachindex(columns)
        value = Tables.getcolumn(row, index)
        retained = _materializedsum(retained,
            _writerowretain!(budget, value, limits))
        push!(columns[index], value)
    end
    return retained
end

function _copywriterowarrays(columns::Vector{AbstractVector}, types,
        count::Int, budget::_LiveByteBudget)
    outputcharge = _materializedsum(
        _materializedarraybytes(AbstractVector, length(types)),
        _writerowarraycharge(types, count))
    _reserve!(budget, outputcharge)
    output = AbstractVector[]
    sizehint!(output, length(types))
    for column in columns
        push!(output, copy(column))
    end
    return output, outputcharge
end

function _unknownwriterowcolumns(rows, types, limits::Limits,
        budget::_LiveByteBudget)
    start = _budgetused(budget)
    scratchcharge = Int64(0)
    retainedcharge = Int64(0)
    try
        scratchcharge = _materializedsum(scratchcharge,
            _reservearray!(budget, AbstractVector, length(types)))
        headers = _writerowarraycharge(types, 0)
        _reserve!(budget, headers)
        scratchcharge = _materializedsum(scratchcharge, headers)
        columns = _writerowarrays(types, 0)
        growth = _writerowgrowthcharge(types)
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
            _reserve!(budget, growth)
            scratchcharge = _materializedsum(scratchcharge, growth)
            retainedcharge = _materializedsum(retainedcharge,
                _pushwriterow!(columns, row, limits, budget))
            count = requested
        end
        output, outputcharge = _copywriterowarrays(columns, types, count,
            budget)
        _release!(budget, scratchcharge)
        return output, count,
            _materializedsum(outputcharge, retainedcharge)
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writeinputrowcolumns(table, limits::Limits,
        budget::_LiveByteBudget)
    Base.@nospecialize table
    rowsource = Tables.rows(table)
    names, types = _writerowschema(rowsource, limits, budget)
    start = _budgetused(budget)
    try
        normalized, namearraycharge = _validatewritecolumnnames(names, budget)
        _preflightwriterowtypes(normalized, types, limits, budget)
        count = _writerowcount(rowsource, limits)
        columns, rowcount, rowcharge = count === nothing ?
            _unknownwriterowcolumns(rowsource, types, limits, budget) :
            _knownwriterowcolumns(rowsource, types, count, limits, budget)
        paircharge = _reservearray!(budget, Pair{String,AbstractVector},
            length(columns))
        output = Pair{String,AbstractVector}[]
        sizehint!(output, length(columns))
        for index in eachindex(columns, normalized)
            push!(output, Pair{String,AbstractVector}(
                normalized[index], columns[index]))
        end
        _release!(budget, namearraycharge)
        return output, rowcount, _materializedsum(rowcharge, paircharge), nothing
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

struct _WriteColumnAccessValidator
    table::Any
    columns::Vector{Pair{String,AbstractVector}}
end

function _writecolumnnameequal(raw::AbstractString, expected::String)
    ncodeunits(raw) == ncodeunits(expected) || return false
    rawunits = codeunits(raw)
    expectedunits = codeunits(expected)
    for index in eachindex(rawunits, expectedunits)
        rawunits[index] == expectedunits[index] || return false
    end
    return true
end

function _writecolumnnameequal(raw::Symbol, expected::String)
    return String(raw) == expected
end

function _writecolumnnameequal(raw, ::String)
    _writecolumnnamebytes(raw)
    return false
end

function _validatewriteinput(::Nothing, ::_LiveByteBudget)
    return
end

function _validatewriteinput(validator::_WriteColumnAccessValidator,
        budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        columns = Tables.columns(validator.table)
        names = Tables.columnnames(columns)
        length(names) == length(validator.columns) || throw(ArgumentError(
            "Parquet table column count changed during write"))
        index = 0
        for raw in names
            index += 1
            index <= length(validator.columns) || throw(ArgumentError(
                "Parquet table column count changed during write"))
            expected = validator.columns[index]
            namebytes = _writecolumnnamebytes(raw)
            temporary = _materializedsum(_MATERIALIZED_OBJECT_BYTES, namebytes)
            _reserve!(budget, temporary)
            matches = _writecolumnnameequal(raw, first(expected))
            _release!(budget, temporary)
            matches || throw(ArgumentError(
                "Parquet table column name or order changed during write"))
            values = Tables.getcolumn(columns, raw)
            values === last(expected) || throw(ArgumentError(
                "Parquet table column identity changed during write"))
        end
        index == length(validator.columns) || throw(ArgumentError(
            "Parquet table column count changed during write"))
        return
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writeinputcolumns(table, limits::Limits, budget::_LiveByteBudget)
    Base.@nospecialize table
    Tables.columnaccess(table) || return _writeinputrowcolumns(table,
        limits, budget)
    columns = Tables.columns(table)
    raw_names = Tables.columnnames(columns)
    rawcharge = _reservearray!(budget, Any, length(raw_names))
    try
        names = collect(raw_names)
        isempty(names) && throw(ArgumentError(
            "a Parquet table must have at least one column"))
        normalized, namearraycharge = _validatewritecolumnnames(names, budget)
        try
            paircharge = _reservearray!(budget,
                Pair{String,AbstractVector}, length(names))
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
                push!(output, Pair{String,AbstractVector}(name, values))
            end
            validatorcharge = _reserveobjects!(budget)
            validator = _WriteColumnAccessValidator(table, output)
            return output, something(rows, 0),
                _materializedsum(paircharge, validatorcharge), validator
        finally
            _release!(budget, namearraycharge)
        end
    finally
        _release!(budget, rawcharge)
    end
end

function _preflightwriteencoding(semantic::_NestedSchemaPlan, encoding,
        dictionary::Bool, budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        count = length(semantic.leaves)
        choicecharge = _reservearray!(budget, WriteEncodingChoice, count)
        _reservearray!(budget, WriteLeafPlan, count)
        _reserveobjects!(budget, 3 * count + 2)
        emptyschema = Metadata.SchemaElement[]
        leaves = WriteLeafPlan[]
        sizehint!(leaves, count)
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
        choices = _writeencodingchoices(leaves, encoding, dictionary)
        used = _budgetused(budget)
        temporary = used - start - choicecharge
        temporary > 0 && _release!(budget, temporary)
        return choices, choicecharge
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writefieldsencoded(table, limits::Limits,
        budget::_LiveByteBudget, encoding, dictionary::Bool)
    Base.@nospecialize table
    start = _budgetused(budget)
    try
        columns, rows, inputcharge, validator = _writeinputcolumns(table,
            limits, budget)
        preflight = semantic -> _preflightwriteencoding(semantic, encoding,
            dictionary, budget)
        fields = _nestedwritefields(columns, rows, limits, budget;
            preflight=preflight, sourcevalidator=validator)
        _release!(budget, inputcharge)
        return fields, rows
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writefields(table, limits::Limits, budget::_LiveByteBudget)
    Base.@nospecialize table
    return _writefieldsencoded(table, limits, budget, nothing, false)
end

function _writefields(table, limits::Limits)
    return _writefields(table, limits, _LiveByteBudget(limits))
end

function _presentvalues(column::WriteColumn, ::Type{T}) where {T}
    output = T[]
    sizehint!(output, length(column.values))
    for value in column.values
        ismissing(value) && continue
        push!(output, convert(T, value))
    end
    return output
end

function _plainbytearray(column::WriteColumn, limits::Limits)
    output = UInt8[]
    for value in column.values
        ismissing(value) && continue
        bytes = value isa AbstractString ? codeunits(value) : value
        _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
        length(bytes) <= typemax(Int32) || throw(ArgumentError("byte array exceeds Int32 length"))
        requested = Base.checked_add(length(output), Base.checked_add(4, length(bytes)))
        _checklimit(:page_bytes, requested, limits.max_page_bytes)
        _writelittle!(output, reinterpret(UInt32, Int32(length(bytes))))
        append!(output, bytes)
    end
    return output
end

function _presentfixed(column::WriteColumn, limits::Limits)
    width = column.type_length
    width === nothing && throw(ArgumentError("fixed byte-array column has no width"))
    fixedwidth = Int(width)
    _checklimit(:string_bytes, fixedwidth, limits.max_string_bytes)
    present = length(column.values) - count(ismissing, column.values)
    total = Base.checked_mul(Int64(fixedwidth), Int64(present))
    _checklimit(:page_bytes, total, limits.max_page_bytes)
    output = Matrix{UInt8}(undef, fixedwidth, present)
    columnindex = 0
    for value in column.values
        ismissing(value) && continue
        _checkfixedvalue(value, width)
        columnindex += 1
        @inbounds for byteindex in 1:fixedwidth
            output[byteindex, columnindex] = value[byteindex]
        end
    end
    return output
end

function _plainpayload(column::WriteColumn, limits::Limits)
    T = Base.nonmissingtype(eltype(column.values))
    if column.physical == Metadata.Type.BOOLEAN
        return encode_plain(_presentvalues(column, Bool))
    elseif column.physical == Metadata.Type.INT32
        return encode_plain(_presentvalues(column, Int32))
    elseif column.physical == Metadata.Type.INT64
        return encode_plain(_presentvalues(column, Int64))
    elseif column.physical == Metadata.Type.FLOAT
        return encode_plain(_presentvalues(column, Float32))
    elseif column.physical == Metadata.Type.DOUBLE
        return encode_plain(_presentvalues(column, Float64))
    elseif column.physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY
        return encode_plain_fixed(_presentfixed(column, limits))
    elseif T <: AbstractString || T <: AbstractVector{UInt8}
        return _plainbytearray(column, limits)
    end
    throw(ArgumentError("unsupported PLAIN writer physical type $(column.physical)"))
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
    dictionary && throw(ArgumentError(
        "dictionary=true conflicts with a table-wide encoding; use :dictionary or a per-column mapping"))
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

function _singlewriteencodingmatch(matches::Vector{Int}, selector; kind::String)
    isempty(matches) && throw(ArgumentError(
        "unknown Parquet writer $kind $(repr(selector)) in encoding policy"))
    length(matches) == 1 || throw(ArgumentError(
        "ambiguous Parquet writer $kind $(repr(selector)) selects multiple physical leaves; use a positive leaf ordinal"))
    return only(matches)
end

function _writeencodingleafindex(leaves::Vector{WriteLeafPlan},
    selector::Tuple{Symbol,<:Integer})
    ordinal = selector[2]
    matches = Int[]
    for (index, leaf) in enumerate(leaves)
        leaf.ordinal == ordinal && push!(matches, index)
    end
    return _singlewriteencodingmatch(matches, ordinal;
        kind="physical leaf ordinal")
end

function _writeencodingleafindex(leaves::Vector{WriteLeafPlan},
    selector::Tuple{Symbol,<:Tuple})
    path = selector[2]
    matches = Int[]
    for (index, leaf) in enumerate(leaves)
        _writepathmatches(leaf.path, path) && push!(matches, index)
    end
    return _singlewriteencodingmatch(matches, path;
        kind="physical leaf path")
end

function _writeencodingleafindex(leaves::Vector{WriteLeafPlan},
    selector::Tuple{Symbol,String})
    name = selector[2]
    flat = Int[]
    for (index, leaf) in enumerate(leaves)
        length(leaf.path) == 1 && only(leaf.path) == name && push!(flat, index)
    end
    isempty(flat) || return _singlewriteencodingmatch(flat, name;
        kind="column")
    group = Int[]
    for (index, leaf) in enumerate(leaves)
        !isempty(leaf.path) && first(leaf.path) == name && push!(group, index)
    end
    return _singlewriteencodingmatch(group, name;
        kind="top-level field")
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
    dictionary && throw(ArgumentError(
        "dictionary=true conflicts with a table-wide encoding; use :dictionary or a per-column mapping"))
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

function _presentbytearrays(column::WriteColumn, limits::Limits)
    T = Base.nonmissingtype(eltype(column.values))
    output = T[]
    sizehint!(output, length(column.values))
    total = Int64(0)
    for value in column.values
        ismissing(value) && continue
        bytes = value isa AbstractString ? codeunits(value) : value
        _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
        length(bytes) <= typemax(Int32) || throw(ArgumentError("byte array exceeds Int32 length"))
        total = try
            Base.checked_add(total, Int64(length(bytes)))
        catch err
            err isa OverflowError || rethrow()
            throw(LimitError(:page_bytes, typemax(Int64), limits.max_page_bytes))
        end
        _checklimit(:page_bytes, total, limits.max_page_bytes)
        push!(output, value)
    end
    return output
end

function _encodedpayload(column::WriteColumn, encoding::Metadata.Encoding.T, limits::Limits)
    _validatewriteencoding(column, encoding)
    payload = if encoding == Metadata.Encoding.PLAIN
        _plainpayload(column, limits)
    elseif encoding == Metadata.Encoding.DELTA_BINARY_PACKED
        T = column.physical == Metadata.Type.INT32 ? Int32 : Int64
        encode_delta_binary_packed(_presentvalues(column, T))
    elseif encoding == Metadata.Encoding.DELTA_LENGTH_BYTE_ARRAY
        encode_delta_length_byte_array(_presentbytearrays(column, limits))
    elseif encoding == Metadata.Encoding.DELTA_BYTE_ARRAY
        column.physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY ?
            encode_delta_byte_array_fixed(_presentfixed(column, limits)) :
            encode_delta_byte_array(_presentbytearrays(column, limits))
    elseif encoding == Metadata.Encoding.BYTE_STREAM_SPLIT
        if column.physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY
            encode_byte_stream_split_fixed(_presentfixed(column, limits))
        else
            T = column.physical == Metadata.Type.INT32 ? Int32 :
                column.physical == Metadata.Type.INT64 ? Int64 :
                column.physical == Metadata.Type.FLOAT ? Float32 : Float64
            encode_byte_stream_split(_presentvalues(column, T))
        end
    else
        values = UInt64[value ? 1 : 0 for value in _presentvalues(column, Bool)]
        encode_hybrid(values, 1; length_prefix=true)
    end
    _checklimit(:page_bytes, length(payload), limits.max_page_bytes)
    length(payload) <= typemax(Int32) ||
        throw(ArgumentError("encoded Parquet values exceed Int32 bytes"))
    return payload
end

function _columnentrycount(column::WriteColumn)
    column.definitions === nothing && return length(column.values)
    return length(column.definitions)
end

function _writerpresentcount(column::WriteColumn)
    return length(column.values) - count(ismissing, column.values)
end

function _writerrawpayloadbytes(column::WriteColumn)
    present = _writerpresentcount(column)
    physical = column.physical
    physical == Metadata.Type.BOOLEAN && return Int64(cld(present, 8))
    physical in (Metadata.Type.INT32, Metadata.Type.FLOAT) &&
        return _materializedproduct(present, 4)
    physical in (Metadata.Type.INT64, Metadata.Type.DOUBLE) &&
        return _materializedproduct(present, 8)
    if physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY
        width = column.type_length
        width === nothing && throw(ArgumentError(
            "fixed byte-array column has no width"))
        return _materializedproduct(present, width)
    end
    physical == Metadata.Type.BYTE_ARRAY || return Int64(0)
    bytes = Int64(0)
    for value in column.values
        ismissing(value) && continue
        payload = value isa AbstractString ? ncodeunits(value) : length(value)
        bytes = _materializedsum(bytes, _materializedsum(4, payload))
    end
    return bytes
end

function _writerpageworkingbytes(column::WriteColumn, dictionary::Bool)
    entries = _columnentrycount(column)
    raw = _writerrawpayloadbytes(column)
    encoded = _materializedsum(raw, _materializedsum(
        _materializedproduct(entries, 24), 2048))
    factor = dictionary && column.physical != Metadata.Type.BOOLEAN ? 16 : 8
    bytes = _materializedproduct(encoded, factor)
    bytes = _materializedsum(bytes,
        _materializedproduct(entries, _MATERIALIZED_OBJECT_BYTES))
    return _materializedsum(bytes,
        _materializedproduct(8, _MATERIALIZED_OBJECT_BYTES))
end

function _columnpageslivebytes(pages::ColumnPages)
    bytes = _materializedarraybytes(UInt8, length(pages.bytes))
    bytes = _materializedsum(bytes,
        _materializedarraybytes(Metadata.Encoding.T, length(pages.encodings)))
    bytes = _materializedsum(bytes,
        _materializedarraybytes(Metadata.PageEncodingStats,
            length(pages.encoding_stats)))
    bytes = _materializedsum(bytes,
        _materializedarraybytes(Metadata.PageLocation,
            length(pages.page_locations)))
    return _materializedsum(bytes,
        _materializedproduct(3, _MATERIALIZED_OBJECT_BYTES))
end

function _columnpagestransferredbytes(pages::ColumnPages)
    bytes = _materializedarraybytes(Metadata.Encoding.T,
        length(pages.encodings))
    return _materializedsum(bytes,
        _materializedarraybytes(Metadata.PageEncodingStats,
            length(pages.encoding_stats)))
end

function _budgetedcolumnpages(column::WriteColumn, limits::Limits,
    budget::_LiveByteBudget; checksum::Bool, dictionary::Bool,
    codec::Metadata.CompressionCodec.T,
    compressionlevel::Union{Nothing,Integer}, pageversion::Symbol,
    encoding::Union{Nothing,Metadata.Encoding.T}=nothing)
    working = _writerpageworkingbytes(column, dictionary)
    _reserve!(budget, working)
    pages = try
        _columnpages(column, limits; checksum=checksum,
            dictionary=dictionary, codec=codec,
            compressionlevel=compressionlevel,
            pageversion=pageversion, encoding=encoding)
    catch
        _release!(budget, working)
        rethrow()
    end
    live = _columnpageslivebytes(pages)
    if live > working
        _release!(budget, working)
        throw(AssertionError(
            "writer page allocation exceeded its materialization preflight"))
    end
    _release!(budget, working - live)
    return pages, live
end

function _columnnullcount(column::WriteColumn)
    column.definitions === nothing && return count(ismissing, column.values)
    maximum = UInt64(column.max_definition_level)
    return count(!=(maximum), column.definitions)
end

function _repetitionpayload(column::WriteColumn; length_prefix::Bool=true)
    iszero(column.max_repetition_level) && return UInt8[]
    levels = something(column.repetitions)
    return encode_hybrid(levels, _levelbitwidth(column.max_repetition_level);
        length_prefix=length_prefix)
end

function _definitionpayload(column::WriteColumn; length_prefix::Bool=true)
    iszero(column.max_definition_level) && return UInt8[]
    levels = column.definitions === nothing ?
        UInt64[ismissing(value) ? 0 : 1 for value in column.values] :
        column.definitions
    return encode_hybrid(levels, _levelbitwidth(column.max_definition_level);
        length_prefix=length_prefix)
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
    name === :lz4 && return _writecodec(Metadata.CompressionCodec.LZ4)
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

function _framedpage(payload::Vector{UInt8}, type::Metadata.PageType.T, limits::Limits;
    checksum::Bool, codec::Metadata.CompressionCodec.T,
    compressionlevel::Union{Nothing,Integer}, data_header=nothing, dictionary_header=nothing)
    _checklimit(:page_bytes, length(payload), limits.max_page_bytes)
    length(payload) <= typemax(Int32) || throw(ArgumentError("Parquet page exceeds Int32 bytes"))
    encoded = compress(codec, payload; level=compressionlevel)
    _checklimit(:page_bytes, length(encoded), limits.max_page_bytes)
    length(encoded) <= typemax(Int32) || throw(ArgumentError("compressed Parquet page exceeds Int32 bytes"))
    crc = checksum ? reinterpret(Int32, pagechecksum(encoded)) : nothing
    header = Metadata.PageHeader(
        type_=type,
        uncompressed_page_size=Int32(length(payload)),
        compressed_page_size=Int32(length(encoded)),
        crc=crc,
        data_page_header=data_header,
        dictionary_page_header=dictionary_header,
    )
    headerbytes = Thrift.encode(header)
    _checklimit(:page_header_bytes, length(headerbytes), limits.max_page_header_bytes)
    return vcat(headerbytes, encoded), length(headerbytes), length(payload)
end

function _framedpagev2(column::WriteColumn, values::Vector{UInt8},
    encoding::Metadata.Encoding.T, limits::Limits; checksum::Bool,
    codec::Metadata.CompressionCodec.T, compressionlevel::Union{Nothing,Integer})
    repetition = _repetitionpayload(column; length_prefix=false)
    definition = _definitionpayload(column; length_prefix=false)
    candidate = compress(codec, values; level=compressionlevel)
    compressedvalues = codec != Metadata.CompressionCodec.UNCOMPRESSED &&
        !isempty(values) && length(candidate) < length(values)
    encoded = compressedvalues ? candidate : copy(values)
    uncompressed = Int64(length(repetition)) + Int64(length(definition)) +
        Int64(length(values))
    compressed = Int64(length(repetition)) + Int64(length(definition)) +
        Int64(length(encoded))
    _checklimit(:page_bytes, uncompressed, limits.max_page_bytes)
    _checklimit(:page_bytes, compressed, limits.max_page_bytes)
    uncompressed <= typemax(Int32) || throw(ArgumentError("Parquet page exceeds Int32 bytes"))
    compressed <= typemax(Int32) || throw(ArgumentError("compressed Parquet page exceeds Int32 bytes"))
    payload = vcat(repetition, definition, encoded)
    crc = checksum ? reinterpret(Int32, pagechecksum(payload)) : nothing
    data_header = Metadata.DataPageHeaderV2(
        num_values=Int32(_columnentrycount(column)),
        num_nulls=Int32(_columnnullcount(column)),
        num_rows=Int32(column.rows),
        encoding=encoding,
        definition_levels_byte_length=Int32(length(definition)),
        repetition_levels_byte_length=Int32(length(repetition)),
        is_compressed=compressedvalues,
    )
    header = Metadata.PageHeader(
        type_=Metadata.PageType.DATA_PAGE_V2,
        uncompressed_page_size=Int32(uncompressed),
        compressed_page_size=Int32(compressed),
        crc=crc,
        data_page_header_v2=data_header,
    )
    headerbytes = Thrift.encode(header)
    _checklimit(:page_header_bytes, length(headerbytes), limits.max_page_header_bytes)
    return vcat(headerbytes, payload), length(headerbytes), Int(uncompressed)
end

function _datapagebytes(column::WriteColumn, values::Vector{UInt8},
    encoding::Metadata.Encoding.T, pageversion::Symbol, limits::Limits; checksum::Bool,
    codec::Metadata.CompressionCodec.T, compressionlevel::Union{Nothing,Integer})
    if pageversion === :v2
        return _framedpagev2(column, values, encoding, limits; checksum=checksum,
            codec=codec, compressionlevel=compressionlevel)
    end
    payload = _repetitionpayload(column)
    append!(payload, _definitionpayload(column))
    append!(payload, values)
    data_header = Metadata.DataPageHeader(
        num_values=Int32(_columnentrycount(column)),
        encoding=encoding,
        definition_level_encoding=Metadata.Encoding.RLE,
        repetition_level_encoding=Metadata.Encoding.RLE,
    )
    return _framedpage(payload, Metadata.PageType.DATA_PAGE, limits;
        checksum=checksum, codec=codec, compressionlevel=compressionlevel,
        data_header=data_header)
end

function _datapagetype(pageversion::Symbol)
    pageversion === :v1 && return Metadata.PageType.DATA_PAGE
    return Metadata.PageType.DATA_PAGE_V2
end

function _encodedcolumnpages(column::WriteColumn, encoding::Metadata.Encoding.T,
    limits::Limits; checksum::Bool,
    codec::Metadata.CompressionCodec.T, compressionlevel::Union{Nothing,Integer},
    pageversion::Symbol)
    page, headerlength, payloadlength = _datapagebytes(column,
        _encodedpayload(column, encoding, limits), encoding, pageversion, limits;
        checksum=checksum, codec=codec,
        compressionlevel=compressionlevel)
    encodings = Metadata.Encoding.T[]
    (!iszero(column.max_repetition_level) || !iszero(column.max_definition_level)) &&
        encoding != Metadata.Encoding.RLE &&
        push!(encodings, Metadata.Encoding.RLE)
    push!(encodings, encoding)
    stats = Metadata.PageEncodingStats[
        Metadata.PageEncodingStats(
            page_type=_datapagetype(pageversion),
            encoding=encoding,
            count=Int32(1),
        ),
    ]
    uncompressed = Int64(headerlength) + Int64(payloadlength)
    return ColumnPages(page, uncompressed, Int64(0), nothing, encodings, stats)
end

function _plaincolumnpages(column::WriteColumn, limits::Limits; checksum::Bool,
    codec::Metadata.CompressionCodec.T, compressionlevel::Union{Nothing,Integer},
    pageversion::Symbol)
    return _encodedcolumnpages(column, Metadata.Encoding.PLAIN, limits; checksum=checksum,
        codec=codec, compressionlevel=compressionlevel, pageversion=pageversion)
end

function _dictionarycolumnpages(column::WriteColumn, limits::Limits; checksum::Bool,
    codec::Metadata.CompressionCodec.T, compressionlevel::Union{Nothing,Integer},
    pageversion::Symbol)
    plan = _dictionaryplan(column, limits)
    dictionary_header = Metadata.DictionaryPageHeader(
        num_values=Int32(length(plan.values)),
        encoding=Metadata.Encoding.PLAIN,
        is_sorted=false,
    )
    dictionary_page, dictionary_headerlength, dictionary_payloadlength = _framedpage(plan.dictionary_payload,
        Metadata.PageType.DICTIONARY_PAGE, limits; checksum=checksum,
        codec=codec, compressionlevel=compressionlevel,
        dictionary_header=dictionary_header)
    data_page, data_headerlength, data_payloadlength = _datapagebytes(column,
        plan.index_payload, Metadata.Encoding.RLE_DICTIONARY, pageversion, limits;
        checksum=checksum, codec=codec, compressionlevel=compressionlevel)
    bytes = vcat(dictionary_page, data_page)
    uncompressed = Int64(dictionary_headerlength) + Int64(dictionary_payloadlength) +
        Int64(data_headerlength) + Int64(data_payloadlength)
    encodings = Metadata.Encoding.T[Metadata.Encoding.PLAIN, Metadata.Encoding.RLE,
        Metadata.Encoding.RLE_DICTIONARY]
    stats = Metadata.PageEncodingStats[
        Metadata.PageEncodingStats(
            page_type=Metadata.PageType.DICTIONARY_PAGE,
            encoding=Metadata.Encoding.PLAIN,
            count=Int32(1),
        ),
        Metadata.PageEncodingStats(
            page_type=_datapagetype(pageversion),
            encoding=Metadata.Encoding.RLE_DICTIONARY,
            count=Int32(1),
        ),
    ]
    return ColumnPages(bytes, uncompressed, Int64(length(dictionary_page)), Int64(0),
        encodings, stats)
end

function _columnpages(column::WriteColumn, limits::Limits; checksum::Bool, dictionary::Bool,
    codec::Metadata.CompressionCodec.T, compressionlevel::Union{Nothing,Integer},
    pageversion::Symbol, encoding::Union{Nothing,Metadata.Encoding.T}=nothing)
    encoding === nothing || return _encodedcolumnpages(column, encoding, limits;
        checksum=checksum, codec=codec, compressionlevel=compressionlevel,
        pageversion=pageversion)
    plain = _plaincolumnpages(column, limits; checksum=checksum, codec=codec,
        compressionlevel=compressionlevel, pageversion=pageversion)
    dictionary || return plain
    column.physical == Metadata.Type.BOOLEAN && return plain
    encoded = try
        _dictionarycolumnpages(column, limits; checksum=checksum, codec=codec,
            compressionlevel=compressionlevel, pageversion=pageversion)
    catch err
        # The optional candidate may exceed a page limit even when PLAIN fits.
        err isa LimitError && return plain
        rethrow()
    end
    length(encoded.bytes) < length(plain.bytes) || return plain
    return encoded
end

function _schemaelements(column::WriteColumn)
    return column.schema
end

function _withoutcolumnschema(column::WriteColumn, path::Vector{String}=column.path)
    return WriteColumn(column.name, column.values, column.physical, column.type_length,
        column.optional, column.logical, column.converted, copy(path), column.repetitions,
        column.definitions, column.max_repetition_level, column.max_definition_level,
        column.rows, Metadata.SchemaElement[])
end

function _writefieldplan(column::WriteColumn)
    schema = _schemaelements(column)
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

function _flattenwriteschema(fields::Vector{WriteFieldPlan}, limits::Limits,
    budget::_LiveByteBudget)
    isempty(fields) && throw(ArgumentError("a Parquet table must have at least one column"))
    length(fields) <= typemax(Int32) ||
        throw(ArgumentError("Parquet schema has more than Int32 top-level fields"))
    count = _writeschemaelementcount(fields, limits)
    _reservearray!(budget, Metadata.SchemaElement, count)
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

function _validatewriteleaf(node::SchemaNode, column::WriteColumn, rows::Int)
    isempty(column.schema) || throw(ArgumentError(
        "row-group leaf $(repr(column.name)) still owns a schema fragment"))
    column.rows == rows || throw(ArgumentError(
        "row-group leaf $(repr(column.name)) has $(column.rows) rows but $rows were expected"))
    column.path == node.path || throw(ArgumentError(
        "writer leaf path $(repr(column.path)) does not match schema path $(repr(node.path))"))
    column.physical == node.element.type_ || throw(ArgumentError(
        "writer leaf $(repr(node.path)) has a physical type that does not match its schema"))
    column.type_length == node.element.type_length || throw(ArgumentError(
        "writer leaf $(repr(node.path)) has a type length that does not match its schema"))
    column.logical == node.element.logicalType || throw(ArgumentError(
        "writer leaf $(repr(node.path)) has a logical type that does not match its schema"))
    column.converted == node.element.converted_type || throw(ArgumentError(
        "writer leaf $(repr(node.path)) has a converted type that does not match its schema"))
    column.max_repetition_level == node.max_repetition_level || throw(ArgumentError(
        "writer leaf $(repr(node.path)) has a repetition level that does not match its schema"))
    column.max_definition_level == node.max_definition_level || throw(ArgumentError(
        "writer leaf $(repr(node.path)) has a definition level that does not match its schema"))
    optional = node.element.repetition_type == Metadata.FieldRepetitionType.OPTIONAL
    column.optional == optional || throw(ArgumentError(
        "writer leaf $(repr(node.path)) has optionality that does not match its schema"))
    return
end

function _writeleafplanbytes(nodes)
    bytes = _materializedarraybytes(WriteLeafPlan, length(nodes))
    for node in nodes
        bytes = _materializedsum(bytes,
            _materializedarraybytes(String, length(node.path)))
        bytes = _materializedsum(bytes, _MATERIALIZED_OBJECT_BYTES)
    end
    return bytes
end

function _writeplanleaves(fields::Vector{WriteFieldPlan}, schema::Schema, rows::Int,
    limits::Limits, budget::_LiveByteBudget)
    start = _budgetused(budget)
    length(fields) == length(schema.root.children) || throw(ArgumentError(
        "writer top-level fields do not match the parsed schema"))
    columncharge = _reservearray!(budget, WriteColumn, length(schema.leaves))
    try
        columns = WriteColumn[]
        sizehint!(columns, length(schema.leaves))
        for (field, node) in zip(fields, schema.root.children)
            length(field.leaves) == _writeschemaleafcount(node) ||
                throw(ArgumentError(
                    "writer field $(repr(node.element.name)) does not own its schema leaves"))
            append!(columns, field.leaves)
        end
        length(columns) == length(schema.leaves) || throw(ArgumentError(
            "writer physical leaves do not match the parsed schema"))
        leafcharge = _writeleafplanbytes(schema.leaves)
        _reserve!(budget, leafcharge)
        leaves = WriteLeafPlan[]
        sizehint!(leaves, length(columns))
        for (index, (node, column)) in enumerate(zip(schema.leaves, columns))
            prepared, entries, dense, payload, preparedcharge =
                _writeprepareleaf(node, column, rows, limits, budget)
            leafcharge = _materializedsum(leafcharge, preparedcharge)
            _validatewriteleaf(node, prepared, rows)
            ordinal = Int32(index)
            node.column_index == ordinal || throw(ArgumentError(
                "writer leaf ordinal does not match the parsed schema"))
            push!(leaves, WriteLeafPlan(ordinal, copy(node.path), prepared,
                entries, dense, payload))
        end
        _release!(budget, columncharge)
        return leaves, leafcharge
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writeplan(fields::Vector{WriteFieldPlan}, rows::Int, limits::Limits,
    budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        rows >= 0 || throw(ArgumentError(
            "Parquet row count must be nonnegative"))
        elements = _flattenwriteschema(fields, limits, budget)
        schema = Schema(elements; limits=limits, budget=budget)
        leaves, leafcharge = _writeplanleaves(fields, schema, rows, limits,
            budget)
        _reservearray!(budget, WriteRowGroupPlan, iszero(rows) ? 0 : 1)
        _reserveobjects!(budget, 2)
        rowgroups = iszero(rows) ? WriteRowGroupPlan[] :
            WriteRowGroupPlan[WriteRowGroupPlan(rows, leaves)]
        plan = WritePlan(elements, schema, rows, rowgroups)
        iszero(rows) && _release!(budget, leafcharge)
        return plan
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writeplan(fields::Vector{WriteFieldPlan}, rows::Int, limits::Limits)
    return _writeplan(fields, rows, limits, _LiveByteBudget(limits))
end

function _writefieldplan(column::WriteColumn, budget::_LiveByteBudget)
    schema = _schemaelements(column)
    isempty(schema) && throw(ArgumentError(
        "writer column $(repr(column.name)) has no top-level field schema"))
    _reservearray!(budget, Metadata.SchemaElement, length(schema))
    _reservearray!(budget, WriteColumn, 1)
    _reservearray!(budget, String, length(column.path))
    _reserveobjects!(budget, 2)
    return WriteFieldPlan(copy(schema), WriteColumn[_withoutcolumnschema(column)])
end

function _writeplan(columns::Vector{WriteColumn}, rows::Int, limits::Limits,
    budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        _reservearray!(budget, WriteFieldPlan, length(columns))
        fields = WriteFieldPlan[]
        sizehint!(fields, length(columns))
        for column in columns
            push!(fields, _writefieldplan(column, budget))
        end
        return _writeplan(fields, rows, limits, budget)
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writeplan(columns::Vector{WriteColumn}, rows::Int, limits::Limits)
    return _writeplan(columns, rows, limits, _LiveByteBudget(limits))
end

function _writeplan(table, limits::Limits, budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        fields, rows = _writefields(table, limits, budget)
        return _writeplan(fields, rows, limits, budget)
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writeplan(table, limits::Limits)
    return _writeplan(table, limits, _LiveByteBudget(limits))
end

function _writeplan(table)
    return _writeplan(table, Limits())
end

function _columnchunk(leaf::WriteLeafPlan, offset::Int64, pages::ColumnPages,
    codec::Metadata.CompressionCodec.T,
    statistics::Union{Nothing,Metadata.Statistics}=nothing)
    column = leaf.column
    dictionary_offset = pages.dictionary_offset === nothing ? nothing :
        offset + pages.dictionary_offset
    compressed = Int64(length(pages.bytes))
    metadata = Metadata.ColumnMetaData(
        type_=column.physical,
        encodings=pages.encodings,
        path_in_schema=leaf.path,
        codec=codec,
        num_values=Int64(_columnentrycount(column)),
        total_uncompressed_size=pages.uncompressed_size,
        total_compressed_size=compressed,
        data_page_offset=offset + pages.data_offset,
        dictionary_page_offset=dictionary_offset,
        statistics=statistics,
        encoding_stats=pages.encoding_stats,
    )
    return Metadata.ColumnChunk(file_offset=Int64(0), meta_data=metadata)
end

function _addgroupsize(total::Int64, value::Int64)
    return try
        Base.checked_add(total, value)
    catch err
        err isa OverflowError || rethrow()
        throw(ArgumentError("Parquet row group byte size exceeds Int64"))
    end
end

const _WRITE_FOOTER_CONTROL_BYTES =
    _materializedproduct(2, _MATERIALIZED_OBJECT_BYTES)

function _writeencodefooter(value, limits::Limits, budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        _reserve!(budget, _WRITE_FOOTER_CONTROL_BYTES)
        exact = try
            Thrift._encodedsize(value)
        finally
            _release!(budget, _WRITE_FOOTER_CONTROL_BYTES)
        end
        exact <= typemax(UInt32) || throw(ArgumentError(
            "Parquet footer exceeds UInt32 bytes"))
        _checklimit(:footer_bytes, exact, limits.max_footer_bytes)
        _reserve!(budget, _WRITE_FOOTER_CONTROL_BYTES)
        charge = _reservearray!(budget, UInt8, exact)
        bytes = Vector{UInt8}(undef, Int(exact))
        Thrift._encodefixed!(bytes, value)
        _release!(budget, _WRITE_FOOTER_CONTROL_BYTES)
        return bytes, charge
    catch err
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        err isa Thrift._WriteCountOverflow && throw(ArgumentError(
            "Parquet footer exceeds UInt32 bytes"))
        rethrow()
    end
end

function _reserveoutputgrowth!(budget::_LiveByteBudget, bytes::Integer)
    charge = _materializedproduct(bytes, 2)
    _reserve!(budget, charge)
    return charge
end

function _encodefile(table; checksum::Bool=true, dictionary::Bool=false,
    codec=:uncompressed, compressionlevel::Union{Nothing,Integer}=nothing,
    pageversion=:v1, encoding=nothing, rowgroupsize=1_048_576,
    pagesize=1024 * 1024, pageindex::Bool=true, statistics::Bool=true,
    limits::Limits=Limits())
    Base.@nospecialize table
    statisticslimit = _statisticlimit(limits)
    budget = _LiveByteBudget(limits)
    _reserveobjects!(budget, 2)
    compression = _writecodec(codec)
    version = _writepageversion(pageversion)
    target = _writepagesize(pagesize)
    _writerowgroupsize(rowgroupsize, 0)
    fields, rows = _writefieldsencoded(table, limits, budget, encoding,
        dictionary)
    initialplan = _writeplan(fields, rows, limits, budget)
    selectorcharge = Int64(0)
    leaves = if isempty(initialplan.rowgroups)
        selected, charge = _writeplanleaves(fields, initialplan.schema, rows, limits,
            budget)
        selectorcharge = charge
        selected
    else
        only(initialplan.rowgroups).leaves
    end
    _reservearray!(budget, WriteEncodingChoice, length(leaves))
    _reserveobjects!(budget, 4 * length(leaves) + 1)
    choices = try
        _writeencodingchoices(leaves, encoding, dictionary)
    finally
        iszero(selectorcharge) || _release!(budget, selectorcharge)
    end
    plan = _splitwriteplan(initialplan, rowgroupsize, limits, budget)
    leaves = nothing
    initialplan = nothing
    columnorders = statistics ? _writecolumnorders(plan.schema, budget) : nothing
    _reservearray!(budget, UInt8, length(PARQUET_MAGIC))
    output = copy(PARQUET_MAGIC)
    _reservearray!(budget, Metadata.RowGroup, length(plan.rowgroups))
    rowgroups = Metadata.RowGroup[]
    sizehint!(rowgroups, length(plan.rowgroups))
    indexgroups = nothing
    if pageindex
        _reservearray!(budget, Vector{Metadata.OffsetIndex},
            length(plan.rowgroups))
        indexgroups = Vector{Metadata.OffsetIndex}[]
        sizehint!(indexgroups, length(plan.rowgroups))
    end
    emitordinals = length(plan.rowgroups) <= Int(typemax(Int16)) + 1
    for (groupindex, rowgroupplan) in enumerate(plan.rowgroups)
        _validatewritechoicecount(rowgroupplan.leaves, choices)
        _reservearray!(budget, Metadata.ColumnChunk,
            length(rowgroupplan.leaves))
        chunks = Metadata.ColumnChunk[]
        sizehint!(chunks, length(rowgroupplan.leaves))
        indexes = nothing
        if pageindex
            _reservearray!(budget, Metadata.OffsetIndex,
                length(rowgroupplan.leaves))
            indexes = Metadata.OffsetIndex[]
            sizehint!(indexes, length(rowgroupplan.leaves))
        end
        uncompressedsize = Int64(0)
        compressedsize = Int64(0)
        groupoffset = Int64(length(output))
        for (leaf, choice) in zip(rowgroupplan.leaves, choices)
            column = leaf.column
            columnstatistics = if statistics
                element = plan.schema.leaves[Int(leaf.ordinal)].element
                _writecolumnstatistics(leaf, element, statisticslimit, budget)
            else
                nothing
            end
            offset = Int64(length(output))
            pages, pagecharge = _budgetedsplitcolumnpages(leaf, limits, budget;
                pagesize=target, checksum=checksum,
                dictionary=choice.dictionary, codec=compression,
                compressionlevel=compressionlevel, pageversion=version,
                encoding=choice.encoding, capturelocations=pageindex)
            _reserveoutputgrowth!(budget, length(pages.bytes))
            append!(output, pages.bytes)
            _reserveobjects!(budget, 4)
            push!(chunks, _columnchunk(leaf, offset, pages, compression,
                columnstatistics))
            pageindex && push!(indexes, _writeabsoluteoffsetindex(pages,
                offset, rowgroupplan.rows, budget))
            transferred = _columnpagestransferredbytes(pages)
            transferred <= pagecharge || throw(AssertionError(
                "writer page metadata exceeds its live-page charge"))
            uncompressedsize = _addgroupsize(uncompressedsize, pages.uncompressed_size)
            compressedsize = _addgroupsize(compressedsize, Int64(length(pages.bytes)))
            _release!(budget, pagecharge - transferred)
        end
        _reserveobjects!(budget, 2)
        push!(rowgroups, Metadata.RowGroup(
            columns=chunks,
            total_byte_size=uncompressedsize,
            num_rows=Int64(rowgroupplan.rows),
            total_compressed_size=compressedsize,
            file_offset=groupoffset,
            ordinal=emitordinals ? Int16(groupindex - 1) : nothing,
        ))
        pageindex && push!(indexgroups, indexes)
    end
    if pageindex
        obsolete = _writeoffsetindexobsoletebytes(rowgroups, indexgroups)
        rowgroups = _writeoffsetindexsection!(output, rowgroups,
            indexgroups, limits, budget)
        indexgroups = nothing
        indexes = nothing
        chunks = nothing
        _release!(budget, obsolete)
    end
    _reserveobjects!(budget, 2)
    metadata = Metadata.FileMetaData(
        version=Int32(1),
        schema=plan.elements,
        num_rows=Int64(plan.rows),
        row_groups=rowgroups,
        created_by="Parquet.jl version 1.0.0-DEV",
        column_orders=columnorders,
    )
    footer, footercharge = _writeencodefooter(metadata, limits, budget)
    try
        _reserveoutputgrowth!(budget, _materializedsum(length(footer), 8))
        append!(output, footer)
        _writelittle!(output, UInt32(length(footer)))
        append!(output, PARQUET_MAGIC)
    finally
        _release!(budget, footercharge)
    end
    return output
end

"""
    Parquet.write(sink, table; encoding=nothing, dictionary=false,
        rowgroupsize=1_048_576, pagesize=1024 * 1024, pageindex=true,
        statistics=true,
        kwargs...)

Write a Tables.jl source to a path or `IO`. An encoding Symbol or string applies to
every column. A `Pair`, `NamedTuple`, or dictionary overrides exact column names.
Unlisted columns use PLAIN, or adaptive dictionary encoding when `dictionary=true`.
Use `:dictionary` to request adaptive dictionary encoding for one selected column.
Footer statistics and a complete column-order declaration are emitted by default.
Set `statistics=false` to omit them. `limits.max_statistics_value_bytes` limits
each emitted bound.
"""
function write(io::IO, table; checksum::Bool=true, dictionary::Bool=false,
    encoding=nothing, codec=:uncompressed,
    compressionlevel::Union{Nothing,Integer}=nothing, pageversion=:v1,
    rowgroupsize=1_048_576, pagesize=1024 * 1024, pageindex::Bool=true,
    statistics::Bool=true, limits::Limits=Limits())
    _statisticlimit(limits)
    bytes = _encodefile(table; checksum=checksum, dictionary=dictionary, codec=codec,
        compressionlevel=compressionlevel, pageversion=pageversion,
        encoding=encoding, rowgroupsize=rowgroupsize, pagesize=pagesize,
        pageindex=pageindex, statistics=statistics, limits=limits)
    Base.write(io, bytes)
    return
end

function write(path::AbstractString, table; checksum::Bool=true, dictionary::Bool=false,
    encoding=nothing, codec=:uncompressed, compressionlevel::Union{Nothing,Integer}=nothing,
    pageversion=:v1, rowgroupsize=1_048_576, pagesize=1024 * 1024,
    pageindex::Bool=true, statistics::Bool=true, limits::Limits=Limits())
    _statisticlimit(limits)
    bytes = _encodefile(table; checksum=checksum, dictionary=dictionary, codec=codec,
        compressionlevel=compressionlevel, pageversion=pageversion,
        encoding=encoding, rowgroupsize=rowgroupsize, pagesize=pagesize,
        pageindex=pageindex, statistics=statistics, limits=limits)
    open(path, "w") do io
        Base.write(io, bytes)
    end
    return
end
