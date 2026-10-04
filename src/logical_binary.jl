import UUIDs

"""An encoded JSON document whose UTF-8 bytes are preserved exactly."""
struct JSONValue
    bytes::Base.CodeUnits{UInt8,String}

    function JSONValue(bytes::AbstractVector{UInt8}; limits::Limits=Limits())
        _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
        owned = collect(bytes)
        _validatejson(owned, limits, ArgumentError)
        return new(codeunits(String(owned)))
    end

    function JSONValue(bytes::Vector{UInt8}, ::Val{:validated})
        return new(codeunits(String(bytes)))
    end
end

"""An encoded BSON document whose bytes are preserved exactly."""
struct BSONValue
    bytes::Base.CodeUnits{UInt8,String}

    function BSONValue(bytes::AbstractVector{UInt8}; limits::Limits=Limits())
        _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
        owned = collect(bytes)
        _validatebson(owned, limits, ArgumentError)
        return new(codeunits(String(owned)))
    end

    function BSONValue(bytes::Vector{UInt8}, ::Val{:validated})
        return new(codeunits(String(bytes)))
    end
end

"""A Parquet INTERVAL with independent unsigned month, day, and millisecond fields."""
struct Interval
    months::UInt32
    days::UInt32
    milliseconds::UInt32
end

function Interval(months::Integer, days::Integer, milliseconds::Integer)
    0 <= months <= typemax(UInt32) ||
        throw(ArgumentError("INTERVAL months must fit UInt32"))
    0 <= days <= typemax(UInt32) ||
        throw(ArgumentError("INTERVAL days must fit UInt32"))
    0 <= milliseconds <= typemax(UInt32) ||
        throw(ArgumentError("INTERVAL milliseconds must fit UInt32"))
    return Interval(UInt32(months), UInt32(days), UInt32(milliseconds))
end

function Base.:(==)(left::JSONValue, right::JSONValue)
    return left.bytes == right.bytes
end

function Base.isequal(left::JSONValue, right::JSONValue)
    return isequal(left.bytes, right.bytes)
end

function Base.hash(value::JSONValue, seed::UInt)
    return hash(value.bytes, hash(:JSONValue, seed))
end

function Base.copy(value::JSONValue)
    return JSONValue(collect(value.bytes), Val(:validated))
end

function Base.:(==)(left::BSONValue, right::BSONValue)
    return left.bytes == right.bytes
end

function Base.isequal(left::BSONValue, right::BSONValue)
    return isequal(left.bytes, right.bytes)
end

function Base.hash(value::BSONValue, seed::UInt)
    return hash(value.bytes, hash(:BSONValue, seed))
end

function Base.copy(value::BSONValue)
    return BSONValue(collect(value.bytes), Val(:validated))
end

function Base.:(==)(left::Interval, right::Interval)
    return left.months == right.months && left.days == right.days &&
        left.milliseconds == right.milliseconds
end

function Base.isequal(left::Interval, right::Interval)
    return isequal(left.months, right.months) && isequal(left.days, right.days) &&
        isequal(left.milliseconds, right.milliseconds)
end

function Base.hash(value::Interval, seed::UInt)
    return hash((value.months, value.days, value.milliseconds),
        hash(:Interval, seed))
end

function Base.copy(value::Interval)
    return value
end

function _requirefixedlogical(element::Metadata.SchemaElement, width::Int,
    annotation::Symbol)
    _requirelogicalphysical(element, Metadata.Type.FIXED_LEN_BYTE_ARRAY, annotation)
    element.type_length == width && return
    throw(FormatError("$annotation annotation on $(repr(element.name)) requires " *
        "FIXED_LEN_BYTE_ARRAY length $width, got $(element.type_length)"))
end

function _requireunknownphysical(element::Metadata.SchemaElement)
    element.type_ === nothing &&
        throw(FormatError("UNKNOWN annotation on $(repr(element.name)) requires " *
            "a primitive physical type"))
    element.repetition_type == Metadata.FieldRepetitionType.REQUIRED &&
        throw(FormatError("UNKNOWN annotation on required field " *
            "$(repr(element.name)) cannot represent null"))
    return
end

function _binarymodernkind(element::Metadata.SchemaElement,
    logical::Metadata.LogicalType)
    if logical.ENUM !== nothing
        _requirelogicalphysical(element, Metadata.Type.BYTE_ARRAY, :ENUM)
        return :enum
    elseif logical.UNKNOWN !== nothing
        _requireunknownphysical(element)
        return :unknown
    elseif logical.JSON !== nothing
        _requirelogicalphysical(element, Metadata.Type.BYTE_ARRAY, :JSON)
        return :json
    elseif logical.BSON !== nothing
        _requirelogicalphysical(element, Metadata.Type.BYTE_ARRAY, :BSON)
        return :bson
    elseif logical.UUID !== nothing
        _requirefixedlogical(element, 16, :UUID)
        return :uuid
    elseif logical.FLOAT16 !== nothing
        _requirefixedlogical(element, 2, :FLOAT16)
        return :float16
    end
    return nothing
end

function _binarylegacykind(element::Metadata.SchemaElement)
    converted = element.converted_type
    if converted == Metadata.ConvertedType.ENUM
        _requirelogicalphysical(element, Metadata.Type.BYTE_ARRAY, :ENUM)
        return :enum
    elseif converted == Metadata.ConvertedType.JSON
        _requirelogicalphysical(element, Metadata.Type.BYTE_ARRAY, :JSON)
        return :json
    elseif converted == Metadata.ConvertedType.BSON
        _requirelogicalphysical(element, Metadata.Type.BYTE_ARRAY, :BSON)
        return :bson
    elseif converted == Metadata.ConvertedType.INTERVAL
        _requirefixedlogical(element, 12, :INTERVAL)
        return :interval
    end
    return nothing
end

function _binarylogicalkind(element::Metadata.SchemaElement)
    logical = element.logicalType
    logical === nothing || return _binarymodernkind(element, logical)
    return _binarylegacykind(element)
end

function _binarylogicalkind(node::SchemaNode)
    return _binarylogicalkind(node.element)
end

function _binarylogicaleltype(kind::Symbol, physical::Type)
    kind === :enum && return String
    kind === :uuid && return UUIDs.UUID
    kind === :float16 && return Float16
    kind === :json && return JSONValue
    kind === :bson && return BSONValue
    kind === :interval && return Interval
    kind === :unknown && return Missing
    return nothing
end

function _binarylogicaleltype(element::Metadata.SchemaElement, physical::Type)
    kind = _binarylogicalkind(element)
    kind === nothing && return nothing
    return _binarylogicaleltype(kind, physical)
end

function _binarylogicaleltype(node::SchemaNode, physical::Type)
    return _binarylogicaleltype(node.element, physical)
end

function _requirelogicalbytes(value, element::Metadata.SchemaElement,
    annotation::Symbol)
    value isa AbstractVector{UInt8} && return value
    throw(FormatError("$annotation column $(repr(element.name)) contains a non-byte-array value"))
end

function _fixedlogicalbytes(value, element::Metadata.SchemaElement, width::Int,
    annotation::Symbol)
    bytes = _requirelogicalbytes(value, element, annotation)
    length(bytes) == width ||
        throw(FormatError("$annotation column $(repr(element.name)) contains " *
            "$(length(bytes)) bytes, expected $width"))
    return bytes
end

function _uuidfrombytes(bytes::AbstractVector{UInt8})
    value = UInt128(0)
    for byte in bytes
        value = (value << 8) | UInt128(byte)
    end
    return UUIDs.UUID(value)
end

function _uuidbytes(value::UUIDs.UUID)
    raw = UInt128(value)
    bytes = Vector{UInt8}(undef, 16)
    for index in eachindex(bytes)
        shift = 8 * (length(bytes) - index)
        bytes[index] = UInt8((raw >> shift) & 0xff)
    end
    return bytes
end

function _float16frombytes(bytes::AbstractVector{UInt8})
    bits = UInt16(bytes[1]) | (UInt16(bytes[2]) << 8)
    return reinterpret(Float16, bits)
end

function _float16bytes(value::Float16)
    bits = reinterpret(UInt16, value)
    return UInt8[UInt8(bits & 0xff), UInt8(bits >> 8)]
end

function _readuint32le(bytes::AbstractVector{UInt8}, offset::Int)
    return UInt32(bytes[offset]) |
        (UInt32(bytes[offset + 1]) << 8) |
        (UInt32(bytes[offset + 2]) << 16) |
        (UInt32(bytes[offset + 3]) << 24)
end

function _intervalfrombytes(bytes::AbstractVector{UInt8})
    return Interval(_readuint32le(bytes, 1), _readuint32le(bytes, 5),
        _readuint32le(bytes, 9))
end

function _appenduint32le!(bytes::Vector{UInt8}, value::UInt32)
    push!(bytes, UInt8(value & 0xff))
    push!(bytes, UInt8((value >> 8) & 0xff))
    push!(bytes, UInt8((value >> 16) & 0xff))
    push!(bytes, UInt8(value >> 24))
    return
end

function _intervalbytes(value::Interval)
    bytes = UInt8[]
    sizehint!(bytes, 12)
    _appenduint32le!(bytes, value.months)
    _appenduint32le!(bytes, value.days)
    _appenduint32le!(bytes, value.milliseconds)
    return bytes
end

function _taggedlogicalvalue(::Type{JSONValue}, value,
    element::Metadata.SchemaElement, limits::Limits)
    bytes = _requirelogicalbytes(value, element, :JSON)
    _validatejson(bytes, limits, FormatError)
    return JSONValue(collect(bytes), Val(:validated))
end

function _taggedlogicalvalue(::Type{BSONValue}, value,
    element::Metadata.SchemaElement, limits::Limits)
    bytes = _requirelogicalbytes(value, element, :BSON)
    _validatebson(bytes, limits, FormatError)
    return BSONValue(collect(bytes), Val(:validated))
end

function _binarylogicalvalue(kind::Symbol, element::Metadata.SchemaElement, value,
    limits::Limits)
    if kind === :unknown
        ismissing(value) && return missing
        throw(FormatError("UNKNOWN column $(repr(element.name)) contains a non-null value"))
    end
    ismissing(value) && return missing
    if kind === :enum
        bytes = _requirelogicalbytes(value, element, :ENUM)
        return _fromparquetstring(bytes, limits)
    elseif kind === :uuid
        return _uuidfrombytes(_fixedlogicalbytes(value, element, 16, :UUID))
    elseif kind === :float16
        return _float16frombytes(_fixedlogicalbytes(value, element, 2, :FLOAT16))
    elseif kind === :json
        return _taggedlogicalvalue(JSONValue, value, element, limits)
    elseif kind === :bson
        return _taggedlogicalvalue(BSONValue, value, element, limits)
    elseif kind === :interval
        return _intervalfrombytes(_fixedlogicalbytes(value, element, 12, :INTERVAL))
    end
    return nothing
end

function _binarylogicalvalue(element::Metadata.SchemaElement, value;
    limits::Limits=Limits())
    kind = _binarylogicalkind(element)
    kind === nothing && return nothing
    return _binarylogicalvalue(kind, element, value, limits)
end

function _binarylogicalvalue(node::SchemaNode, value; limits::Limits=Limits())
    return _binarylogicalvalue(node.element, value; limits=limits)
end

function _taggedphysicalvalue(value::JSONValue, limits::Limits)
    _validatejson(value.bytes, limits, ArgumentError)
    return collect(value.bytes)
end

function _taggedphysicalvalue(value::BSONValue, limits::Limits)
    _validatebson(value.bytes, limits, ArgumentError)
    return collect(value.bytes)
end

function _binaryphysicalvalue(kind::Symbol, element::Metadata.SchemaElement, value,
    limits::Limits)
    if kind === :unknown
        ismissing(value) && return missing
        throw(ArgumentError("UNKNOWN column $(repr(element.name)) contains a non-null value"))
    end
    ismissing(value) && return missing
    if kind === :enum
        value isa AbstractString ||
            throw(ArgumentError("ENUM column $(repr(element.name)) contains a non-string value"))
        return _toparquetstring(value, limits)
    elseif kind === :uuid
        value isa UUIDs.UUID ||
            throw(ArgumentError("UUID column $(repr(element.name)) contains a non-UUID value"))
        return _uuidbytes(value)
    elseif kind === :float16
        value isa Float16 ||
            throw(ArgumentError("FLOAT16 column $(repr(element.name)) contains " *
                "a non-Float16 value"))
        return _float16bytes(value)
    elseif kind === :json
        value isa JSONValue ||
            throw(ArgumentError("JSON column $(repr(element.name)) contains an untagged value"))
        return _taggedphysicalvalue(value, limits)
    elseif kind === :bson
        value isa BSONValue ||
            throw(ArgumentError("BSON column $(repr(element.name)) contains an untagged value"))
        return _taggedphysicalvalue(value, limits)
    elseif kind === :interval
        value isa Durations.Duration && (value = Interval(value))
        value isa Interval ||
            throw(ArgumentError("INTERVAL column $(repr(element.name)) contains " *
                "a non-Interval value"))
        return _intervalbytes(value)
    end
    return nothing
end

function _binaryphysicalvalue(element::Metadata.SchemaElement, value;
    limits::Limits=Limits())
    kind = _binarylogicalkind(element)
    kind === nothing && return nothing
    return _binaryphysicalvalue(kind, element, value, limits)
end

function _binaryphysicalvalue(node::SchemaNode, value; limits::Limits=Limits())
    return _binaryphysicalvalue(node.element, value; limits=limits)
end

function _unknownlogicalvalues(element::Metadata.SchemaElement,
    values::AbstractVector, limits::Limits)
    _checklimit(:container_elements, length(values), limits.max_container_elements)
    output = Vector{Missing}(undef, length(values))
    for (index, value) in enumerate(values)
        output[index] = _binarylogicalvalue(:unknown, element, value, limits)
    end
    return output
end

function _unknownphysicalvalues(element::Metadata.SchemaElement,
    values::AbstractVector, limits::Limits)
    _checklimit(:container_elements, length(values), limits.max_container_elements)
    output = Vector{Missing}(undef, length(values))
    for (index, value) in enumerate(values)
        output[index] = _binaryphysicalvalue(:unknown, element, value, limits)
    end
    return output
end

function _binarylogicalvalues(element::Metadata.SchemaElement,
    values::AbstractVector; limits::Limits=Limits())
    kind = _binarylogicalkind(element)
    kind === nothing && return nothing
    kind === :unknown && return _unknownlogicalvalues(element, values, limits)
    _checklimit(:container_elements, length(values), limits.max_container_elements)
    T = _binarylogicaleltype(kind, eltype(values))
    output = _convertedvector(T, values)
    for (index, value) in enumerate(values)
        output[index] = _binarylogicalvalue(kind, element, value, limits)
    end
    return output
end

function _binarylogicalvalues(node::SchemaNode, values::AbstractVector;
    limits::Limits=Limits())
    return _binarylogicalvalues(node.element, values; limits=limits)
end

function _binaryphysicalvalues(element::Metadata.SchemaElement,
    values::AbstractVector; limits::Limits=Limits())
    kind = _binarylogicalkind(element)
    kind === nothing && return nothing
    kind === :unknown && return _unknownphysicalvalues(element, values, limits)
    _checklimit(:container_elements, length(values), limits.max_container_elements)
    output = _convertedvector(Vector{UInt8}, values)
    for (index, value) in enumerate(values)
        output[index] = _binaryphysicalvalue(kind, element, value, limits)
    end
    return output
end

function _binaryphysicalvalues(node::SchemaNode, values::AbstractVector;
    limits::Limits=Limits())
    return _binaryphysicalvalues(node.element, values; limits=limits)
end

function Interval(value::Durations.Duration)
    iszero(rem(value.nanoseconds, 1_000_000)) || throw(ArgumentError("Parquet INTERVAL requires exact milliseconds"))
    return Interval(value.months, value.days, div(value.nanoseconds, 1_000_000))
end
function Durations.Duration(value::Interval)
    return Durations.Duration(value.months, value.days, Int64(value.milliseconds) * 1_000_000)
end
