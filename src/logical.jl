import Dates

const _PARQUET_DATE_EPOCH = Dates.value(Dates.Date(1970, 1, 1))

function _requirelogicalphysical(element::Metadata.SchemaElement,
    expected::Metadata.Type.T, annotation::Symbol)
    element.type_ == expected && return
    throw(FormatError("$annotation annotation on $(repr(element.name)) requires " *
        "physical type $expected, got $(element.type_)"))
end

function _logicalkind(element::Metadata.SchemaElement)
    logical = element.logicalType
    if logical !== nothing
        if logical.STRING !== nothing
            _requirelogicalphysical(element, Metadata.Type.BYTE_ARRAY, :STRING)
            return :string
        elseif logical.DATE !== nothing
            _requirelogicalphysical(element, Metadata.Type.INT32, :DATE)
            return :date
        end
        kind = _temporallogicalkind(element)
        kind === nothing || return kind
        kind = _binarylogicalkind(element)
        kind === nothing || return kind
        kind = _decimallogicalkind(element)
        kind === nothing || return kind
        return nothing
    end
    converted = element.converted_type
    if converted == Metadata.ConvertedType.UTF8
        _requirelogicalphysical(element, Metadata.Type.BYTE_ARRAY, :UTF8)
        return :string
    elseif converted == Metadata.ConvertedType.DATE
        _requirelogicalphysical(element, Metadata.Type.INT32, :DATE)
        return :date
    end
    kind = _temporallogicalkind(element)
    kind === nothing || return kind
    kind = _binarylogicalkind(element)
    kind === nothing || return kind
    kind = _decimallogicalkind(element)
    kind === nothing || return kind
    return nothing
end

function _logicalkind(node::SchemaNode)
    return _logicalkind(node.element)
end

function _logicaleltype(element::Metadata.SchemaElement, physical::Type)
    kind = _logicalkind(element)
    kind === nothing && return physical
    kind === :string && return String
    kind === :date && return Dates.Date
    kind isa Union{_TimeLogicalKind,_TimestampLogicalKind,_IntegerLogicalKind} &&
        return _temporaljuliatype(kind)
    logical = _binarylogicaleltype(kind, physical)
    logical === nothing || return logical
    logical = _decimallogicaleltype(kind)
    logical === nothing || return logical
    return physical
end

function _logicaleltype(node::SchemaNode, physical::Type)
    return _logicaleltype(node.element, physical)
end

function _fromparquetdate(value::Int32)
    ordinal = try
        Base.checked_add(_PARQUET_DATE_EPOCH, Int64(value))
    catch err
        err isa OverflowError || rethrow()
        throw(FormatError("DATE value $value overflows the Julia date range"))
    end
    return Dates.Date(Dates.UTD(ordinal))
end

function _toparquetdate(value::Dates.Date)
    days = try
        Base.checked_sub(Dates.value(value), _PARQUET_DATE_EPOCH)
    catch err
        err isa OverflowError || rethrow()
        throw(ArgumentError("DATE value $(repr(value)) is outside the Parquet INT32 day range"))
    end
    typemin(Int32) <= days <= typemax(Int32) ||
        throw(ArgumentError("DATE value $(repr(value)) is outside the Parquet INT32 day range"))
    return Int32(days)
end

function _fromparquetstring(value::AbstractVector{UInt8}, limits::Limits)
    _checklimit(:string_bytes, length(value), limits.max_string_bytes)
    isvalid(String, value) || throw(FormatError("STRING value contains invalid UTF-8"))
    return String(copy(value))
end

function _toparquetstring(value::AbstractString, limits::Limits)
    bytes = codeunits(value)
    _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
    isvalid(String, bytes) || throw(ArgumentError("STRING value contains invalid UTF-8"))
    return collect(bytes)
end

function _logicalvalue(kind, element::Metadata.SchemaElement, value, limits::Limits)
    ismissing(value) && return missing
    if kind isa Union{_TimeLogicalKind,_TimestampLogicalKind,_IntegerLogicalKind}
        return _temporallogicalvalue(kind, element, value)
    elseif kind === :string
        value isa AbstractVector{UInt8} ||
            throw(FormatError("STRING column $(repr(element.name)) contains a " *
                "non-byte-array value"))
        return _fromparquetstring(value, limits)
    elseif kind === :date
        value isa Int32 ||
            throw(FormatError("DATE column $(repr(element.name)) contains a non-INT32 value"))
        return _fromparquetdate(value)
    elseif kind === :decimal
        return _decimallogicalvalue(kind, element, value, limits)
    end
    converted = _binarylogicalvalue(kind, element, value, limits)
    converted === nothing && throw(FormatError(
        "unsupported logical annotation $kind on $(repr(element.name))"))
    return converted
end

function _logicalvalue(element::Metadata.SchemaElement, value; limits::Limits=Limits())
    kind = _logicalkind(element)
    kind === nothing && return value
    return _logicalvalue(kind, element, value, limits)
end

function _logicalvalue(node::SchemaNode, value; limits::Limits=Limits())
    return _logicalvalue(node.element, value; limits=limits)
end

function _physicalvalue(kind, element::Metadata.SchemaElement, value, limits::Limits)
    ismissing(value) && return missing
    if kind isa Union{_TimeLogicalKind,_TimestampLogicalKind,_IntegerLogicalKind}
        return _temporalphysicalvalue(kind, element, value)
    elseif kind === :string
        value isa AbstractString ||
            throw(ArgumentError("STRING column $(repr(element.name)) contains a non-string value"))
        return _toparquetstring(value, limits)
    elseif kind === :date
        value isa Dates.Date ||
            throw(ArgumentError("DATE column $(repr(element.name)) contains a non-Date value"))
        return _toparquetdate(value)
    elseif kind === :decimal
        return _decimalphysicalvalue(kind, element, value, limits)
    end
    converted = _binaryphysicalvalue(kind, element, value, limits)
    converted === nothing && throw(ArgumentError(
        "unsupported logical annotation $kind on $(repr(element.name))"))
    return converted
end

function _physicalvalue(element::Metadata.SchemaElement, value; limits::Limits=Limits())
    kind = _logicalkind(element)
    kind === nothing && return value
    return _physicalvalue(kind, element, value, limits)
end

function _physicalvalue(node::SchemaNode, value; limits::Limits=Limits())
    return _physicalvalue(node.element, value; limits=limits)
end

function _convertedvector(::Type{T}, values::AbstractVector) where {T}
    U = Missing <: eltype(values) ? Union{Missing,T} : T
    return Vector{U}(undef, length(values))
end

function _logicalvalues(element::Metadata.SchemaElement, values::AbstractVector;
    limits::Limits=Limits())
    kind = _logicalkind(element)
    kind === nothing && return values
    _checklimit(:container_elements, length(values), limits.max_container_elements)
    kind === :decimal && _preflightdecimalconversion(element, limits)
    physical = _physicaleltype(element.type_)
    T = _logicaleltype(element, physical)
    output = _convertedvector(T, values)
    for (index, value) in enumerate(values)
        output[index] = _logicalvalue(kind, element, value, limits)
    end
    return output
end

function _logicalvalues(node::SchemaNode, values::AbstractVector; limits::Limits=Limits())
    return _logicalvalues(node.element, values; limits=limits)
end

function _physicalvalues(element::Metadata.SchemaElement, values::AbstractVector;
    limits::Limits=Limits())
    kind = _logicalkind(element)
    kind === nothing && return values
    _checklimit(:container_elements, length(values), limits.max_container_elements)
    kind === :decimal && _preflightdecimalconversion(element, limits)
    T = _physicaleltype(element.type_)
    output = _convertedvector(T, values)
    for (index, value) in enumerate(values)
        output[index] = _physicalvalue(kind, element, value, limits)
    end
    return output
end

function _physicalvalues(node::SchemaNode, values::AbstractVector; limits::Limits=Limits())
    return _physicalvalues(node.element, values; limits=limits)
end
