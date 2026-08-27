mutable struct _JSONValidator{B<:AbstractVector{UInt8}}
    bytes::B
    position::Int
    elements::Int64
    limits::Limits
end

function _jsonfail(::Type{E}, message::String) where {E<:Exception}
    throw(E("invalid JSON: $message"))
end

function _jsonwhitespace(byte::UInt8)
    return byte == 0x20 || byte == 0x09 || byte == 0x0a || byte == 0x0d
end

function _jsonskipwhitespace!(validator::_JSONValidator)
    while validator.position <= length(validator.bytes) &&
            _jsonwhitespace(validator.bytes[validator.position])
        validator.position += 1
    end
    return
end

function _jsonrequire(validator::_JSONValidator, count::Int, ::Type{E}) where {E<:Exception}
    count <= length(validator.bytes) - validator.position + 1 && return
    _jsonfail(E, "input ends inside a token")
end

function _jsonhexvalue(byte::UInt8)
    0x30 <= byte <= 0x39 && return Int(byte - 0x30)
    0x41 <= byte <= 0x46 && return Int(byte - 0x41 + 10)
    0x61 <= byte <= 0x66 && return Int(byte - 0x61 + 10)
    return -1
end

function _jsonunicodeescape!(validator::_JSONValidator, ::Type{E}) where {E<:Exception}
    _jsonrequire(validator, 4, E)
    for _ in 1:4
        digit = _jsonhexvalue(validator.bytes[validator.position])
        digit >= 0 || _jsonfail(E, "invalid hexadecimal Unicode escape")
        validator.position += 1
    end
    return
end

function _jsonstring!(validator::_JSONValidator, ::Type{E}) where {E<:Exception}
    validator.bytes[validator.position] == 0x22 || _jsonfail(E, "expected a string")
    validator.position += 1
    while validator.position <= length(validator.bytes)
        byte = validator.bytes[validator.position]
        validator.position += 1
        byte == 0x22 && return
        byte < 0x20 && _jsonfail(E, "unescaped control byte in a string")
        byte == 0x5c || continue
        validator.position <= length(validator.bytes) ||
            _jsonfail(E, "input ends after a string escape")
        escaped = validator.bytes[validator.position]
        validator.position += 1
        escaped in (0x22, 0x5c, 0x2f, 0x62, 0x66, 0x6e, 0x72, 0x74) && continue
        escaped == 0x75 || _jsonfail(E, "invalid string escape")
        _jsonunicodeescape!(validator, E)
    end
    _jsonfail(E, "unterminated string")
end

function _jsonliteral!(validator::_JSONValidator, literal::String,
    ::Type{E}) where {E<:Exception}
    bytes = codeunits(literal)
    _jsonrequire(validator, length(bytes), E)
    for byte in bytes
        validator.bytes[validator.position] == byte ||
            _jsonfail(E, "invalid literal")
        validator.position += 1
    end
    return
end

function _jsondigits!(validator::_JSONValidator)
    start = validator.position
    while validator.position <= length(validator.bytes) &&
            0x30 <= validator.bytes[validator.position] <= 0x39
        validator.position += 1
    end
    return validator.position - start
end

function _jsonnumber!(validator::_JSONValidator, ::Type{E}) where {E<:Exception}
    bytes = validator.bytes
    bytes[validator.position] == 0x2d && (validator.position += 1)
    validator.position <= length(bytes) || _jsonfail(E, "incomplete number")
    if bytes[validator.position] == 0x30
        validator.position += 1
        validator.position <= length(bytes) &&
            0x30 <= bytes[validator.position] <= 0x39 &&
            _jsonfail(E, "leading zero in a number")
    elseif 0x31 <= bytes[validator.position] <= 0x39
        _jsondigits!(validator)
    else
        _jsonfail(E, "invalid number")
    end
    if validator.position <= length(bytes) && bytes[validator.position] == 0x2e
        validator.position += 1
        _jsondigits!(validator) > 0 || _jsonfail(E, "fraction has no digits")
    end
    if validator.position <= length(bytes) &&
            bytes[validator.position] in (0x65, 0x45)
        validator.position += 1
        validator.position <= length(bytes) &&
            bytes[validator.position] in (0x2b, 0x2d) && (validator.position += 1)
        _jsondigits!(validator) > 0 || _jsonfail(E, "exponent has no digits")
    end
    return
end

function _jsonitem!(validator::_JSONValidator)
    requested = validator.elements + 1
    _checklimit(:container_elements, requested, validator.limits.max_container_elements)
    validator.elements = requested
    return
end

function _jsonarray!(validator::_JSONValidator, depth::Int,
    ::Type{E}) where {E<:Exception}
    _checklimit(:metadata_depth, depth, validator.limits.max_metadata_depth)
    validator.position += 1
    _jsonskipwhitespace!(validator)
    validator.position <= length(validator.bytes) || _jsonfail(E, "unterminated array")
    if validator.bytes[validator.position] == 0x5d
        validator.position += 1
        return
    end
    while true
        _jsonitem!(validator)
        _jsonvalue!(validator, depth, E)
        _jsonskipwhitespace!(validator)
        validator.position <= length(validator.bytes) || _jsonfail(E, "unterminated array")
        byte = validator.bytes[validator.position]
        validator.position += 1
        byte == 0x5d && return
        byte == 0x2c || _jsonfail(E, "expected a comma or closing bracket")
        _jsonskipwhitespace!(validator)
        validator.position <= length(validator.bytes) || _jsonfail(E, "unterminated array")
    end
end

function _jsonobject!(validator::_JSONValidator, depth::Int,
    ::Type{E}) where {E<:Exception}
    _checklimit(:metadata_depth, depth, validator.limits.max_metadata_depth)
    validator.position += 1
    _jsonskipwhitespace!(validator)
    validator.position <= length(validator.bytes) || _jsonfail(E, "unterminated object")
    if validator.bytes[validator.position] == 0x7d
        validator.position += 1
        return
    end
    while true
        validator.bytes[validator.position] == 0x22 ||
            _jsonfail(E, "object key is not a string")
        _jsonstring!(validator, E)
        _jsonskipwhitespace!(validator)
        validator.position <= length(validator.bytes) &&
            validator.bytes[validator.position] == 0x3a ||
            _jsonfail(E, "object key is not followed by a colon")
        validator.position += 1
        _jsonskipwhitespace!(validator)
        _jsonitem!(validator)
        _jsonvalue!(validator, depth, E)
        _jsonskipwhitespace!(validator)
        validator.position <= length(validator.bytes) || _jsonfail(E, "unterminated object")
        byte = validator.bytes[validator.position]
        validator.position += 1
        byte == 0x7d && return
        byte == 0x2c || _jsonfail(E, "expected a comma or closing brace")
        _jsonskipwhitespace!(validator)
        validator.position <= length(validator.bytes) || _jsonfail(E, "unterminated object")
    end
end

function _jsonvalue!(validator::_JSONValidator, depth::Int,
    ::Type{E}) where {E<:Exception}
    validator.position <= length(validator.bytes) || _jsonfail(E, "missing value")
    byte = validator.bytes[validator.position]
    byte == 0x7b && return _jsonobject!(validator, depth + 1, E)
    byte == 0x5b && return _jsonarray!(validator, depth + 1, E)
    byte == 0x22 && return _jsonstring!(validator, E)
    byte == 0x74 && return _jsonliteral!(validator, "true", E)
    byte == 0x66 && return _jsonliteral!(validator, "false", E)
    byte == 0x6e && return _jsonliteral!(validator, "null", E)
    (byte == 0x2d || 0x30 <= byte <= 0x39) && return _jsonnumber!(validator, E)
    _jsonfail(E, "unexpected byte at the start of a value")
end

function _validatejson(bytes::AbstractVector{UInt8}, limits::Limits,
    ::Type{E}) where {E<:Exception}
    _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
    isvalid(String, bytes) || _jsonfail(E, "input is not valid UTF-8")
    validator = _JSONValidator(bytes, 1, Int64(0), limits)
    _jsonskipwhitespace!(validator)
    validator.position <= length(bytes) || _jsonfail(E, "document is empty")
    _jsonvalue!(validator, 0, E)
    _jsonskipwhitespace!(validator)
    validator.position == length(bytes) + 1 ||
        _jsonfail(E, "trailing bytes after the root value")
    return
end
