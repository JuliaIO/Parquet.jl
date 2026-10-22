mutable struct _BSONValidator{B<:AbstractVector{UInt8}}
    bytes::B
    elements::Int64
    limits::Limits
end

function _bsonfail(::Type{E}, message::String) where {E<:Exception}
    throw(E("invalid BSON: $message"))
end

function _bsonrequire(position::Int, count::Int, stop::Int,
    ::Type{E}) where {E<:Exception}
    count <= stop - position + 1 && return
    _bsonfail(E, "input ends inside a value")
end

function _bsonint32(bytes::AbstractVector{UInt8}, position::Int)
    raw = UInt32(bytes[position]) |
        (UInt32(bytes[position + 1]) << 8) |
        (UInt32(bytes[position + 2]) << 16) |
        (UInt32(bytes[position + 3]) << 24)
    return reinterpret(Int32, raw)
end

function _bsoncstring(validator::_BSONValidator, position::Int, stop::Int,
    ::Type{E}) where {E<:Exception}
    start = position
    while position <= stop && !iszero(validator.bytes[position])
        position += 1
    end
    position <= stop || _bsonfail(E, "unterminated cstring")
    value = @view validator.bytes[start:(position - 1)]
    isvalid(String, value) || _bsonfail(E, "cstring is not valid UTF-8")
    return position + 1, value
end

function _bsonstring(validator::_BSONValidator, position::Int, stop::Int,
    ::Type{E}) where {E<:Exception}
    _bsonrequire(position, 4, stop, E)
    length = Int(_bsonint32(validator.bytes, position))
    length > 0 || _bsonfail(E, "string has a nonpositive length")
    position += 4
    _bsonrequire(position, length, stop, E)
    last = position + length - 1
    iszero(validator.bytes[last]) || _bsonfail(E, "string has no trailing null byte")
    value = @view validator.bytes[position:(last - 1)]
    isvalid(String, value) || _bsonfail(E, "string is not valid UTF-8")
    return last + 1
end

function _bsonitem!(validator::_BSONValidator)
    requested = validator.elements + 1
    _checklimit(:container_elements, requested, validator.limits.max_container_elements)
    validator.elements = requested
    return
end

function _bsonarraykey(key::AbstractVector{UInt8}, index::Int)
    isempty(key) && return false
    length(key) > 1 && first(key) == 0x30 && return false
    value = 0
    for byte in key
        0x30 <= byte <= 0x39 || return false
        digit = Int(byte - 0x30)
        value <= div(typemax(Int) - digit, 10) || return false
        value = 10 * value + digit
    end
    return value == index
end

function _bsonregexoptions(options::AbstractVector{UInt8})
    previous = UInt8(0)
    for option in options
        option in (0x69, 0x6c, 0x6d, 0x73, 0x75, 0x78) || return false
        option > previous || return false
        previous = option
    end
    return true
end

function _bsonbinary(validator::_BSONValidator, position::Int, stop::Int,
    ::Type{E}) where {E<:Exception}
    _bsonrequire(position, 5, stop, E)
    length = Int(_bsonint32(validator.bytes, position))
    length >= 0 || _bsonfail(E, "binary value has a negative length")
    subtype = validator.bytes[position + 4]
    (subtype <= 0x09 || subtype >= 0x80) ||
        _bsonfail(E, "binary subtype is reserved")
    position += 5
    _bsonrequire(position, length, stop, E)
    if subtype == 0x02
        length >= 4 || _bsonfail(E, "old binary subtype omits its inner length")
        inner = Int(_bsonint32(validator.bytes, position))
        inner == length - 4 ||
            _bsonfail(E, "old binary subtype lengths do not agree")
    end
    return position + length
end

function _bsonregex(validator::_BSONValidator, position::Int, stop::Int,
    ::Type{E}) where {E<:Exception}
    position, _ = _bsoncstring(validator, position, stop, E)
    position, options = _bsoncstring(validator, position, stop, E)
    _bsonregexoptions(options) ||
        _bsonfail(E, "regular-expression options are invalid or unsorted")
    return position
end

function _bsoncodewithscope(validator::_BSONValidator, position::Int, stop::Int,
    depth::Int, ::Type{E}) where {E<:Exception}
    start = position
    _bsonrequire(position, 4, stop, E)
    length = Int(_bsonint32(validator.bytes, position))
    length >= 14 || _bsonfail(E, "code-with-scope value is too short")
    length <= stop - start + 1 || _bsonfail(E, "code-with-scope value exceeds its document")
    last = start + length - 1
    position = _bsonstring(validator, position + 4, last, E)
    position = _bsondocument(validator, position, last, depth + 1, false, E)
    position == last + 1 || _bsonfail(E, "code-with-scope length is inconsistent")
    return position
end

function _bsonfixed(position::Int, count::Int, stop::Int,
    ::Type{E}) where {E<:Exception}
    _bsonrequire(position, count, stop, E)
    return position + count
end

function _bsonelementvalue(validator::_BSONValidator, type::UInt8, position::Int,
    stop::Int, depth::Int, ::Type{E}) where {E<:Exception}
    type == 0x01 && return _bsonfixed(position, 8, stop, E)
    type == 0x02 && return _bsonstring(validator, position, stop, E)
    type == 0x03 && return _bsondocument(validator, position, stop, depth + 1, false, E)
    type == 0x04 && return _bsondocument(validator, position, stop, depth + 1, true, E)
    type == 0x05 && return _bsonbinary(validator, position, stop, E)
    type == 0x06 && return position
    type == 0x07 && return _bsonfixed(position, 12, stop, E)
    if type == 0x08
        _bsonrequire(position, 1, stop, E)
        validator.bytes[position] in (0x00, 0x01) ||
            _bsonfail(E, "Boolean value is not zero or one")
        return position + 1
    end
    type == 0x09 && return _bsonfixed(position, 8, stop, E)
    type == 0x0a && return position
    type == 0x0b && return _bsonregex(validator, position, stop, E)
    if type == 0x0c
        position = _bsonstring(validator, position, stop, E)
        return _bsonfixed(position, 12, stop, E)
    end
    type == 0x0d && return _bsonstring(validator, position, stop, E)
    type == 0x0e && return _bsonstring(validator, position, stop, E)
    type == 0x0f && return _bsoncodewithscope(validator, position, stop, depth, E)
    type == 0x10 && return _bsonfixed(position, 4, stop, E)
    type == 0x11 && return _bsonfixed(position, 8, stop, E)
    type == 0x12 && return _bsonfixed(position, 8, stop, E)
    type == 0x13 && return _bsonfixed(position, 16, stop, E)
    type in (0x7f, 0xff) && return position
    _bsonfail(E, "unknown element type 0x$(string(type, base=16, pad=2))")
end

function _bsondocument(validator::_BSONValidator, start::Int, bound::Int,
    depth::Int, array::Bool, ::Type{E}) where {E<:Exception}
    _checklimit(:metadata_depth, depth, validator.limits.max_metadata_depth)
    _bsonrequire(start, 4, bound, E)
    length = Int(_bsonint32(validator.bytes, start))
    length >= 5 || _bsonfail(E, "document length is less than five bytes")
    length <= bound - start + 1 || _bsonfail(E, "document length exceeds its parent")
    stop = start + length - 1
    position = start + 4
    index = 0
    while position < stop
        type = validator.bytes[position]
        iszero(type) && _bsonfail(E, "document terminates before its declared length")
        position += 1
        position, key = _bsoncstring(validator, position, stop - 1, E)
        array && !_bsonarraykey(key, index) &&
            _bsonfail(E, "array keys are not consecutive decimal indexes")
        _bsonitem!(validator)
        position = _bsonelementvalue(validator, type, position, stop - 1, depth, E)
        position <= stop || _bsonfail(E, "element exceeds its document")
        index += 1
    end
    position == stop || _bsonfail(E, "document length ends inside an element")
    iszero(validator.bytes[stop]) || _bsonfail(E, "document has no trailing null byte")
    return stop + 1
end

function _validatebson(bytes::AbstractVector{UInt8}, limits::Limits,
    ::Type{E}) where {E<:Exception}
    _checklimit(:string_bytes, length(bytes), limits.max_string_bytes)
    validator = _BSONValidator(bytes, Int64(0), limits)
    position = _bsondocument(validator, 1, length(bytes), 1, false, E)
    position == length(bytes) + 1 || _bsonfail(E, "trailing bytes after the root document")
    return
end
