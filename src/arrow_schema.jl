# The Arrow schema that pyarrow, Polars, and other Arrow-based writers store base64-encoded
# under the `ARROW:schema` key: an IPC Schema message, decoded here with a minimal
# FlatBuffers reader and written back with a minimal FlatBuffers builder. Only what the
# Parquet schema cannot carry is used on read: time zones of UTC-adjusted timestamps,
# durations stored as INT64, and field metadata.

import Base64

# An Arrow field: its name, nullability, type, children, and key-value metadata. `type`
# is the Arrow Type union tag (`_ARROW_TIMESTAMP`, ...); `unit` is the TimeUnit of a
# timestamp, time, or duration; `timezone` that of a timestamp.
struct _ArrowField
    name::String
    nullable::Bool
    type::UInt8
    unit::Int16
    timezone::Union{Nothing,String}
    children::Vector{_ArrowField}
    metadata::Vector{Pair{String,String}}
end

const _ARROW_TIMESTAMP = UInt8(10)
const _ARROW_DURATION = UInt8(18)
const _ARROW_SCHEMA_HEADER = UInt8(1)

# Arrow's TimeUnit enum, in order: SECOND, MILLISECOND, MICROSECOND, NANOSECOND.
const _ARROW_TIME_UNITS = (Dates.Second, Dates.Millisecond, Dates.Microsecond,
    Dates.Nanosecond)

_arrowschemaerror(message) = throw(FormatError("ARROW:schema metadata is invalid: $message"))

function _fbcheck(bytes::Vector{UInt8}, position::Int, count::Int)
    position >= 0 && count >= 0 && position + count <= length(bytes) ||
        _arrowschemaerror("an offset points outside the message")
    return
end

function _fbload(::Type{T}, bytes::Vector{UInt8}, position::Int) where {T}
    _fbcheck(bytes, position, sizeof(T))
    return GC.@preserve bytes ltoh(unsafe_load(Ptr{T}(pointer(bytes, position + 1))))
end

# A FlatBuffers table at zero-based `position`, with its vtable.
struct _FBTable
    bytes::Vector{UInt8}
    position::Int
    vtable::Int
    vsize::Int
end

function _FBTable(bytes::Vector{UInt8}, position::Int)
    vtable = position - Int(_fbload(Int32, bytes, position))
    vsize = Int(_fbload(UInt16, bytes, vtable))
    vsize >= 4 && iseven(vsize) || _arrowschemaerror("a table has a malformed vtable")
    _fbcheck(bytes, vtable, vsize)
    return _FBTable(bytes, position, vtable, vsize)
end

# Position of field `slot` (zero-based), or -1 when absent.
function _fbfield(table::_FBTable, slot::Int)
    entry = 4 + 2slot
    entry + 2 <= table.vsize || return -1
    offset = Int(_fbload(UInt16, table.bytes, table.vtable + entry))
    return offset == 0 ? -1 : table.position + offset
end

function _fbscalar(table::_FBTable, slot::Int, ::Type{T}, default::T) where {T}
    position = _fbfield(table, slot)
    return position < 0 ? default : _fbload(T, table.bytes, position)
end

# Target of the uoffset stored at `position`.
_fbtarget(bytes::Vector{UInt8}, position::Int) = position + Int(_fbload(UInt32, bytes, position))

function _fbstring(table::_FBTable, slot::Int)
    position = _fbfield(table, slot)
    position < 0 && return nothing
    start = _fbtarget(table.bytes, position)
    count = Int(_fbload(UInt32, table.bytes, start))
    _fbcheck(table.bytes, start + 4, count)
    return String(table.bytes[(start + 5):(start + 4 + count)])
end

function _fbsubtable(table::_FBTable, slot::Int)
    position = _fbfield(table, slot)
    position < 0 && return nothing
    return _FBTable(table.bytes, _fbtarget(table.bytes, position))
end

# The tables of the vector field `slot`.
function _fbtables(table::_FBTable, slot::Int, limits::Limits)
    position = _fbfield(table, slot)
    position < 0 && return _FBTable[]
    start = _fbtarget(table.bytes, position)
    count = Int(_fbload(UInt32, table.bytes, start))
    _fbcheck(table.bytes, start + 4, 4count)
    _checklimit(:container_elements, count, limits.max_container_elements)
    return _FBTable[_FBTable(table.bytes, _fbtarget(table.bytes, start + 4 + 4index))
        for index in 0:(count - 1)]
end

function _arrowkeyvalues(table::_FBTable, slot::Int, limits::Limits)
    return Pair{String,String}[something(_fbstring(entry, 0), "") =>
        something(_fbstring(entry, 1), "") for entry in _fbtables(table, slot, limits)]
end

function _arrowfield(table::_FBTable, limits::Limits, depth::Int)
    depth <= limits.max_metadata_depth ||
        throw(LimitError(:metadata_depth, Int64(depth), limits.max_metadata_depth))
    type = _fbscalar(table, 2, UInt8, 0x00)
    unit, timezone = Int16(0), nothing
    typetable = _fbsubtable(table, 3)
    if typetable !== nothing && type == _ARROW_TIMESTAMP
        unit = _fbscalar(typetable, 0, Int16, Int16(0))
        timezone = _fbstring(typetable, 1)
    elseif typetable !== nothing && type == _ARROW_DURATION
        unit = _fbscalar(typetable, 0, Int16, Int16(1))
    end
    children = _ArrowField[_arrowfield(child, limits, depth + 1)
        for child in _fbtables(table, 5, limits)]
    return _ArrowField(something(_fbstring(table, 0), ""), _fbscalar(table, 1, Bool, false),
        type, unit, timezone, children, _arrowkeyvalues(table, 6, limits))
end

# The fields of a base64 `ARROW:schema` value.
function _decodearrowschema(text::AbstractString, limits::Limits)
    bytes = try
        Base64.base64decode(text)
    catch
        _arrowschemaerror("it is not base64")
    end
    # An encapsulated IPC message: an optional 0xFFFFFFFF continuation, the metadata
    # length, then the Message flatbuffer.
    position = 0
    _fbload(UInt32, bytes, 0) == 0xffffffff && (position = 4)
    length = Int(_fbload(Int32, bytes, position))
    position += 4
    _fbcheck(bytes, position, length)
    message = _FBTable(bytes, _fbtarget(bytes, position))
    _fbscalar(message, 1, UInt8, 0x00) == _ARROW_SCHEMA_HEADER ||
        _arrowschemaerror("the message is not a schema")
    schema = _fbsubtable(message, 2)
    schema === nothing && _arrowschemaerror("the schema is missing")
    return _ArrowField[_arrowfield(field, limits, 1) for field in _fbtables(schema, 1, limits)]
end

# A FlatBuffers table to write: one entry per field slot, `nothing` when absent. Scalars
# are `Bool`, `UInt8`, `Int16`, `Int32`, or `Int64`; strings, tables, and vectors of
# tables are written out of line after the table.
struct _FBObject
    fields::Vector{Any}
end

_fbobject(fields...) = _FBObject(Any[fields...])

_fbsize(x) = x isa Union{Bool,UInt8,Int16,Int32,Int64} ? sizeof(x) : 4

function _fbpad!(bytes::Vector{UInt8}, align::Int)
    while length(bytes) % align != 0
        push!(bytes, 0x00)
    end
    return
end

_fbappend!(bytes::Vector{UInt8}, x) = append!(bytes, reinterpret(UInt8, [htol(x)]))

function _fbpatch!(bytes::Vector{UInt8}, at::Int, value::UInt32)
    for index in 0:3
        bytes[at + 1 + index] = UInt8((value >> (8index)) & 0xff)
    end
    return
end

# Writes `object` (its vtable, then the table, then what its fields point to) at the end
# of `bytes` and returns the table's position. Offsets point forward, as FlatBuffers
# requires, and each scalar sits at a multiple of its size.
function _fbwrite!(bytes::Vector{UInt8}, object::_FBObject)
    present = [slot for slot in eachindex(object.fields) if object.fields[slot] !== nothing]
    order = sort(present; by=slot -> -_fbsize(object.fields[slot]))
    offsets = zeros(Int, length(object.fields))
    size = 4
    for slot in order
        width = _fbsize(object.fields[slot])
        size = cld(size, width) * width
        offsets[slot] = size
        size += width
    end
    size = cld(size, 4) * 4
    _fbpad!(bytes, 2)
    vtable = length(bytes)
    _fbappend!(bytes, UInt16(4 + 2length(object.fields)))
    _fbappend!(bytes, UInt16(size))
    foreach(offset -> _fbappend!(bytes, UInt16(offset)), offsets)
    _fbpad!(bytes, 8)
    table = length(bytes)
    _fbappend!(bytes, Int32(table - vtable))
    append!(bytes, zeros(UInt8, size - 4))
    pending = Tuple{Int,Any}[]
    for slot in present
        value, at = object.fields[slot], table + offsets[slot]
        if value isa Union{Bool,UInt8,Int16,Int32,Int64}
            for (index, byte) in enumerate(reinterpret(UInt8, [htol(value)]))
                bytes[at + index] = byte
            end
        else
            push!(pending, (at, value))
        end
    end
    for (at, value) in pending
        _fbpatch!(bytes, at, UInt32(_fbwritevalue!(bytes, value) - at))
    end
    return table
end

function _fbwritevalue!(bytes::Vector{UInt8}, value::String)
    _fbpad!(bytes, 4)
    position = length(bytes)
    _fbappend!(bytes, UInt32(sizeof(value)))
    append!(bytes, codeunits(value))
    push!(bytes, 0x00)
    return position
end

_fbwritevalue!(bytes::Vector{UInt8}, value::_FBObject) = _fbwrite!(bytes, value)

function _fbwritevalue!(bytes::Vector{UInt8}, values::Vector{_FBObject})
    _fbpad!(bytes, 4)
    position = length(bytes)
    _fbappend!(bytes, UInt32(length(values)))
    slots = length(bytes)
    append!(bytes, zeros(UInt8, 4length(values)))
    for (index, value) in enumerate(values)
        at = slots + 4(index - 1)
        _fbpatch!(bytes, at, UInt32(_fbwrite!(bytes, value) - at))
    end
    return position
end

# The FlatBuffer whose root table is `root`.
function _fbbuffer(root::_FBObject)
    bytes = zeros(UInt8, 8)
    _fbpatch!(bytes, 0, UInt32(_fbwrite!(bytes, root)))
    _fbpad!(bytes, 8)
    return bytes
end

# The Arrow type pyarrow reads the Parquet leaf `element` as, as (Type union tag, type
# table). `hint` overrides it: a `String` is the time zone of a UTC-adjusted timestamp,
# an `Int16` the TimeUnit of a duration stored as INT64.
function _arrowleaftype(element::Metadata.SchemaElement, hint)
    kind = _logicalkind(element)
    if kind isa _TimestampLogicalKind
        zone = hint isa String && kind.is_adjusted_to_utc ? hint :
            kind.is_adjusted_to_utc ? "UTC" : nothing
        return _ARROW_TIMESTAMP, _fbobject(Int16(kind.unit), zone)
    elseif kind isa _TimeLogicalKind
        return 0x09, _fbobject(Int16(kind.unit), Int32(kind.unit == _TEMPORAL_MILLIS ? 32 : 64))
    elseif kind isa _IntegerLogicalKind
        return 0x02, _fbobject(Int32(kind.bitwidth), kind.signed)
    elseif kind === :int96
        return _ARROW_TIMESTAMP, _fbobject(Int16(3), nothing)
    elseif kind in (:string, :enum, :json)
        return 0x05, _fbobject()
    elseif kind === :bson
        return 0x04, _fbobject()
    elseif kind === :date
        return 0x08, _fbobject(Int16(0))
    elseif kind === :decimal
        precision, scale = something(_decimalparameters(element))
        return 0x07, _fbobject(Int32(precision), Int32(scale), Int32(precision <= 38 ? 128 : 256))
    elseif kind === :float16
        return 0x03, _fbobject(Int16(0))
    elseif kind === :uuid
        return 0x0f, _fbobject(Int32(16))
    elseif kind === :interval
        return 0x0f, _fbobject(Int32(12))
    elseif kind === :unknown
        return 0x01, _fbobject()
    end
    physical = element.type_
    physical == Metadata.Type.BOOLEAN && return 0x06, _fbobject()
    physical == Metadata.Type.INT32 && return 0x02, _fbobject(Int32(32), true)
    physical == Metadata.Type.INT64 && hint isa Int16 && return _ARROW_DURATION, _fbobject(hint)
    physical == Metadata.Type.INT64 && return 0x02, _fbobject(Int32(64), true)
    physical == Metadata.Type.FLOAT && return 0x03, _fbobject(Int16(1))
    physical == Metadata.Type.DOUBLE && return 0x03, _fbobject(Int16(2))
    physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
        return 0x0f, _fbobject(Int32(something(element.type_length, Int32(0))))
    return 0x04, _fbobject()
end

function _arrowkeyvalueobjects(pairs)
    isempty(pairs) && return nothing
    return _FBObject[_fbobject(String(first(pair)), String(last(pair))) for pair in pairs]
end

_arrowfieldobject(name, nullable, (tag, type), children) =
    _fbobject(name, nullable, tag, type, nothing, children, nothing)

# The Arrow field for `node`. LIST and MAP groups take the standard three-level layout
# (`list` / `element`, `key_value` / `key`, `value`); other groups become structs.
function _arrowfieldobject(node::SchemaNode, hints)
    element = node.element
    nullable = element.repetition_type != Metadata.FieldRepetitionType.REQUIRED
    element.type_ === nothing ||
        return _arrowfieldobject(element.name, nullable,
            _arrowleaftype(element, get(hints, node.path, nothing)), _FBObject[])
    logical, converted = element.logicalType, element.converted_type
    repeated = length(node.children) == 1 ? only(node.children) : nothing
    if repeated !== nothing && (logical !== nothing && logical.LIST !== nothing ||
            converted == Metadata.ConvertedType.LIST)
        item = length(repeated.children) == 1 ? only(repeated.children) : repeated
        return _arrowfieldobject(element.name, nullable, (0x0c, _fbobject()),
            _FBObject[_arrowfieldobject(item, hints)])
    elseif repeated !== nothing && (logical !== nothing && logical.MAP !== nothing ||
            converted in (Metadata.ConvertedType.MAP, Metadata.ConvertedType.MAP_KEY_VALUE))
        # A map without values is a list of its keys to pyarrow; an Arrow map needs both.
        length(repeated.children) == 1 && return _arrowfieldobject(element.name,
            nullable, (0x0c, _fbobject()),
            _FBObject[_arrowfieldobject(only(repeated.children), hints)])
        entries = _arrowfieldobject(repeated.element.name, false, (0x0d, _fbobject()),
            _FBObject[_arrowfieldobject(child, hints) for child in repeated.children])
        return _arrowfieldobject(element.name, nullable, (0x11, _fbobject(false)),
            _FBObject[entries])
    end
    return _arrowfieldobject(element.name, nullable, (0x0d, _fbobject()),
        _FBObject[_arrowfieldobject(child, hints) for child in node.children])
end

"""
    _arrowschema(schema::Schema; hints=Dict(), metadata=(), fieldmetadata=Dict()) -> String

The base64 `ARROW:schema` value for a file with Parquet `schema`, as pyarrow would
infer each column from its Parquet type, so pyarrow, Polars, and DuckDB read the file
the same with or without it. `hints` maps a leaf's path to what the Parquet type cannot
say: the time zone (`String`) of a UTC-adjusted timestamp, or the Arrow TimeUnit
(`Int16`: 0 seconds to 3 nanoseconds) of a duration stored as INT64. `metadata` is the
schema-level Arrow key-value metadata and `fieldmetadata` maps a top-level column name to
its field's, as `key => value` pairs; pyarrow takes a file's schema and field metadata
from them.
"""
function _arrowschema(schema::Schema; hints=Dict{Vector{String},Any}(), metadata=(),
        fieldmetadata=Dict{String,Any}())
    fields = _FBObject[_arrowfieldobject(child, hints) for child in schema.root.children]
    for (field, child) in zip(fields, schema.root.children)
        field.fields[7] = _arrowkeyvalueobjects(get(fieldmetadata, child.element.name, ()))
    end
    message = _fbobject(Int16(4), _ARROW_SCHEMA_HEADER,
        _fbobject(Int16(0), fields, _arrowkeyvalueobjects(metadata), nothing), nothing,
        nothing)
    flat = _fbbuffer(message)
    framed = UInt8[]
    _fbappend!(framed, 0xffffffff)
    _fbappend!(framed, Int32(length(flat)))
    append!(framed, flat)
    return Base64.base64encode(framed)
end

# What the file's ARROW:schema says about leaves the Parquet schema cannot describe, by
# leaf path: the time zone (`String`) of a UTC-adjusted TIMESTAMP, and the TimeUnit
# (`Int16`) of a duration stored as an unannotated INT64. Fields are matched to the
# Parquet schema by position and name; a missing, malformed, or mismatched ARROW:schema
# leaves the Parquet types as they are.
function _arrowhints(metadata::Metadata.FileMetaData, schema::Schema, limits::Limits)
    hints = Dict{Vector{String},Any}()
    entries = something(metadata.key_value_metadata, Metadata.KeyValue[])
    index = findlast(entry -> entry.key == "ARROW:schema", entries)
    index === nothing && return hints
    text = entries[index].value
    text === nothing && return hints
    fields = try
        _decodearrowschema(text, limits)
    catch err
        err isa Union{FormatError,LimitError} || rethrow()
        return hints
    end
    _arrowhints!(hints, schema.root.children, fields)
    return hints
end

function _arrowhints!(hints, nodes::Vector{SchemaNode}, fields::Vector{_ArrowField})
    length(nodes) == length(fields) || return
    for (node, field) in zip(nodes, fields)
        node.element.name == field.name && _arrowhints!(hints, node, field)
    end
    return
end

function _arrowhints!(hints, node::SchemaNode, field::_ArrowField)
    element = node.element
    if element.type_ !== nothing
        # An annotation this package cannot read gets no hint; reading the column
        # reports it, so the rest of the file still opens.
        kind = try
            _logicalkind(element)
        catch err
            err isa Union{FormatError,UnsupportedFeatureError} || rethrow()
            return
        end
        # A zone with a NUL byte cannot name a zone (or a Julia Symbol); the column then
        # reads as UTC.
        if field.type == _ARROW_TIMESTAMP && kind isa _TimestampLogicalKind &&
                kind.is_adjusted_to_utc && field.timezone !== nothing &&
                !isempty(field.timezone) && field.timezone != "UTC" &&
                !occursin('\0', field.timezone)
            hints[node.path] = field.timezone
        elseif field.type == _ARROW_DURATION && kind === nothing &&
                element.type_ == Metadata.Type.INT64 && 0 <= field.unit <= 3
            hints[node.path] = field.unit
        end
        return
    end
    logical, converted = element.logicalType, element.converted_type
    islist = logical !== nothing && logical.LIST !== nothing ||
        converted == Metadata.ConvertedType.LIST
    ismap = logical !== nothing && logical.MAP !== nothing ||
        converted in (Metadata.ConvertedType.MAP, Metadata.ConvertedType.MAP_KEY_VALUE)
    if (islist || ismap) && length(node.children) == 1 && length(field.children) == 1
        repeated = only(node.children)
        if islist
            # Three-level lists hold the element under the repeated group; two-level
            # lists repeat the element itself.
            item = length(repeated.children) == 1 && repeated.element.type_ === nothing ?
                only(repeated.children) : repeated
            _arrowhints!(hints, item, only(field.children))
        else
            _arrowhints!(hints, repeated.children, only(field.children).children)
        end
    elseif !islist && !ismap
        _arrowhints!(hints, node.children, field.children)
    end
    return
end
