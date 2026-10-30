function _isdictionaryencoding(encoding::Metadata.Encoding.T)
    return encoding == Metadata.Encoding.PLAIN_DICTIONARY ||
        encoding == Metadata.Encoding.RLE_DICTIONARY
end

function _checkdictionarypageencoding(encoding::Metadata.Encoding.T)
    encoding == Metadata.Encoding.PLAIN && return
    encoding == Metadata.Encoding.PLAIN_DICTIONARY && return
    throw(FormatError("dictionary page encoding $encoding is not PLAIN"))
end

# Decodes a dictionary page's PLAIN values, converted with `convert`, into `dictionary`.
function _dictionarypage!(dictionary::Vector{L}, ::Type{P}, convert, frame,
        md::Metadata.ColumnMetaData, node::SchemaNode, buffers, limits::Limits) where {L,P}
    header = frame.header.dictionary_page_header
    count = Int(header.num_values)
    count >= 0 || throw(FormatError("negative dictionary entry count"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    _checkdictionarypageencoding(header.encoding)
    bytes = _pagebytes!(convert, buffers, md.codec, _byteview(frame.payload),
        frame.header.uncompressed_page_size, limits)
    empty!(dictionary)
    position = if P === Vector{UInt8}
        _plainbytes!(dictionary, 0, count, convert, _fixedwidth(node), bytes, 1, limits)
    else
        _plain!(dictionary, 0, count, P, convert, bytes, 1)
    end
    position == length(bytes) + 1 ||
        throw(FormatError("dictionary page has $(length(bytes) - position + 1) trailing bytes"))
    return dictionary
end

function _dictionarycopy(value::Vector{UInt8})
    return copy(value)
end

function _dictionarycopy(value)
    return value
end

# Decodes `count` dictionary indices from `bytes[position:]` (a bit-width byte, then
# RLE/bit-packed hybrid runs ending by `stop`) into `indices`.
function _dictionaryindices!(indices::Vector{UInt32}, count::Int, bytes::_ByteView,
        position::Int, stop::Int)
    count == 0 && position == stop && return position
    _requirebytes(bytes, position, 1)
    bitwidth = Int(bytes[position])
    bitwidth <= 32 || throw(FormatError("dictionary index bit width $bitwidth exceeds 32"))
    _growto!(indices, _claimcapacity(count, stop - position))
    return _decodehybrid!(indices, 0, count, bitwidth, bytes, position + 1, stop)
end

@noinline function _dictionaryindexerror(raw::UInt32, entries::UInt32)
    throw(FormatError("dictionary index $raw is outside a dictionary of $entries entries"))
end

# Writes the `count` dictionary entries selected by the indices in `bytes[position:]`
# (a bit-width byte, then RLE/bit-packed hybrid runs ending by `stop`) to
# `dest[(offset + 1):(offset + count)]`, using `indices` as scratch.
function _dictionary!(dest::Vector{L}, offset::Int, count::Int, dictionary::Vector{L},
        indices::Vector{UInt32}, bytes::_ByteView, position::Int, stop::Int) where {L}
    position = _dictionaryindices!(indices, count, bytes, position, stop)
    _growto!(dest, offset + count)
    entries = UInt32(length(dictionary))
    @inbounds for index in 1:count
        raw = indices[index]
        raw < entries || _dictionaryindexerror(raw, entries)
        dest[offset + index] = _dictionarycopy(dictionary[raw + 1])
    end
    return position
end
