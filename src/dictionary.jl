struct DecodedDictionary{T}
    values::Vector{T}
end

struct DictionaryPlan{V}
    values::V
    indices::Vector{UInt64}
    dictionary_payload::Vector{UInt8}
    index_payload::Vector{UInt8}
end

function _isdictionaryencoding(encoding::Metadata.Encoding.T)
    return encoding == Metadata.Encoding.PLAIN_DICTIONARY ||
        encoding == Metadata.Encoding.RLE_DICTIONARY
end

function _checkdictionarypageencoding(encoding::Metadata.Encoding.T)
    encoding == Metadata.Encoding.PLAIN && return
    encoding == Metadata.Encoding.PLAIN_DICTIONARY && return
    throw(FormatError("dictionary page encoding $encoding is not PLAIN"))
end

function _decodedictionarypage(::Type{T}, frame, md::Metadata.ColumnMetaData,
        node::SchemaNode, limits::Limits, budget::_LiveByteBudget) where {T}
    header = frame.header.dictionary_page_header
    count = Int(header.num_values)
    count >= 0 || throw(FormatError("negative dictionary entry count"))
    _checklimit(:container_elements, count, limits.max_container_elements)
    _checkdictionarypageencoding(header.encoding)
    bytes = decompresspage(frame, md.codec; limits=limits, budget=budget)
    values, position = _decodevalues(T, bytes, count, _fixedwidth(node), 1, limits)
    position == length(bytes) + 1 ||
        throw(FormatError("dictionary page has $(length(bytes) - position + 1) trailing bytes"))
    return DecodedDictionary(values)
end

function _decodedictionaryindices(bytes::AbstractVector{UInt8}, count::Int, offset::Int,
    limits::Limits)
    count >= 0 || throw(ArgumentError("dictionary index count must be nonnegative"))
    if count == 0 && offset == length(bytes) + 1
        return UInt64[], offset
    end
    _requirebytes(bytes, offset, 1)
    bitwidth = Int(bytes[offset])
    bitwidth <= 32 || throw(FormatError("dictionary index bit width $bitwidth exceeds 32"))
    return decode_hybrid(bytes, count, bitwidth; offset=offset + 1, limits=limits)
end

function _dictionarycopy(value::Vector{UInt8})
    return copy(value)
end

function _dictionarycopy(value)
    return value
end

function _lookupdictionary(dictionary::DecodedDictionary{T}, indices::Vector{UInt64}) where {T}
    output = Vector{T}(undef, length(indices))
    count = UInt64(length(dictionary.values))
    for index in eachindex(indices)
        raw = indices[index]
        raw < count || throw(FormatError("dictionary index $raw is outside a dictionary of $count entries"))
        output[index] = _dictionarycopy(dictionary.values[Int(raw) + 1])
    end
    return output
end

function _decodedictionaryvalues(dictionary, bytes::AbstractVector{UInt8}, count::Int,
    offset::Int, limits::Limits)
    dictionary === nothing && throw(FormatError("dictionary-encoded data page has no dictionary page"))
    indices, position = _decodedictionaryindices(bytes, count, offset, limits)
    return _lookupdictionary(dictionary, indices), position
end

function _dictionarykey(value::Float32)
    return reinterpret(UInt32, value)
end

function _dictionarykey(value::Float64)
    return reinterpret(UInt64, value)
end

function _dictionarykey(value::AbstractString)
    return String(value)
end

function _dictionarykey(value::AbstractVector{UInt8})
    return String(collect(value))
end

function _dictionarykey(value)
    return value
end

function _dictionaryentries(column)
    T = Base.nonmissingtype(eltype(column.values))
    values = T[]
    indices = UInt64[]
    sizehint!(indices, length(column.values))
    lookup = Dict{Any,UInt64}()
    for rawvalue in column.values
        ismissing(rawvalue) && continue
        value = convert(T, rawvalue)
        key = _dictionarykey(value)
        index = get(lookup, key, nothing)
        if index === nothing
            index = UInt64(length(values))
            push!(values, value)
            lookup[key] = index
        end
        push!(indices, index)
    end
    return values, indices
end

function _dictionarybitwidth(count::Int)
    count >= 0 || throw(ArgumentError("dictionary entry count must be nonnegative"))
    count <= 1 && return 0
    return 64 - leading_zeros(UInt64(count - 1))
end

function _encodedictionaryindices(indices::Vector{UInt64}, bitwidth::Int)
    output = UInt8[UInt8(bitwidth)]
    isempty(indices) && return output
    if all(==(first(indices)), indices)
        _writehybridvarint!(output, UInt64(length(indices)) << 1)
        value = first(indices)
        for index in 0:(cld(bitwidth, 8) - 1)
            push!(output, UInt8((value >> (8 * index)) & 0xff))
        end
        return output
    end
    append!(output, encode_hybrid(indices, bitwidth))
    return output
end

function _dictionaryplan(column, limits::Limits)
    values, indices = _dictionaryentries(column)
    length(values) <= typemax(Int32) || throw(ArgumentError("dictionary exceeds Int32 entries"))
    dictionary_column = WriteColumn(column.name, values, column.physical,
        column.type_length, false, column.logical, column.converted)
    dictionary_payload = _plainpayload(dictionary_column, limits)
    bitwidth = _dictionarybitwidth(length(values))
    bitwidth <= 32 || throw(ArgumentError("dictionary index bit width exceeds 32"))
    index_payload = _encodedictionaryindices(indices, bitwidth)
    _checklimit(:page_bytes, length(dictionary_payload), limits.max_page_bytes)
    _checklimit(:page_bytes, length(index_payload), limits.max_page_bytes)
    return DictionaryPlan(values, indices, dictionary_payload, index_payload)
end
