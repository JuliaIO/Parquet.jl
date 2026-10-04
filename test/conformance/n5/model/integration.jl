function _n5normalize(node::N5Primitive, value)
    value === missing && return missing
    return value
end

function _n5normalize(node::N5Struct, value)
    value === missing && return missing
    values = Any[]
    sizehint!(values, length(node.fields))
    for (index, child) in enumerate(node.fields)
        childvalue = try
            value[child.name]
        catch error
            error isa MethodError || error isa KeyError || rethrow()
            value[index]
        end
        push!(values, _n5normalize(child, childvalue))
    end
    return N5Record(values)
end

function _n5normalize(node::N5List, value)
    value === missing && return missing
    output = []
    sizehint!(output, length(value))
    for element in value
        push!(output, _n5normalize(node.element, element))
    end
    return output
end

function _n5normalize(node::N5Map, value)
    value === missing && return missing
    entries = N5Entry[]
    sizehint!(entries, length(value))
    for pair in value
        key = _n5normalize(node.key, pair.first)
        if node.value === nothing
            push!(entries, N5Entry(key, nothing, false))
        else
            push!(entries, N5Entry(key,
                _n5normalize(node.value, pair.second), true))
        end
    end
    return N5MapValue(entries)
end

function n5normalizetable(node::N5Node, table)
    columns = values(table.columns)
    column = first(columns)
    output = []
    sizehint!(output, length(column))
    for value in column
        push!(output, _n5normalize(node, value))
    end
    return output
end

function _n5samevalidity(expected::Nothing, actual)
    return actual === nothing
end

function _n5samevalidity(expected::BitVector, actual)
    actual === nothing && return false
    return expected == BitVector(actual)
end

function _n5expectedleafbase(expected::N5ExpectedLeafVector)
    expected.logical === :string && return Parquet.DataStrings.DataString
    expected.physical == MD.Type.BOOLEAN && return Bool
    expected.physical == MD.Type.INT32 && return Int32
    expected.physical == MD.Type.INT64 && return Int64
    expected.physical == MD.Type.FLOAT && return Float32
    expected.physical == MD.Type.DOUBLE && return Float64
    expected.physical == MD.Type.BYTE_ARRAY && return Vector{UInt8}
    expected.physical == MD.Type.FIXED_LEN_BYTE_ARRAY && return Vector{UInt8}
    throw(ArgumentError("unsupported N5 expected leaf type $(expected.physical)"))
end

function n5comparevectortree(expected::N5ExpectedLeafVector, actual)
    if expected.physical == MD.Type.FIXED_LEN_BYTE_ARRAY
        actual isa Parquet.FixedByteArrayVector || return false
    else
        actual isa Vector || return false
    end
    base = _n5expectedleafbase(expected)
    expectedtype = expected.nullable ? Union{Missing,base} : base
    eltype(actual) === expectedtype || return false
    return isequal(expected.values, Any[actual...])
end

function n5comparevectortree(expected::N5ExpectedStructVector, actual)
    actual isa Parquet.StructVector || return false
    expected.names == actual.names || return false
    expected.rows == actual.rows || return false
    if expected.ranks === nothing
        actual.ranks === nothing || return false
    else
        actual.ranks === nothing && return false
        expected.ranks == Int[actual.ranks...] || return false
    end
    length(expected.children) == length(actual.children) || return false
    for index in eachindex(expected.children, actual.children)
        n5comparevectortree(expected.children[index], actual.children[index]) ||
            return false
    end
    return true
end

function n5comparevectortree(expected::N5ExpectedListVector, actual)
    actual isa Parquet.ListVector || return false
    expected.offsets == Int[actual.offsets...] || return false
    _n5samevalidity(expected.validity, actual.validity) || return false
    return n5comparevectortree(expected.values, actual.values)
end

function n5comparevectortree(expected::N5ExpectedMapVector, actual)
    actual isa Parquet.MapVector || return false
    expected.offsets == Int[actual.offsets...] || return false
    _n5samevalidity(expected.validity, actual.validity) || return false
    n5comparevectortree(expected.keys, actual.keys) || return false
    if expected.values === nothing
        return actual.values === nothing
    end
    actual.values === nothing && return false
    return n5comparevectortree(expected.values, actual.values)
end

function n5comparevectortree(expected::N5ExpectedTableVector, columns)
    names = String[String(name) for name in keys(columns)]
    names == expected.names || return false
    actual = values(columns)
    length(actual) == length(expected.children) || return false
    for index in eachindex(expected.children)
        length(actual[index]) == expected.rows || return false
        n5comparevectortree(expected.children[index], actual[index]) ||
            return false
    end
    return true
end

function n5productionencodedbytes(table, pageversion::Symbol;
        codec::Symbol=:uncompressed)
    options = (; checksum=false, dictionary=false, codec=codec,
        pageversion=pageversion, encoding=:plain, rowgroupsize=nothing,
        pagesize=nothing, pageindex=false, statistics=false)
    privatefirst = Parquet._encodefile(table; options...)
    privatesecond = Parquet._encodefile(table; options...)
    firstio = IOBuffer()
    Parquet.write(firstio, table; options...)
    publicfirst = take!(firstio)
    secondio = IOBuffer()
    Parquet.write(secondio, table; options...)
    publicsecond = take!(secondio)
    return (; privatefirst, privatesecond, publicfirst, publicsecond)
end

struct N5PropertyCodecFixture
    caseid::Int
    name::String
    pageversion::Symbol
    codec::Symbol
    filename::String
    sha256::String
    bytes::Vector{UInt8}
    schema::Vector{MD.SchemaElement}
    paths::Vector{Vector{String}}
    rows::Vector{Any}
end

function n5propertyencodedbytes(case::N5PropertyCase, codec::Symbol)
    codec in N5_PROPERTY_CODECS || throw(ArgumentError(
        "unsupported N5 property codec $codec"))
    source = n5emitfile(case.schema, case.streams, length(case.rows);
        pageversion=case.pageversion)
    table = Parquet.Table(source)
    try
        return n5productionencodedbytes(table, case.pageversion; codec=codec)
    finally
        close(table)
    end
end

function n5propertycodecfixture(case::N5PropertyCase, codec::Symbol)
    outputs = n5propertyencodedbytes(case, codec)
    outputs.privatefirst == outputs.privatesecond == outputs.publicfirst ==
        outputs.publicsecond || throw(ArgumentError(
            "N5 property codec output is not deterministic"))
    bytes = outputs.publicfirst
    source = n5emitfile(case.schema, case.streams, length(case.rows);
        pageversion=case.pageversion)
    canonicalschema = n5decodefile(source).metadata.schema
    filename = string(case.name, "-", case.pageversion, "-", codec,
        ".parquet")
    return N5PropertyCodecFixture(case.id, case.name, case.pageversion, codec,
        filename, bytes2hex(SHA.sha256(bytes)), bytes, canonicalschema,
        case.paths, case.rows)
end

function n5propertycodecfixtures(cases::Vector{N5PropertyCase})
    fixtures = N5PropertyCodecFixture[]
    sizehint!(fixtures, N5_PROPERTY_CODEC_COUNT * length(N5_PROPERTY_CODECS))
    for case in n5propertycodecsubset(cases)
        for codec in N5_PROPERTY_CODECS
            push!(fixtures, n5propertycodecfixture(case, codec))
        end
    end
    return fixtures
end

function n5propertycodecfixtures()
    cases, _ = n5propertycases()
    return n5propertycodecfixtures(cases)
end

function _n5metadataexact(left::TH.RawField, right::TH.RawField)
    return left.id == right.id && left.type == right.type &&
        left.previd == right.previd && left.headerlength == right.headerlength &&
        left.bytes == right.bytes
end

function _n5metadataexact(left::Tuple, right::Tuple)
    length(left) == length(right) || return false
    for index in eachindex(left, right)
        _n5metadataexact(left[index], right[index]) || return false
    end
    return true
end

function _n5metadataexact(left, right)
    typeof(left) === typeof(right) || return false
    T = typeof(left)
    if isstructtype(T) && hasfield(T, :unknown_fields)
        for index in 1:fieldcount(T)
            _n5metadataexact(getfield(left, index), getfield(right, index)) ||
                return false
        end
        return true
    end
    return isequal(left, right)
end

function n5schemaexact(left::Vector{MD.SchemaElement},
        right::Vector{MD.SchemaElement})
    length(left) == length(right) || return false
    for index in eachindex(left, right)
        _n5metadataexact(left[index], right[index]) || return false
    end
    return true
end

function _n5productionlevels(levels, entries::Int)
    levels === nothing && return fill(UInt64(0), entries)
    return UInt64[levels...]
end

function _n5physicalexpected(values::Vector{Any}, leaf::N5LeafSpec)
    if leaf.logical === :string
        return Any[Vector{UInt8}(codeunits(value)) for value in values]
    end
    return values
end

function n5productionstreams(table, compiled::N5Compiled)
    fields, rows = Parquet._writefields(table, Parquet.Limits())
    columns = Parquet.WriteColumn[]
    for field in fields
        append!(columns, field.leaves)
    end
    length(columns) == length(compiled.leaves) || throw(ArgumentError(
        "production pre-encode leaf count differs from N5 model"))
    streams = N5LeafStream[]
    sizehint!(streams, length(columns))
    for (column, leaf) in zip(columns, compiled.leaves)
        entries = column.repetitions === nothing ?
            column.definitions === nothing ? length(column.values) :
                length(column.definitions) : length(column.repetitions)
        repetition = _n5productionlevels(column.repetitions, entries)
        definition = _n5productionlevels(column.definitions, entries)
        values = Any[column.values...]
        push!(streams, N5LeafStream(repetition, definition, values,
            Int(column.max_repetition_level),
            Int(column.max_definition_level)))
    end
    return streams, fields, rows
end

function n5compareproductionstreams(actual::Vector{N5LeafStream},
        expected::Vector{N5LeafStream}, compiled::N5Compiled)
    length(actual) == length(expected) == length(compiled.leaves) ||
        return false
    for index in eachindex(actual)
        left = actual[index]
        right = expected[index]
        left.repetition == right.repetition || return false
        left.definition == right.definition || return false
        left.max_repetition == right.max_repetition || return false
        left.max_definition == right.max_definition || return false
        isequal(left.values,
            _n5physicalexpected(right.values, compiled.leaves[index])) ||
            return false
    end
    return true
end
