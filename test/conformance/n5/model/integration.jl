function _n5normalize(node::N5Primitive, value)
    value === missing && return missing
    return value
end

function _n5normalize(node::N5Struct, value)
    value === missing && return missing
    values = Any[]
    sizehint!(values, length(node.fields))
    for (index, child) in enumerate(node.fields)
        childvalue = value isa Parquet.StructValue ? value[index].second : value[index]
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
    columns = values(Parquet.Tables.columns(table))
    column = first(columns)
    output = []
    sizehint!(output, length(column))
    for value in column
        push!(output, _n5normalize(node, value))
    end
    return output
end

# The element type a leaf reads as; unannotated FIXED_LEN_BYTE_ARRAY values are
# NTuples of the leaf's width, taken from a present value.
function _n5expectedleafbase(expected::N5ExpectedLeafVector)
    expected.logical === :string && return Parquet.DataStrings.DataString
    expected.physical == MD.Type.BOOLEAN && return Bool
    expected.physical == MD.Type.INT32 && return Int32
    expected.physical == MD.Type.INT64 && return Int64
    expected.physical == MD.Type.FLOAT && return Float32
    expected.physical == MD.Type.DOUBLE && return Float64
    expected.physical == MD.Type.BYTE_ARRAY && return Parquet.DataStrings.DataBytes
    if expected.physical == MD.Type.FIXED_LEN_BYTE_ARRAY
        present = filter(!ismissing, expected.values)
        return isempty(present) ? nothing : NTuple{length(first(present)),UInt8}
    end
    throw(ArgumentError("unsupported N5 expected leaf type $(expected.physical)"))
end

_n5bytes(value) = value
_n5bytes(value::Tuple) = collect(value)

# The model's vectors hold only the present rows of a struct's children; a read struct's
# children have one entry per struct row. `rows` are the entries of `actual` that the
# model's entries stand for.
function n5comparevectortree(expected::N5ExpectedLeafVector, actual, rows)
    actual isa Union{Vector,Parquet.DataStrings.StringVector,
        Parquet.DataStrings.BytesVector} || return false
    base = _n5expectedleafbase(expected)
    if base !== nothing
        expectedtype = expected.nullable ? Union{Missing,base} : base
        eltype(actual) === expectedtype || return false
    end
    return isequal(expected.values, Any[_n5bytes(actual[row]) for row in rows])
end

function n5comparevectortree(expected::N5ExpectedStructVector, actual, rows)
    actual isa Parquet.StructVector || return false
    expected.names == actual.names || return false
    expected.rows == length(rows) || return false
    present = rows
    if expected.ranks === nothing
        Missing <: eltype(actual) && return false
    else
        Missing <: eltype(actual) || return false
        expected.ranks == cumsum([0; [Int(actual.validity[row]) for row in rows]]) ||
            return false
        present = [row for row in rows if actual.validity[row]]
    end
    length(expected.children) == length(actual.children) || return false
    for index in eachindex(expected.children, actual.children)
        n5comparevectortree(expected.children[index], actual.children[index],
            present) || return false
    end
    return true
end

# The offsets, validity, and entries of `actual` restricted to `rows`.
function _n5spans(actual, rows)
    offsets = Int[0]
    validity = BitVector()
    entries = Int[]
    for row in rows
        push!(validity, !(Missing <: eltype(actual)) || actual.validity[row])
        append!(entries, (actual.offsets[row] + 1):actual.offsets[row + 1])
        push!(offsets, length(entries))
    end
    return offsets, validity, entries
end

function _n5samevalidity(expected, validity::BitVector, actual)
    expected === nothing && return !(Missing <: eltype(actual))
    return Missing <: eltype(actual) && expected == validity
end

function n5comparevectortree(expected::N5ExpectedListVector, actual, rows)
    actual isa Parquet.ListVector || return false
    offsets, validity, entries = _n5spans(actual, rows)
    expected.offsets == offsets || return false
    _n5samevalidity(expected.validity, validity, actual) || return false
    return n5comparevectortree(expected.values, actual.values, entries)
end

function n5comparevectortree(expected::N5ExpectedMapVector, actual, rows)
    actual isa Parquet.MapVector || return false
    offsets, validity, entries = _n5spans(actual, rows)
    expected.offsets == offsets || return false
    _n5samevalidity(expected.validity, validity, actual) || return false
    n5comparevectortree(expected.keys, actual.keys, entries) || return false
    if expected.values === nothing
        return eltype(actual.values) === Missing
    end
    return n5comparevectortree(expected.values, actual.values, entries)
end

function n5comparevectortree(expected::N5ExpectedTableVector, columns)
    names = String[String(name) for name in keys(columns)]
    names == expected.names || return false
    actual = values(columns)
    length(actual) == length(expected.children) || return false
    for index in eachindex(expected.children)
        length(actual[index]) == expected.rows || return false
        n5comparevectortree(expected.children[index], actual[index],
            1:expected.rows) || return false
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

# The production writer's level streams and values for each leaf of a stored-schema
# `Parquet.Table`, before page encoding.
function n5productionstreams(table, compiled::N5Compiled)
    options = Parquet._writeoptions()
    schema = Parquet._storedschema(table, options)
    rows = Parquet._tablerows(table)
    streams = N5LeafStream[]
    for (index, column) in enumerate(values(Parquet._tablecolumns(table)))
        node = Parquet._provenancenode(schema.semantic.root.children[index])
        leaves = Parquet._nestedwriterows(schema.semantic, node, column, options.limits)
        for (leafindex, leaf) in zip(node.leaves, leaves)
            source = schema.schema.leaves[Int(leafindex)]
            # STRING leaves may hold strings rather than bytes before encoding.
            values = Any[value isa AbstractString ?
                Vector{UInt8}(codeunits(value)) : value for value in leaf.values]
            maxrep = Int(source.max_repetition_level)
            maxdef = Int(source.max_definition_level)
            entries = maxrep > 0 ? length(leaf.repetition) :
                maxdef > 0 ? length(leaf.definition) : length(leaf.values)
            push!(streams, N5LeafStream(
                _n5productionlevels(maxrep > 0 ? leaf.repetition : nothing, entries),
                _n5productionlevels(maxdef > 0 ? leaf.definition : nothing, entries),
                values, maxrep, maxdef))
        end
    end
    length(streams) == length(compiled.leaves) || throw(ArgumentError(
        "production pre-encode leaf count differs from N5 model"))
    return streams, (elements=schema.elements,), rows
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
