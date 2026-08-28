struct N5GoldenManifestEntry
    name::String
    schema::Vector{MD.SchemaElement}
    paths::Vector{Vector{String}}
    streams::Vector{N5LeafStream}
    v1_sha256::String
    v2_sha256::String
end

const N5_GOLDEN_FILE_SHA256 = (
    ("list-rule-1",
        "54a9e6c9bcb218b20cabfadbe8a5f54b5f247a363fd8f8a9d158f8abe8cbfc0f",
        "bc5e6432b63dd5c3280fdaeed954005732d87164336b6a8e0da5ff78518ee354"),
    ("list-rule-2",
        "2992c963e21533f8a55ef2cc62e3187b40ca2ebf32321d677a394529238da861",
        "4a46cc1bee0ee24f405fa5585606f90e4fb5b021f1886c8268270ff9e080cd52"),
    ("list-rule-3",
        "d4b62a49c69f89938dae4d02c31e725f8362fe70a8cb3571a92423f8a210a121",
        "00f663c97d1f919ea16ee489ced5d53d23d8c0f8927351767ed43c4ecda3e15b"),
    ("list-rule-4-array",
        "95924bc48dd8b746a1bc18d1819f4e4a164f37a342744fe57a3ce935c818e488",
        "e56cd1ec4bc6983c517c734c5e47486e4d491e010a8f2b631fe00d09f6df895d"),
    ("list-rule-4-tuple",
        "1fb94f3f7cb694f148bbc0cd6de380f979e2479c9b31482250da8986de191507",
        "949a2102b8868e1feedf99657a8bcef7336b3025474ee67a464cdd0bb2a63a76"),
    ("list-rule-5-required",
        "b355239079e012cd40660be9daa98ffcb53bd0eb34fb9752988aa65bd0031a26",
        "9c5173a38162e20ac8bd5bd6e416f02a5abe7839320f17f04b46d3b296169482"),
    ("list-rule-5-paired",
        "b9feac1fa757c89acb9b5169c63eba043d41614922d5fa3c024b75510eafde88",
        "60d8739a9e86d4d0f1d048018d0f8490d5427cc47e335f33ec014455ec625dc7"),
    ("list-rule-5-extended",
        "590b16afb079231fda4193c6f5d921e74f1c9aee590de1422df04288aa111fca",
        "34a9335bea7be8eb96f562e53753cbc8e37a93e1611e14a5950fbb195e6ead29"),
    ("direct-list-of-map",
        "b804532d75ab1b2de0715321d3c9006b625e8e22b479ae423b9656ba7a0598bc",
        "df227a3c97364237a4a769e3ddf851943bf879e84df4a7d347c694c7722fa83e"),
    ("map-standard",
        "813baf63ad2d7db577c7f03a28e45fc9c337f30745f0b6c68e9b166724430cd2",
        "951118322b172a4f705541e2c15db2cb0bcdf187994ca5f7b7426455d19af38e"),
    ("map-key-only",
        "8d4a2cd87b8c796252340035e38fd0031d954a97dc791d38b13ef2fe01e65aed",
        "299e6c8ea9ff0727100640869286a656566cedf6d99d899fe45773d58b87e871"),
    ("map-optional-key",
        "9c7c47cebaae6a52117c6fe5b27a0be3184e99c9266e3d9dc5d65476def98c8b",
        "77fab91a3365c936131bb9043524196f4ae7b838a90b473ce134c6673c3c6080"),
)

const N5_GOLDEN_MANIFEST_SHA256 =
    "185ad7b78e36b9e50128a96d81d0bf43bebce31cea4c4277fcdcee16214b31cd"
const N5_PROPERTY_MANIFEST_SHA256 =
    "44d96748fb168f51343677f84097498626d8584a7d76fed2525ef6cbcf84ca3a"

function _n5manifestinteger!(output::Vector{UInt8}, value::Integer)
    negative = value < 0
    magnitude = negative ? -Int128(value) : Int128(value)
    digits = string(magnitude)
    length(digits) <= 39 || throw(ArgumentError(
        "N5 manifest integer exceeds 39 digits"))
    push!(output, negative ? UInt8('-') : UInt8('+'))
    append!(output, codeunits(lpad(digits, 39, '0')))
    return
end

function _n5manifestbytes!(output::Vector{UInt8}, value)
    _n5manifestinteger!(output, length(value))
    append!(output, value)
    return
end

function _n5manifeststring!(output::Vector{UInt8}, value::AbstractString)
    _n5manifestbytes!(output, codeunits(value))
    return
end

function _n5manifestoptionalinteger!(output::Vector{UInt8}, value)
    if value === nothing
        push!(output, 0x00)
    else
        push!(output, 0x01)
        _n5manifestinteger!(output, value)
    end
    return
end

function _n5manifestoptionalenum!(output::Vector{UInt8}, value)
    value === nothing && return _n5manifestoptionalinteger!(output, nothing)
    return _n5manifestoptionalinteger!(output, value.value)
end

function _n5manifestrawfields!(output::Vector{UInt8}, fields::Vector{TH.RawField})
    _n5manifestinteger!(output, length(fields))
    for field in fields
        _n5manifestinteger!(output, field.id)
        _n5manifestinteger!(output, field.type)
        _n5manifestinteger!(output, field.previd)
        _n5manifestinteger!(output, field.headerlength)
        _n5manifestbytes!(output, field.bytes)
    end
    return
end

function _n5manifestlogicalmember!(output::Vector{UInt8}, value)
    value isa Union{MD.StringType,MD.MapType,MD.ListType} ||
        throw(ArgumentError("unsupported N5 manifest logical member $(typeof(value))"))
    _n5manifestrawfields!(output, value.unknown_fields)
    return
end

function _n5manifestlogical!(output::Vector{UInt8}, logical::MD.LogicalType)
    members = (
        (1, logical.STRING),
        (2, logical.MAP),
        (3, logical.LIST),
        (4, logical.ENUM),
        (5, logical.DECIMAL),
        (6, logical.DATE),
        (7, logical.TIME),
        (8, logical.TIMESTAMP),
        (10, logical.INTEGER),
        (11, logical.UNKNOWN),
        (12, logical.JSON),
        (13, logical.BSON),
        (14, logical.UUID),
        (15, logical.FLOAT16),
        (16, logical.VARIANT),
        (17, logical.GEOMETRY),
        (18, logical.GEOGRAPHY),
    )
    for (id, member) in members
        member === nothing && continue
        _n5manifestinteger!(output, id)
        _n5manifestlogicalmember!(output, member)
        return
    end
    if isempty(logical.unknown_fields)
        _n5manifestinteger!(output, 0)
    else
        _n5manifestinteger!(output, -1)
        _n5manifestrawfields!(output, logical.unknown_fields)
    end
    return
end

function _n5manifestoptionallogical!(output::Vector{UInt8}, logical)
    if logical === nothing
        push!(output, 0x00)
    else
        push!(output, 0x01)
        _n5manifestlogical!(output, logical)
    end
    return
end

function _n5manifestschemaelement!(output::Vector{UInt8}, element::MD.SchemaElement)
    _n5manifestoptionalenum!(output, element.type_)
    _n5manifestoptionalinteger!(output, element.type_length)
    _n5manifestoptionalenum!(output, element.repetition_type)
    _n5manifeststring!(output, element.name)
    _n5manifestoptionalinteger!(output, element.num_children)
    _n5manifestoptionalenum!(output, element.converted_type)
    _n5manifestoptionalinteger!(output, element.scale)
    _n5manifestoptionalinteger!(output, element.precision)
    _n5manifestoptionalinteger!(output, element.field_id)
    _n5manifestoptionallogical!(output, element.logicalType)
    _n5manifestrawfields!(output, element.unknown_fields)
    return
end

function _n5manifestvalue!(output::Vector{UInt8}, value)
    if value isa Int32
        push!(output, UInt8('i'))
        _n5manifestinteger!(output, value)
    elseif value isa Int64
        push!(output, UInt8('I'))
        _n5manifestinteger!(output, value)
    elseif value isa Float32
        push!(output, UInt8('f'))
        _n5manifestinteger!(output, reinterpret(UInt32, value))
    elseif value isa Float64
        push!(output, UInt8('F'))
        _n5manifestinteger!(output, reinterpret(UInt64, value))
    elseif value isa Bool
        push!(output, value ? UInt8('t') : UInt8('b'))
    elseif value isa AbstractString
        push!(output, UInt8('s'))
        _n5manifeststring!(output, value)
    elseif value isa AbstractVector{UInt8}
        push!(output, UInt8('x'))
        _n5manifestbytes!(output, value)
    else
        throw(ArgumentError("unsupported N5 manifest value type $(typeof(value))"))
    end
    return
end

function _n5manifeststream!(output::Vector{UInt8}, stream::N5LeafStream)
    _n5manifestinteger!(output, stream.max_repetition)
    _n5manifestinteger!(output, stream.max_definition)
    _n5manifestinteger!(output, length(stream.repetition))
    for value in stream.repetition
        _n5manifestinteger!(output, value)
    end
    _n5manifestinteger!(output, length(stream.definition))
    for value in stream.definition
        _n5manifestinteger!(output, value)
    end
    _n5manifestinteger!(output, length(stream.values))
    for value in stream.values
        _n5manifestvalue!(output, value)
    end
    return
end

function n5goldenmanifest()
    entries = N5GoldenManifestEntry[]
    for case in n5bindinggoldens()
        v1 = n5emitfile(case.schema, case.streams, length(case.rows);
            pageversion=:v1)
        v2 = n5emitfile(case.schema, case.streams, length(case.rows);
            pageversion=:v2)
        push!(entries, N5GoldenManifestEntry(case.name, case.schema,
            case.paths, case.streams, bytes2hex(SHA.sha256(v1)),
            bytes2hex(SHA.sha256(v2))))
    end
    return entries
end

function n5encodemanifest(entries::Vector{N5GoldenManifestEntry})
    output = UInt8[]
    append!(output, codeunits("PARQUET-N5-A-MANIFEST-V2"))
    _n5manifestinteger!(output, length(entries))
    for entry in entries
        _n5manifeststring!(output, entry.name)
        _n5manifestinteger!(output, length(entry.schema))
        for element in entry.schema
            _n5manifestschemaelement!(output, element)
        end
        _n5manifestinteger!(output, length(entry.paths))
        for path in entry.paths
            _n5manifestinteger!(output, length(path))
            for component in path
                _n5manifeststring!(output, component)
            end
        end
        _n5manifestinteger!(output, length(entry.streams))
        for stream in entry.streams
            _n5manifeststream!(output, stream)
        end
        _n5manifeststring!(output, entry.v1_sha256)
        _n5manifeststring!(output, entry.v2_sha256)
    end
    return output
end

function n5manifestsha256(entries::Vector{N5GoldenManifestEntry})
    return bytes2hex(SHA.sha256(n5encodemanifest(entries)))
end

function _n5propertynode!(output::Vector{UInt8}, node::N5Primitive)
    push!(output, UInt8('P'))
    _n5manifeststring!(output, node.name)
    push!(output, node.optional ? 0x01 : 0x00)
    _n5manifestinteger!(output, node.physical.value)
    _n5manifeststring!(output, String(node.logical))
    return
end

function _n5propertynode!(output::Vector{UInt8}, node::N5Struct)
    push!(output, UInt8('S'))
    _n5manifeststring!(output, node.name)
    push!(output, node.optional ? 0x01 : 0x00)
    _n5manifestinteger!(output, length(node.fields))
    for field in node.fields
        _n5propertynode!(output, field)
    end
    return
end

function _n5propertynode!(output::Vector{UInt8}, node::N5List)
    push!(output, UInt8('L'))
    _n5manifeststring!(output, node.name)
    push!(output, node.optional ? 0x01 : 0x00)
    _n5manifeststring!(output, String(node.layout))
    _n5manifeststring!(output, node.wrapper)
    _n5manifeststring!(output, String(node.annotation))
    _n5propertynode!(output, node.element)
    return
end

function _n5propertynode!(output::Vector{UInt8}, node::N5Map)
    push!(output, UInt8('M'))
    _n5manifeststring!(output, node.name)
    push!(output, node.optional ? 0x01 : 0x00)
    _n5manifeststring!(output, String(node.marker))
    _n5manifeststring!(output, String(node.entrymarker))
    _n5manifeststring!(output, node.entryname)
    _n5propertynode!(output, node.key)
    if node.value === nothing
        push!(output, 0x00)
    else
        push!(output, 0x01)
        _n5propertynode!(output, node.value)
    end
    return
end

function _n5propertysemantic!(output::Vector{UInt8}, value)
    if ismissing(value)
        push!(output, UInt8('m'))
    elseif value isa N5Record
        push!(output, UInt8('r'))
        _n5manifestinteger!(output, length(value.values))
        for field in value.values
            _n5propertysemantic!(output, field)
        end
    elseif value isa N5MapValue
        push!(output, UInt8('M'))
        _n5manifestinteger!(output, length(value.entries))
        for entry in value.entries
            push!(output, entry.hasvalue ? 0x01 : 0x00)
            _n5propertysemantic!(output, entry.key)
            entry.hasvalue && _n5propertysemantic!(output, entry.value)
        end
    elseif value isa AbstractVector{UInt8}
        _n5manifestvalue!(output, value)
    elseif value isa AbstractVector
        push!(output, UInt8('l'))
        _n5manifestinteger!(output, length(value))
        for element in value
            _n5propertysemantic!(output, element)
        end
    else
        _n5manifestvalue!(output, value)
    end
    return
end

function _n5propertyschema!(output::Vector{UInt8},
        schema::Vector{MD.SchemaElement})
    _n5manifestinteger!(output, length(schema))
    for element in schema
        _n5manifestschemaelement!(output, element)
    end
    return
end

function _n5propertypaths!(output::Vector{UInt8},
        paths::Vector{Vector{String}})
    _n5manifestinteger!(output, length(paths))
    for path in paths
        _n5manifestinteger!(output, length(path))
        for component in path
            _n5manifeststring!(output, component)
        end
    end
    return
end

function _n5propertystreams!(output::Vector{UInt8},
        streams::Vector{N5LeafStream})
    _n5manifestinteger!(output, length(streams))
    for stream in streams
        _n5manifeststream!(output, stream)
    end
    return
end

function _n5propertyrows!(output::Vector{UInt8}, rows::Vector{Any})
    _n5manifestinteger!(output, length(rows))
    for row in rows
        _n5propertysemantic!(output, row)
    end
    return
end

function _n5propertycount!(output::Vector{UInt8}, label::String,
        counts, key)
    _n5manifeststring!(output, label)
    _n5manifestinteger!(output, get(() -> 0, counts, key))
    return
end

function _n5propertycoverage!(output::Vector{UInt8},
        coverage::N5PropertyCoverage)
    for kind in (:primitive, :struct, :list, :map)
        _n5propertycount!(output, "node:$kind", coverage.nodes, kind)
        _n5propertycount!(output, "recursive-node:$kind",
            coverage.recursive_nodes, kind)
    end
    for layout in N5_PROPERTY_LIST_LAYOUTS
        _n5propertycount!(output, "list-layout:$layout",
            coverage.list_layouts, layout)
    end
    for annotation in N5_PROPERTY_LIST_ANNOTATIONS
        _n5propertycount!(output, "list-annotation:$annotation",
            coverage.list_annotations, annotation)
    end
    for marker in N5_PROPERTY_MAP_MARKERS
        _n5propertycount!(output, "map-marker:$marker",
            coverage.map_markers, marker)
    end
    for marker in N5_PROPERTY_ENTRY_MARKERS
        _n5propertycount!(output, "entry-marker:$marker",
            coverage.entry_markers, marker)
    end
    for repetition in (:required, :optional, :repeated)
        _n5propertycount!(output, "repetition:$repetition",
            coverage.repetitions, repetition)
    end
    for mode in (:absent, :required, :optional)
        _n5propertycount!(output, "map-value:$mode",
            coverage.value_modes, mode)
    end
    for mode in (:canonical, :arbitrary)
        _n5propertycount!(output, "map-name:$mode", coverage.name_modes, mode)
    end
    for state in (:null, :empty, :present)
        _n5propertycount!(output, "container:$state",
            coverage.container_states, state)
    end
    for state in (:null, :present)
        _n5propertycount!(output, "element:$state",
            coverage.element_states, state)
        _n5propertycount!(output, "value:$state", coverage.value_states, state)
    end
    for lengthvalue in 0:N5_PROPERTY_MAX_COLLECTION_LENGTH
        _n5propertycount!(output, "collection:$lengthvalue",
            coverage.collection_lengths, lengthvalue)
    end
    for depth in 1:6
        _n5propertycount!(output, "depth:$depth", coverage.depths, depth)
    end
    for width in 1:4
        _n5propertycount!(output, "width:$width", coverage.widths, width)
    end
    for rows in 0:24
        _n5propertycount!(output, "rows:$rows", coverage.row_counts, rows)
    end
    for pageversion in (:v1, :v2)
        _n5propertycount!(output, "page:$pageversion",
            coverage.page_versions, pageversion)
    end
    for physical in (MD.Type.INT32, MD.Type.INT64, MD.Type.FLOAT,
            MD.Type.DOUBLE, MD.Type.BOOLEAN, MD.Type.BYTE_ARRAY)
        _n5propertycount!(output, "physical:$(physical.value)",
            coverage.physical_types, physical)
    end
    for (label, count) in (("duplicate-map", coverage.duplicate_maps),
            ("complex-key", coverage.complex_keys),
            ("complex-value", coverage.complex_values),
            ("row-start", coverage.row_starts),
            ("row-continuation", coverage.row_continuations),
            ("rejected", coverage.rejected_candidates))
        _n5manifeststring!(output, label)
        _n5manifestinteger!(output, count)
    end
    return
end

function _n5propertycase!(output::Vector{UInt8}, case::N5PropertyCase)
    _n5manifestinteger!(output, case.id)
    _n5manifeststring!(output, case.name)
    _n5manifeststring!(output, String(case.pageversion))
    for metric in (case.depth, case.width, case.astnodes, case.leaves,
            case.levelentries, case.densevalues, case.payloadbytes)
        _n5manifestinteger!(output, metric)
    end
    _n5propertynode!(output, case.node)
    _n5propertyrows!(output, case.rows)
    _n5propertyschema!(output, case.schema)
    _n5propertypaths!(output, case.paths)
    _n5propertystreams!(output, case.streams)
    bytes = n5emitfile(case.schema, case.streams, length(case.rows);
        pageversion=case.pageversion)
    _n5manifeststring!(output, bytes2hex(SHA.sha256(bytes)))
    return
end

function n5encodepropertymanifest(cases::Vector{N5PropertyCase}, rejected::Int)
    length(cases) == N5_PROPERTY_CASE_COUNT || throw(ArgumentError(
        "N5 property manifest needs exactly 256 cases"))
    output = UInt8[]
    append!(output, codeunits("PARQUET-N5-B-PROPERTY-MANIFEST-V1"))
    _n5manifestinteger!(output, N5_PROPERTY_SEED)
    _n5manifestinteger!(output, N5_PROPERTY_CANDIDATE_LAST)
    _n5manifestinteger!(output, length(cases))
    for case in cases
        _n5propertycase!(output, case)
    end
    coverage = n5propertycoverage(cases, rejected)
    _n5propertycoverage!(output, coverage)
    _n5manifestinteger!(output, length(N5_PROPERTY_CODEC_CASE_IDS))
    for id in N5_PROPERTY_CODEC_CASE_IDS
        _n5manifestinteger!(output, id)
    end
    return output
end

function n5propertymanifestsha256(cases::Vector{N5PropertyCase}, rejected::Int)
    return bytes2hex(SHA.sha256(n5encodepropertymanifest(cases, rejected)))
end

function _n5propertydiagnosticpart(writer, value)
    output = UInt8[]
    writer(output, value)
    return bytes2hex(output)
end

function n5propertydiagnostic(case::N5PropertyCase)
    ast = _n5propertydiagnosticpart(_n5propertynode!, case.node)
    schema = _n5propertydiagnosticpart(_n5propertyschema!, case.schema)
    rows = _n5propertydiagnosticpart(_n5propertyrows!, case.rows)
    streams = _n5propertydiagnosticpart(_n5propertystreams!, case.streams)
    seed = string(N5_PROPERTY_SEED; base=16, pad=16)
    return string("seed=0x", seed, " case=", case.id,
        " ast=", ast, " schema=", schema, " rows=", rows,
        " streams=", streams)
end
