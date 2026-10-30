struct N5Golden
    name::String
    node::N5Node
    rows::Vector{Any}
    schema::Vector{MD.SchemaElement}
    paths::Vector{Vector{String}}
    streams::Vector{N5LeafStream}
end

struct N5GeneratedCase
    name::String
    node::N5Node
    rows::Vector{Any}
end

struct N5SchemaControl
    name::String
    schema::Vector{MD.SchemaElement}
    streams::Vector{N5LeafStream}
    rows::Vector{Any}
    expected::Symbol
    node::Union{Nothing,N5Node}
end

struct N5SchemaFailure
    name::String
    schema::Vector{MD.SchemaElement}
    streams::Vector{N5LeafStream}
end

function n5stream(repetition, definition, values, maxrepetition::Integer,
        maxdefinition::Integer)
    return N5LeafStream(UInt64[repetition...], UInt64[definition...],
        Any[values...], Int(maxrepetition), Int(maxdefinition))
end

function _n5golden(name::AbstractString, node::N5Node, rows,
        paths::Vector{Vector{String}}, streams::Vector{N5LeafStream})
    return N5Golden(String(name), node, Any[rows...], n5schema(node), paths,
        streams)
end

function _n5rule1()
    element = n5primitive("element", MD.Type.INT32)
    node = n5list("items", element; optional=true, layout=:rule1,
        annotation=:dual)
    rows = Any[missing, [], Any[Int32(10)], Any[Int32(20), Int32(30)]]
    streams = N5LeafStream[n5stream([0, 0, 0, 0, 1],
        [0, 1, 2, 2, 2], Int32[10, 20, 30], 1, 2)]
    return _n5golden("list-rule-1", node, rows,
        [String["items", "element"]], streams)
end

function _n5rule2()
    fields = N5Node[
        n5primitive("x", MD.Type.INT32),
        n5primitive("y", MD.Type.INT32; optional=true),
    ]
    element = n5struct("element", fields)
    node = n5list("items", element; optional=true, layout=:rule2,
        wrapper="element", annotation=:dual)
    rows = Any[
        missing,
        [],
        Any[n5record(Int32(1), missing)],
        Any[n5record(Int32(2), Int32(20)),
            n5record(Int32(3), Int32(30))],
    ]
    streams = N5LeafStream[
        n5stream([0, 0, 0, 0, 1], [0, 1, 2, 2, 2],
            Int32[1, 2, 3], 1, 2),
        n5stream([0, 0, 0, 0, 1], [0, 1, 2, 3, 3],
            Int32[20, 30], 1, 3),
    ]
    paths = [String["items", "element", "x"],
        String["items", "element", "y"]]
    return _n5golden("list-rule-2", node, rows, paths, streams)
end

function _n5rule3()
    inner = n5list("array", n5primitive("array", MD.Type.INT32);
        layout=:rule1, annotation=:dual)
    node = n5list("items", inner; optional=true, layout=:rule3,
        wrapper="array", annotation=:dual)
    rows = Any[
        missing,
        [],
        Any[[]],
        Any[Any[Int32(1), Int32(2)], [], Any[Int32(3)]],
    ]
    streams = N5LeafStream[n5stream([0, 0, 0, 0, 2, 1, 1],
        [0, 1, 2, 3, 3, 2, 3], Int32[1, 2, 3], 2, 3)]
    return _n5golden("list-rule-3", node, rows,
        [String["items", "array", "array"]], streams)
end

function _n5rule4array()
    element = n5struct("array", N5Node[
        n5primitive("v", MD.Type.INT32; optional=true),
    ])
    node = n5list("items", element; optional=true, layout=:rule4,
        wrapper="array", annotation=:dual)
    rows = Any[
        missing,
        [],
        Any[n5record(missing)],
        Any[n5record(Int32(4)), n5record(missing)],
    ]
    streams = N5LeafStream[n5stream([0, 0, 0, 0, 1],
        [0, 1, 2, 3, 2], Int32[4], 1, 3)]
    return _n5golden("list-rule-4-array", node, rows,
        [String["items", "array", "v"]], streams)
end

function _n5rule4tuple()
    element = n5struct("items_tuple", N5Node[
        n5primitive("v", MD.Type.INT32; optional=true),
    ])
    node = n5list("items", element; optional=true, layout=:rule4,
        wrapper="items_tuple", annotation=:dual)
    rows = Any[
        missing,
        [],
        Any[n5record(Int32(7))],
        Any[n5record(missing), n5record(Int32(8))],
    ]
    streams = N5LeafStream[n5stream([0, 0, 0, 0, 1],
        [0, 1, 3, 2, 3], Int32[7, 8], 1, 3)]
    return _n5golden("list-rule-4-tuple", node, rows,
        [String["items", "items_tuple", "v"]], streams)
end

function _n5rule5required()
    element = n5primitive("value", MD.Type.INT32)
    node = n5list("items", element; optional=true, layout=:rule5,
        wrapper="element", annotation=:dual)
    rows = Any[missing, [], Any[Int32(10)], Any[Int32(20), Int32(30)]]
    streams = N5LeafStream[n5stream([0, 0, 0, 0, 1],
        [0, 1, 2, 2, 2], Int32[10, 20, 30], 1, 2)]
    return _n5golden("list-rule-5-required", node, rows,
        [String["items", "element", "value"]], streams)
end

function _n5rule5paired()
    element = n5primitive("value", MD.Type.INT32; optional=true)
    node = n5list("items", element; optional=true, layout=:rule5,
        wrapper="element", annotation=:dual)
    rows = Any[
        missing,
        [],
        Any[missing],
        Any[Int32(4), missing],
    ]
    streams = N5LeafStream[n5stream([0, 0, 0, 0, 1],
        [0, 1, 2, 3, 2], Int32[4], 1, 3)]
    return _n5golden("list-rule-5-paired", node, rows,
        [String["items", "element", "value"]], streams)
end

function _n5rule5extended()
    element = n5primitive("value", MD.Type.INT32; optional=true)
    node = n5list("items", element; optional=true, layout=:rule5,
        wrapper="element", annotation=:dual)
    rows = Any[
        missing,
        [],
        Any[missing],
        Any[Int32(5), missing, Int32(6)],
    ]
    streams = N5LeafStream[n5stream([0, 0, 0, 0, 1, 1],
        [0, 1, 2, 3, 2, 3], Int32[5, 6], 1, 3)]
    return _n5golden("list-rule-5-extended", node, rows,
        [String["items", "element", "value"]], streams)
end

function _n5listofmap()
    mapnode = n5map("map", n5primitive("key", MD.Type.INT32),
        n5primitive("value", MD.Type.INT32); marker=:legacy,
        entrymarker=:marked, entryname="entries")
    node = n5list("items", mapnode; optional=true, layout=:rule3,
        wrapper="map", annotation=:legacy)
    rows = Any[
        missing,
        [],
        Any[n5mapvalue()],
        Any[
            n5mapvalue(Int32(1) => Int32(10), Int32(1) => Int32(20)),
            n5mapvalue(),
            n5mapvalue(Int32(2) => Int32(30)),
        ],
    ]
    streams = N5LeafStream[
        n5stream([0, 0, 0, 0, 2, 1, 1], [0, 1, 2, 3, 3, 2, 3],
            Int32[1, 1, 2], 2, 3),
        n5stream([0, 0, 0, 0, 2, 1, 1], [0, 1, 2, 3, 3, 2, 3],
            Int32[10, 20, 30], 2, 3),
    ]
    paths = [String["items", "map", "entries", "key"],
        String["items", "map", "entries", "value"]]
    return _n5golden("direct-list-of-map", node, rows, paths, streams)
end

function _n5standardmap()
    key = n5primitive("key", MD.Type.BYTE_ARRAY; logical=:string)
    value = n5primitive("value", MD.Type.INT32; optional=true)
    node = n5map("attrs", key, value; optional=true, marker=:dual,
        entrymarker=:marked)
    rows = Any[
        missing,
        n5mapvalue(),
        n5mapvalue("a" => missing),
        n5mapvalue("a" => Int32(1), "a" => Int32(2),
            "b" => Int32(3)),
        n5mapvalue("c" => Int32(4)),
    ]
    streams = N5LeafStream[
        n5stream([0, 0, 0, 0, 1, 1, 0], [0, 1, 2, 2, 2, 2, 2],
            ["a", "a", "a", "b", "c"], 1, 2),
        n5stream([0, 0, 0, 0, 1, 1, 0], [0, 1, 2, 3, 3, 3, 3],
            Int32[1, 2, 3, 4], 1, 3),
    ]
    paths = [String["attrs", "key_value", "key"],
        String["attrs", "key_value", "value"]]
    return _n5golden("map-standard", node, rows, paths, streams)
end

function _n5keyonlymap()
    key = n5primitive("key", MD.Type.BYTE_ARRAY; logical=:string)
    node = n5map("attrs", key, nothing; marker=:dual,
        entrymarker=:marked)
    rows = Any[
        n5mapvalue(),
        n5keyset("k1"),
        n5keyset("k2", "k2"),
    ]
    streams = N5LeafStream[n5stream([0, 0, 0, 1], [0, 1, 1, 1],
        ["k1", "k2", "k2"], 1, 1)]
    return _n5golden("map-key-only", node, rows,
        [String["attrs", "key_value", "key"]], streams)
end

function _n5optionalkeymap()
    key = n5primitive("key", MD.Type.BYTE_ARRAY; optional=true,
        logical=:string)
    value = n5primitive("value", MD.Type.INT32)
    node = n5map("attrs", key, value; optional=true, marker=:legacy,
        entrymarker=:marked)
    rows = Any[
        missing,
        n5mapvalue(),
        n5mapvalue("a" => Int32(1)),
        n5mapvalue("b" => Int32(2), "c" => Int32(3)),
    ]
    streams = N5LeafStream[
        n5stream([0, 0, 0, 0, 1], [0, 1, 3, 3, 3],
            ["a", "b", "c"], 1, 3),
        n5stream([0, 0, 0, 0, 1], [0, 1, 2, 2, 2],
            Int32[1, 2, 3], 1, 2),
    ]
    paths = [String["attrs", "key_value", "key"],
        String["attrs", "key_value", "value"]]
    return _n5golden("map-optional-key", node, rows, paths, streams)
end

function n5bindinggoldens()
    return N5Golden[
        _n5rule1(),
        _n5rule2(),
        _n5rule3(),
        _n5rule4array(),
        _n5rule4tuple(),
        _n5rule5required(),
        _n5rule5paired(),
        _n5rule5extended(),
        _n5listofmap(),
        _n5standardmap(),
        _n5keyonlymap(),
        _n5optionalkeymap(),
    ]
end

function n5recursivecases()
    structkey = n5struct("key", N5Node[
        n5primitive("id", MD.Type.INT32),
        n5primitive("tag", MD.Type.INT32; optional=true),
    ])
    listvalue = n5list("value", n5primitive("element", MD.Type.INT32);
        optional=true)
    structlist = n5map("attrs", structkey, listvalue; optional=true,
        marker=:dual, entrymarker=:marked)
    structlistrows = Any[
        missing,
        n5mapvalue(),
        n5mapvalue(n5record(Int32(1), missing) =>
            Any[Int32(10), Int32(20)],
            n5record(Int32(2), Int32(7)) => missing),
    ]

    listkey = n5list("key", n5primitive("element", MD.Type.INT32))
    nestedmap = n5map("value",
        n5primitive("nested_key", MD.Type.BYTE_ARRAY; logical=:string),
        n5primitive("nested_value", MD.Type.INT32; optional=true);
        optional=true, marker=:dual, entrymarker=:marked)
    listmap = n5map("attrs", listkey, nestedmap; optional=true,
        marker=:modern_alias, entrymarker=:future_marked)
    listmaprows = Any[
        missing,
        n5mapvalue(),
        n5mapvalue(Any[Int32(1), Int32(2)] =>
            n5mapvalue("a" => Int32(1), "a" => Int32(2)),
            Any[] => missing),
    ]

    mapkey = n5map("key", n5primitive("inner_key", MD.Type.INT32),
        n5primitive("inner_value", MD.Type.INT32; optional=true);
        marker=:legacy, entrymarker=:marked)
    structvalue = n5struct("value", N5Node[
        n5primitive("payload", MD.Type.INT32),
        n5list("items", n5primitive("element", MD.Type.INT32);
            optional=true),
    ]; optional=true)
    mapstruct = n5map("attrs", mapkey, structvalue; optional=true,
        marker=:modern_primitive, entrymarker=:empty)
    mapstructrows = Any[
        missing,
        n5mapvalue(),
        n5mapvalue(n5mapvalue(Int32(1) => Int32(10),
            Int32(1) => missing) =>
            n5record(Int32(5), Any[Int32(6), Int32(7)]),
            n5mapvalue() => missing),
    ]
    return N5GeneratedCase[
        N5GeneratedCase("recursive-struct-key-list-value", structlist,
            structlistrows),
        N5GeneratedCase("recursive-list-key-map-value", listmap,
            listmaprows),
        N5GeneratedCase("recursive-map-key-struct-value", mapstruct,
            mapstructrows),
    ]
end

function _n5matrixrows(optional::Bool, valuemode::Symbol)
    empty = n5mapvalue()
    present = if valuemode === :absent
        n5keyset("a", "a", "b")
    elseif valuemode === :required
        n5mapvalue("a" => Int32(1), "a" => Int32(2), "b" => Int32(3))
    else
        n5mapvalue("a" => missing, "a" => Int32(2), "b" => Int32(3))
    end
    return optional ? Any[missing, empty, present] : Any[empty, present]
end

function n5mapmatrix()
    cases = N5GeneratedCase[]
    for marker in (:modern, :dual, :modern_alias, :conflict,
            :modern_primitive, :modern_unknown, :legacy, :alias)
        for optional in (false, true)
            for entrymarker in (:none, :marked, :empty, :future,
                    :future_marked, :unknown_converted)
                for optionalkey in (false, true)
                    for valuemode in (:absent, :required, :optional)
                        for arbitrary in (false, true)
                            entryname = arbitrary ? "entries_any" : "key_value"
                            keyname = arbitrary ? "left" : "key"
                            valuename = arbitrary ? "right" : "value"
                            key = n5primitive(keyname, MD.Type.BYTE_ARRAY;
                                optional=optionalkey, logical=:string)
                            value = valuemode === :absent ? nothing :
                                n5primitive(valuename, MD.Type.INT32;
                                    optional=valuemode === :optional)
                            node = n5map("attrs", key, value;
                                optional=optional, marker=marker,
                                entrymarker=entrymarker,
                                entryname=entryname)
                            name = join((marker, optional ? :optional : :required,
                                entrymarker, optionalkey ? :optional_key : :required_key,
                                valuemode, arbitrary ? :arbitrary : :canonical), "-")
                            push!(cases, N5GeneratedCase(name, node,
                                _n5matrixrows(optional, valuemode)))
                        end
                    end
                end
            end
        end
    end
    return cases
end

function n5annotationcontrols()
    cases = N5GeneratedCase[]
    rows = Any[missing, [], Any[Int32(1), Int32(2)]]
    for annotation in (:modern, :legacy, :dual, :conflict)
        node = n5list("items", n5primitive("element", MD.Type.INT32);
            optional=true, layout=:rule1, annotation=annotation)
        push!(cases, N5GeneratedCase("list-annotation-$annotation", node, rows))
    end
    maprows = Any[missing, n5mapvalue(), n5mapvalue("a" => Int32(1))]
    for marker in (:modern, :legacy, :alias, :dual, :conflict,
            :modern_alias, :modern_primitive, :modern_unknown)
        node = n5map("attrs",
            n5primitive("key", MD.Type.BYTE_ARRAY; logical=:string),
            n5primitive("value", MD.Type.INT32); optional=true,
            marker=marker, entrymarker=:marked)
        push!(cases, N5GeneratedCase("map-annotation-$marker", node, maprows))
    end
    return cases
end

function _n5element(element::MD.SchemaElement;
        type_=element.type_, type_length=element.type_length,
        repetition_type=element.repetition_type, name=element.name,
        num_children=element.num_children,
        converted_type=element.converted_type, scale=element.scale,
        precision=element.precision, field_id=element.field_id,
        logicalType=element.logicalType,
        unknown_fields=element.unknown_fields)
    return MD.SchemaElement(type_=type_, type_length=type_length,
        repetition_type=repetition_type, name=name,
        num_children=num_children, converted_type=converted_type,
        scale=scale, precision=precision, field_id=field_id,
        logicalType=logicalType, unknown_fields=unknown_fields)
end

function _n5variantlogical()
    return MD.LogicalType(VARIANT=MD.VariantType(specification_version=Int8(1)))
end

function _n5rawi32(id::Integer, value::Integer)
    writer = TH.Writer()
    TH.writei32!(writer, Int32(value))
    return TH.RawField(id, TH.I32, writer.buffer)
end

function n5provenancegolden()
    base = _n5rule1()
    schema = copy(base.schema)
    root = schema[1]
    schema[1] = _n5element(root; field_id=Int32(101),
        unknown_fields=(root.unknown_fields..., _n5rawi32(90, 900)))
    outer = schema[2]
    listtype = MD.ListType(unknown_fields=(_n5rawi32(91, 910),))
    schema[2] = _n5element(outer; field_id=Int32(102),
        logicalType=MD.LogicalType(LIST=listtype),
        unknown_fields=(outer.unknown_fields..., _n5rawi32(92, 920)))
    leaf = schema[3]
    schema[3] = _n5element(leaf; field_id=Int32(103),
        logicalType=_n5futurelogical(Int16(2556)),
        unknown_fields=(leaf.unknown_fields..., _n5rawi32(93, 930)))
    return N5Golden("schema-provenance", base.node, base.rows, schema,
        base.paths, base.streams)
end

function _n5entrycontrol(name::String, logical, converted)
    base = _n5standardmap()
    schema = copy(base.schema)
    schema[3] = _n5element(schema[3]; logicalType=logical,
        converted_type=converted)
    return N5SchemaControl(name, schema, base.streams, base.rows, :map,
        base.node)
end

function _n5mapentrynode(node::N5Map)
    fields = N5Node[node.key]
    node.value === nothing || push!(fields, node.value)
    return n5struct(node.entryname, fields)
end

function _n5mapentryrows(node::N5Map, rows::Vector{Any})
    output = Any[]
    sizehint!(output, length(rows))
    for row in rows
        if ismissing(row)
            push!(output, missing)
            continue
        end
        entries = Any[]
        sizehint!(entries, length(row.entries))
        for entry in row.entries
            values = Any[entry.key]
            node.value === nothing || push!(values, entry.value)
            push!(entries, N5Record(values))
        end
        push!(output, entries)
    end
    return output
end

function _n5mapalternate(node::N5Map, rows::Vector{Any}, expected::Symbol)
    entries = _n5mapentrynode(node)
    entryrows = _n5mapentryrows(node, rows)
    if expected === :list
        semantic = n5list(node.name, entries; optional=node.optional,
            layout=:rule2, wrapper=node.entryname)
        return semantic, entryrows
    elseif expected === :struct
        repeated = n5list(node.entryname, entries; layout=:rule2,
            wrapper=node.entryname)
        semantic = n5struct(node.name, N5Node[repeated]; optional=node.optional)
        structrows = Any[ismissing(row) ? missing : n5record(row)
            for row in entryrows]
        return semantic, structrows
    end
    throw(ArgumentError("unsupported N5 alternate MAP meaning $expected"))
end

function _n5outercontrol(name::String, logical, converted,
        expected::Symbol; node::Union{Nothing,N5Node}=nothing)
    base = _n5standardmap()
    schema = copy(base.schema)
    schema[2] = _n5element(schema[2]; logicalType=logical,
        converted_type=converted)
    expected === :map || (schema[3] = _n5element(schema[3];
        logicalType=nothing, converted_type=nothing))
    if node !== nothing
        semantic = node
        rows = base.rows
    elseif expected === :map
        semantic = base.node
        rows = base.rows
    else
        semantic, rows = _n5mapalternate(base.node, base.rows, expected)
    end
    return N5SchemaControl(name, schema, base.streams, rows, expected, semantic)
end

function n5mapbindingcontrols()
    maplogical = MD.LogicalType(MAP=MD.MapType())
    listlogical = MD.LogicalType(LIST=MD.ListType())
    controls = N5SchemaControl[
        _n5outercontrol("outer-modern-map", maplogical, nothing, :map),
        _n5outercontrol("outer-modern-map-matching", maplogical,
            MD.ConvertedType.MAP, :map),
        _n5outercontrol("outer-modern-map-alias", maplogical,
            MD.ConvertedType.MAP_KEY_VALUE, :map),
        _n5outercontrol("outer-modern-map-conflict", maplogical,
            MD.ConvertedType.LIST, :map),
        _n5outercontrol("outer-modern-map-primitive", maplogical,
            MD.ConvertedType.UTF8, :map),
        _n5outercontrol("outer-modern-map-unknown-converted", maplogical,
            MD.ConvertedType.T(99), :map),
        _n5outercontrol("outer-legacy-map", nothing,
            MD.ConvertedType.MAP, :map),
        _n5outercontrol("outer-legacy-map-alias", nothing,
            MD.ConvertedType.MAP_KEY_VALUE, :map),
        _n5outercontrol("outer-unknown-blocks-map", _n5futurelogical(),
            MD.ConvertedType.MAP, :struct),
        _n5outercontrol("outer-unknown-blocks-alias", _n5futurelogical(),
            MD.ConvertedType.MAP_KEY_VALUE, :struct),
        _n5outercontrol("outer-unannotated", nothing, nothing, :struct),
        _n5outercontrol("outer-unknown-converted", nothing,
            MD.ConvertedType.T(99), :struct),
        _n5outercontrol("outer-list-wins-map", listlogical,
            MD.ConvertedType.MAP, :list),
        _n5outercontrol("outer-list-wins-alias", listlogical,
            MD.ConvertedType.MAP_KEY_VALUE, :list),
        _n5outercontrol("outer-variant-wins", _n5variantlogical(),
            MD.ConvertedType.MAP, :struct),
        _n5outercontrol("outer-empty-wins", MD.LogicalType(),
            MD.ConvertedType.MAP, :struct),
        _n5outercontrol("outer-converted-list", nothing,
            MD.ConvertedType.LIST, :list),
        _n5entrycontrol("entry-unmarked", nothing, nothing),
        _n5entrycontrol("entry-map-key-value", nothing,
            MD.ConvertedType.MAP_KEY_VALUE),
        _n5entrycontrol("entry-empty-logical", MD.LogicalType(), nothing),
        _n5entrycontrol("entry-unknown-logical", _n5futurelogical(), nothing),
        _n5entrycontrol("entry-unknown-blocks-map-key-value",
            _n5futurelogical(), MD.ConvertedType.MAP_KEY_VALUE),
        _n5entrycontrol("entry-unknown-converted", nothing,
            MD.ConvertedType.T(99)),
    ]
    return controls
end

function _n5generatedcontrol(name::String, node::N5Node, rows::Vector{Any})
    compiled = n5compile(node)
    return N5SchemaControl(name, n5schema(node), n5shred(compiled, rows),
        rows, :list, node)
end

function n5listbindingcontrols()
    rule2 = _n5rule2()
    rule3 = _n5rule3()
    rule2node = n5list("items", rule2.node.element; optional=true,
        layout=:rule2, wrapper="array", annotation=:dual)
    rows = Any[missing, [], Any[missing], Any[Int32(4), missing]]
    optional = n5primitive("value", MD.Type.INT32; optional=true)
    unannotatedschema = copy(rule3.schema)
    unannotatedschema[3] = _n5element(unannotatedschema[3];
        logicalType=nothing, converted_type=nothing)
    innervalue = n5list("array", n5primitive("array", MD.Type.INT32);
        layout=:rule1)
    innerstruct = n5struct("array", N5Node[innervalue])
    unannotatednode = n5list("items", innerstruct; optional=true,
        layout=:rule2, wrapper="array")
    unannotatedrows = Any[
        missing,
        [],
        Any[n5record(Any[])],
        Any[n5record(Any[Int32(1), Int32(2)]), n5record(Any[]),
            n5record(Any[Int32(3)])],
    ]
    controls = N5SchemaControl[
        _n5generatedcontrol("list-rule-2-multifield-array", rule2node,
            rule2.rows),
        N5SchemaControl("list-rule-3-repeated-array", rule3.schema,
            rule3.streams, rule3.rows, :list, rule3.node),
        N5SchemaControl("list-rule-3-unannotated-group", unannotatedschema,
            rule3.streams, unannotatedrows, :list, unannotatednode),
        _n5generatedcontrol("list-rule-5-Array",
            n5list("items", optional; optional=true, layout=:rule5,
                wrapper="Array", annotation=:dual), rows),
        _n5generatedcontrol("list-rule-5-ARRAY",
            n5list("items", optional; optional=true, layout=:rule5,
                wrapper="ARRAY", annotation=:dual), rows),
        _n5generatedcontrol("list-rule-5-case-mismatched-tuple",
            n5list("items", optional; optional=true, layout=:rule5,
                wrapper="Items_tuple", annotation=:dual), rows),
    ]
    blocked = _n5rule1()
    schema = copy(blocked.schema)
    schema[2] = _n5element(schema[2]; logicalType=_n5futurelogical(),
        converted_type=MD.ConvertedType.LIST)
    element = blocked.node.element
    repeated = n5list(element.name, element; layout=:rule1)
    semantic = n5struct(blocked.node.name, N5Node[repeated]; optional=true)
    semanticrows = Any[ismissing(row) ? missing : n5record(row)
        for row in blocked.rows]
    push!(controls, N5SchemaControl("list-unknown-blocks-legacy", schema,
        blocked.streams, semanticrows, :struct, semantic))
    return controls
end

function _n5emptystreams(schema::Vector{MD.SchemaElement})
    return N5LeafStream[N5LeafStream(UInt64[], UInt64[], Any[],
        leaf.max_repetition, leaf.max_definition)
        for leaf in n5physicalleaves(schema)]
end

function _n5schemafailure(name::String, schema::Vector{MD.SchemaElement})
    return N5SchemaFailure(name, schema, _n5emptystreams(schema))
end

function n5mapbindingfailures()
    base = _n5standardmap().schema
    failures = N5SchemaFailure[]
    for (name, logical, converted) in (
            ("entry-logical-map", MD.LogicalType(MAP=MD.MapType()), nothing),
            ("entry-logical-list", MD.LogicalType(LIST=MD.ListType()), nothing),
            ("entry-logical-variant", _n5variantlogical(), nothing),
            ("entry-logical-primitive",
                MD.LogicalType(STRING=MD.StringType()), nothing),
            ("entry-converted-map", nothing, MD.ConvertedType.MAP),
            ("entry-converted-list", nothing, MD.ConvertedType.LIST),
            ("entry-converted-primitive", nothing, MD.ConvertedType.UTF8))
        schema = copy(base)
        schema[3] = _n5element(schema[3]; logicalType=logical,
            converted_type=converted)
        push!(failures, _n5schemafailure(name, schema))
    end
    schema = copy(base)
    schema[2] = _n5element(schema[2];
        logicalType=MD.LogicalType(STRING=MD.StringType()),
        converted_type=MD.ConvertedType.MAP)
    push!(failures, _n5schemafailure("outer-primitive-logical", schema))
    schema = copy(base)
    schema[2] = _n5element(schema[2]; logicalType=nothing,
        converted_type=MD.ConvertedType.UTF8)
    push!(failures, _n5schemafailure("outer-primitive-converted", schema))
    schema = copy(base)
    schema[2] = _n5element(schema[2];
        repetition_type=MD.FieldRepetitionType.REPEATED)
    push!(failures, _n5schemafailure("outer-repeated-outside-list", schema))
    schema = MD.SchemaElement[base[1], _n5element(base[2]; num_children=Int32(0))]
    push!(failures, _n5schemafailure("outer-zero-child", schema))
    extra = MD.SchemaElement(type_=MD.Type.INT32,
        repetition_type=MD.FieldRepetitionType.REQUIRED, name="extra")
    schema = MD.SchemaElement[base[1],
        _n5element(base[2]; num_children=Int32(2)), base[3:5]..., extra]
    push!(failures, _n5schemafailure("outer-two-children", schema))
    primitiveentry = MD.SchemaElement(type_=MD.Type.INT32,
        repetition_type=MD.FieldRepetitionType.REPEATED, name="key_value")
    schema = MD.SchemaElement[base[1], base[2], primitiveentry]
    push!(failures, _n5schemafailure("entry-primitive", schema))
    schema = copy(base)
    schema[3] = _n5element(schema[3];
        repetition_type=MD.FieldRepetitionType.REQUIRED)
    push!(failures, _n5schemafailure("entry-non-repeated", schema))
    schema = MD.SchemaElement[base[1], base[2],
        _n5element(base[3]; num_children=Int32(0))]
    push!(failures, _n5schemafailure("entry-zero-child-key-absent", schema))
    schema = MD.SchemaElement[base[1], base[2],
        _n5element(base[3]; num_children=Int32(3)), base[4], base[5], extra]
    push!(failures, _n5schemafailure("entry-three-children", schema))
    schema = copy(base)
    schema[4] = _n5element(schema[4];
        repetition_type=MD.FieldRepetitionType.REPEATED)
    push!(failures, _n5schemafailure("key-repeated", schema))
    schema = copy(base)
    schema[5] = _n5element(schema[5];
        repetition_type=MD.FieldRepetitionType.REPEATED)
    push!(failures, _n5schemafailure("value-repeated", schema))
    return failures
end
