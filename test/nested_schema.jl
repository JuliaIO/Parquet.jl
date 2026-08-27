if !@isdefined(TH)
    const TH = Parquet.Thrift
end
if !@isdefined(MD)
    const MD = Parquet.Metadata
end
if !isdefined(Parquet, :_NestedSchemaPlan)
    Base.include(Parquet, joinpath(@__DIR__, "..", "src", "nested_schema.jl"))
end

function nestedschemaelement(name; physical=nothing, repetition=nothing,
    children=nothing, logical=nothing, converted=nothing)
    return MD.SchemaElement(name=name, type_=physical,
        repetition_type=repetition, num_children=children,
        logicalType=logical, converted_type=converted)
end

function nestedroot(children; logical=nothing, converted=nothing)
    return nestedschemaelement("schema"; children=Int32(children),
        logical=logical, converted=converted)
end

function nestedlogical(kind::Symbol)
    kind === :list && return MD.LogicalType(LIST=MD.ListType())
    kind === :map && return MD.LogicalType(MAP=MD.MapType())
    kind === :string && return MD.LogicalType(STRING=MD.StringType())
    kind === :variant && return MD.LogicalType(
        VARIANT=MD.VariantType(specification_version=Int8(1)))
    kind === :empty && return MD.LogicalType()
    kind === :future && return MD.LogicalType(
        unknown_fields=(TH.RawField(2555, TH.STRUCT, UInt8[0x00]),))
    throw(ArgumentError("unknown nested test logical type $kind"))
end

function nestedcompile(elements; limits=Parquet.Limits())
    schema = Parquet.Schema(elements)
    return Parquet._nestedplan(schema; limits=limits)
end

@testset "nested canonical plans and thresholds" begin
    required = MD.FieldRepetitionType.REQUIRED
    optional = MD.FieldRepetitionType.OPTIONAL
    repeated = MD.FieldRepetitionType.REPEATED
    elements = MD.SchemaElement[
        nestedroot(2),
        nestedschemaelement("names"; repetition=optional, children=Int32(1),
            logical=nestedlogical(:list), converted=MD.ConvertedType.LIST),
        nestedschemaelement("list"; repetition=repeated, children=Int32(1)),
        nestedschemaelement("element"; physical=MD.Type.BYTE_ARRAY,
            repetition=optional),
        nestedschemaelement("record"; repetition=required, children=Int32(2)),
        nestedschemaelement("id"; physical=MD.Type.INT64, repetition=required),
        nestedschemaelement("score"; physical=MD.Type.DOUBLE,
            repetition=optional),
    ]
    plan = nestedcompile(elements)
    @test plan.source isa Parquet.Schema
    @test plan.root.parent_definition == 0
    @test plan.root.present_definition == 0
    @test plan.root.leaf_range == Int32(1):Int32(3)
    @test plan.plan_count == 6
    @test length(plan.leaves) == 3

    list = plan.root.children[1]
    @test list isa Parquet._NestedListPlan
    @test list.annotation === :modern_list
    @test list.rule == 0x06
    @test list.parent_definition == 0
    @test list.present_definition == 1
    @test list.entry_definition == 2
    @test list.repetition_level == 1
    @test list.leaf_range == Int32(1):Int32(1)
    @test list.element isa Parquet._NestedLeafPlan
    @test list.element.parent_definition == 2
    @test list.element.present_definition == 3

    record = plan.root.children[2]
    @test record isa Parquet._NestedStructPlan
    @test record.parent_definition == 0
    @test record.present_definition == 0
    @test record.leaf_range == Int32(2):Int32(3)
    @test record.children[1].parent_definition == 0
    @test record.children[1].present_definition == 0
    @test record.children[2].parent_definition == 0
    @test record.children[2].present_definition == 1
end

@testset "nested logical annotation precedence and placement" begin
    required = MD.FieldRepetitionType.REQUIRED
    repeated = MD.FieldRepetitionType.REPEATED

    modernlist = nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("values"; repetition=required, children=Int32(1),
            logical=nestedlogical(:list), converted=MD.ConvertedType.MAP),
        nestedschemaelement("items"; physical=MD.Type.INT32,
            repetition=repeated),
    ])
    @test modernlist.root.children[1] isa Parquet._NestedListPlan
    @test modernlist.root.children[1].annotation === :modern_list

    modernmap = nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("values"; repetition=required, children=Int32(1),
            logical=nestedlogical(:map), converted=MD.ConvertedType.LIST),
        nestedschemaelement("pairs"; repetition=repeated, children=Int32(1)),
        nestedschemaelement("first"; physical=MD.Type.INT64,
            repetition=required),
    ])
    @test modernmap.root.children[1] isa Parquet._NestedMapPlan
    @test modernmap.root.children[1].annotation === :modern_map

    for modern in (:future, :empty, :variant)
        ordinary = nestedcompile(MD.SchemaElement[
            nestedroot(1),
            nestedschemaelement("values"; repetition=required,
                children=Int32(1), logical=nestedlogical(modern),
                converted=MD.ConvertedType.LIST),
            nestedschemaelement("items"; physical=MD.Type.INT32,
                repetition=repeated),
        ])
        @test ordinary.root.children[1] isa Parquet._NestedStructPlan
        @test ordinary.root.children[1].children[1] isa Parquet._NestedListPlan
        @test ordinary.root.children[1].children[1].annotation ===
            :unannotated_repeated
    end

    unknownmap = nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("values"; repetition=required, children=Int32(1),
            logical=nestedlogical(:future),
            converted=MD.ConvertedType.MAP_KEY_VALUE),
        nestedschemaelement("items"; physical=MD.Type.INT32,
            repetition=repeated),
    ])
    @test unknownmap.root.children[1] isa Parquet._NestedStructPlan

    for converted in (MD.ConvertedType.LIST, MD.ConvertedType.MAP,
            MD.ConvertedType.MAP_KEY_VALUE)
        blocked = nestedcompile(MD.SchemaElement[
            nestedroot(1),
            nestedschemaelement("values"; repetition=required,
                children=Int32(1), logical=nestedlogical(:future),
                converted=converted),
            nestedschemaelement("items"; physical=MD.Type.INT32,
                repetition=repeated),
        ])
        @test blocked.root.children[1] isa Parquet._NestedStructPlan
        @test blocked.root.children[1].children[1] isa Parquet._NestedListPlan
    end

    @test_throws Parquet.FormatError nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("bad"; repetition=required, children=Int32(1),
            logical=nestedlogical(:string), converted=MD.ConvertedType.LIST),
        nestedschemaelement("item"; physical=MD.Type.INT32,
            repetition=repeated),
    ])
    @test_throws Parquet.FormatError nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("bad"; physical=MD.Type.INT32,
            repetition=required, logical=nestedlogical(:list)),
    ])
    @test_throws Parquet.FormatError nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("bad"; physical=MD.Type.INT32,
            repetition=required, logical=nestedlogical(:future),
            converted=MD.ConvertedType.MAP),
    ])
    @test_throws Parquet.FormatError nestedcompile(MD.SchemaElement[
        nestedroot(0; logical=nestedlogical(:list)),
    ])
    @test_throws Parquet.FormatError nestedcompile(MD.SchemaElement[
        nestedroot(0; logical=nestedlogical(:future),
            converted=MD.ConvertedType.MAP_KEY_VALUE),
    ])
end

@testset "all ordered LIST compatibility rules" begin
    required = MD.FieldRepetitionType.REQUIRED
    optional = MD.FieldRepetitionType.OPTIONAL
    repeated = MD.FieldRepetitionType.REPEATED
    list = nestedlogical(:list)
    elements = MD.SchemaElement[
        nestedroot(6),
        nestedschemaelement("rule1"; repetition=required, children=Int32(1),
            logical=list),
        nestedschemaelement("scalar"; physical=MD.Type.INT32,
            repetition=repeated),
        nestedschemaelement("rule2"; repetition=required, children=Int32(1),
            logical=list),
        nestedschemaelement("record"; repetition=repeated, children=Int32(2)),
        nestedschemaelement("left"; physical=MD.Type.INT32,
            repetition=required),
        nestedschemaelement("right"; physical=MD.Type.INT64,
            repetition=required),
        nestedschemaelement("rule3"; repetition=required, children=Int32(1),
            logical=list),
        nestedschemaelement("nested"; repetition=repeated, children=Int32(1),
            logical=list),
        nestedschemaelement("value"; physical=MD.Type.INT32,
            repetition=repeated),
        nestedschemaelement("rule4"; repetition=required, children=Int32(1),
            logical=list),
        nestedschemaelement("array"; repetition=repeated, children=Int32(1)),
        nestedschemaelement("value"; physical=MD.Type.INT32,
            repetition=required),
        nestedschemaelement("rule5"; repetition=required, children=Int32(1),
            logical=list),
        nestedschemaelement("rule5_tuple"; repetition=repeated,
            children=Int32(1)),
        nestedschemaelement("value"; physical=MD.Type.INT32,
            repetition=required),
        nestedschemaelement("rule6"; repetition=required, children=Int32(1),
            logical=list),
        nestedschemaelement("items"; repetition=repeated, children=Int32(1)),
        nestedschemaelement("value"; physical=MD.Type.INT32,
            repetition=optional),
    ]
    plan = nestedcompile(elements)
    lists = plan.root.children
    @test [item.rule for item in lists] == UInt8[1, 2, 3, 4, 5, 6]
    @test [item.leaf_range for item in lists] == UnitRange{Int32}[
        Int32(1):Int32(1), Int32(2):Int32(3), Int32(4):Int32(4),
        Int32(5):Int32(5), Int32(6):Int32(6), Int32(7):Int32(7)]
    @test lists[1].element isa Parquet._NestedLeafPlan
    @test lists[2].element isa Parquet._NestedStructPlan
    @test lists[3].element isa Parquet._NestedListPlan
    @test lists[3].element.annotation === :modern_list
    @test lists[3].element.parent_definition == lists[3].entry_definition
    @test lists[4].element isa Parquet._NestedStructPlan
    @test lists[5].element isa Parquet._NestedStructPlan
    @test lists[6].element isa Parquet._NestedLeafPlan
    @test lists[6].element.parent_definition == lists[6].entry_definition
    @test lists[6].element.present_definition ==
        lists[6].entry_definition + Int16(1)

    ordinaryrule3 = nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("outer"; repetition=required, children=Int32(1),
            logical=list),
        nestedschemaelement("wrapper"; repetition=repeated, children=Int32(1)),
        nestedschemaelement("inner"; physical=MD.Type.INT32,
            repetition=repeated),
    ]).root.children[1]
    @test ordinaryrule3.rule == 0x03
    @test ordinaryrule3.element isa Parquet._NestedStructPlan
    @test ordinaryrule3.element.children[1] isa Parquet._NestedListPlan

    @test_throws Parquet.FormatError nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("empty"; repetition=required, children=Int32(1),
            logical=list),
        nestedschemaelement("array"; repetition=repeated, children=Int32(0)),
    ])
end

@testset "malformed LIST schemas" begin
    required = MD.FieldRepetitionType.REQUIRED
    optional = MD.FieldRepetitionType.OPTIONAL
    repeated = MD.FieldRepetitionType.REPEATED
    list = nestedlogical(:list)
    malformed = Vector{MD.SchemaElement}[]
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("list"; repetition=required, children=Int32(0),
            logical=list),
    ])
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("list"; repetition=required, children=Int32(2),
            logical=list),
        nestedschemaelement("one"; physical=MD.Type.INT32,
            repetition=repeated),
        nestedschemaelement("two"; physical=MD.Type.INT32,
            repetition=repeated),
    ])
    for entryrepetition in (required, optional)
        push!(malformed, MD.SchemaElement[
            nestedroot(1),
            nestedschemaelement("list"; repetition=required,
                children=Int32(1), logical=list),
            nestedschemaelement("entry"; physical=MD.Type.INT32,
                repetition=entryrepetition),
        ])
    end
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("list"; repetition=required, children=Int32(1),
            logical=list),
        nestedschemaelement("wrapper"; repetition=repeated,
            children=Int32(1), converted=MD.ConvertedType.MAP),
        nestedschemaelement("only"; physical=MD.Type.INT32,
            repetition=required),
    ])
    for elements in malformed
        @test_throws Parquet.FormatError nestedcompile(elements)
    end
end

@testset "repeated annotated collection exceptions" begin
    required = MD.FieldRepetitionType.REQUIRED
    repeated = MD.FieldRepetitionType.REPEATED
    for annotation in (:list, :map)
        elements = if annotation === :list
            MD.SchemaElement[
                nestedroot(1),
                nestedschemaelement("bad"; repetition=repeated,
                    children=Int32(1), logical=nestedlogical(:list)),
                nestedschemaelement("items"; physical=MD.Type.INT32,
                    repetition=repeated),
            ]
        else
            MD.SchemaElement[
                nestedroot(1),
                nestedschemaelement("bad"; repetition=repeated,
                    children=Int32(1), logical=nestedlogical(:map)),
                nestedschemaelement("pairs"; repetition=repeated,
                    children=Int32(1)),
                nestedschemaelement("key"; physical=MD.Type.INT32,
                    repetition=required),
            ]
        end
        @test_throws Parquet.FormatError nestedcompile(elements)
    end
    @test_throws Parquet.FormatError nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("bad"; repetition=repeated, children=Int32(1),
            converted=MD.ConvertedType.MAP_KEY_VALUE),
        nestedschemaelement("pairs"; repetition=repeated, children=Int32(1)),
        nestedschemaelement("key"; physical=MD.Type.INT32,
            repetition=required),
    ])

    listmap = nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("maps"; repetition=required, children=Int32(1),
            converted=MD.ConvertedType.LIST),
        nestedschemaelement("map"; repetition=repeated, children=Int32(1),
            converted=MD.ConvertedType.MAP),
        nestedschemaelement("entries"; repetition=repeated, children=Int32(2),
            converted=MD.ConvertedType.MAP_KEY_VALUE),
        nestedschemaelement("key"; physical=MD.Type.INT32,
            repetition=required),
        nestedschemaelement("value"; physical=MD.Type.INT64,
            repetition=required),
    ])
    outer = listmap.root.children[1]
    @test outer isa Parquet._NestedListPlan
    @test outer.rule == 0x03
    @test outer.annotation === :legacy_list
    @test outer.element isa Parquet._NestedMapPlan
    @test outer.element.annotation === :legacy_map
    @test outer.element.parent_definition == outer.entry_definition
    @test outer.element.entry_has_map_key_value

    listalias = nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("maps"; repetition=required, children=Int32(1),
            logical=nestedlogical(:list)),
        nestedschemaelement("map"; repetition=repeated, children=Int32(1),
            converted=MD.ConvertedType.MAP_KEY_VALUE),
        nestedschemaelement("entries"; repetition=repeated,
            children=Int32(1)),
        nestedschemaelement("key"; physical=MD.Type.INT32,
            repetition=required),
    ])
    @test listalias.root.children[1].element isa Parquet._NestedMapPlan
    @test listalias.root.children[1].element.annotation ===
        :legacy_map_key_value
end

@testset "MAP compatibility forms" begin
    required = MD.FieldRepetitionType.REQUIRED
    optional = MD.FieldRepetitionType.OPTIONAL
    repeated = MD.FieldRepetitionType.REPEATED
    elements = MD.SchemaElement[
        nestedroot(4),
        nestedschemaelement("canonical"; repetition=optional, children=Int32(1),
            logical=nestedlogical(:map), converted=MD.ConvertedType.MAP),
        nestedschemaelement("anything"; repetition=repeated, children=Int32(2)),
        nestedschemaelement("not_key"; physical=MD.Type.BYTE_ARRAY,
            repetition=required),
        nestedschemaelement("not_value"; physical=MD.Type.INT32,
            repetition=optional),
        nestedschemaelement("legacy"; repetition=required, children=Int32(1),
            converted=MD.ConvertedType.MAP),
        nestedschemaelement("pairs"; repetition=repeated, children=Int32(2),
            converted=MD.ConvertedType.MAP_KEY_VALUE),
        nestedschemaelement("left"; physical=MD.Type.INT64,
            repetition=optional),
        nestedschemaelement("right"; physical=MD.Type.INT32,
            repetition=required),
        nestedschemaelement("alias"; repetition=optional, children=Int32(1),
            converted=MD.ConvertedType.MAP_KEY_VALUE),
        nestedschemaelement("pairs"; repetition=repeated, children=Int32(1)),
        nestedschemaelement("only"; physical=MD.Type.INT32,
            repetition=required),
        nestedschemaelement("future_entry"; repetition=required,
            children=Int32(1), logical=nestedlogical(:map)),
        nestedschemaelement("pairs"; repetition=repeated, children=Int32(1),
            logical=nestedlogical(:future),
            converted=MD.ConvertedType.MAP_KEY_VALUE),
        nestedschemaelement("key"; physical=MD.Type.INT32,
            repetition=required),
    ]
    plan = nestedcompile(elements)
    canonical, legacy, alias, futureentry = plan.root.children

    @test canonical isa Parquet._NestedMapPlan
    @test canonical.annotation === :modern_map
    @test canonical.parent_definition == 0
    @test canonical.present_definition == 1
    @test canonical.entry_definition == 2
    @test canonical.repetition_level == 1
    @test !canonical.optional_key
    @test !canonical.entry_has_map_key_value
    @test canonical.key.source.element.name == "not_key"
    @test canonical.value.source.element.name == "not_value"
    @test canonical.key.parent_definition == 2
    @test canonical.key.present_definition == 2
    @test canonical.value.parent_definition == 2
    @test canonical.value.present_definition == 3

    @test legacy.annotation === :legacy_map
    @test legacy.optional_key
    @test legacy.entry_has_map_key_value
    @test legacy.key.present_definition == 2
    @test legacy.value.present_definition == 1
    @test alias.annotation === :legacy_map_key_value
    @test alias.value === nothing
    @test !alias.optional_key
    @test futureentry.annotation === :modern_map
    @test !futureentry.entry_has_map_key_value
    @test plan.root.leaf_range == Int32(1):Int32(6)
end

@testset "MAP malformed schemas" begin
    required = MD.FieldRepetitionType.REQUIRED
    optional = MD.FieldRepetitionType.OPTIONAL
    repeated = MD.FieldRepetitionType.REPEATED
    map = nestedlogical(:map)

    malformed = Vector{MD.SchemaElement}[]
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("map"; repetition=required, children=Int32(0),
            logical=map),
    ])
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("map"; repetition=required, children=Int32(2),
            logical=map),
        nestedschemaelement("one"; physical=MD.Type.INT32,
            repetition=required),
        nestedschemaelement("two"; physical=MD.Type.INT32,
            repetition=required),
    ])
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("map"; repetition=required, children=Int32(1),
            logical=map),
        nestedschemaelement("entry"; physical=MD.Type.INT32,
            repetition=repeated),
    ])
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("map"; repetition=required, children=Int32(1),
            logical=map),
        nestedschemaelement("entry"; repetition=required, children=Int32(1)),
        nestedschemaelement("key"; physical=MD.Type.INT32,
            repetition=required),
    ])
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("map"; repetition=required, children=Int32(1),
            logical=map),
        nestedschemaelement("entry"; repetition=repeated, children=Int32(0)),
    ])
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("map"; repetition=required, children=Int32(1),
            logical=map),
        nestedschemaelement("entry"; repetition=repeated, children=Int32(3)),
        nestedschemaelement("key"; physical=MD.Type.INT32,
            repetition=required),
        nestedschemaelement("value"; physical=MD.Type.INT32,
            repetition=optional),
        nestedschemaelement("extra"; physical=MD.Type.INT32,
            repetition=optional),
    ])
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("map"; repetition=required, children=Int32(1),
            logical=map),
        nestedschemaelement("entry"; repetition=repeated, children=Int32(1)),
        nestedschemaelement("key"; physical=MD.Type.INT32,
            repetition=repeated),
    ])
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("map"; repetition=required, children=Int32(1),
            logical=map),
        nestedschemaelement("entry"; repetition=repeated, children=Int32(2)),
        nestedschemaelement("key"; physical=MD.Type.INT32,
            repetition=required),
        nestedschemaelement("value"; physical=MD.Type.INT32,
            repetition=repeated),
    ])
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("map"; repetition=required, children=Int32(1),
            logical=map),
        nestedschemaelement("entry"; repetition=repeated, children=Int32(1),
            converted=MD.ConvertedType.LIST),
        nestedschemaelement("key"; physical=MD.Type.INT32,
            repetition=required),
    ])
    push!(malformed, MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("map"; repetition=required, children=Int32(1),
            logical=map),
        nestedschemaelement("entry"; repetition=repeated, children=Int32(1),
            logical=nestedlogical(:map)),
        nestedschemaelement("key"; physical=MD.Type.INT32,
            repetition=required),
    ])
    for elements in malformed
        @test_throws Parquet.FormatError nestedcompile(elements)
    end
end

@testset "unannotated repeated fields and duplicate names" begin
    required = MD.FieldRepetitionType.REQUIRED
    optional = MD.FieldRepetitionType.OPTIONAL
    repeated = MD.FieldRepetitionType.REPEATED
    plan = nestedcompile(MD.SchemaElement[
        nestedroot(3),
        nestedschemaelement("numbers"; physical=MD.Type.INT32,
            repetition=repeated),
        nestedschemaelement("records"; repetition=repeated, children=Int32(2)),
        nestedschemaelement("same"; physical=MD.Type.INT32,
            repetition=required),
        nestedschemaelement("same"; physical=MD.Type.INT64,
            repetition=optional),
        nestedschemaelement("holder"; repetition=required, children=Int32(2)),
        nestedschemaelement("same"; physical=MD.Type.FLOAT,
            repetition=required),
        nestedschemaelement("same"; physical=MD.Type.DOUBLE,
            repetition=required),
    ])
    numbers, records, holder = plan.root.children
    @test numbers isa Parquet._NestedListPlan
    @test numbers.annotation === :unannotated_repeated
    @test numbers.rule == 0x00
    @test numbers.parent_definition == 0
    @test numbers.present_definition == 0
    @test numbers.entry_definition == 1
    @test numbers.element.parent_definition == 1
    @test numbers.element.present_definition == 1

    @test records isa Parquet._NestedListPlan
    @test records.element isa Parquet._NestedStructPlan
    @test [child.source.element.name for child in records.element.children] ==
        ["same", "same"]
    @test records.leaf_range == Int32(2):Int32(3)
    @test holder isa Parquet._NestedStructPlan
    @test [child.source.element.name for child in holder.children] ==
        ["same", "same"]
    @test holder.leaf_range == Int32(4):Int32(5)
    @test [leaf.source.column_index for leaf in plan.leaves] ==
        Int32[1, 2, 3, 4, 5]
end

@testset "leafless groups and plan limits" begin
    required = MD.FieldRepetitionType.REQUIRED
    optional = MD.FieldRepetitionType.OPTIONAL
    repeated = MD.FieldRepetitionType.REPEATED

    emptyroot = nestedcompile(MD.SchemaElement[nestedroot(0)])
    @test isempty(emptyroot.root.children)
    @test isempty(emptyroot.root.leaf_range)
    @test isempty(emptyroot.leaves)

    onlyrequired = nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("empty"; repetition=required, children=Int32(0)),
    ])
    @test onlyrequired.root.children[1] isa Parquet._NestedStructPlan
    @test isempty(onlyrequired.root.children[1].leaf_range)
    @test isempty(onlyrequired.leaves)

    requiredempty = nestedcompile(MD.SchemaElement[
        nestedroot(2),
        nestedschemaelement("empty"; repetition=required, children=Int32(0)),
        nestedschemaelement("anchor"; physical=MD.Type.INT32,
            repetition=required),
    ])
    @test requiredempty.root.children[1] isa Parquet._NestedStructPlan
    @test isempty(requiredempty.root.children[1].leaf_range)
    @test requiredempty.root.leaf_range == Int32(1):Int32(1)

    @test_throws Parquet.FormatError nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("empty"; repetition=optional, children=Int32(0)),
    ])
    @test_throws Parquet.FormatError nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("empty"; repetition=repeated, children=Int32(0)),
    ])
    @test_throws Parquet.FormatError nestedcompile(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("outer"; repetition=optional, children=Int32(1)),
        nestedschemaelement("empty"; repetition=required, children=Int32(0)),
    ])

    schema = Parquet.Schema(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("value"; physical=MD.Type.INT32,
            repetition=required),
    ])
    error = try
        Parquet._nestedplan(schema;
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test error isa Parquet.LimitError
    @test error.resource == :container_elements
    @test error.requested == 2
    @test error.maximum == 1
end

function nestedmanualchain(depth::Int)
    depth >= 2 || throw(ArgumentError("manual schema depth must be at least two"))
    required = MD.FieldRepetitionType.REQUIRED
    path = String[]
    leaf = Parquet.SchemaNode(nestedschemaelement("leaf";
        physical=MD.Type.INT32, repetition=required), path, Int16(0),
        Int16(0), Int32(1), Parquet.SchemaNode[])
    node = leaf
    for _ in 1:(depth - 2)
        node = Parquet.SchemaNode(nestedschemaelement("group";
            repetition=required, children=Int32(1)), path, Int16(0),
            Int16(0), Int32(0), Parquet.SchemaNode[node])
    end
    root = Parquet.SchemaNode(nestedroot(1), path, Int16(0), Int16(0),
        Int32(0), Parquet.SchemaNode[node])
    return Parquet.Schema(root, Parquet.SchemaNode[leaf])
end

@testset "iterative nested limits and rollback" begin
    required = MD.FieldRepetitionType.REQUIRED
    schema = Parquet.Schema(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("value"; physical=MD.Type.INT32,
            repetition=required),
    ])
    exactdepth = Parquet._nestedplan(schema;
        limits=Parquet.Limits(max_metadata_depth=2,
            max_container_elements=2))
    @test exactdepth.plan_count == 2
    retainedlimits = Parquet.Limits()
    retainedbudget = Parquet._LiveByteBudget(retainedlimits)
    Parquet._reserve!(retainedbudget, 64)
    retained = Parquet._nestedplan(schema; limits=retainedlimits,
        budget=retainedbudget)
    expectedretained =
        Parquet._materializedarraybytes(Parquet._NestedLeafPlan, 1) +
        Parquet._materializedarraybytes(Parquet._NestedPlan, 1) +
        3 * Parquet._MATERIALIZED_OBJECT_BYTES
    @test retained.plan_count == 2
    @test Parquet._budgetused(retainedbudget) == 64 + expectedretained
    deptherror = try
        Parquet._nestedplan(schema;
            limits=Parquet.Limits(max_metadata_depth=1))
        nothing
    catch err
        err
    end
    @test deptherror isa Parquet.LimitError
    @test deptherror.resource == :metadata_depth
    @test deptherror.requested == 2
    @test deptherror.maximum == 1

    counterror = try
        Parquet._nestedplan(schema;
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test counterror isa Parquet.LimitError
    @test counterror.resource == :container_elements
    @test counterror.requested == 2
    @test counterror.maximum == 1

    path = String[]
    malformedleaf = Parquet.SchemaNode(nestedschemaelement("bad";
        physical=MD.Type.INT32), path, Int16(0), Int16(0), Int32(1),
        Parquet.SchemaNode[])
    malformedroot = Parquet.SchemaNode(nestedroot(1), path, Int16(0),
        Int16(0), Int32(0), Parquet.SchemaNode[malformedleaf])
    malformed = Parquet.Schema(malformedroot,
        Parquet.SchemaNode[malformedleaf])
    limits = Parquet.Limits(max_metadata_depth=1,
        max_container_elements=1)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    malformederror = try
        Parquet._nestedplan(malformed; limits=limits, budget=budget)
        nothing
    catch err
        err
    end
    @test malformederror isa Parquet.FormatError
    @test occursin("no valid repetition type", malformederror.message)
    @test Parquet._budgetused(budget) == 64

    listschema = Parquet.Schema(MD.SchemaElement[
        nestedroot(1),
        nestedschemaelement("values"; repetition=required,
            children=Int32(1), logical=nestedlogical(:list)),
        nestedschemaelement("array"; repetition=MD.FieldRepetitionType.REPEATED,
            children=Int32(0)),
    ])
    precedencebudget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(precedencebudget, 64)
    precedenceerror = try
        Parquet._nestedplan(listschema; limits=limits,
            budget=precedencebudget)
        nothing
    catch err
        err
    end
    @test precedenceerror isa Parquet.FormatError
    @test occursin("zero-field repeated wrapper", precedenceerror.message)
    @test Parquet._budgetused(precedencebudget) == 64

    firstleaf = Parquet.SchemaNode(nestedschemaelement("first";
        physical=MD.Type.INT32, repetition=required), path, Int16(0),
        Int16(0), Int32(1), Parquet.SchemaNode[])
    secondleaf = Parquet.SchemaNode(nestedschemaelement("second";
        physical=MD.Type.INT32, repetition=required), path, Int16(0),
        Int16(0), Int32(0), Parquet.SchemaNode[])
    partialroot = Parquet.SchemaNode(nestedroot(2), path, Int16(0), Int16(0),
        Int32(0), Parquet.SchemaNode[firstleaf, secondleaf])
    partial = Parquet.Schema(partialroot,
        Parquet.SchemaNode[firstleaf, secondleaf])
    partiallimits = Parquet.Limits()
    partialbudget = Parquet._LiveByteBudget(partiallimits)
    Parquet._reserve!(partialbudget, 64)
    @test_throws Parquet.FormatError Parquet._nestedplan(partial;
        limits=partiallimits, budget=partialbudget)
    @test Parquet._budgetused(partialbudget) == 64

    aliasedroot = Parquet.SchemaNode(nestedroot(2), path, Int16(0), Int16(0),
        Int32(0), Parquet.SchemaNode[firstleaf, firstleaf])
    aliased = Parquet.Schema(aliasedroot, Parquet.SchemaNode[firstleaf])
    aliasbudget = Parquet._LiveByteBudget(partiallimits)
    Parquet._reserve!(aliasbudget, 64)
    aliaserror = try
        Parquet._nestedplan(aliased; limits=partiallimits,
            budget=aliasbudget)
        nothing
    catch err
        err
    end
    @test aliaserror isa Parquet.FormatError
    @test occursin("more physical leaf occurrences", aliaserror.message)
    @test Parquet._budgetused(aliasbudget) == 64
end

@testset "50,000-node iterative nested plan" begin
    depth = 50_000
    schema = nestedmanualchain(depth)
    limits = Parquet.Limits(max_metadata_depth=depth,
        max_container_elements=depth, max_materialized_bytes=128 * 1024 * 1024)
    plan = Parquet._nestedplan(schema; limits=limits)
    @test plan.plan_count == depth
    @test plan.root.leaf_range == Int32(1):Int32(1)
    current::Parquet._NestedPlan = plan.root
    visited = 1
    singlechild = true
    while current isa Parquet._NestedStructPlan
        if length(current.children) != 1
            singlechild = false
            break
        end
        current = current.children[1]
        visited += 1
    end
    @test singlechild
    @test current isa Parquet._NestedLeafPlan
    @test visited == depth

    faillimits = Parquet.Limits(max_metadata_depth=depth - 1,
        max_container_elements=depth, max_materialized_bytes=128 * 1024 * 1024)
    failbudget = Parquet._LiveByteBudget(faillimits)
    Parquet._reserve!(failbudget, 64)
    deptherror = try
        Parquet._nestedplan(schema; limits=faillimits, budget=failbudget)
        nothing
    catch err
        err
    end
    @test deptherror isa Parquet.LimitError
    @test deptherror.resource == :metadata_depth
    @test deptherror.requested == depth
    @test deptherror.maximum == depth - 1
    @test Parquet._budgetused(failbudget) == 64
end
