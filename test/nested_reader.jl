using Test
import Dates

if !isdefined(Parquet, :_assemblenested)
    Base.include(Parquet, joinpath(@__DIR__, "..", "src", "nested_reader.jl"))
end

if !@isdefined(NRMD)
    const NRMD = Parquet.Metadata
end

function nrelement(name; physical=nothing, repetition=nothing,
    children=nothing, logical=nothing, converted=nothing, width=nothing)
    return NRMD.SchemaElement(name=name, type_=physical,
        repetition_type=repetition, num_children=children,
        logicalType=logical, converted_type=converted, type_length=width)
end

function nrroot(children)
    return nrelement("schema"; children=Int32(children))
end

function nrlogical(kind::Symbol)
    kind === :list && return NRMD.LogicalType(LIST=NRMD.ListType())
    kind === :map && return NRMD.LogicalType(MAP=NRMD.MapType())
    kind === :string && return NRMD.LogicalType(STRING=NRMD.StringType())
    kind === :date && return NRMD.LogicalType(DATE=NRMD.DateType())
    throw(ArgumentError("unknown nested reader logical type $kind"))
end

function nrplan(elements; limits=Parquet.Limits())
    schema = Parquet.Schema(NRMD.SchemaElement[elements...]; limits=limits)
    return Parquet._nestedplan(schema; limits=limits)
end

function nrstream(plan, index, repetitions, definitions, values; rows)
    leaf = plan.leaves[index].source
    return Parquet.LeafStream(UInt64[repetitions...], UInt64[definitions...],
        values, leaf.max_repetition_level, leaf.max_definition_level;
        expected_rows=rows)
end

@testset "nested reader structs and logical leaves" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    optional = NRMD.FieldRepetitionType.OPTIONAL
    elements = NRMD.SchemaElement[
        nrroot(3),
        nrelement("id"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("record"; repetition=optional, children=Int32(1)),
        nrelement("name"; physical=NRMD.Type.BYTE_ARRAY,
            repetition=required, logical=nrlogical(:string)),
        nrelement("day"; physical=NRMD.Type.INT32, repetition=optional,
            logical=nrlogical(:date)),
    ]
    plan = nrplan(elements)
    streams = Parquet.LeafStream[
        nrstream(plan, 1, [0, 0, 0], [0, 0, 0], Int32[1, 2, 3]; rows=3),
        nrstream(plan, 2, [0, 0, 0], [1, 0, 1],
            [UInt8[0x61], UInt8[0x63]]; rows=3),
        nrstream(plan, 3, [0, 0, 0], [1, 0, 1], Int32[0, 2]; rows=3),
    ]
    result = Parquet._assemblenested(plan, streams, 3)
    @test result isa Parquet.StructVector
    @test length(result) == 3
    @test result[1]["id"] == 1
    @test result[2]["record"] === missing
    @test result[3]["record"]["name"] == "c"
    @test result[1]["day"] == Dates.Date(1970, 1, 1)
    @test result[2]["day"] === missing
    @test eltype(result.children[2]) == Union{Missing,Parquet.StructValue}
    @test result.children[2].ranks == Int32[0, 1, 1, 2]
    @test eltype(result.children[2].children[1]) == Parquet.DataStrings.DataString

    empty = Parquet._assemblenested(plan, Parquet.LeafStream[
        nrstream(plan, 1, Int[], Int[], Int32[]; rows=0),
        nrstream(plan, 2, Int[], Int[], Vector{UInt8}[]; rows=0),
        nrstream(plan, 3, Int[], Int[], Int32[]; rows=0),
    ], 0)
    @test isempty(empty)
    @test length(empty.children) == 3
end

@testset "nested reader physical leaf types" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    elements = NRMD.SchemaElement[
        nrroot(7),
        nrelement("flag"; physical=NRMD.Type.BOOLEAN, repetition=required),
        nrelement("small"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("large"; physical=NRMD.Type.INT64, repetition=required),
        nrelement("single"; physical=NRMD.Type.FLOAT, repetition=required),
        nrelement("double"; physical=NRMD.Type.DOUBLE, repetition=required),
        nrelement("bytes"; physical=NRMD.Type.BYTE_ARRAY, repetition=required),
        nrelement("fixed"; physical=NRMD.Type.FIXED_LEN_BYTE_ARRAY,
            repetition=required, width=Int32(2)),
    ]
    plan = nrplan(elements)
    streams = Parquet.LeafStream[
        nrstream(plan, 1, [0], [0], Bool[true]; rows=1),
        nrstream(plan, 2, [0], [0], Int32[2]; rows=1),
        nrstream(plan, 3, [0], [0], Int64[3]; rows=1),
        nrstream(plan, 4, [0], [0], Float32[4]; rows=1),
        nrstream(plan, 5, [0], [0], Float64[5]; rows=1),
        nrstream(plan, 6, [0], [0], [UInt8[0x06]]; rows=1),
        nrstream(plan, 7, [0], [0], [UInt8[0x07, 0x08]]; rows=1),
    ]
    result = Parquet._assemblenested(plan, streams, 1)
    @test [result[1][index] for index in 1:5] == Any[true, 2, 3, 4, 5]
    @test result[1][6] == UInt8[0x06]
    @test result.children[7] isa Parquet.FixedByteArrayVector
    @test result[1][7] == UInt8[0x07, 0x08]

    wrongtype = copy(streams)
    wrongtype[2] = nrstream(plan, 2, [0], [0], Any["wrong"]; rows=1)
    @test_throws Parquet.FormatError Parquet._assemblenested(
        plan, wrongtype, 1)
    wrongwidth = copy(streams)
    wrongwidth[7] = nrstream(plan, 7, [0], [0], [UInt8[0x07]]; rows=1)
    @test_throws Parquet.FormatError Parquet._assemblenested(
        plan, wrongwidth, 1)
end

@testset "nested reader list null, empty, and present states" begin
    optional = NRMD.FieldRepetitionType.OPTIONAL
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("items"; repetition=optional, children=Int32(1),
            logical=nrlogical(:list), converted=NRMD.ConvertedType.LIST),
        nrelement("list"; repetition=repeated, children=Int32(1)),
        nrelement("element"; physical=NRMD.Type.INT32, repetition=optional),
    ]
    plan = nrplan(elements)
    stream = nrstream(plan, 1, [0, 0, 0, 1, 1, 0],
        [0, 1, 2, 3, 3, 3], Int32[10, 20, 30]; rows=4)
    result = Parquet._assemblenested(plan, [stream], 4)
    column = result.children[1]
    @test column isa Parquet.ListVector
    @test column[1] === missing
    @test collect(column[2]) == Int32[]
    @test isequal(collect(column[3]), Union{Missing,Int32}[missing, 10, 20])
    @test collect(column[4]) == Int32[30]
    @test column.offsets == Int32[0, 0, 0, 3, 4]
    @test column.validity == Bool[false, true, true, true]

    continuation = nrstream(plan, 1, [0, 1, 0], [0, 3, 1], Int32[1]; rows=2)
    @test_throws Parquet.FormatError Parquet._assemblenested(
        plan, [continuation], 2)

    altered = nrstream(plan, 1, [0], [3], Int32[1]; rows=1)
    push!(altered.values, Int32(2))
    @test_throws Parquet.FormatError Parquet._assemblenested(plan, [altered], 1)
end

@testset "nested reader list of list" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("outer"; repetition=required, children=Int32(1),
            logical=nrlogical(:list)),
        nrelement("inner"; repetition=repeated, children=Int32(1),
            logical=nrlogical(:list)),
        nrelement("element"; physical=NRMD.Type.INT32,
            repetition=repeated),
    ]
    plan = nrplan(elements)
    stream = nrstream(plan, 1, [0, 0, 1, 2, 1], [0, 1, 2, 2, 2],
        Int32[1, 2, 3]; rows=2)
    result = Parquet._assemblenested(plan, [stream], 2)
    column = result.children[1]
    @test collect(column[1]) == Parquet.ListValue{Int32}[]
    @test [collect(item) for item in column[2]] ==
        [Int32[], Int32[1, 2], Int32[3]]
    @test column.values isa Parquet.ListVector
end

@testset "nested reader unannotated repeated boundary" begin
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("items"; physical=NRMD.Type.INT32, repetition=repeated),
    ]
    plan = nrplan(elements)
    @test plan.root.children[1].annotation === :unannotated_repeated
    stream = nrstream(plan, 1, [0, 0, 1, 0], [0, 1, 1, 1],
        Int32[1, 2, 3]; rows=3)
    result = Parquet._assemblenested(plan, [stream], 3)
    @test isempty(result[1]["items"])
    @test collect(result[2]["items"]) == Int32[1, 2]
    @test collect(result[3]["items"]) == Int32[3]

    continuation = nrstream(plan, 1, [0, 1, 0], [0, 1, 0],
        Int32[1]; rows=2)
    @test_throws Parquet.FormatError Parquet._assemblenested(
        plan, [continuation], 2)
end

@testset "nested reader list of structs and sibling alignment" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    optional = NRMD.FieldRepetitionType.OPTIONAL
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("rows"; repetition=required, children=Int32(1),
            logical=nrlogical(:list)),
        nrelement("list"; repetition=repeated, children=Int32(2)),
        nrelement("x"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("x"; physical=NRMD.Type.BYTE_ARRAY, repetition=optional,
            logical=nrlogical(:string)),
    ]
    plan = nrplan(elements)
    left = nrstream(plan, 1, [0, 0, 1, 0], [0, 1, 1, 1],
        Int32[1, 2, 3]; rows=3)
    right = nrstream(plan, 2, [0, 0, 1, 0], [0, 2, 1, 2],
        [UInt8[0x61], UInt8[0x63]]; rows=3)
    result = Parquet._assemblenested(plan, [left, right], 3)
    column = result.children[1]
    @test isempty(column[1])
    @test length(column[2]) == 2
    @test column[2][1][1] == 1
    @test column[2][1][2] == "a"
    @test column[2][2][2] === missing
    @test_throws ArgumentError column[2][1]["x"]
    @test column[3][1][1] == 3

    short = nrstream(plan, 2, [0, 0, 0], [0, 2, 2],
        [UInt8[0x61], UInt8[0x63]]; rows=3)
    @test_throws Parquet.FormatError Parquet._assemblenested(
        plan, [left, short], 3)

    disagreement = nrstream(plan, 2, [0, 0, 1, 0], [0, 0, 1, 2],
        [UInt8[0x63]]; rows=3)
    @test_throws Parquet.FormatError Parquet._assemblenested(
        plan, [left, disagreement], 3)
end

@testset "nested reader struct with list" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    optional = NRMD.FieldRepetitionType.OPTIONAL
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("record"; repetition=optional, children=Int32(2)),
        nrelement("id"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("items"; repetition=optional, children=Int32(1),
            logical=nrlogical(:list)),
        nrelement("list"; repetition=repeated, children=Int32(1)),
        nrelement("element"; physical=NRMD.Type.INT32, repetition=optional),
    ]
    plan = nrplan(elements)
    id = nrstream(plan, 1, [0, 0, 0, 0], [0, 1, 1, 1],
        Int32[2, 3, 4]; rows=4)
    items = nrstream(plan, 2, [0, 0, 0, 0, 1], [0, 1, 2, 3, 4],
        Int32[7]; rows=4)
    result = Parquet._assemblenested(plan, [id, items], 4)
    record = result.children[1]
    @test record[1] === missing
    @test record[2]["id"] == 2
    @test record[2]["items"] === missing
    @test isempty(record[3]["items"])
    @test isequal(collect(record[4]["items"]),
        Union{Missing,Int32}[missing, 7])
end

@testset "nested reader maps, duplicates, and omitted values" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    optional = NRMD.FieldRepetitionType.OPTIONAL
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("lookup"; repetition=optional, children=Int32(1),
            logical=nrlogical(:map), converted=NRMD.ConvertedType.MAP),
        nrelement("key_value"; repetition=repeated, children=Int32(2)),
        nrelement("key"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("value"; physical=NRMD.Type.BYTE_ARRAY,
            repetition=optional, logical=nrlogical(:string)),
    ]
    plan = nrplan(elements)
    keys = nrstream(plan, 1, [0, 0, 0, 1, 1, 0], [0, 1, 2, 2, 2, 2],
        Int32[1, 1, 2, 3]; rows=4)
    values = nrstream(plan, 2, [0, 0, 0, 1, 1, 0], [0, 1, 3, 2, 3, 3],
        [UInt8[0x61], UInt8[0x62], UInt8[0x63]]; rows=4)
    result = Parquet._assemblenested(plan, [keys, values], 4)
    lookup = result.children[1]
    @test lookup[1] === missing
    @test isempty(lookup[2])
    @test isequal(collect(lookup[3]), Pair{Int32,Union{Missing,String}}[
        1 => "a", 1 => missing, 2 => "b"])
    @test Parquet.maplookup(lookup[3], 1) === missing
    @test isequal(Dict(lookup[3]), Dict{Int32,Union{Missing,String}}(
        1 => missing, 2 => "b"))
    @test collect(lookup[4]) == Pair{Int32,Union{Missing,String}}[3 => "c"]

    omittedelements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("lookup"; repetition=required, children=Int32(1),
            logical=nrlogical(:map)),
        nrelement("entries"; repetition=repeated, children=Int32(1),
            converted=NRMD.ConvertedType.MAP_KEY_VALUE),
        nrelement("key"; physical=NRMD.Type.INT32, repetition=required),
    ]
    omittedplan = nrplan(omittedelements)
    omittedkeys = nrstream(omittedplan, 1, [0, 1], [1, 1],
        Int32[4, 5]; rows=1)
    omitted = Parquet._assemblenested(omittedplan, [omittedkeys], 1)
    @test isequal(collect(omitted[1]["lookup"]), Pair{Int32,Missing}[
        4 => missing, 5 => missing])
end

@testset "nested reader map of struct and optional key rejection" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    optional = NRMD.FieldRepetitionType.OPTIONAL
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("lookup"; repetition=required, children=Int32(1),
            logical=nrlogical(:map)),
        nrelement("entries"; repetition=repeated, children=Int32(2)),
        nrelement("key"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("value"; repetition=optional, children=Int32(2)),
        nrelement("left"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("right"; physical=NRMD.Type.BYTE_ARRAY,
            repetition=optional, logical=nrlogical(:string)),
    ]
    plan = nrplan(elements)
    keys = nrstream(plan, 1, [0, 0, 1], [0, 1, 1], Int32[1, 2]; rows=2)
    left = nrstream(plan, 2, [0, 0, 1], [0, 2, 1], Int32[10]; rows=2)
    right = nrstream(plan, 3, [0, 0, 1], [0, 3, 1],
        [UInt8[0x78]]; rows=2)
    result = Parquet._assemblenested(plan, [keys, left, right], 2)
    lookup = result[2]["lookup"]
    @test lookup[1].first == 1
    @test lookup[1].second["left"] == 10
    @test lookup[1].second["right"] == "x"
    @test isequal(lookup[2], 2 => missing)

    optionalkeyelements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("lookup"; repetition=required, children=Int32(1),
            logical=nrlogical(:map)),
        nrelement("entries"; repetition=repeated, children=Int32(2)),
        nrelement("key"; physical=NRMD.Type.INT32, repetition=optional),
        nrelement("value"; physical=NRMD.Type.INT32, repetition=optional),
    ]
    optionalkeyplan = nrplan(optionalkeyelements)
    nullkey = nrstream(optionalkeyplan, 1, [0], [1], Int32[]; rows=1)
    nullvalue = nrstream(optionalkeyplan, 2, [0], [1], Int32[]; rows=1)
    @test_throws Parquet.FormatError Parquet._assemblenested(
        optionalkeyplan, [nullkey, nullvalue], 1)

    presentkey = nrstream(optionalkeyplan, 1, [0], [2], Int32[7]; rows=1)
    present = Parquet._assemblenested(optionalkeyplan,
        [presentkey, nullvalue], 1)
    @test !(Missing <: eltype(present.children[1].keys))
    @test isequal(collect(present[1]["lookup"]),
        Pair{Int32,Union{Missing,Int32}}[7 => missing])
end

@testset "nested reader malformed alignment and resource order" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    optional = NRMD.FieldRepetitionType.OPTIONAL
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("record"; repetition=optional, children=Int32(2)),
        nrelement("left"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("right"; physical=NRMD.Type.INT32, repetition=required),
    ]
    plan = nrplan(elements)
    left = nrstream(plan, 1, [0], [0], Int32[]; rows=1)
    right = nrstream(plan, 2, [0], [1], Int32[1]; rows=1)
    tiny = Parquet.Limits(max_materialized_bytes=1)
    @test_throws Parquet.LimitError Parquet._assemblenested(
        plan, [left, right], 1; limits=tiny,
        budget=Parquet._LiveByteBudget(tiny))

    validleft = nrstream(plan, 1, [0], [1], Int32[1]; rows=1)
    passbytes = Parquet._nestedreadpassbytes(plan)
    passlimits = Parquet.Limits(max_materialized_bytes=passbytes)
    invalidbudget = Parquet._LiveByteBudget(passlimits)
    @test_throws Parquet.FormatError Parquet._assemblenested(
        plan, [left, right], 1; limits=passlimits,
        budget=invalidbudget)
    @test Parquet._budgetused(invalidbudget) == 0
    finalbudget = Parquet._LiveByteBudget(passlimits)
    @test_throws Parquet.LimitError Parquet._assemblenested(
        plan, [validleft, right], 1; limits=passlimits,
        budget=finalbudget)
    @test Parquet._budgetused(finalbudget) == 0
    @test_throws Parquet.FormatError Parquet._assemblenested(
        plan, [validleft], 1)
    @test_throws Parquet.FormatError Parquet._assemblenested(
        plan, [validleft, right], 2)

    budget = Parquet._LiveByteBudget(Parquet.Limits())
    output = Parquet._assemblenested(plan, [validleft, right], 1;
        budget=budget)
    @test output[1]["record"]["left"] == 1
    @test Parquet._budgetused(budget) > 0
end

@testset "nested reader required leafless structs" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("empty"; repetition=required, children=Int32(0)),
    ]
    plan = nrplan(elements)
    result = Parquet._assemblenested(plan, Parquet.LeafStream[], 3)
    @test length(result) == 3
    @test length(result[1]["empty"]) == 0
    @test result.children[1].rows == 3
end

@testset "nested reader list of maps, Bool keys, and zero rows" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    optional = NRMD.FieldRepetitionType.OPTIONAL
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("outer"; repetition=optional, children=Int32(1),
            logical=nrlogical(:list)),
        nrelement("list"; repetition=repeated, children=Int32(1)),
        nrelement("element"; repetition=required, children=Int32(1),
            logical=nrlogical(:map)),
        nrelement("key_value"; repetition=repeated, children=Int32(2)),
        nrelement("key"; physical=NRMD.Type.BOOLEAN, repetition=required),
        nrelement("value"; physical=NRMD.Type.INT32, repetition=optional),
    ]
    plan = nrplan(elements)
    repetitions = [0, 0, 0, 1, 2, 0]
    keys = nrstream(plan, 1, repetitions, [0, 1, 2, 3, 3, 3],
        Bool[true, false, true]; rows=4)
    values = nrstream(plan, 2, repetitions, [0, 1, 2, 4, 3, 4],
        Int32[10, 20]; rows=4)
    result = Parquet._assemblenested(plan, [keys, values], 4)
    outer = result.children[1]
    @test outer[1] === missing
    @test isempty(outer[2])
    @test isempty(outer[3][1])
    @test isequal(collect(outer[3][2]), Pair{Bool,Union{Missing,Int32}}[
        true => 10, false => missing])
    @test collect(outer[4][1]) == Pair{Bool,Union{Missing,Int32}}[true => 20]
    @test outer.values.keys isa Vector{Bool}
    @test Parquet.maplookup(outer[3][2], false) === missing
    @test isequal(Dict(outer[3][2]),
        Dict{Bool,Union{Missing,Int32}}(true => 10, false => missing))

    emptykeys = nrstream(plan, 1, Int[], Int[], Bool[]; rows=0)
    emptyvalues = nrstream(plan, 2, Int[], Int[], Int32[]; rows=0)
    empty = Parquet._assemblenested(plan, [emptykeys, emptyvalues], 0)
    @test isempty(empty)
    @test isempty(empty.children[1])
    @test isempty(empty.children[1].values)
    @test isempty(empty.children[1].values.keys)
end

@testset "nested reader complex map keys" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("lookup"; repetition=required, children=Int32(1),
            logical=nrlogical(:map)),
        nrelement("entries"; repetition=repeated, children=Int32(2)),
        nrelement("key"; repetition=required, children=Int32(2)),
        nrelement("id"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("items"; repetition=required, children=Int32(1),
            logical=nrlogical(:list)),
        nrelement("list"; repetition=repeated, children=Int32(1)),
        nrelement("element"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("value"; physical=NRMD.Type.INT32, repetition=required),
    ]
    plan = nrplan(elements)
    ids = nrstream(plan, 1, [0, 1], [1, 1], Int32[1, 2]; rows=1)
    items = nrstream(plan, 2, [0, 2, 1], [2, 2, 1],
        Int32[10, 11]; rows=1)
    values = nrstream(plan, 3, [0, 1], [1, 1], Int32[100, 200]; rows=1)
    result = Parquet._assemblenested(plan, [ids, items, values], 1)
    lookup = result[1]["lookup"]
    @test length(lookup) == 2
    @test lookup[1].first["id"] == 1
    @test collect(lookup[1].first["items"]) == Int32[10, 11]
    @test isempty(lookup[2].first["items"])
    dictionary = Dict(lookup)
    fresh = Parquet.StructVector(["id", "items"],
        (Int32[1], Parquet.ListVector(Int32[0, 2], Int32[10, 11])))[1]
    @test dictionary[fresh] == 100
    lookup.keys.children[1][1] = Int32(9)
    lookup.keys.children[2].values[1] = Int32(99)
    @test dictionary[fresh] == 100
    @test !haskey(dictionary, lookup[1].first)
end

@testset "nested reader special-name legacy lists" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    optional = NRMD.FieldRepetitionType.OPTIONAL
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(2),
        nrelement("items"; repetition=required, children=Int32(1),
            converted=NRMD.ConvertedType.LIST),
        nrelement("array"; repetition=repeated, children=Int32(1)),
        nrelement("value"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("pairs"; repetition=required, children=Int32(1),
            converted=NRMD.ConvertedType.LIST),
        nrelement("pairs_tuple"; repetition=repeated, children=Int32(1)),
        nrelement("member"; physical=NRMD.Type.INT32, repetition=optional),
    ]
    plan = nrplan(elements)
    @test plan.root.children[1].rule == UInt8(4)
    @test plan.root.children[2].rule == UInt8(5)
    items = nrstream(plan, 1, [0, 0, 1], [0, 1, 1],
        Int32[10, 20]; rows=2)
    pairs = nrstream(plan, 2, [0, 0], [1, 2], Int32[7]; rows=2)
    result = Parquet._assemblenested(plan, [items, pairs], 2)
    @test isempty(result[1]["items"])
    @test length(result[2]["items"]) == 2
    @test result[2]["items"][1]["value"] == 10
    @test result[2]["items"][2]["value"] == 20
    @test result[1]["pairs"][1]["member"] === missing
    @test result[2]["pairs"][1]["member"] == 7
end

@testset "nested reader repetition projection and all-null required struct" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    optional = NRMD.FieldRepetitionType.OPTIONAL
    repeated = NRMD.FieldRepetitionType.REPEATED
    projectionelements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("record"; repetition=required, children=Int32(2)),
        nrelement("left"; repetition=required, children=Int32(1),
            logical=nrlogical(:list)),
        nrelement("list"; repetition=repeated, children=Int32(1)),
        nrelement("element"; physical=NRMD.Type.INT32, repetition=required),
        nrelement("right"; repetition=required, children=Int32(1),
            logical=nrlogical(:list)),
        nrelement("list"; repetition=repeated, children=Int32(1)),
        nrelement("element"; physical=NRMD.Type.INT32, repetition=required),
    ]
    projectionplan = nrplan(projectionelements)
    left = nrstream(projectionplan, 1, [0, 1, 0], [1, 1, 0],
        Int32[1, 2]; rows=2)
    right = nrstream(projectionplan, 2, [0, 0, 1], [1, 1, 1],
        Int32[10, 20, 30]; rows=2)
    projected = Parquet._assemblenested(projectionplan, [left, right], 2)
    @test collect(projected[1]["record"]["left"]) == Int32[1, 2]
    @test collect(projected[1]["record"]["right"]) == Int32[10]
    @test isempty(projected[2]["record"]["left"])
    @test collect(projected[2]["record"]["right"]) == Int32[20, 30]

    nullelements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("record"; repetition=required, children=Int32(2)),
        nrelement("day"; physical=NRMD.Type.INT32, repetition=optional,
            logical=nrlogical(:date)),
        nrelement("name"; physical=NRMD.Type.BYTE_ARRAY, repetition=optional,
            logical=nrlogical(:string)),
    ]
    nullplan = nrplan(nullelements)
    day = nrstream(nullplan, 1, [0], [0], Int32[]; rows=1)
    name = nrstream(nullplan, 2, [0], [0], Vector{UInt8}[]; rows=1)
    allnull = Parquet._assemblenested(nullplan, [day, name], 1)
    @test allnull[1]["record"] isa Parquet.StructValue
    @test allnull[1]["record"]["day"] === missing
    @test allnull[1]["record"]["name"] === missing
    @test eltype(allnull.children[1].children[1]) == Union{Missing,Dates.Date}
    @test eltype(allnull.children[1].children[2]) == Union{Missing,Parquet.DataStrings.DataString}
end

@testset "nested reader metadata depth 128" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    repeated = NRMD.FieldRepetitionType.REPEATED
    elements = NRMD.SchemaElement[
        nrroot(1),
        nrelement("outer"; repetition=required, children=Int32(1),
            logical=nrlogical(:list)),
    ]
    for depth in 1:125
        push!(elements, nrelement("group_$depth"; repetition=repeated,
            children=Int32(1), logical=nrlogical(:list)))
    end
    push!(elements, nrelement("element"; physical=NRMD.Type.INT32,
        repetition=repeated))
    plan = nrplan(elements)
    leaf = plan.leaves[1].source
    @test length(elements) == 128
    @test length(leaf.path) == 127
    @test leaf.max_definition_level == Int16(126)
    @test leaf.max_repetition_level == Int16(126)
    stream = nrstream(plan, 1, [0], [leaf.max_definition_level],
        Int32[1]; rows=1)
    result = Parquet._assemblenested(plan, [stream], 1)
    column = result.children[1]
    typerepr = sprint(show, typeof(column))
    @test count("ListVector", typerepr) == 1
    @test ncodeunits(typerepr) < 16 * 1024
    value = result[1]["outer"]
    for _ in 1:126
        value = only(value)
    end
    @test value == Int32(1)
end

@testset "nested reader raw and logical byte ownership" begin
    required = NRMD.FieldRepetitionType.REQUIRED
    elements = NRMD.SchemaElement[
        nrroot(2),
        nrelement("raw"; physical=NRMD.Type.BYTE_ARRAY, repetition=required),
        nrelement("text"; physical=NRMD.Type.BYTE_ARRAY, repetition=required,
            logical=nrlogical(:string)),
    ]
    plan = nrplan(elements)
    rawbytes = UInt8[0x61]
    textbytes = UInt8[0x62]
    raw = nrstream(plan, 1, [0], [0], [rawbytes]; rows=1)
    text = nrstream(plan, 2, [0], [0], [textbytes]; rows=1)
    result = Parquet._assemblenested(plan, [raw, text], 1)
    @test result[1]["raw"] === rawbytes
    @test result[1]["text"] == "b"
    rawbytes[1] = 0x63
    textbytes[1] = 0x64
    @test result[1]["raw"] == UInt8[0x63]
    @test result[1]["text"] == "b"
end

@testset "deep plans are rejected before recursive assembly" begin
    # A plan deeper than the reader's recursion cap must fail with a clean
    # LimitError instead of overflowing the stack. The guard is checked before
    # any recursion, so this test never recurses to the cap depth itself.
    maxdepth = Parquet._NESTED_READ_MAX_DEPTH
    @test maxdepth == 1024
    required = NRMD.FieldRepetitionType.REQUIRED
    limits = Parquet.Limits(max_metadata_depth=maxdepth + 8,
        max_container_elements=maxdepth + 8)
    function nrdeepchain(depth)
        elements = NRMD.SchemaElement[nrroot(1)]
        for _ in 1:(depth - 2)
            push!(elements, nrelement("group"; repetition=required,
                children=Int32(1)))
        end
        push!(elements, nrelement("leaf"; physical=NRMD.Type.INT32,
            repetition=required))
        return nrplan(elements; limits=limits)
    end
    deepplan = nrdeepchain(maxdepth + 1)
    @test deepplan.depth == maxdepth + 1
    deepstreams = Parquet.LeafStream[
        nrstream(deepplan, 1, [0], [0], Int32[7]; rows=1)]
    deeperror = try
        Parquet._assemblenested(deepplan, deepstreams, 1; limits=limits)
        nothing
    catch err
        err
    end
    @test deeperror isa Parquet.LimitError
    @test deeperror.resource == :nested_read_depth

    # A plan at the cap still assembles (moderate depth exercises the recursion).
    okplan = nrdeepchain(64)
    @test okplan.depth <= maxdepth
    result = Parquet._assemblenested(okplan,
        Parquet.LeafStream[nrstream(okplan, 1, [0], [0], Int32[7]; rows=1)], 1;
        limits=limits)
    @test length(result) == 1
end
