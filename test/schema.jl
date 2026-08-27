if !@isdefined(TH)
    const TH = Parquet.Thrift
end
if !@isdefined(MD)
    const MD = Parquet.Metadata
end

function schemaelement(name; type=nothing, repetition=nothing, children=nothing, length=nothing)
    return MD.SchemaElement(name=name, type_=type, repetition_type=repetition,
        num_children=children, type_length=length)
end

@testset "schema tree and levels" begin
    elements = [
        schemaelement("root"; children=Int32(3)),
        schemaelement("id"; type=MD.Type.INT64, repetition=MD.FieldRepetitionType.REQUIRED),
        schemaelement("items"; repetition=MD.FieldRepetitionType.OPTIONAL, children=Int32(1)),
        schemaelement("list"; repetition=MD.FieldRepetitionType.REPEATED, children=Int32(1)),
        schemaelement("element"; type=MD.Type.BYTE_ARRAY, repetition=MD.FieldRepetitionType.OPTIONAL),
        schemaelement("pair"; repetition=MD.FieldRepetitionType.REPEATED, children=Int32(2)),
        schemaelement("key"; type=MD.Type.BYTE_ARRAY, repetition=MD.FieldRepetitionType.REQUIRED),
        schemaelement("value"; type=MD.Type.INT32, repetition=MD.FieldRepetitionType.OPTIONAL),
    ]
    schema = Parquet.Schema(elements)
    @test length(schema.root.children) == 3
    @test length(schema.leaves) == 4
    @test [leaf.column_index for leaf in schema.leaves] == Int32[1, 2, 3, 4]
    @test schema.leaves[1].path == ["id"]
    @test schema.leaves[2].path == ["items", "list", "element"]
    @test schema.leaves[2].max_definition_level == 3
    @test schema.leaves[2].max_repetition_level == 1
    @test schema.leaves[3].path == ["pair", "key"]
    @test schema.leaves[3].max_definition_level == 1
    @test schema.leaves[3].max_repetition_level == 1
    @test schema.leaves[4].max_definition_level == 2
    @test schema.root.path == String[] && schema.root.column_index == 0
end

@testset "schema validation" begin
    required = MD.FieldRepetitionType.REQUIRED
    @test_throws Parquet.FormatError Parquet.Schema(MD.SchemaElement[])
    @test_throws Parquet.FormatError Parquet.Schema([schemaelement("root"; type=MD.Type.INT32)])
    @test_throws Parquet.FormatError Parquet.Schema([schemaelement("root")])
    @test Parquet.Schema([
        schemaelement("root"; repetition=MD.FieldRepetitionType.REQUIRED,
            children=Int32(0)),
    ]).root.element.repetition_type == MD.FieldRepetitionType.REQUIRED
    for repetition in (MD.FieldRepetitionType.OPTIONAL, MD.FieldRepetitionType.REPEATED,
            MD.FieldRepetitionType.T(Int32(99)))
        @test_throws Parquet.FormatError Parquet.Schema([
            schemaelement("root"; repetition=repetition, children=Int32(0)),
        ])
    end
    @test_throws Parquet.FormatError Parquet.Schema([schemaelement("root"; children=Int32(-1))])
    @test_throws Parquet.FormatError Parquet.Schema([
        schemaelement("root"; children=Int32(1)),
        schemaelement("leaf"; type=MD.Type.INT32),
    ])
    @test_throws Parquet.FormatError Parquet.Schema([
        schemaelement("root"; children=Int32(1)),
        schemaelement("leaf"; type=MD.Type.INT32, repetition=required, children=Int32(1)),
    ])
    @test_throws Parquet.FormatError Parquet.Schema([
        schemaelement("root"; children=Int32(1)),
        schemaelement("fixed"; type=MD.Type.FIXED_LEN_BYTE_ARRAY, repetition=required),
    ])
    oversized = [
        schemaelement("root"; children=Int32(1)),
        schemaelement("fixed"; type=MD.Type.FIXED_LEN_BYTE_ARRAY,
            repetition=required, length=Int32(1024)),
    ]
    error = try
        Parquet.Schema(oversized; limits=Parquet.Limits(max_string_bytes=16))
        nothing
    catch err
        err
    end
    @test error isa Parquet.LimitError
    @test error.resource == :string_bytes
    @test error.requested == 1024
    @test error.maximum == 16
    @test_throws Parquet.FormatError Parquet.Schema([
        schemaelement("root"; children=Int32(2)),
        schemaelement("leaf"; type=MD.Type.INT32, repetition=required),
    ])
    malformedbudget = Parquet._LiveByteBudget(Parquet.Limits())
    @test_throws Parquet.FormatError Parquet.Schema([
        schemaelement("root"; children=Int32(1_000_000)),
    ]; budget=malformedbudget)
    @test Parquet._budgetused(malformedbudget) < 1024
    @test_throws Parquet.FormatError Parquet.Schema([
        schemaelement("root"; children=Int32(0)),
        schemaelement("extra"; type=MD.Type.INT32, repetition=required),
    ])
    @test_throws Parquet.LimitError Parquet.Schema([
        schemaelement("root"; children=Int32(1)),
        schemaelement("group"; repetition=required, children=Int32(1)),
        schemaelement("leaf"; type=MD.Type.INT32, repetition=required),
    ]; limits=Parquet.Limits(max_metadata_depth=2))
end

@testset "iterative schema limits and rollback" begin
    required = MD.FieldRepetitionType.REQUIRED
    chain = MD.SchemaElement[
        schemaelement("root"; children=Int32(1)),
        schemaelement("group"; repetition=required, children=Int32(1)),
        schemaelement("leaf"; type=MD.Type.INT32, repetition=required),
    ]
    exactdepth = Parquet.Schema(chain;
        limits=Parquet.Limits(max_metadata_depth=3))
    @test exactdepth.leaves[1].path == ["group", "leaf"]
    deptherror = try
        Parquet.Schema(chain; limits=Parquet.Limits(max_metadata_depth=2))
        nothing
    catch err
        err
    end
    @test deptherror isa Parquet.LimitError
    @test deptherror.resource == :metadata_depth
    @test deptherror.requested == 3
    @test deptherror.maximum == 2

    @test length(Parquet.Schema(chain;
        limits=Parquet.Limits(max_container_elements=3)).leaves) == 1
    nodeerror = try
        Parquet.Schema(chain;
            limits=Parquet.Limits(max_container_elements=2))
        nothing
    catch err
        err
    end
    @test nodeerror isa Parquet.LimitError
    @test nodeerror.resource == :container_elements
    @test nodeerror.requested == 3
    @test nodeerror.maximum == 2

    retainedlimits = Parquet.Limits()
    retainedbudget = Parquet._LiveByteBudget(retainedlimits)
    Parquet._reserve!(retainedbudget, 64)
    retained = Parquet.Schema(MD.SchemaElement[
        schemaelement("root"; children=Int32(1)),
        schemaelement("leaf"; type=MD.Type.INT32, repetition=required),
    ]; limits=retainedlimits, budget=retainedbudget)
    expectedretained =
        Parquet._materializedarraybytes(Parquet.SchemaNode, 2) +
        Parquet._materializedarraybytes(String, 0) +
        Parquet._materializedarraybytes(Parquet.SchemaNode, 1) +
        Parquet._materializedarraybytes(String, 1) +
        Parquet._materializedarraybytes(Parquet.SchemaNode, 0) +
        3 * Parquet._MATERIALIZED_OBJECT_BYTES
    @test retained.leaves[1].column_index == 1
    @test Parquet._budgetused(retainedbudget) == 64 + expectedretained

    unclaimed = MD.SchemaElement[
        schemaelement("root"; children=Int32(0)),
        schemaelement("extra"; type=MD.Type.INT32, repetition=required),
    ]
    unclaimederror = try
        Parquet.Schema(unclaimed;
            limits=Parquet.Limits(max_container_elements=1))
        nothing
    catch err
        err
    end
    @test unclaimederror isa Parquet.FormatError
    @test occursin("unclaimed elements", unclaimederror.message)

    malformed = MD.SchemaElement[
        schemaelement("root"; children=Int32(1)),
        schemaelement("bad"; repetition=required, children=Int32(1)),
    ]
    limits = Parquet.Limits(max_metadata_depth=1)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, 64)
    malformederror = try
        Parquet.Schema(malformed; limits=limits, budget=budget)
        nothing
    catch err
        err
    end
    @test malformederror isa Parquet.FormatError
    @test occursin("declares more direct children", malformederror.message)
    @test Parquet._budgetused(budget) == 64

    limitbudget = Parquet._LiveByteBudget(Parquet.Limits(
        max_metadata_depth=2))
    Parquet._reserve!(limitbudget, 64)
    @test_throws Parquet.LimitError Parquet.Schema(chain;
        limits=Parquet.Limits(max_metadata_depth=2), budget=limitbudget)
    @test Parquet._budgetused(limitbudget) == 64
end

@testset "official corpus schemas" begin
    corpus = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))
    datadir = joinpath(corpus, "data")
    if !isdir(datadir)
        @warn "parquet-testing corpus not found; skipping schema corpus tests" corpus
    else
        count = 0
        for name in sort(readdir(datadir))
            endswith(name, ".parquet") || continue
            file = Parquet.File(joinpath(datadir, name))
            metadata = TH.decode(file.footer.bytes, MD.FileMetaData)
            close(file)
            schema = Parquet.Schema(metadata)
            @test !isempty(schema.leaves)
            @test all(length(group.columns) == length(schema.leaves) for group in metadata.row_groups)
            count += 1
        end
        @test count >= 65
    end
end
