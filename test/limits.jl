@testset "shared live-byte budget" begin
    limits = Parquet.Limits(max_materialized_bytes=10)
    budget = Parquet._LiveByteBudget(limits)
    @test Parquet._budgetused(budget) == 0
    Parquet._reserve!(budget, 4)
    @test Parquet._budgetused(budget) == 4
    @test_throws Parquet.LimitError Parquet._reserve!(budget, 7)
    @test Parquet._budgetused(budget) == 4
    Parquet._release!(budget, 3)
    @test Parquet._budgetused(budget) == 1
    @test_throws ArgumentError Parquet._reserve!(budget, -1)
    @test_throws ArgumentError Parquet._release!(budget, 2)
    @test Parquet._budgetused(budget) == 1
end

@testset "bounded schema-name interning" begin
    before = Parquet._internedschemanamebytes()
    name = "__parquet_schema_name_budget_regression__"
    charge = Parquet._schemanamecharge(name, typemax(Int64))
    small = Parquet.Limits(max_schema_name_bytes=charge - 1)
    @test_throws Parquet.LimitError Parquet._internschemanames(String[name], small)
    @test Parquet._internedschemanamebytes() == before

    exact = Parquet.Limits(max_schema_name_bytes=charge)
    @test Parquet._internschemanames(String[name], exact) == Symbol[Symbol(name)]
    @test Parquet._internedschemanamebytes() == before + charge
    @test Parquet._internschemanames(String[name],
        Parquet.Limits(max_schema_name_bytes=0)) == Symbol[Symbol(name)]
    @test Parquet._internedschemanamebytes() == before + charge

    # Regression: the limit admits each operation's new names independently, so
    # earlier interning (hostile or not) must not consume later operations' budget.
    second = "__parquet_schema_name_budget_regression_second__"
    secondcharge = Parquet._schemanamecharge(second, typemax(Int64))
    @test Parquet._internschemanames(String[second],
        Parquet.Limits(max_schema_name_bytes=secondcharge)) == Symbol[Symbol(second)]
    @test Parquet._internedschemanamebytes() == before + charge + secondcharge

    @test_throws Parquet.UnsupportedFeatureError Parquet._internschemanames(
        String["duplicate", "duplicate"], Parquet.Limits())
    @test_throws Parquet.UnsupportedFeatureError Parquet._internschemanames(
        String["nul\0name"], Parquet.Limits())
    @test Parquet._internedschemanamebytes() == before + charge + secondcharge
end

@testset "isolated schema-name boundary and precedence" begin
    project = dirname(something(Base.active_project()))
    script = raw"""
        using Parquet
        Parquet._internedschemanamebytes() == 0 || exit(10)
        name = "__parquet_isolated_exact_schema_name__"
        charge = Parquet._schemanamecharge(name, typemax(Int64))
        try
            Parquet._internschemanames(String[name],
                Parquet.Limits(max_schema_name_bytes=charge - 1))
            exit(11)
        catch err
            err isa Parquet.LimitError || exit(12)
        end
        Parquet._internedschemanamebytes() == 0 || exit(13)
        for invalid in (String["duplicate", "duplicate"],
                String["valid", "nul\0name"])
            try
                Parquet._internschemanames(invalid,
                    Parquet.Limits(max_schema_name_bytes=0))
                exit(14)
            catch err
                err isa Parquet.UnsupportedFeatureError || exit(15)
            end
            Parquet._internedschemanamebytes() == 0 || exit(16)
        end
        exact = Parquet.Limits(max_schema_name_bytes=charge)
        budget = Parquet._LiveByteBudget(exact)
        Parquet._reserve!(budget, Int64(64))
        Parquet._internschemanames(String[name], exact, budget) ==
            Symbol[Symbol(name)] || exit(17)
        Parquet._internedschemanamebytes() == charge || exit(18)
        expected = Int64(64) +
            Parquet._materializedarraybytes(Symbol, 1)
        Parquet._budgetused(budget) == expected || exit(19)
        second = "__parquet_isolated_exact_schema_name_second__"
        secondcharge = Parquet._schemanamecharge(second, typemax(Int64))
        Parquet._internschemanames(String[second],
            Parquet.Limits(max_schema_name_bytes=secondcharge)) ==
            Symbol[Symbol(second)] || exit(20)
        Parquet._internedschemanamebytes() == charge + secondcharge || exit(21)
        """
    command = `$(Base.julia_cmd()) --startup-file=no --project=$project -e $script`
    @test success(command)
end

function minimummaterializedlimit(f; maximum::Int64=1_000_000)
    low = Int64(-1)
    high = maximum
    f(Parquet.Limits(max_materialized_bytes=high))
    while high - low > 1
        middle = (low + high) ÷ 2
        try
            f(Parquet.Limits(max_materialized_bytes=middle))
            high = middle
        catch err
            err isa Parquet.LimitError || rethrow()
            low = middle
        end
    end
    return high
end

@testset "operation materialization budgets" begin
    one = (a=Int32[1, 2, 3, 4],)
    two = (a=Int32[1, 2, 3, 4], b=Int32[5, 6, 7, 8])
    for maximum in (0, 1)
        limits = Parquet.Limits(max_materialized_bytes=maximum)
        @test_throws Parquet.LimitError Parquet._encodefile(one; limits=limits)
    end

    onebytes = Parquet._encodefile(one)
    twobytes = Parquet._encodefile(two)
    for maximum in (0, 1)
        limits = Parquet.Limits(max_materialized_bytes=maximum)
        @test_throws Parquet.LimitError Parquet.Table(onebytes; limits=limits)
    end

    file = Parquet.File(onebytes)
    metadata = Parquet.Thrift.decode(file.footer.bytes,
        Parquet.Metadata.FileMetaData)
    schema = Parquet.Schema(metadata)
    close(file)
    for maximum in (0, 1)
        limits = Parquet.Limits(max_materialized_bytes=maximum)
        @test_throws Parquet.LimitError Parquet._nestedplan(schema; limits=limits)
    end

    writeone = minimummaterializedlimit() do limits
        Parquet._encodefile(one; limits=limits)
        return
    end
    writetwo = minimummaterializedlimit() do limits
        Parquet._encodefile(two; limits=limits)
        return
    end
    @test writetwo > writeone
    @test_throws Parquet.LimitError Parquet._encodefile(two;
        limits=Parquet.Limits(max_materialized_bytes=writeone))

    readone = minimummaterializedlimit() do limits
        table = Parquet.Table(onebytes; limits=limits)
        close(table)
        return
    end
    readtwo = minimummaterializedlimit() do limits
        table = Parquet.Table(twobytes; limits=limits)
        close(table)
        return
    end
    @test readtwo > readone
    @test_throws Parquet.LimitError Parquet.Table(twobytes;
        limits=Parquet.Limits(max_materialized_bytes=readone))

    tworowgroups = Parquet._encodefile((a=vcat(one.a, one.a),);
        rowgroupsize=length(one.a))
    readtwogroups = minimummaterializedlimit() do limits
        table = Parquet.Table(tworowgroups; limits=limits)
        close(table)
        return
    end
    @test readtwogroups > readone
    @test_throws Parquet.LimitError Parquet.Table(tworowgroups;
        limits=Parquet.Limits(max_materialized_bytes=readone))
end

@testset "footer metadata structural preflight" begin
    MD = Parquet.Metadata
    count = 2000
    schema = MD.SchemaElement[
        MD.SchemaElement(name="schema", num_children=Int32(count)),
    ]
    for _ in 1:count
        push!(schema, MD.SchemaElement(name="", type_=MD.Type.INT32,
            repetition_type=MD.FieldRepetitionType.REQUIRED))
    end
    metadata = MD.FileMetaData(version=Int32(1), schema=schema,
        num_rows=Int64(0), row_groups=MD.RowGroup[])
    footer = Parquet.Thrift.encode(metadata)
    bytes = copy(Parquet.PARQUET_MAGIC)
    append!(bytes, footer)
    Parquet._writelittle!(bytes, UInt32(length(footer)))
    append!(bytes, Parquet.PARQUET_MAGIC)
    file = Parquet.File(bytes)
    maximum = Int64(128 + 4 * length(footer))
    limits = Parquet.Limits(max_materialized_bytes=maximum,
        max_container_elements=count + 10)
    budget = Parquet._LiveByteBudget(limits)
    @test_throws Parquet.LimitError Parquet._readfilemetadata(file, limits,
        budget)
    @test Parquet._budgetused(budget) == 0
    close(file)
end

@testset "schema-name validation preflight" begin
    before = Parquet._internedschemanamebytes()
    first = "__parquet_new_name_rejected_before_set_growth__"
    names = fill(first, 100_000)
    limits = Parquet.Limits(max_schema_name_bytes=0,
        max_materialized_bytes=1024 * 1024)
    budget = Parquet._LiveByteBudget(limits)
    @test_throws Parquet.UnsupportedFeatureError Parquet._internschemanames(
        names, limits, budget)
    @test Parquet._budgetused(budget) == 0
    @test Parquet._internedschemanamebytes() == before

    Parquet._reserve!(budget, Int64(64))
    for invalid in (String[first, first], String["nul\0name"])
        @test_throws Parquet.UnsupportedFeatureError Parquet._internschemanames(
            invalid, limits, budget)
        @test Parquet._budgetused(budget) == 64
        @test Parquet._internedschemanamebytes() == before
    end
    Parquet._release!(budget, Int64(64))

    firstatomic = "__parquet_atomic_batch_first__"
    secondatomic = "__parquet_atomic_batch_second__"
    firstcharge = Parquet._schemanamecharge(firstatomic, typemax(Int64))
    atomiclimits = Parquet.Limits(max_schema_name_bytes=firstcharge)
    atomicbudget = Parquet._LiveByteBudget(atomiclimits)
    Parquet._reserve!(atomicbudget, Int64(64))
    @test_throws Parquet.LimitError Parquet._internschemanames(
        String[firstatomic, secondatomic], atomiclimits, atomicbudget)
    @test Parquet._budgetused(atomicbudget) == 64
    @test Parquet._internedschemanamebytes() == before
    lock(Parquet._SCHEMA_NAME_REGISTRY.lock)
    try
        state = Parquet._SCHEMA_NAME_REGISTRY.state
        @test !(firstatomic in state.names)
        @test !(secondatomic in state.names)
    finally
        unlock(Parquet._SCHEMA_NAME_REGISTRY.lock)
    end

    atomic = "__parquet_name_output_preflight_is_atomic__"
    charge = Parquet._schemanamecharge(atomic, typemax(Int64))
    temporary = Parquet._materializedarraybytes(String, 0) +
        2 * Parquet._MATERIALIZED_OBJECT_BYTES +
        Parquet._materializedarraybytes(String, 2; header=false)
    tight = Parquet.Limits(max_schema_name_bytes=charge,
        max_materialized_bytes=temporary)
    tightbudget = Parquet._LiveByteBudget(tight)
    @test_throws Parquet.LimitError Parquet._internschemanames(String[atomic],
        tight, tightbudget)
    @test Parquet._budgetused(tightbudget) == 0
    @test Parquet._internedschemanamebytes() == before
end
