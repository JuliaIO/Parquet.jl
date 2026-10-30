using Tables

@testset "read byte budget" begin
    budget = Parquet._ByteBudget(Parquet.Limits(max_materialized_bytes=10))
    Parquet._charge!(budget, 4)
    @test budget.used == 4
    @test_throws Parquet.LimitError Parquet._charge!(budget, 7)
    @test_throws Parquet.LimitError Parquet._charge!(budget, typemax(Int64))
    @test_throws ArgumentError Parquet._charge!(budget, -1)
    uncapped = Parquet._ByteBudget(Parquet.Limits())
    Parquet._charge!(uncapped, typemax(Int64))
    @test uncapped.used == 0
    # Tasks of one read share a budget; every charge is counted.
    shared = Parquet._ByteBudget(Parquet.Limits(max_materialized_bytes=1_000_000))
    @sync for _ in 1:8
        errormonitor(Threads.@spawn begin
            for _ in 1:1000
                Parquet._charge!(shared, 3)
            end
        end)
    end
    @test shared.used == 24_000

    # Nullable isbits arrays store one tag byte per value as well as their payload.
    nullable = Union{Missing,Int64}
    exact = Parquet._ByteBudget(Parquet.Limits(max_materialized_bytes=18))
    Parquet._chargearray!(exact, nullable, 2)
    @test exact.used == 18
    short = Parquet._ByteBudget(Parquet.Limits(max_materialized_bytes=17))
    @test_throws Parquet.LimitError Parquet._chargearray!(short, nullable, 2)
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
        String["nul\0name"], Parquet.Limits())
    @test Parquet._internedschemanamebytes() == before + charge + secondcharge
end

@testset "repeated top-level names are made unique" begin
    @test Parquet._uniquenames(["a", "b"]) == ["a", "b"]
    @test Parquet._uniquenames(["a", "a", "b", "a"]) == ["a", "a_1", "b", "a_2"]
    # A suffix never takes a name the file already has.
    @test Parquet._uniquenames(["a", "a", "a_1"]) == ["a", "a_2", "a_1"]
    @test Parquet._uniquenames(["a_1", "a", "a"]) == ["a_1", "a", "a_2"]
end

@testset "wide schemas read under default limits" begin
    # Names never interned before, so all 5,000 count against one read's budget.
    names = ["wide_schema_regression_$i" for i in 1:5_000]
    bytes = take!(Parquet.write(IOBuffer(), Tables.table(ones(Int, 2, 5_000); header=names)))
    table = Parquet.Table(bytes)
    @test collect(Tables.columnnames(table)) == Symbol.(names)
    @test Tables.getcolumn(table, Symbol(names[end])) == [1, 1]
end

@testset "limit errors name the Limits field" begin
    for resource in (:container_elements, :decimal_bytes, :footer_bytes, :materialized_bytes,
            :metadata_depth, :page_bytes, :page_header_bytes, :page_index_bytes,
            :schema_name_bytes, :string_bytes)
        @test hasfield(Parquet.Limits, Symbol(:max_, resource))
        @test occursin("limits=Parquet.Limits(max_$resource=...)",
            sprint(showerror, Parquet.LimitError(resource, 2, 1)))
    end
    @test occursin("drop the column",
        sprint(showerror, Parquet.LimitError(:nested_read_depth, 2000, 1024)))
    bytes = take!(Parquet.write(IOBuffer(), (id=collect(1:100),)))
    err = try
        Parquet.Table(bytes; limits=Parquet.Limits(max_container_elements=10))
    catch e
        e
    end
    @test err isa Parquet.LimitError &&
        occursin("Parquet.Limits(max_container_elements=", sprint(showerror, err))
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
        for invalid in (String["valid", "nul\0name"],)
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
        Parquet._internschemanames(String[name], exact) ==
            Symbol[Symbol(name)] || exit(17)
        Parquet._internedschemanamebytes() == charge || exit(18)
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
    onebytes = Parquet._encodefile(one)
    twobytes = Parquet._encodefile(two)
    # The byte cap applies to reads only.
    @test Parquet._encodefile(one; limits=Parquet.Limits(max_materialized_bytes=0)) ==
        onebytes
    for maximum in (0, 1)
        limits = Parquet.Limits(max_materialized_bytes=maximum)
        @test_throws Parquet.LimitError Parquet.Table(onebytes; limits=limits)
    end

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

function cappedread(bytes, options)
    try
        Parquet.Table(bytes; limits=Parquet.Limits(max_materialized_bytes=100_000),
            ntasks=1, options...)
    catch err
        err isa Parquet.LimitError || rethrow()
        return err
    end
    return nothing
end

@testset "read budgets precede flat output allocation" begin
    rows = 1_000_000
    bytes = take!(Parquet.write(IOBuffer(), (x=collect(1:rows),);
        compress=:none, dictionary=false, pagesize=65_536))
    for options in ((;), (; limit=rows - 1))
        @test cappedread(bytes, options) isa Parquet.LimitError
        # Warm the failure path; the cap must reject before reserving an 8 MB column.
        cappedread(bytes, options)
        @test (@allocated cappedread(bytes, options)) < 1_000_000
    end
end

@testset "filtered iterator windows share the decoding budget" begin
    rows = 1000
    bytes = take!(Parquet.write(IOBuffer(), (x=collect(1:rows),)))
    filter = Parquet.col(:x) > 0
    for dataset in (false, true)
        required = map((nothing, rows - 1)) do limit
            minimummaterializedlimit() do limits
                source = dataset ? Parquet.Dataset([bytes]; filter, limit, limits) :
                    Parquet.File(bytes; filter, limit, limits)
                try
                    return only(source)
                finally
                    dataset || close(source)
                end
            end
        end
        # Filtering reads the same row group; the window then copies its retained rows.
        @test required[2] > required[1]
    end
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
    @test_throws Parquet.LimitError Parquet._readfilemetadata(file, limits)
    close(file)
end

@testset "schema-name validation preflight" begin
    before = Parquet._internedschemanamebytes()
    first = "__parquet_new_name_rejected_before_set_growth__"
    limits = Parquet.Limits(max_schema_name_bytes=0)
    @test_throws Parquet.UnsupportedFeatureError Parquet._internschemanames(
        String[first, "nul\0name"], limits)
    @test Parquet._internedschemanamebytes() == before

    firstatomic = "__parquet_atomic_batch_first__"
    secondatomic = "__parquet_atomic_batch_second__"
    firstcharge = Parquet._schemanamecharge(firstatomic, typemax(Int64))
    atomiclimits = Parquet.Limits(max_schema_name_bytes=firstcharge)
    @test_throws Parquet.LimitError Parquet._internschemanames(
        String[firstatomic, secondatomic], atomiclimits)
    @test Parquet._internedschemanamebytes() == before
    lock(Parquet._SCHEMA_NAME_REGISTRY.lock)
    try
        state = Parquet._SCHEMA_NAME_REGISTRY.state
        @test !(firstatomic in state.names)
        @test !(secondatomic in state.names)
    finally
        unlock(Parquet._SCHEMA_NAME_REGISTRY.lock)
    end
end
