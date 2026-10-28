using Test

@testset "list columns" begin
    E = Union{Missing,Int64}
    column = Parquet.ListVector([0, 2, 2, 3], E[1, missing, 3];
        validity=Bool[true, false, true])
    @test column isa Parquet.ListVector{Union{Missing,Parquet.ListValue{E}}}
    @test eltype(column) == Union{Missing,Parquet.ListValue{E}}
    @test isequal(column[1], [1, missing])
    @test column[2] === missing
    @test column[3] == [3]
    @test (@inferred Union{Missing,Parquet.ListValue{E}} column[3]) == [3]
    row = column[1]
    @test row isa AbstractVector{E}
    @test copy(row) isa Vector{E}
    @test isequal(copy(row), E[1, missing])
    @test hash(column[3]) == hash([3])
    @test isequal(collect(column), Any[E[1, missing], missing, E[3]])
    @test isempty(Parquet.ListVector([0, 0], Int[])[1])
    @test_throws BoundsError column[4]
    @test_throws ArgumentError Parquet.ListVector([0, 2], Int[1])
    @test_throws ArgumentError Parquet.ListVector([1, 1], Int[1])
    @test_throws ArgumentError Parquet.ListVector([0, 1], Int[1]; validity=[false])
    strings = Parquet.ListVector([0, 2], ["a", "a longer string"])
    @test strings[1] == ["a", "a longer string"]
end

@testset "struct columns" begin
    R = NamedTuple{(:a, :b),Tuple{Int32,Union{Missing,String}}}
    column = Parquet.StructVector(["a", "b"],
        (Int32[1, 2, 3], Union{Missing,String}["x", missing, "z"]);
        validity=Bool[true, true, false])
    @test eltype(column) == Union{Missing,R}
    @test column[1] == (a=Int32(1), b="x")
    @test column[1] isa R
    @test isequal(column[2], (a=Int32(2), b=missing))
    @test column[3] === missing
    firsta(rows) = coalesce(rows[1], (a=Int32(0), b=missing)).a
    @test (@inferred firsta(column)) === Int32(1)
    # Struct rows are built without allocating.
    many = Parquet.StructVector(["a", "b"], (collect(Int32, 1:1000), collect(1.0:1000.0)))
    sumrows(rows) = sum(row -> row.a + row.b, rows)
    sumrows(many)
    @test @allocated(sumrows(many)) < 1000

    nested = Parquet.StructVector(["s"], (column,))
    @test nested[1].s.a === Int32(1)
    @test nested[3].s === missing

    duplicate = Parquet.StructVector(["x", "x"], (Int32[1], ["a"]))
    @test eltype(duplicate) == Parquet.StructValue
    @test duplicate[1] == ["x" => 1, "x" => "a"]
    @test duplicate[1][2] == ("x" => "a")
    @test_throws ArgumentError duplicate[1]["x"]

    names = ["c$i" for i in 1:65]
    wide = Parquet.StructVector(names, [Int32[i] for i in 1:65])
    @test eltype(wide) == Parquet.StructValue
    @test wide[1]["c65"] == 65
    @test wide[1][:c1] == 1
    @test_throws KeyError wide[1]["absent"]
    @test eltype(Parquet.StructVector(names[1:64], [Int32[i] for i in 1:64])) <: NamedTuple

    empty = Parquet.StructVector(String[], (); rows=2)
    @test empty[2] == NamedTuple()
    @test_throws ArgumentError Parquet.StructVector(["a"], (Int32[1, 2],); rows=3)
    @test_throws ArgumentError Parquet.StructVector(["a", "b"], (Int32[1],))
end

@testset "map columns" begin
    column = Parquet.MapVector([0, 3, 3, 3], ["a", "b", "a"], Union{Missing,Int}[1, 2, 3];
        validity=Bool[true, false, true])
    @test eltype(column) == Union{Missing,Parquet.MapValue{String,Union{Missing,Int}}}
    row = column[1]
    @test row isa AbstractDict{String,Union{Missing,Int}}
    # Entries in stored order, duplicate keys included; a lookup finds the last one, as
    # Dict(row) keeps it.
    @test collect(row) == ["a" => 1, "b" => 2, "a" => 3] && length(row) == 3
    @test keys(row) == ["a", "b", "a"] && values(row) == [1, 2, 3]
    @test row["a"] == 3 && row["b"] == 2 && get(row, "a", 0) == 3 && get(row, "z", 0) == 0
    @test get(() -> 0, row, "z") == 0 && haskey(row, "b") && !haskey(row, "z")
    @test_throws KeyError row["z"]
    @test Dict(row) == copy(row) == Dict("a" => 3, "b" => 2)
    @test column[2] === missing
    @test isempty(column[3]) && column[3] == Dict{String,Int}()
    keyonly = Parquet.MapVector([0, 2], Int32[1, 2])
    @test isequal(collect(keyonly[1]), [Int32(1) => missing, Int32(2) => missing])
    @test keyonly[1][2] === missing &&
        isequal(keyonly[1], Dict(Int32(1) => missing, 2 => missing))
    @test_throws ArgumentError Parquet.MapVector([0, 1], Union{Missing,Int}[missing], [1])
    @test_throws ArgumentError Parquet.MapVector([0, 1], [1], [1, 2])
end

@testset "nested column types take only their element type" begin
    values::AbstractVector = Int[1]
    for _ in 1:128
        values = Parquet.ListVector([0, 1], values)
    end
    typerepr = sprint(show, typeof(values))
    @test count("ListVector", typerepr) == 1
    @test ncodeunits(typerepr) < 16 * 1024
    row = values[1]
    for _ in 1:127
        row = only(row)
    end
    @test row == [1]
    maps::AbstractVector = Int[1]
    for _ in 1:20
        maps = Parquet.MapVector([0, 1], Int[1], maps)
    end
    @test count("MapVector", sprint(show, typeof(maps))) == 1
    # Struct rows are NamedTuples up to 8 levels deep; structs above that read as
    # Pair rows, so a deep schema has at most 8 NamedTuple row types along a path.
    structs::AbstractVector = Int32[7]
    for level in 1:40
        structs = Parquet.StructVector(["f$level"], (structs,))
    end
    @test eltype(structs) == Parquet.StructValue
    inner = structs
    for _ in 1:32
        inner = only(inner.children)
    end
    @test eltype(inner) <: NamedTuple
    @test ncodeunits(sprint(show, eltype(inner))) < 16 * 1024
    @test eltype(only(inner.children)) <: NamedTuple
    leaf = structs[1]
    for level in 40:-1:2
        leaf = leaf isa NamedTuple ? only(leaf) : only(leaf).second
    end
    @test only(leaf) === Int32(7)
end

@testset "nested rows in plain vectors" begin
    E = Union{Missing,Int64}
    lists = Parquet.ListVector([0, 2, 2, 3], E[1, missing, 3];
        validity=Bool[true, false, true])
    structs = Parquet.StructVector(["a", "b"], (Int32[1, 2, 3], ["x", "y", "z"]))
    maps = Parquet.MapVector([0, 1, 1, 2], ["k", "q"], [1, 2])
    for column in (lists, structs, maps)
        @test copy(column) isa Vector{eltype(column)}
        @test isequal(copy(column), collect(column))
        @test isequal(column[[3, 1]], collect(column)[[3, 1]])
        @test isequal(column[Bool[true, false, true]], collect(column)[[1, 3]])
        @test isequal(vcat(column, column), vcat(collect(column), collect(column)))
        @test isequal(filter(!ismissing, column), filter(!ismissing, collect(column)))
    end
    rows = copy(lists)
    push!(rows, [7, 8])
    @test rows[end] isa Parquet.ListValue{E}
    @test rows[end] == [7, 8]
    rows[1] = E[missing]
    @test isequal(rows[1], [missing])
    rows = copy(structs)
    push!(rows, (a=4, b="w"))
    @test rows[end] === (a=Int32(4), b="w")
    rows = copy(maps)
    push!(rows, Dict("n" => 5))
    @test collect(rows[end]) == ["n" => 5]
    push!(rows, ["d" => 1, "d" => 2])
    @test collect(rows[end]) == ["d" => 1, "d" => 2]
    nested = Parquet.ListVector([0, 1], structs[1:1])
    rows = copy(nested)
    push!(rows, [(a=Int32(9), b="n")])
    @test rows[end][1] === (a=Int32(9), b="n")
    @test sort(copy(structs); by=row -> row.b, rev=true)[1].b == "z"
end

@testset "nested columns from a read write back" begin
    E = Union{Missing,Int32}
    S = NamedTuple{(:x, :tags),Tuple{E,Union{Missing,Vector{String}}}}
    source = (
        s=Union{Missing,S}[S((1, ["a"])), missing, S((missing, missing)), S((4, String[]))],
        l=Union{Missing,Vector{E}}[E[1, missing], missing, E[], E[3]],
        m=[Dict("k" => Int32(1)), Dict{String,Int32}(), Dict("q" => Int32(2), "r" => Int32(3)),
            Dict{String,Int32}()],
    )
    table = Parquet.Table(Parquet._encodefile(source))
    columns = Parquet.Tables.columns(table)
    for selection in ([3, 1], Bool[true, false, true, true], Int[], :)
        subset = map(column -> column[selection], columns)
        for name in keys(columns)
            @test isequal(getproperty(subset, name), collect(getproperty(columns, name))[selection])
        end
        written = Parquet.Tables.columns(Parquet.Table(Parquet._encodefile(subset)))
        for name in keys(columns)
            @test isequal(collect(getproperty(written, name)),
                collect(getproperty(columns, name))[selection])
        end
    end
    @test_throws BoundsError columns.l[[5]]
    @test_throws BoundsError columns.m[Bool[true]]
    # Plain pair-vector maps (as other readers' map rows are) write as MAP (S3).
    plainmaps = (m=[["a" => 1], Pair{String,Int}[]],)
    @test map(collect, Parquet.Tables.columns(
        Parquet.Table(Parquet._encodefile(plainmaps))).m) == [["a" => 1], Pair{String,Int}[]]
end

@testset "nested values and columns are read-only" begin
    table = Parquet.Table(take!(Parquet.write(IOBuffer(),
        (l=[[1, 2], [3]], s=[(a=1,), (a=2,)], m=[Dict("k" => 1), Dict("j" => 2)]))))
    for (x, name) in ((table.l[1], "ListValue"), (table.m[1], "MapValue"),
            (table.l, "ListVector"), (table.s, "StructVector"), (table.m, "MapVector"))
        for mutate! in (x -> push!(x, first(x)), x -> (x[1] = first(x)),
                x -> x isa AbstractDict ? merge!(x, x) : append!(x, x))
            err = try
                mutate!(x)
            catch e
                e
            end
            @test err isa ArgumentError && occursin("Parquet.$name is read-only", err.msg)
        end
    end
    row = copy(table.l[1])
    push!(row, 9)
    @test row == [1, 2, 9] && table.l[1] == [1, 2]
    entries = copy(table.m[1])
    entries["n"] = 3
    @test entries == Dict("k" => 1, "n" => 3) && table.m[1] == Dict("k" => 1)
end

@testset "rows print as plain values in containers and DataFrame cells" begin
    lists = Parquet.ListVector([0, 2, 2], Union{Missing,Int}[1, missing];
        validity=Bool[true, false])
    maps = Parquet.MapVector([0, 2], ["a", "b"], lists)
    compact(x) = sprint(show, x; context=:compact => true)
    @test compact(lists[1]) == "[1, missing]"
    @test compact(maps[1]) == "Dict(\"a\"=>[1, missing], \"b\"=>missing)"
    @test endswith(sprint(show, collect(lists)), "[[1, missing], missing]")
    @test endswith(sprint(show, [maps[1]]), "[Dict(\"a\" => [1, missing], \"b\" => missing)]")
    @test compact(Parquet.ListVector([0, 0], Int[])[1]) == "[]"
    long = Parquet.MapVector([0, 12], string.(1:12), collect(1:12))[1]
    @test endswith(sprint(show, long; context=(:compact => true, :limit => true)),
        "\"10\"=>10, …)")
    # Outside a container, rows print as Base prints their supertypes.
    @test sprint(show, lists[1]) == "Union{Missing, Int64}[1, missing]"
    @test startswith(sprint(show, MIME("text/plain"), maps[1]),
        "Parquet.MapValue{String, Union{Missing, Parquet.ListValue{Union{Missing, Int64}}}} with 2 entries")
end
