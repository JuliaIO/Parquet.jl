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
    @test column[1] == ["a" => 1, "b" => 2, "a" => 3]
    @test column[1] isa AbstractVector{Pair{String,Union{Missing,Int}}}
    @test Dict(column[1]) == Dict("a" => 3, "b" => 2)
    @test column[2] === missing
    @test isempty(column[3])
    keyonly = Parquet.MapVector([0, 2], Int32[1, 2])
    @test isequal(keyonly[1], [Int32(1) => missing, Int32(2) => missing])
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
    # Struct rows are NamedTuples up to 32 levels deep; structs above that read as
    # Pair rows, so a deep schema has at most 32 NamedTuple row types along a path.
    structs::AbstractVector = Int32[7]
    for level in 1:40
        structs = Parquet.StructVector(["f$level"], (structs,))
    end
    @test eltype(structs) == Parquet.StructValue
    inner = structs
    for _ in 1:8
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
    @test rows[end] == ["n" => 5]
    push!(rows, ["d" => 1, "d" => 2])
    @test rows[end] == ["d" => 1, "d" => 2]
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
    # Plain pair-vector maps (as other readers' map rows are) should write as MAP (S3).
    plainmaps = (m=[["a" => 1], Pair{String,Int}[]],)
    @test_broken collect(Parquet.Tables.columns(
        Parquet.Table(Parquet._encodefile(plainmaps))).m) == [["a" => 1], Pair{String,Int}[]]
end
