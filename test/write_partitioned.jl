using Dates
using Tables

function wpfiles(root)
    files = String[]
    for (directory, _, names) in walkdir(root), name in names
        push!(files, replace(relpath(joinpath(directory, name), root), '\\' => '/'))
    end
    return sort!(files)
end

# A stream of tables that must be written partition by partition, never as a whole.
struct WPStream
    parts::Vector{Any}
end
Tables.istable(::Type{WPStream}) = true
Tables.partitions(stream::WPStream) = stream.parts
Tables.columns(::WPStream) = error("the stream was read as one table")

wpsorted(table) = (order = sortperm(collect(table.id)); map(c -> collect(c)[order],
    NamedTuple{Tuple(Tables.columnnames(table))}(Tuple(Tables.getcolumn(table, n)
        for n in Tables.columnnames(table)))))

@testset "partitioned writes" begin
    root = mktempdir()
    data = (id=collect(1:8), value=[i / 2 for i in 1:8],
        s=["a/b c", "a/b c", "%=", "é", "", missing, "~x_.y-", "a/b c"],
        flag=[true, false, true, false, missing, true, false, true],
        day=[Date(2024, 1, i % 2 + 1) for i in 1:8],
        n=[1, 2, 1, 2, 1, 2, 1, 2])
    dir = joinpath(root, "escapes")
    @test Parquet.write(dir, data; partitionby=[:s, :flag]) == dir
    # Values are percent-encoded like pyarrow's: everything but A-Z a-z 0-9 - . _ ~.
    @test wpfiles(dir) == [
        "s=%25%3D/flag=true/part-0.parquet",
        "s=/flag=__HIVE_DEFAULT_PARTITION__/part-0.parquet",
        "s=__HIVE_DEFAULT_PARTITION__/flag=true/part-0.parquet",
        "s=a%2Fb%20c/flag=false/part-0.parquet",
        "s=a%2Fb%20c/flag=true/part-0.parquet",
        "s=~x_.y-/flag=false/part-0.parquet",
        "s=%C3%A9/flag=false/part-0.parquet",
    ] |> sort
    # The key columns live only in the paths.
    file = Parquet.Table(joinpath(dir, "s=a%2Fb%20c/flag=true/part-0.parquet"))
    @test Tables.columnnames(file) == (:id, :value, :day, :n)
    @test file.id == [1, 8]
    back = wpsorted(Parquet.Table(dir))
    @test back.id == data.id && back.value == data.value && back.day == data.day
    # An empty string round-trips, as in pyarrow, Polars, and DuckDB.
    @test isequal(back.s, data.s)
    @test isequal(back.flag, data.flag)

    typed = joinpath(root, "typed")
    Parquet.write(typed, data; partitionby=(:day, :n))
    @test "day=2024-01-02/n=1/part-0.parquet" in wpfiles(typed)
    back = wpsorted(Parquet.Table(typed))
    @test back.day == data.day && back.n == data.n && isequal(back.s, data.s)
    floats = joinpath(root, "floats")
    Parquet.write(floats, (id=[1, 2], f=[1.0, 2.5]); partitionby=:f)
    @test wpfiles(floats) == ["f=1.0/part-0.parquet", "f=2.5/part-0.parquet"]
    @test wpsorted(Parquet.Table(floats)).f == [1.0, 2.5]

    # A non-empty directory is an error unless the matching partitions may be replaced.
    @test_throws "not empty" Parquet.write(typed, data; partitionby=[:day, :n])
    Parquet.write(typed, (id=[100], s=["new"], day=[Date(2024, 1, 2)], n=[1]);
        partitionby=[:day, :n], existing=:delete_matching)
    @test readdir(joinpath(typed, "day=2024-01-02", "n=1")) == ["part-0.parquet"]
    back = Parquet.Table(typed)
    @test sort(collect(back.id)) == [2, 4, 6, 8, 100]
    @test_throws "existing must be" Parquet.write(joinpath(root, "x"), data;
        partitionby=:n, existing=:overwrite)
    @test_throws "only to partitioned" Parquet.write(joinpath(root, "y.parquet"), data;
        existing=:delete_matching)
    @test_throws "must be a path" Parquet.write(IOBuffer(), data; partitionby=:n)
    @test_throws "not a column" Parquet.write(joinpath(root, "z"), data;
        partitionby=:missingcolumn)
    @test_throws "every column" Parquet.write(joinpath(root, "z"), (n=[1],);
        partitionby=:n)
    @test_throws "partition by integer" Parquet.write(joinpath(root, "z"),
        (id=[1], t=[DateTime(2024)]); partitionby=:t)
    @test !isdir(joinpath(root, "z")) || isempty(wpfiles(joinpath(root, "z")))

    # Partitions of the input stream into the same files, every column nullable. Past
    # the open-file limit the oldest file is finished, so with more keys than the limit
    # in every input partition each key continues in a new file.
    stream = joinpath(root, "stream")
    keys = Parquet._MAX_OPEN_PARTITIONS + 6
    parts = [(id=collect(1:keys) .+ keys * (p - 1), k=collect(1:keys),
        l=[[i, i] for i in 1:keys]) for p in 1:2]
    Parquet.write(stream, Tables.partitioner(parts); partitionby=:k, rowgroupsize=1)
    files = wpfiles(stream)
    @test count(endswith("part-1.parquet"), files) == keys
    @test length(files) == 2keys
    back = wpsorted(Parquet.Table(stream))
    @test back.id == collect(1:2keys)
    @test back.l == [[i, i] for i in vcat(1:keys, 1:keys)]
    file = Parquet.File(joinpath(stream, "k=70", "part-0.parquet"))
    @test Missing <: Tables.schema(file).types[1]
    close(file)

    streamed = joinpath(root, "streamed")
    Parquet.write(streamed, WPStream(Any[(k=[1, 2], v=[1, 2]), (k=[1], v=[3])]);
        partitionby=:k)
    @test sort(collect(Parquet.Table(streamed).v)) == [1, 2, 3]

    # File metadata comes from the input; partition columns have no column metadata.
    meta = joinpath(root, "meta")
    Parquet.write(meta, data; partitionby=:n, metadata=["origin" => "test"],
        colmetadata=(n=["unit" => "count"], id=["unit" => "row"]))
    file = Parquet.File(joinpath(meta, "n=1", "part-0.parquet"))
    @test Parquet.DataAPI.metadata(file, "origin") == "test"
    @test Parquet.DataAPI.colmetadata(file, :id, "unit") == "row"
    close(file)
end
