using Dates
using Tables
import DataAPI

# Writes `table` to `dir/relative`, creating its directories.
function dswrite(dir, relative, table; kw...)
    path = joinpath(dir, relative)
    mkpath(dirname(path))
    Parquet.write(path, table; kw...)
    return path
end

dsrows(table) = Tables.rowcount(table)
plainvalues(column) = [value isa Parquet.DataStrings.DataString ? String(value) : value
    for value in column]

@testset "datasets: hive partitions, hidden files, and column types" begin
    mktempdir() do dir
        dswrite(dir, "year=2024/city=New%20York/part-0.parquet",
            (id=[1, 2], s=["a", "b"]))
        dswrite(dir, "year=2024/city=__HIVE_DEFAULT_PARTITION__/part-0.parquet",
            (id=[3], s=["c"]))
        dswrite(dir, "year=2025/city=Zürich/part-0.parquet", (id=[4, 5, 6],
            s=["d", "e", "a much longer string value"]); rowgroupsize=2)
        dswrite(dir, "year=2025/city=/part-1.parquet", (id=[7], s=["f"]))
        # Names starting with `.` or `_` are skipped, files and directories alike.
        write(joinpath(dir, "_SUCCESS"), "")
        write(joinpath(dir, "year=2024", ".part-0.parquet.crc"), "crc")
        dswrite(dir, "_temporary/part-9.parquet", (id=[99], s=["x"]))
        dswrite(dir, ".hidden/part-9.parquet", (id=[98], s=["y"]))
        # Files are read in path order.
        table = Parquet.Table(dir)
        @test Tables.columnnames(table) == (:id, :s, :year, :city)
        @test table.id == [1, 2, 3, 7, 4, 5, 6]
        @test eltype(table.year) == Int64
        @test table.year == [2024, 2024, 2024, 2025, 2025, 2025, 2025]
        @test eltype(table.city) == Union{Missing,Parquet.DataStrings.DataString}
        # `city=` is the empty string, as pyarrow, Polars, and DuckDB read it.
        @test isequal(plainvalues(table.city), ["New York", "New York", missing, "",
            "Zürich", "Zürich", "Zürich"])
        @test Parquet.Table(dir; filter=Parquet.colcmp(==, Parquet.col(:city), "")).id == [7]
        @test plainvalues(table.s)[7] == "a much longer string value"

        ds = Parquet.Dataset(dir)
        @test length(ds.files) == 4
        @test Tables.partitions(ds) === ds
        @test Tables.schema(ds) == Tables.schema(table)
        @test DataAPI.nrow(ds) == 7
        @test length(ds) == 5
        @test [dsrows(part) for part in ds] == [2, 1, 1, 2, 1]
        @test ds[4].id == [4, 5] && ds[4].year == [2025, 2025]
        @test occursin("4 files", sprint(show, ds))
        @test Tables.columnnames(Parquet.Table(dir; partitioning=nothing)) == (:id, :s)

        # Value types: Int64, Float64, Date, Bool, and strings when values disagree.
        mktempdir() do typed
            dswrite(typed, "r=0.5/d=2024-01-02/b=true/n=10/m=1/x.parquet", (id=[1],))
            dswrite(typed, "r=2/d=2024-02-29/b=false/n=-3/m=a/x.parquet", (id=[2],))
            t = Parquet.Table(typed)
            @test eltype(t.r) == Float64 && t.r == [0.5, 2.0]
            @test eltype(t.d) == Date && t.d == [Date(2024, 1, 2), Date(2024, 2, 29)]
            @test eltype(t.b) == Bool && t.b == [true, false]
            @test eltype(t.n) == Int64 && t.n == [10, -3]
            @test plainvalues(t.m) == ["1", "a"]
        end
        # An empty value makes a key a string column, as in pyarrow and DuckDB.
        mktempdir() do empty
            dswrite(empty, "y=2024/e=/x.parquet", (id=[1],))
            dswrite(empty, "y=/e=/x.parquet", (id=[2],))
            dswrite(empty, "y=__HIVE_DEFAULT_PARTITION__/e=/x.parquet", (id=[3],))
            t = Parquet.Table(empty)
            @test eltype(t.y) == Union{Missing,Parquet.DataStrings.DataString}
            @test t.id == [2, 1, 3] && isequal(plainvalues(t.y), ["", "2024", missing])
            @test eltype(t.e) == Parquet.DataStrings.DataString && plainvalues(t.e) == fill("", 3)
        end
    end
end

@testset "datasets: filters prune files, row groups, and rows" begin
    mktempdir() do dir
        for year in 2022:2025, part in 1:2
            ids = collect((1:50) .+ 100 * (year - 2022) .+ 50 * (part - 1))
            dswrite(dir, "year=$year/part-$part.parquet", (id=ids,
                v=[isodd(i) ? missing : i / 2 for i in ids], s=["s$i" for i in ids]);
                rowgroupsize=20)
        end
        full = Tables.columns(Parquet.Table(dir))
        filters = [Parquet.colcmp(==, Parquet.col(:year), 2024),
            Parquet.colcmp(>=, Parquet.col(:year), 2024) & (Parquet.col(:id) < 230),
            (Parquet.colcmp(==, Parquet.col(:year), 2022)) | (Parquet.col(:id) > 390),
            !Parquet.colin(Parquet.col(:year), (2022, 2023)),
            !(Parquet.colcmp(==, Parquet.col(:year), 2024) & Parquet.isnull(Parquet.col(:v))),
            Parquet.isnull(Parquet.col(:year)), Parquet.colcmp(==, Parquet.col(:s), "s311")]
        for filter in filters, (limit, offset) in ((nothing, 0), (17, 5), (1_000, 390))
            scan = Tables.Scan(; filter=filter, limit=limit, offset=offset)
            expected = Tables.scan(full, scan)
            got = Parquet.Table(dir; scan=scan)
            @test Tables.columnnames(got) == Tables.columnnames(expected)
            for name in Tables.columnnames(expected)
                @test isequal(plainvalues(Tables.getcolumn(got, name)),
                    plainvalues(Tables.getcolumn(expected, name)))
            end
            ds = Parquet.Dataset(dir; scan=scan)
            @test sum(dsrows, ds; init=0) == Tables.rowcount(expected)
        end
        # A partition filter leaves the other files unread.
        ds = Parquet.Dataset(dir; filter=Parquet.colcmp(==, Parquet.col(:year), 2024))
        @test count(file -> Parquet._filefilter(ds, file) !== false, ds.files) == 2
        @test length(ds) == 6
        @test_throws ArgumentError DataAPI.nrow(ds)
        windowed = Parquet.Dataset(dir; filter=Parquet.col(:id) > 10, limit=5)
        @test_throws ArgumentError length(windowed)
        @test [dsrows(part) for part in windowed] == [5]
        @test DataAPI.nrow(Parquet.Dataset(dir; limit=75, scan=nothing)) == 75
        @test Parquet.Table(dir; select=[:year], limit=3).year == [2022, 2022, 2022]
        # A filter no file can match reads no rows but keeps the columns and types.
        none = Parquet.Table(dir; filter=Parquet.colcmp(==, Parquet.col(:year), 1999))
        @test dsrows(none) == 0
        @test Tables.schema(none) == Tables.schema(Parquet.Table(dir))
    end
end

@testset "datasets: globs, path vectors, and sources" begin
    mktempdir() do dir
        dswrite(dir, "a/year=2024/x.parquet", (id=[1],))
        dswrite(dir, "a/year=2025/y.parquet", (id=[2],))
        dswrite(dir, "a/year=2025/notes.txt.parquet", (id=[3],))
        dswrite(dir, "b/z.parquet", (id=[4],))
        root = joinpath(dir, "a")
        @test Parquet.Table(joinpath(root, "**", "*.parquet")).id == [1, 3, 2]
        @test Parquet.Table(joinpath(root, "year=2025", "?.parquet")).id == [2]
        @test Parquet.Table(joinpath(root, "*", "[xy].parquet")).year == [2024, 2025]
        @test Parquet.Table(joinpath(dir, "**", "*.parquet")).id == [1, 3, 2, 4]
        @test_throws ArgumentError Parquet.Table(joinpath(dir, "*.csv"))
        paths = [joinpath(root, "year=2024", "x.parquet"), joinpath(root, "year=2025", "y.parquet")]
        t = Parquet.Table(paths)
        @test t.id == [1, 2] && t.year == [2024, 2025]
        sources = [read(path) for path in paths]
        @test Tables.columnnames(Parquet.Table(sources)) == (:id,)
        @test Parquet.Table(sources).id == [1, 2]
        @test Parquet.read(root, Tables.columntable).year == [2024, 2025, 2025]
    end
end

@testset "datasets: schema evolution and conflicts" begin
    mktempdir() do dir
        dswrite(dir, "p0.parquet", (id=[1, 2], a=Int32[1, 2], b=["x", "y"],
            n=[missing, missing]))
        dswrite(dir, "p1.parquet", (id=[3], a=Union{Missing,Int32}[missing], c=[1.5],
            n=[7]))
        t = Parquet.Table(dir)
        @test Tables.columnnames(t) == (:id, :a, :b, :n, :c)
        @test eltype(t.a) == Union{Missing,Int32} && isequal(t.a, [1, 2, missing])
        @test isequal(plainvalues(t.b), ["x", "y", missing])
        @test eltype(t.n) == Union{Missing,Int64} && isequal(t.n, [missing, missing, 7])
        @test isequal(t.c, [missing, missing, 1.5])
        # The read writes back with every column, partition and missing ones included.
        back = Parquet.Table(take!(Parquet.write(IOBuffer(), t)))
        @test Tables.columnnames(back) == Tables.columnnames(t)
        @test isequal(back.n, t.n) && isequal(back.c, t.c)
        # Filters on a column some files lack see `missing` there.
        @test Parquet.Table(dir; filter=Parquet.col(:c) > 1.0).id == [3]
        @test Parquet.Table(dir; filter=Parquet.isnull(Parquet.col(:c))).id == [1, 2]
        @test Parquet.Table(dir; select=[:c, :b]).b[1:2] == ["x", "y"]
    end
    mktempdir() do dir
        dswrite(dir, "p0.parquet", (a=[1],))
        dswrite(dir, "p1.parquet", (a=["x"],))
        err = try
            Parquet.Dataset(dir)
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("\"a\"", err.msg) && occursin("p0.parquet", err.msg) &&
            occursin("p1.parquet", err.msg)
    end
    # Types that widen without loss read as the wider one, whichever file comes first.
    Durations = Parquet.Durations
    stamp(P, n) = Durations.Timestamp{P}(Dates.UTInstant(P(n)))
    for (first_, second_, T) in ((Int32[1, 2], [3, typemax(Int64)], Int64),
            ([3, typemax(Int64)], Int32[1, 2], Int64),
            (UInt32[1, typemax(UInt32)], Int32[-1, 2], Int64),
            (UInt8[1, 255], UInt16[3, 4], UInt16),
            (Float32[1.5, 2.5], [3.25, 1e300], Float64),
            ([stamp(Millisecond, 1), stamp(Millisecond, 2)],
                [stamp(Microsecond, 3), stamp(Microsecond, 4)], Durations.Timestamp{Microsecond}),
            (Union{Missing,Int32}[1, missing], [5, 6], Union{Missing,Int64}))
        mktempdir() do dir
            dswrite(dir, "p0.parquet", (a=first_,))
            dswrite(dir, "p1.parquet", (a=second_,))
            t = Parquet.Table(dir)
            @test eltype(t.a) == T &&
                isequal(t.a, T[value for value in Iterators.flatten((first_, second_))])
            @test Tables.schema(Parquet.Dataset(dir)).types == (T,)
            # The table writes back with the wider type's schema.
            @test eltype(Parquet.Table(take!(Parquet.write(IOBuffer(), t))).a) == T
            @test Parquet.Table(dir; filter=Parquet.colcmp(==, Parquet.col(:a),
                last(second_))).a == [last(second_)]
        end
    end
    # The finer unit wins whatever the file order; a value it cannot hold names the file.
    us(y, m, d) = Durations.Timestamp{Microsecond}(Dates.DateTime(y, m, d))
    ns(y, m, d) = Durations.Timestamp{Nanosecond}(Dates.DateTime(y, m, d))
    for order in ((:us, :ns), (:ns, :us))
        mktempdir() do dir
            values = Dict(:us => [us(2024, 1, 1)], :ns => [ns(2024, 6, 1) + Nanosecond(123)])
            dswrite(dir, "p0.parquet", (a=values[order[1]],))
            dswrite(dir, "p1.parquet", (a=values[order[2]],))
            t = Parquet.Table(dir)
            @test eltype(t.a) == Durations.Timestamp{Nanosecond}
            @test sort(t.a) == [ns(2024, 1, 1), ns(2024, 6, 1) + Nanosecond(123)]
        end
    end
    mktempdir() do dir
        dswrite(dir, "p0.parquet", (a=[ns(2024, 6, 1)],))
        far = dswrite(dir, "p1.parquet", (a=[us(9999, 12, 31)],))
        err = try
            Parquet.Table(dir)
        catch e
            e
        end
        @test err isa ArgumentError && occursin("\"a\"", err.msg) && occursin(far, err.msg)
    end
    for (first_, second_) in (([1], [1.5]), ([typemax(UInt64)], [-1]),
            ([1], [Durations.Timestamp{Millisecond}(Dates.UTInstant(Millisecond(1)))]))
        mktempdir() do dir
            dswrite(dir, "p0.parquet", (a=first_,))
            dswrite(dir, "p1.parquet", (a=second_,))
            @test_throws ArgumentError Parquet.Dataset(dir)
        end
    end
    # A directory value replaces a file column of the same name.
    mktempdir() do dir
        dswrite(dir, "year=2000/x.parquet", (id=[1, 2], year=[1999, 1998]))
        t = Parquet.Table(dir)
        @test Tables.columnnames(t) == (:id, :year)
        @test t.year == [2000, 2000]
        @test Parquet.Table(dir; partitioning=nothing).year == [1999, 1998]
    end
    @test_throws ArgumentError Parquet.Dataset(mktempdir())
    @test_throws ArgumentError Parquet.Dataset("no/such/path")
    @test_throws ArgumentError Parquet.Dataset(mktempdir(); partitioning=:directory)
end

@testset "column metadata" begin
    path = joinpath(@__DIR__, "fixtures", "field_metadata.parquet")
    # pyarrow keeps field metadata in the ARROW:schema.
    table = Parquet.Table(path)
    @test DataAPI.colmetadatasupport(typeof(table)).read
    @test DataAPI.colmetadata(table, :a, "unit") == "kg"
    @test DataAPI.colmetadata(table, 1, "note"; style=true) == ("first", :default)
    @test DataAPI.colmetadata(table, :b, "desc") == "label"
    @test isempty(DataAPI.colmetadatakeys(table, :c))
    @test Dict(DataAPI.colmetadatakeys(table)) == Dict(:a => ["unit", "note"], :b => ["desc"])
    @test_throws KeyError DataAPI.colmetadata(table, :c, "unit")
    @test DataAPI.colmetadata(table, :c, "unit", nothing) === nothing
    @test_throws ArgumentError DataAPI.colmetadata(table, :zz, "unit")
    # Selections and renames carry each column's metadata.
    renamed = Parquet.Table(path; scan=Tables.Scan(select=(:b => :label, :a)))
    @test DataAPI.colmetadata(renamed, :label, "desc") == "label"
    @test DataAPI.colmetadata(renamed, :a, "unit") == "kg"
    filtered = Parquet.Table(path; filter=Parquet.col(:a) > 1, select=[:a])
    @test DataAPI.colmetadata(filtered, :a, "unit") == "kg"
    file = Parquet.File(path; select=[:b])
    @test DataAPI.colmetadata(file, :b, "desc") == "label"
    @test DataAPI.colmetadata(first(file), :b, "desc") == "label"
    close(file)
    # Parquet.write stores colmetadata with the column chunks; a read returns it, and a
    # rewrite keeps it.
    bytes = take!(Parquet.write(IOBuffer(), (x=[1, 2], y=["p", "q"]);
        colmetadata=Dict("x" => ["unit" => "m"])))
    written = Parquet.Table(bytes)
    @test DataAPI.colmetadata(written, :x, "unit") == "m"
    @test DataAPI.colmetadata(Parquet.Table(take!(Parquet.write(IOBuffer(), written))),
        :x, "unit") == "m"
    mktempdir() do dir
        cp(path, joinpath(dir, "part-0.parquet"))
        ds = Parquet.Dataset(dir)
        @test DataAPI.colmetadata(ds, :a, "unit") == "kg"
        @test DataAPI.colmetadata(Parquet.Table(ds), :b, "desc") == "label"
        @test DataAPI.metadata(ds, "source") == "test"
    end
end

@testset "row windows keep column kinds" begin
    bytes = take!(Parquet.write(IOBuffer(), (a=collect(1:100),
        s=["a long string value number $i" for i in 1:100]); rowgroupsize=30))
    file = Parquet.File(bytes; filter=Parquet.col(:a) > 10, limit=40)
    parts = collect(file)
    close(file)
    @test sum(Tables.rowcount, parts) == 40
    @test all(part -> part.s isa Parquet.DataStrings.StringVector, parts)
    @test plainvalues(reduce(vcat, [collect(part.s) for part in parts])) ==
        ["a long string value number $i" for i in 11:50]
end
