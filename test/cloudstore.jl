using CloudBaseTest, CloudStore
using Tables

@testset "CloudStore snapshots without a supplied ETag" begin
    Minio.with() do conf
        credentials, bucket = conf
        original = take!(Parquet.write(IOBuffer(), (a=Int64[1, 2],);
            compress=nothing, dictionary=false))
        replacement = take!(Parquet.write(IOBuffer(), (a=Int64[3, 4],);
            compress=nothing, dictionary=false))
        @test length(original) == length(replacement)
        CloudStore.put(bucket, "snapshot.parquet", original; credentials)
        tagged = CloudStore.Object(bucket, "snapshot.parquet"; credentials)
        untagged = CloudStore.Object(bucket, credentials, tagged.key, length(tagged), "")
        source = Parquet.source(untagged)
        @test Parquet.readrange(source, 0, 4) == original[1:4]
        dataset = Parquet.Dataset([untagged])
        @test Parquet.Table(dataset).a == [1, 2]

        # A same-size replacement must fail both later ranges and cached-footer reads.
        CloudStore.put(bucket, tagged.key, replacement; credentials)
        @test_throws CloudStore.API.HTTP.StatusError Parquet.readrange(source, 4, 4)
        @test_throws CloudStore.API.HTTP.StatusError Parquet.Table(dataset)
        # A supplied version is kept even if the object has since changed.
        known = Parquet.source(tagged)
        @test known.object === tagged
        @test_throws CloudStore.API.HTTP.StatusError Parquet.readrange(known, 0, 4)
        stale = CloudStore.Object(bucket, credentials, tagged.key, length(tagged) + 1, "")
        @test_throws ArgumentError Parquet.source(stale)
    end
end

@testset "CloudStore objects" begin
    rows = 100_000
    bytes = take!(Parquet.write(IOBuffer(), (a=collect(1:rows), b=[string(i) for i in 1:rows]);
        rowgroupsize=30_000))
    Minio.with() do conf
        credentials, bucket = conf
        CloudStore.put(bucket, "table.parquet", bytes; credentials)
        object = CloudStore.Object(bucket, "table.parquet"; credentials)
        table = Parquet.Table(object)
        @test Tables.rowcount(table) == rows
        @test table.a == 1:rows
        @test table.b[end] == string(rows)
        file = Parquet.File(object; select=[:b])
        @test length(file) == 4
        @test [Tables.rowcount(part) for part in file] == [30_000, 30_000, 30_000, 10_000]
        @test file[4].b[1] == "90001"
        close(file)
        # Filtered and windowed reads fetch the pages they decode.
        filtered = Parquet.Table(object; filter=Parquet.col(:a) > rows - 5)
        @test filtered.a == (rows - 4):rows
        @test filtered.b == string.((rows - 4):rows)
        @test Parquet.Table(object; scan=Tables.Scan(limit=3, offset=50_000)).b ==
            ["50001", "50002", "50003"]
    end
end

@testset "CloudStore datasets" begin
    Minio.with() do conf
        credentials, bucket = conf
        for year in (2024, 2025), part in 1:2
            bytes = take!(Parquet.write(IOBuffer(), (id=[100 * year + part], s=["v$part"])))
            CloudStore.put(bucket, "lake/year=$year/part-$part.parquet", bytes; credentials)
        end
        CloudStore.put(bucket, "lake/_SUCCESS", UInt8[]; credentials)
        CloudStore.put(bucket, "lakehouse/x.parquet",
            take!(Parquet.write(IOBuffer(), (id=[1],))); credentials)
        ds = Parquet.Dataset(bucket, "lake/"; credentials)
        @test length(ds.files) == 4
        t = Parquet.Table(ds)
        @test t.year == [2024, 2024, 2025, 2025]
        @test t.id == [202401, 202402, 202501, 202502]
        filtered = Parquet.Dataset(bucket, "lake/"; credentials,
            filter=Parquet.colcmp(==, Parquet.col(:year), 2025))
        @test Parquet.Table(filtered).id == [202501, 202502]
        @test_throws ArgumentError Parquet.Dataset(bucket, "nothing-here/"; credentials)
    end
end
