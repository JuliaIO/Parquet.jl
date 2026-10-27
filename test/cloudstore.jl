using CloudBaseTest, CloudStore
using Tables

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
