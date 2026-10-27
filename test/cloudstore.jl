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
