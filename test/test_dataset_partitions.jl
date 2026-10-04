using Parquet, Tables, Test

@testset "Collect dataset partitions" begin
    fixture = joinpath(@__DIR__, "booltest", "alltypes_plain.snappy.parquet")
    mktempdir() do path
        for name in ("a.parquet", "b.parquet", "_common_metadata", "_metadata")
            cp(fixture, joinpath(path, name))
        end
        for factory in (Parquet.Dataset, Parquet.read_parquet), (filter, expected) in (
            (p -> true, 2),
            (p -> endswith(p, "a.parquet"), 1),
            (p -> false, 0),
        )
            dataset = factory(path; filter=filter)
            try
                nmanual = 0
                for partition in Tables.partitions(dataset)
                    nmanual += 1
                    close(partition)
                end
                @test nmanual == expected
                partitions = collect(Tables.partitions(dataset))
                try
                    @test partitions isa AbstractVector
                    @test length(partitions) == expected
                    @test all(p -> p isa Parquet.Table, partitions)
                    @test all(p -> Tables.getcolumn(p, :id) == Int32[6, 7], partitions)
                finally
                    foreach(close, partitions)
                end
            finally
                close(dataset)
            end
        end
    end
end
