using Parquet, Tables, Test

@testset "Dataset logical type overrides" begin
    fixture = joinpath(@__DIR__, "booltest", "alltypes_plain.snappy.parquet")
    mapping = Dict(["date_string_col"] => (String, Parquet.logical_string))
    for metadata in (nothing, "_common_metadata", "_metadata")
        mktempdir() do path
            cp(fixture, joinpath(path, "a.parquet"))
            cp(fixture, joinpath(path, "b.parquet"))
            metadata === nothing || cp(fixture, joinpath(path, metadata))

            plain = Parquet.Dataset(path)
            try
                @test Base.nonmissingtype(Tables.schema(plain).types[9]) === Vector{UInt8}
                @test String.(copy.(Tables.getcolumn(plain, :date_string_col))) == fill("04/01/09", 4)
            finally
                close(plain)
            end

            for factory in (Parquet.Dataset, Parquet.read_parquet), use_threads in (false, true)
                overrides = copy(mapping)
                mapped = factory(path; map_logical_types=overrides, batchsize=1, use_threads=use_threads)
                @test mapped isa Parquet.Dataset
                try
                    @test overrides == mapping
                    empty!(overrides)
                    @test Base.nonmissingtype(Tables.schema(mapped).types[9]) === String
                    @test Tables.getcolumn(mapped, :date_string_col) == fill("04/01/09", 4)
                    @test Tables.getcolumn(mapped, :id) == Int32[6, 7, 6, 7]
                    npartitions = 0
                    for partition in Tables.partitions(mapped)
                        npartitions += 1
                        try
                            @test Tables.getcolumn(partition, :date_string_col) == fill("04/01/09", 2)
                        finally
                            close(partition)
                        end
                    end
                    @test npartitions == 2
                finally
                    close(mapped)
                end
            end

            filtered = Parquet.Dataset(path; map_logical_types=mapping, filter=p -> endswith(p, "a.parquet"))
            try
                @test filtered isa Parquet.Dataset
                @test Tables.getcolumn(filtered, :date_string_col) == fill("04/01/09", 2)
            finally
                close(filtered)
            end
        end
    end
end
