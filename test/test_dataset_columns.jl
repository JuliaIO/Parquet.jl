using Parquet, Tables, Test

@testset "Dataset column generators" begin
    path = joinpath(@__DIR__, "datasets", "bool_partition")
    for factory in (Parquet.Dataset, Parquet.read_parquet), batchsize in (nothing, 17),
            (filter, nrows, ntrue) in ((p -> true, 100, 58),
                (p -> occursin("bool=false", lowercase(p)), 42, 0))
        reference = factory(path; filter=filter, batchsize=batchsize)
        expected = try
            @test sum(Tables.getcolumn(reference, :bool)) == ntrue
            collect(Tables.getcolumn(reference, :int32))
        finally
            close(reference)
        end
        calls = Tuple{Symbol,Int}[]
        generator = (table, index, len) -> begin
            push!(calls, (Tables.columnnames(table)[index], len))
            fill(true, len)
        end
        dataset = factory(path; filter=filter, batchsize=batchsize, column_generator=generator)
        try
            @test isempty(calls)
            @test length(Tables.getcolumn(dataset, :bool)) == nrows
            @test all(Tables.getcolumn(dataset, :bool))
            @test Tables.getcolumn(dataset, :int32) == expected
            @test all(call -> first(call) == :bool, calls)
            @test sum(last.(calls)) == nrows
        finally
            close(dataset)
        end
    end
    for factory in (Parquet.Dataset, Parquet.read_parquet)
        dataset = factory(path; column_generator=Parquet.column_generator)
        try
            @test all(ismissing, Tables.getcolumn(dataset, :bool))
        finally
            close(dataset)
        end
        dataset = factory(path; column_generator=(t, c, l) -> throw(ArgumentError("missing column")))
        try
            @test_throws ArgumentError Tables.getcolumn(dataset, :bool)
        finally
            close(dataset)
        end
        dataset = factory(path; batchsize=17, column_generator=(t, c, l) -> fill(true, l))
        nrows = 0
        try
            for table in Tables.partitions(dataset)
                try
                    for batch in Tables.partitions(table)
                        column = Tables.getcolumn(batch, :bool)
                        @test all(column)
                        nrows += length(column)
                    end
                finally
                    close(table)
                end
            end
            @test nrows == 100
        finally
            close(dataset)
        end
    end
end
