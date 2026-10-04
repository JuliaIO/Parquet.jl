using Parquet, Tables, Test

@testset "Required batched columns" begin
    expected = (id = Int32.(1:19), score = Int64.(100:100:1900),
                measure = collect(1:19) ./ 4, active = iseven.(1:19),
                name = ["group" * string(i % 3) for i in 1:19])
    for variant in ("plain", "dictionary")
        path = joinpath(@__DIR__, "required", "required-" * variant * ".parquet")
        file = Parquet.File(path)
        try
            for rows in (1:19, 4:16), batchsize in (1, 3, 7, 64),
                reusebuffer in (false, true), use_threads in (false, true)
                columns = map(empty, expected)
                firstbatch = true
                cursor = Parquet.BatchedColumnsCursor(file; rows=rows, batchsize=batchsize,
                                                       reusebuffer=reusebuffer, use_threads=use_threads)
                for batch in cursor
                    if firstbatch
                        @test map(eltype, values(batch)) == map(eltype, values(expected))
                        firstbatch = false
                    end
                    for name in keys(expected)
                        append!(getproperty(columns, name), getproperty(batch, name))
                    end
                end
                for name in keys(expected)
                    @test getproperty(columns, name) == getproperty(expected, name)[rows]
                end
            end
        finally
            close(file)
        end
        for factory in (Parquet.Table, Parquet.read_parquet), batchsize in (1, 64), use_threads in (false, true)
            table = factory(path; batchsize=batchsize, use_threads=use_threads)
            try
                @test Tables.columnnames(table) == keys(expected)
                for name in keys(expected)
                    column = Tables.getcolumn(table, name)
                    @test column == getproperty(expected, name)
                    @test eltype(column) === eltype(getproperty(expected, name))
                end
            finally
                close(table)
            end
        end
    end
end
