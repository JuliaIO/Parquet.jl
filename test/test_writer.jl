using Parquet
using Test
using Random

Random.seed!(1234567)

function test_write()
    tbl = (
        int32 = Int32.(1:1000),
        int64 = Int64.(1:1000),
        float32 = Float32.(1:1000),
        float64 = Float64.(1:1000),
        bool = rand(Bool, 1000),
        string = [randstring(8) for i in 1:1000],
        int32m = rand([missing, 1:100...], 1000),
        int64m = rand([missing, 1:100...], 1000),
        float32m = rand([missing, Float32.(1:100)...], 1000),
        float64m = rand([missing, Float64.(1:100)...], 1000),
        boolm = rand([missing, true, false], 1000),
        stringm = rand([missing, "abc", "def", "ghi"], 1000)
    )

    write_parquet("tmp_plsdel.parquet", tbl)

    pf = ParFile("tmp_plsdel.parquet")

    # the file is very smalll so only one rowgroup
    col_chunks = columns(pf, 1)

    for colnum in 1:length(col_chunks)
        correct_vals = tbl[colnum]
        coltype = eltype(correct_vals)
        vals_from_file = values(pf, col_chunks, colnum)
        if Missing <: coltype
            @test ismissing.(correct_vals) == (vals_from_file[2] .== 0)
        end

        if nonmissingtype(coltype) == String
            @test all(skipmissing(correct_vals) .== String.(vals_from_file[1]))
        else
            @test all(skipmissing(correct_vals) .== vals_from_file[1])
        end
    end

    # clean up
    close(pf)

    #rm("tmp_plsdel.parquet")
end

test_write()
