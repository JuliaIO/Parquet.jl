using Parquet
using Test
using Random

if VERSION < v"1.3"
    using Missings: nonmissingtype
end

Random.seed!(1234567)

function test_write()
    tbl = (
        int32 = rand(Int32, 1000),
        int64 = rand(Int64, 1000),
        float32 = rand(Float32, 1000),
        float64 = rand(Float64, 1000),
        bool = rand(Bool, 1000),
        string = [randstring(8) for i in 1:1000],
        int32m = rand([missing, rand(Int32, 10)...], 1000),
        int64m = rand([missing, rand(Int64, 10)...], 1000),
        float32m = rand([missing, rand(Float32, 10)...], 1000),
        float64m = rand([missing, rand(Float64, 10)...], 1000),
        boolm = rand([missing, true, false], 1000),
        stringm = rand([missing, "abc", "def", "ghi"], 1000)
    )

    tmpfile = tempname()*".parquet"

    write_parquet(tmpfile, tbl)

    pf = ParFile(tmpfile)

    # the file is very small so only one rowgroup
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
end

test_write()
