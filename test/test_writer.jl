using Parquet
using Test
using Random

if VERSION < v"1.3"
    using Missings: nonmissingtype
end

Random.seed!(1234567)

function test_write()
    N = 1000

    tbl = (
        int32 = rand(Int32, N),
        int64 = rand(Int64, N),
        float32 = rand(Float32, N),
        float64 = rand(Float64, N),
        bool = rand(Bool, N),
        string = [randstring(8) for i in 1:N],
        int32m = rand([missing, rand(Int32, 10)...], N),
        int64m = rand([missing, rand(Int64, 10)...], N),
        float32m = rand([missing, rand(Float32, 10)...], N),
        float64m = rand([missing, rand(Float64, 10)...], N),
        boolm = rand([missing, true, false], N),
        stringm = rand([missing, "abc", "def", "ghi"], N)
    )

    tmpfile = tempname()*".parquet"

    write_parquet(tmpfile, tbl)

    pf = ParFile(tmpfile; map_logical_types=true)

    # the file is very small so only one rowgroup
    col_chunks = columns(pf, 1)

    for (colnum, col_chunk) in enumerate(col_chunks)
        correct_vals = tbl[colnum]
        coltype = eltype(correct_vals)
        vals_from_file = values(pf, col_chunk)
        if Missing <: coltype
            @test ismissing.(correct_vals) == (vals_from_file[2] .== 0)
        end

        non_missing_vals = collect(skipmissing(correct_vals))

        if nonmissingtype(coltype) == String
            non_missing_vals_read = String.(vals_from_file[1][1:sum(vals_from_file[2])])
            @test all(non_missing_vals .== non_missing_vals_read)
        else
            non_missing_vals_read = vals_from_file[1][1:sum(vals_from_file[2])]
            @test all(non_missing_vals .== non_missing_vals_read)
        end
    end

    # test with BatchedColumnsCursor
    cc = Parquet.BatchedColumnsCursor(pf)
    vals, _ = iterate(cc)
    @test length(vals) == 12
    @test length(vals.boolm) == N
    for idx in 1:N
        @test ismissing(vals.boolm[idx]) == ismissing(tbl.boolm[idx])
        if !ismissing(vals.boolm[idx])
            @test vals.boolm[idx] == tbl.boolm[idx]
        end
    end

    # clean up
    close(pf)
end

test_write()
