using Parquet
using Test
using Random

if VERSION < v"1.3"
    using Missings: nonmissingtype
end

Random.seed!(1234567)

function test_written(tbl, tmpfile)
    pf = Parquet.File(tmpfile)
    N = length(tbl.int32)

    # the file is very small so only one rowgroup
    col_chunks = columns(pf, 1)

    for (colnum, col_chunk) in enumerate(col_chunks)
        correct_vals = tbl[colnum]
        coltype = eltype(correct_vals)
        jtype = Parquet.elemtype(Parquet.elem(schema(pf), colname(pf,col_chunk)))
        ccpv = Parquet.ColumnChunkPageValues(pf, col_chunk, jtype)
        resultdata,nextpos = iterate(ccpv)

        if Missing <: coltype
            @test ismissing.(correct_vals) == (resultdata.defn_level.data .== 0)
        end

        non_missing_vals = collect(skipmissing(correct_vals))

        if nonmissingtype(coltype) == String
            @test all(non_missing_vals .== String.(resultdata.value.data))
        else
            @test all(non_missing_vals .== resultdata.value.data)
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

function make_table(N::Int=1000)
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
end

function test_write()
    tbl = make_table()
    tmpfile = tempname()*".parquet"
    write_parquet(tmpfile, tbl)
    test_written(tbl, tmpfile)
end

function test_write_via_buffer()
    tbl = make_table()
    tmpfile = tempname()*".parquet"

    # write to io buffer first
    io = IOBuffer()
    write_parquet(io, tbl)

    # then dump io buffer to disk
    open(tmpfile, "w") do f
        write(f, take!(io))
    end

    #and now test round-trip via file
    test_written(tbl, tmpfile)
end

@testset "writer" begin
    test_write()
    test_write_via_buffer()
end
