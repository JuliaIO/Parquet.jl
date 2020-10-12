using Parquet
using Test

function test_row_cursor(file::String)
    p = ParFile(file)

    t1 = time()
    nr = nrows(p)
    cnames = colnames(p)
    rc = RecordCursor(p)
    rec = nothing
    nread = 0
    for i in rc
        rec = i
        nread += 1
    end
    @test nr == nread
    @debug("loaded", file, count=nr, last_record=rec, time_to_read=time()-t1)

    iob = IOBuffer()
    show(iob, rc)
    sb = take!(iob)
    @test !isempty(sb)
    @debug("row cursor show", file, showbuffer=String(sb))
end

function test_batchedcols_cursor(file::String)
    p = ParFile(file)

    t1 = time()
    nr = nrows(p)
    cnames = colnames(p)
    cc = BatchedColumnsCursor(p)
    batch = nothing
    nread = 0
    for i in cc
        batch = i
        nread += length(first(batch))
    end
    @test nr == nread
    @debug("loaded", file, count=nr, ncols=length(propertynames(batch)), time_to_read=time()-t1)

    iob = IOBuffer()
    show(iob, cc)
    sb = take!(iob)
    @test !isempty(sb)
    @debug("batched column cursor show", file, showbuffer=String(sb))
end

function test_row_cursor_all_files()
    for encformat in ("SNAPPY", "GZIP", "NONE")
        for fname in ("nation", "customer")
            test_row_cursor(joinpath(@__DIR__, "parquet-compatibility", "parquet-testdata", "impala", "1.1.1-$encformat/$fname.impala.parquet"))
        end
    end
end

function test_batchedcols_cursor_all_files()
    for encformat in ("SNAPPY", "GZIP", "NONE")
        for fname in ("nation", "customer")
            test_batchedcols_cursor(joinpath(@__DIR__, "parquet-compatibility", "parquet-testdata", "impala", "1.1.1-$encformat/$fname.impala.parquet"))
        end
    end
end

@testset "cursors" begin
    test_row_cursor_all_files()
    test_batchedcols_cursor_all_files()
end
