using Parquet
using Test

function test_col_cursor(file::String)
    p = ParFile(file)
    println("loaded ", file)

    nr = nrows(p)
    cnames = colnames(p)
    for cname in cnames
        rr = 1:nr
        println("\tcolumn ", cname, " rows:", rr)
        println("\tvalue, defn, repn, next idx")
        t1 = time()
        cc = Parquet.ColCursor(p, rr, cname)
        num_read = 0
        for (v,i) in enumerate(cc)
            val,defn,repn = v
            num_read += 1
        end
        println("\t\t", isnull(val) ? nothing : get(val), ", ", defn, ", ", repn, ", ", i)
        println("\t\tread $num_read values in $(time()-t1) time")
    end
end

function test_row_cursor(file::String)
    p = ParFile(file)

    t1 = time()
    nr = nrows(p)
    cnames = colnames(p)
    rc = RecordCursor(p)
    rec = nothing
    for i in rc
        rec = i
    end
    @info("loaded", file, count=nr, last_record=rec, time_to_read=time()-t1)
end

function test_batchedcols_cursor(file::String)
    p = ParFile(file)

    t1 = time()
    nr = nrows(p)
    cnames = colnames(p)
    cc = BatchedColumnsCursor(p)
    batch = nothing
    for i in cc
        batch = i
    end
    @info("loaded", file, count=nr, ncols=length(propertynames(batch)), time_to_read=time()-t1)
end

function test_col_cursor_all_files()
    for encformat in ("SNAPPY", "GZIP", "NONE")
        for fname in ("nation", "customer")
            test_col_cursor(joinpath(@__DIR__, "parquet-compatibility", "parquet-testdata", "impala", "1.1.1-$encformat/$fname.impala.parquet"))
        end
    end
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

#test_col_cursor_all_files()
test_row_cursor_all_files()
test_batchedcols_cursor_all_files()
