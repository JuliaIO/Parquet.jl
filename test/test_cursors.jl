using Parquet
using Base.Test

function test_cursor(file::ByteString, parcompat::ByteString=joinpath(dirname(@__FILE__), "parquet-compatibility"))
    p = ParFile(joinpath(parcompat, file))
    println("loaded ", file)

    nr = nrows(p)
    cnames = colnames(p)
    for cname in cnames
        rr = 1:nr
        println("\tcolumn ", cname, " rows:", rr)
        println("\tvalue, defn, repn, next idx")
        t1 = time()
        cc = ColCursor(p, rr, cname)
        i = start(cc)
        num_read = 0
        while !done(cc, i)
            v,i = next(cc, i)
            val,defn,repn = v
            done(cc, i) && println("\t\t", isnull(val) ? nothing : get(val), ", ", defn, ", ", repn, ", ", i)
            num_read += 1
        end
        println("\t\tread $num_read values in $(time()-t1) time")
    end
end

function test_cursor_all_files()
    for encformat in ("SNAPPY", "GZIP", "NONE")
        for fname in ("nation", "customer")
            test_cursor("parquet-testdata/impala/1.1.1-$encformat/$fname.impala.parquet")
        end
    end
end

test_cursor_all_files()
