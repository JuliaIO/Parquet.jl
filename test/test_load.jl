using Parquet
using Base.Test

function test_load(file, parcompat=joinpath(dirname(@__FILE__), "parquet-compatibility"))
    p = ParFile(joinpath(parcompat, file))
    println("loaded $file")
    @test isa(p.meta, Parquet.FileMetaData)

    rg = rowgroups(p)
    @test length(rg) > 0
    println("\tfound $(length(rg)) row groups")

    cc = columns(p, 1)
    println("\tfound $(length(cc)) column chunks in row group 1")

    cnames = colnames(rg[1])
    @test length(cnames) == length(cc)
    println("\tcolumns: $cnames")

    pg = pages(p, 1, 1)
    println("\tfound $(length(pg)) pages in column chunk 1 of row group 1")

    bpg = bytes(pg[1])
    println("\tread $(length(bpg)) bytes from page 1, column chunk 1 of row group 1")

    cc1 = columns(p, rg[1], cnames[1:2:3])
    @test length(cc) > length(cc1)
    @test length(rowgroups(p, [cnames[1], "xyz"])) == 0
    println("\tsuccess")
end

function print_names(indent, t)
    for f in fieldnames(t)
        ftype = fieldtype(t, f)
        println("\t", indent, f, "::", ftype)
        if isa(ftype, DataType)
            print_names(indent * "    ", ftype)
        end
    end
end

function test_schema(file, schema_name::Symbol,  parcompat=joinpath(dirname(@__FILE__), "parquet-compatibility"))
    p = ParFile(joinpath(parcompat, file))
    println("loaded $file")

    #mod_name = string(schema_name) * "Module"
    #eval(Main, parse("module $mod_name end"))
    #mod = getfield(Main, Symbol(mod_name))

    mod = Main
    schema(JuliaConverter(mod), schema(p), schema_name)
    @test schema_name in names(mod, true)
    println("\tschema: \n\t", getfield(mod, schema_name))
    print_names("    ", getfield(mod, schema_name))

    io = IOBuffer()
    schema(ThriftConverter(io), schema(p), schema_name)
    thriftsch = strip(takebuf_string(io))
    @test startswith(thriftsch, "struct " * string(schema_name))
    @test endswith(thriftsch, "}")
    println(thriftsch)

    io = IOBuffer()
    schema(ProtoConverter(io), schema(p), schema_name)
    protosch = strip(takebuf_string(io))
    @test startswith(protosch, "message " * string(schema_name))
    @test endswith(protosch, "}")
    println(protosch)
end

function test_load_all_pages()
    for encformat in ("SNAPPY", "GZIP", "NONE")
        for fname in ("nation", "customer")
            test_load("parquet-testdata/impala/1.1.1-$encformat/$fname.impala.parquet")
            test_schema("parquet-testdata/impala/1.1.1-$encformat/$fname.impala.parquet", Symbol(fname * "_" * encformat))
        end
    end
end

test_load_all_pages()
