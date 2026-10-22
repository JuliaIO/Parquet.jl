using SHA
using Test

const N5_CORPUS_COMMIT =
    "09f3cdbde45302f0f0c689c950e465e98a9df960"

const N5_CORPUS_FILES = [
    ("list_columns.parquet",
        "5988ab91b6cb7efa7bf6a77f789b40929212280519be6c9daad56e01d5ceb218"),
    ("null_list.parquet",
        "e64a64ff130c8dff64a6bc41480c51c87918d5e63bc75167b58524aa0fa01496"),
    ("datapage_v2.snappy.parquet",
        "44f29191b5fa8cfe0ab848495bd8ef89344ac0d8f87b3dff12e267631e2b5c03"),
    ("old_list_structure.parquet",
        "065b336c65885ab9dfd97cf85ce39a45488ed12d0183917db0a11621b0711e3b"),
    ("nested_lists.snappy.parquet",
        "2cb2cc0564486a28550429a8b6d0907bbb41e138546797bc91a4ebd850edd5a5"),
    ("nested_maps.snappy.parquet",
        "db1a493003a7dcd2011bf89e460fed007903fcdeb58f53df29387b4e908e2a6d"),
    ("repeated_primitive_no_list.parquet",
        "fcd6152058b8b8259a516105da5919b23cb8ccfc42258de0fe20e3107f8ef809"),
    ("repeated_no_annotation.parquet",
        "97d35acb9721e40fc0f66fba916a442c4a1cf77a35992dc891ad0cdcc5a24cfb"),
    ("nullable.impala.parquet",
        "de9102a599d852be3af1d2af5d3498d8e019c329096a6f2d260f55ae2d6ed0ae"),
    ("nonnullable.impala.parquet",
        "e7927cde24c083e42a3d4b37ac962d34381f71c2d252169b627dd8459a5880e3"),
    ("map_no_value.parquet",
        "5c4fc6c13fe7308acb2fd317a3bd59e5b9c9c206c005e863ae0a1abdbbf5e2ea"),
    ("incorrect_map_schema.parquet",
        "5591dde252b46bc238a88e9c02e35780c5eb086e2677df105aaa91ff1fde8fba"),
    ("nested_structs.rust.parquet",
        "48427178bfef9e6edd9018f2ef7b084077c00057234a780271a8220ca53b33da"),
    ("large_string_map.brotli.parquet",
        "1ce6839f093ebc0699b1e2769ed04036bab40405bacbb5dacdd376dd94c13451"),
]

function n5corpusroot()
    default = normpath(joinpath(@__DIR__, "..", "..", "parquet-testing"))
    return get(ENV, "PARQUET_TESTING_DIR", default)
end

function n5corpusrevision(root::String)
    try
        return readchomp(`git -C $root rev-parse HEAD`)
    catch err
        throw(ErrorException("cannot verify parquet-testing revision at " *
            "$(repr(root)): $(sprint(showerror, err))"))
    end
end

function n5filesha256(path::String)
    return open(path, "r") do input
        return bytes2hex(SHA.sha256(input))
    end
end

@testset "N5 pinned nested corpus" begin
    root = n5corpusroot()
    @test isdir(root)
    @test n5corpusrevision(root) == N5_CORPUS_COMMIT
    for (name, expected) in N5_CORPUS_FILES
        path = joinpath(root, "data", name)
        @test isfile(path)
        @test n5filesha256(path) == expected
    end
end
