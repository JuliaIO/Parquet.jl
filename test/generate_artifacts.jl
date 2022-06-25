using ArtifactUtils, Artifacts

add_artifact!(
    joinpath(@__DIR__, "..", "Artifacts.toml"),
    "parcompat",
    "https://github.com/Parquet/parquet-compatibility/archive/2b47eac447c7a4a88247651a4065984db7b247ff.tar.gz",
    force=true,
    lazy=true,
)

add_artifact!(
    joinpath(@__DIR__, "..", "Artifacts.toml"),
    "julia_parcompat",
    "https://github.com/JuliaIO/parquet-compatibility/archive/3f7586f1b7f2a0c6b048791fb5f97c0b3df52e39.tar.gz",
    force=true,
    lazy=true,
)
