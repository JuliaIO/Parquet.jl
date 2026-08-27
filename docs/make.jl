using Documenter
using Parquet

DocMeta.setdocmeta!(Parquet, :DocTestSetup, :(using Parquet); recursive=true)

makedocs(
    modules=[Parquet],
    sitename="Parquet.jl",
    format=Documenter.HTML(
        prettyurls=true,
        canonical="https://JuliaIO.github.io/Parquet.jl/stable",
        collapselevel=2,
    ),
    pages=[
        "Home" => "index.md",
        "Guide" => "guide.md",
        "API" => "api.md",
    ],
    pagesonly=true,
    checkdocs=:public,
)

deploydocs(
    repo="github.com/JuliaIO/Parquet.jl.git",
    devbranch="master",
    push_preview=false,
)
