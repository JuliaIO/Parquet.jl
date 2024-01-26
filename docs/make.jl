using Parquet
using Documenter

makedocs(;
    modules = [Parquet],
    authors = "JuliaIO and contributors",
    repo = "https://github.com/JuliaIO/Parquet.jl/blob/{commit}{path}#L{line}",
    sitename = "Parquet.jl",
    format = Documenter.HTML(;
        prettyurls = get(ENV, "CI", "false") == "true",
        canonical = "https://JuliaIO.github.io/Parquet.jl",
        assets = String[],
    ),
    pages = [
        "Home" => "index.md",
        "API" => "api.md",
    ]
)

deploydocs(;
    repo="github.com/JuliaIO/Parquet.jl",
)

