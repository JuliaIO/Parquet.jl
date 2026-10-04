using Parquet, Tables, Test

@testset "Empty dataset columns" begin
    fixture = joinpath(@__DIR__, "booltest", "alltypes_plain.snappy.parquet")
    mapping = Dict(["date_string_col"] => (String, Parquet.logical_string))
    for factory in (Parquet.Dataset, Parquet.read_parquet), (metadata, datafile) in (
        (nothing, true), ("_common_metadata", true), ("_metadata", true),
        ("_common_metadata", false), ("_metadata", false),
    )
        mktempdir() do path
            datafile && cp(fixture, joinpath(path, "a.parquet"))
            metadata === nothing || cp(fixture, joinpath(path, metadata))
            generator = (args...) -> error("No columns should be generated without partitions")
            dataset = factory(path; filter=p -> !datafile, batchsize=1,
                              map_logical_types=mapping, column_generator=generator)
            try
                schema = Tables.schema(dataset)
                @test Tables.columnnames(dataset) == schema.names
                @test Base.nonmissingtype(schema.types[9]) === String
                @test isempty(collect(Tables.partitions(dataset)))
                for (index, name) in enumerate(schema.names)
                    column = Tables.getcolumn(dataset, name)
                    @test isempty(column)
                    @test eltype(column) === schema.types[index]
                    @test isempty(Tables.getcolumn(dataset, index))
                end
                columns = Tables.columntable(dataset)
                @test keys(columns) == schema.names
                @test all(isempty, columns)
                @test map(eltype, values(columns)) == schema.types
                @test isempty(Tables.rowtable(dataset))
                close(dataset)
                @test isempty(Tables.getcolumn(dataset, :id))
                @test eltype(Tables.getcolumn(dataset, :id)) === schema.types[1]
            finally
                close(dataset)
            end
        end
    end
end
