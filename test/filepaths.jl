using FilePathsBase: Path

@testset "FilePathsBase local paths" begin
    mktempdir() do directory
        path = Path(joinpath(directory, "data.parquet"))
        table = (a=[1, 2, 3], b=["one", "two", "three"])
        @test Parquet.write(path, table; rowgroupsize=2) === path
        @test Parquet.Table(path).a == table.a
        @test Parquet.read(path, Tables.columntable).b == table.b
        file = Parquet.File(path)
        try
            @test length(file) == 2
            @test file[2].a == [3]
        finally
            close(file)
        end
        original = read(string(path))
        @test_throws ErrorException Parquet.Writer(path) do writer
            Parquet.write(writer, (a=[4], b=["four"]))
            error("abort")
        end
        @test read(string(path)) == original
        @test Parquet.Writer(path) do writer
            Parquet.write(writer, (a=[4], b=["four"]))
        end === path
        @test Parquet.Table(path).a == [4]
        @test (table |> Parquet.write(path)) === path
        @test Parquet.Table([path, path]).a == repeat(table.a, 2)

        root = Path(joinpath(directory, "lake"))
        @test Parquet.write(root, (year=[2024, 2025], value=[10, 20]);
            partitionby=:year) === root
        @test Parquet.Table(root).value == [10, 20]
        @test Parquet.Table(Parquet.Dataset(root)).year == [2024, 2025]
        pattern = Path(joinpath(string(root), "**", "*.parquet"))
        @test Parquet.Table(pattern).value == [10, 20]
        files = [Path(joinpath(string(root), "year=$year", "part-0.parquet"))
            for year in (2024, 2025)]
        @test Parquet.Table(files).year == [2024, 2025]
    end
end
