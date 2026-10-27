@testset "public surface" begin
    @test !Base.isexported(Parquet, :File)
    @test !Base.isexported(Parquet, :Limits)
    @test !Base.isexported(Parquet, :Table)
    @test !Base.isexported(Parquet, :close!)
    @test !Base.isexported(Parquet, :write)
    for name in (:BSONValue, :Decimal, :FormatError, :JSONValue, :LimitError,
        :LogicalColumn, :UnsupportedFeatureError)
        @test !Base.isexported(Parquet, name)
    end
    # Timestamps and INTERVAL values are Durations types.
    @test !isdefined(Parquet, :Timestamp)
    @test !isdefined(Parquet, :Interval)
    if VERSION >= v"1.11"
        @test Base.ispublic(Parquet, :File)
        @test Base.ispublic(Parquet, :Limits)
        @test Base.ispublic(Parquet, :Table)
        @test Base.ispublic(Parquet, :close!)
        @test Base.ispublic(Parquet, :write)
        for name in (:BSONValue, :Decimal, :FormatError, :JSONValue,
            :LimitError, :LogicalColumn, :UnsupportedFeatureError)
            @test Base.ispublic(Parquet, name)
        end
    end
end

@testset "error types are documented" begin
    documented = Base.Docs.meta(Parquet)
    for name in (:FormatError, :LimitError, :UnsupportedFeatureError, :File, :Table)
        @test haskey(documented, Base.Docs.Binding(Parquet, name))
    end
end
