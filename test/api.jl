@testset "public surface" begin
    @test !Base.isexported(Parquet, :File)
    @test !Base.isexported(Parquet, :Limits)
    @test !Base.isexported(Parquet, :Table)
    @test !Base.isexported(Parquet, :write)
    publicnames = (:AbstractSource, :BSONValue, :Decimal, :FormatError, :JSONValue,
        :LimitError, :LogicalColumn, :UnsupportedFeatureError, :col, :colcmp, :colin,
        :concurrentreads, :isnull, :metadata, :read, :readrange, :sourcelength)
    for name in publicnames
        @test !Base.isexported(Parquet, name)
    end
    # Timestamps and INTERVAL values are Durations types.
    @test !isdefined(Parquet, :Timestamp)
    @test !isdefined(Parquet, :Interval)
    if VERSION >= v"1.11"
        @test Base.ispublic(Parquet, :File)
        @test Base.ispublic(Parquet, :Limits)
        @test Base.ispublic(Parquet, :Table)
        @test !Base.ispublic(Parquet, :close!)
        @test Base.ispublic(Parquet, :write)
        for name in publicnames
            @test Base.ispublic(Parquet, name)
        end
    end
end

@testset "error types are documented" begin
    documented = Base.Docs.meta(Parquet)
    for name in (:FormatError, :LimitError, :UnsupportedFeatureError, :File, :Table,
            :AbstractSource, :sourcelength, :readrange, :concurrentreads, :metadata,
            :read, :col, :colcmp, :colin, :isnull)
        @test haskey(documented, Base.Docs.Binding(Parquet, name))
    end
end
