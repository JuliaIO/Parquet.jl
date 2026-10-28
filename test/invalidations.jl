@testset "loading common data packages keeps the precompiled code" begin
    # Packages that add methods to generic functions invalidate precompiled code that was
    # inferred with abstract types, and the next read or write compiles again. Run in a
    # fresh process, where the code is the package image's.
    project = dirname(something(Base.active_project()))
    script = raw"""
        using Parquet
        entries = ccall(:jl_debug_method_invalidation, Any, (Cint,), 1)
        using CSV, CategoricalArrays, PooledArrays, SentinelArrays, TimeZones
        ccall(:jl_debug_method_invalidation, Any, (Cint,), 0)
        instance(x) = x isa Core.MethodInstance ? x : x isa Core.CodeInstance ?
            (x.def isa Core.MethodInstance ? x.def : x.def.def) : nothing
        invalidated = Set{Core.MethodInstance}()
        for x in entries
            mi = instance(x)
            mi isa Core.MethodInstance && mi.def isa Method &&
                Base.moduleroot(mi.def.module) === Parquet && push!(invalidated, mi)
        end
        foreach(mi -> println(stderr, mi), Iterators.take(invalidated, 20))
        print(length(invalidated))
        """
    invalidated = parse(Int, read(`$(Base.julia_cmd()) --startup-file=no
        --project=$project -e $script`, String))
    # Thousands were invalidated when kernels were also inferred for abstract types;
    # the bound leaves room for small changes in those packages. A prerelease Julia
    # adds its own Base code to what those packages invalidate, so there the count is
    # only reported.
    if isempty(VERSION.prerelease)
        @test invalidated < 100
    else
        @info "Parquet method instances invalidated on Julia $VERSION: $invalidated"
    end
end
