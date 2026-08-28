using Parquet
using Test

include(joinpath(@__DIR__, "N6ParquetJLHarness.jl"))

const N6H = N6ParquetJLHarness
const N6MD = Parquet.Metadata
const N6Model = N6H.Model

function _n6unknownorder()
    raw = Parquet.Thrift.RawField(77, Parquet.Thrift.STRUCT, UInt8[0x00])
    return N6MD.ColumnOrder(unknown_fields=(raw,))
end

function _n6factpair(family::Symbol, createdby::String;
        order::Symbol=:type, total::Int64=Int64(4), nulls=nothing,
        lower::Vector{UInt8}=UInt8[0x01],
        upper::Vector{UInt8}=UInt8[0x02], limit::Int64=Int64(4096))
    family in (:modern, :deprecated) || throw(ArgumentError(
        "unsupported statistics family $family"))
    element = N6MD.SchemaElement(type_=N6MD.Type.BYTE_ARRAY,
        repetition_type=N6MD.FieldRepetitionType.OPTIONAL, name="value")
    root = N6MD.SchemaElement(
        repetition_type=N6MD.FieldRepetitionType.REQUIRED, name="schema",
        num_children=Int32(1))
    schema = Parquet.Schema(N6MD.SchemaElement[root, element])
    statistics = family === :modern ? N6MD.Statistics(
        min_value=lower, max_value=upper, null_count=nulls) :
        N6MD.Statistics(min=lower, max=upper, null_count=nulls)
    metadata = N6MD.ColumnMetaData(type_=N6MD.Type.BYTE_ARRAY,
        encodings=[N6MD.Encoding.PLAIN], path_in_schema=["value"],
        codec=N6MD.CompressionCodec.UNCOMPRESSED, num_values=total,
        total_uncompressed_size=Int64(0), total_compressed_size=Int64(0),
        data_page_offset=Int64(0), statistics=statistics)
    productionorders = order === :missing ? nothing : N6MD.ColumnOrder[
        order === :type ? N6MD.ColumnOrder(TYPE_ORDER=N6MD.TypeDefinedOrder()) :
        order === :unknown ? _n6unknownorder() : throw(ArgumentError(
            "unsupported declared order $order"))]
    modelorders = order === :missing ? nothing : N6Model.DeclaredOrder[
        order === :type ? N6Model.ORDER_TYPE : N6Model.ORDER_FUTURE]
    rawstatistics = family === :modern ? N6Model.RawStatistics(
        modern_lower=lower, modern_upper=upper, null_count=nulls) :
        N6Model.RawStatistics(deprecated_lower=lower,
            deprecated_upper=upper, null_count=nulls)
    spec = N6Model.LeafSpec(N6Model.PHYSICAL_BYTE_ARRAY)
    modeled = N6Model.interpret_statistics(spec, total, rawstatistics,
        modelorders; created_by=createdby,
        limits=N6Model.ModelLimits(max_statistics_value_bytes=limit))
    production = Parquet._statisticsfacts(schema, 1, createdby,
        productionorders, metadata;
        limits=Parquet.Limits(max_statistics_value_bytes=limit))
    selectedorder = modelorders === nothing ? nothing : only(modelorders)
    N6H.comparefacts(modeled, production, spec, selectedorder)
    return modeled, production
end

@testset "N6 Parquet.jl deterministic fixture and evidence harness" begin
    first_output = N6H.buildharness()
    second_output = N6H.buildharness()
    @test length(first_output.files) == 12
    @test length(first_output.checked) == 12
    @test first_output.files == second_output.files
    @test first_output.evidence == second_output.evidence
    @test count(==(UInt8('\n')), first_output.evidence) == 96
    @test sum(length(case.column_records) for case in first_output.checked) == 52
    @test sum(1 + length(case.column_records) for case in first_output.checked) == 64
    @test length(unique(Base.first(pair) for pair in first_output.files)) == 12
    @test all(pair -> length(last(pair)) <= N6H.MAX_GENERATED_BYTES,
        first_output.files)
    grouped = [case for case in first_output.checked if
        case.declaration["comparison_group"] ==
            "julia-reader-no-pruning-v1"]
    @test length(grouped) == 5
    @test length(Set(case.no_pruning_sha256 for case in grouped)) == 1
    @test length(Set(case.assertion_facts["body_sha256"] for case in grouped)) == 1
    @test length(Set(case.logical_values_sha256 for case in grouped)) == 1
    @test length(Set(case.assertion_facts["range_trace_sha256"]
        for case in grouped)) == 1
    @test length(Set(case.assertion_facts["read_count"]
        for case in grouped)) == 1
end

@testset "N6 harness destination and independence guards" begin
    @test_throws ArgumentError N6H._safeoutput("../escape.parquet")
    @test_throws ArgumentError N6H._safeoutput("generated/nested/escape.parquet")
    @test_throws ArgumentError N6H._safeoutput("generated/escape\\file.parquet")
    # Durable publication fsyncs its destination directory and relies on POSIX
    # rename and symlink behavior, so the harness refuses to publish anywhere else.
    # Exercise those guarantees only where they exist.
    Sys.isunix() && mktempdir() do directory
        path = joinpath(directory, "value.bin")
        N6H._atomicreplacebytes(path, UInt8[0x01, 0x02])
        @test read(path) == UInt8[0x01, 0x02]
        @test filemode(path) & 0o777 == 0o644
        N6H._atomicreplacebytes(path, UInt8[0x03])
        @test read(path) == UInt8[0x03]
        link = joinpath(directory, "link.bin")
        symlink(path, link)
        @test_throws ArgumentError N6H._atomicreplacebytes(link, UInt8[0x04])
        @test read(path) == UInt8[0x03]
        directory_target = joinpath(directory, "directory.bin")
        mkdir(directory_target)
        @test_throws ArgumentError N6H._atomicreplacebytes(directory_target,
            UInt8[0x04])
        first = joinpath(directory, "first.bin")
        write(first, UInt8[0x05])
        @test_throws ArgumentError N6H._atomicreplacebatch(
            Pair{String,Vector{UInt8}}[
                first => UInt8[0x06],
                directory_target => UInt8[0x07],
            ])
        @test read(first) == UInt8[0x05]
        syncs = Ref(0)
        failfirstsync = function(path)
            syncs[] += 1
            syncs[] == 1 && error("injected directory sync failure")
            return N6H._fsyncdirectory(path)
        end
        @test_throws ErrorException N6H._atomicreplacebatch(
            Pair{String,Vector{UInt8}}[first => UInt8[0x08]];
            syncdirectory=failfirstsync)
        @test read(first) == UInt8[0x05]
        displacement = joinpath(directory, "displacement.bin")
        write(displacement, UInt8[0x09])
        reservationobserved = Ref(false)
        replacement = N6H._renameoverreserved(displacement, directory;
            renamefile=function(source, target)
                reservationobserved[] = isfile(target)
                return N6H._renamefile(source, target)
            end)
        @test reservationobserved[]
        @test !ispath(displacement)
        @test read(replacement) == UInt8[0x09]
        rm(replacement)
        @test N6H._stablefilebytes(first, Int64(1), "test input") == UInt8[0x05]
        @test_throws ArgumentError N6H._stablefilebytes(first, Int64(0),
            "test input")
        inputlink = joinpath(directory, "input-link.bin")
        symlink(first, inputlink)
        @test_throws ArgumentError N6H._stablefilebytes(inputlink, Int64(1),
            "test input")
    end
    stale, stream = mktemp(joinpath(N6H.N6_ROOT, "generated"))
    try
        write(stream, zeros(UInt8, 1024))
        close(stream)
        @test_throws ArgumentError N6H._checkbytes(stale, UInt8[0x01])
    finally
        isopen(stream) && close(stream)
        ispath(stale) && rm(stale; force=true)
    end
    modelsource = String(copy(N6H.FROZEN_MODEL_BYTES))
    @test !occursin(r"(?m)^\s*(using|import)\s+Parquet\b", modelsource)
    for (directory, _, names) in walkdir(joinpath(N6H.REPO_ROOT, "src"))
        for name in names
            endswith(name, ".jl") || continue
            @test !occursin("N6StatisticsModel",
                read(joinpath(directory, name), String))
        end
    end
    if VERSION == N6H.CANONICAL_WRITER_VERSION
        @test isnothing(N6H._checkwriteridentity())
    else
        @test_throws ArgumentError N6H._checkwriteridentity()
    end
end

@testset "N6 statistics policy cross-precedence" begin
    producers = ("parquet-cpp version 1.2.9",
        "parquet-mr version 1.9.9")
    for createdby in producers
        modeled, production = _n6factpair(:modern, createdby)
        @test modeled.comparator == N6Model.COMPARATOR_UNSIGNED_BYTES
        @test modeled.trust.state == N6Model.TRUST_UNTRUSTED
        @test production.comparison == :unsigned_bytes
        modeled, production = _n6factpair(:deprecated, createdby)
        @test modeled.comparator == N6Model.COMPARATOR_UNDEFINED
        @test production.comparison == :undefined
        @test modeled.lower.reason == :deprecated_order_mismatch
        @test production.lower.reason == :deprecated_order_mismatch
        for order in (:missing, :unknown)
            modeled, production = _n6factpair(:modern, createdby;
                order=order, total=Int64(4), nulls=Int64(4))
            expected = order === :missing ? :missing_column_orders :
                :unknown_column_order
            @test modeled.occupancy == N6Model.OCCUPANCY_EMPTY
            @test modeled.trust.state == N6Model.TRUST_UNTRUSTED
            @test modeled.lower.reason == expected
            @test production.occupancy == :no_non_null
        end
        for equal in (false, true)
            lower = fill(UInt8(0x61), 5)
            upper = equal ? copy(lower) : fill(UInt8(0x62), 5)
            modeled, production = _n6factpair(:modern, createdby;
                lower=lower, upper=upper, limit=Int64(4))
            @test modeled.trust.state == N6Model.TRUST_UNTRUSTED
            @test modeled.lower.reason == :over_limit
            @test modeled.upper.reason == :over_limit
            @test production.lower.reason == :over_limit
            @test production.upper.reason == :over_limit
        end
    end
end

@testset "N6 generated output freshness" begin
    output = N6H.runharness(:check)
    @test length(output.files) == 12
    @test isfile(N6H.EVIDENCE_FILE)
    @test !islink(N6H.EVIDENCE_FILE)
    for (relative, _) in output.files
        path = N6H._safeoutput(relative)
        @test isfile(path)
        @test !islink(path)
    end
end
