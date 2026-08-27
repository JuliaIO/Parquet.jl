using SHA

const INPUT_LIMIT = 16 * 1024 * 1024
const MODEL_SHA256 =
    "32c090ed6e0c6af49eabf3f96afc6e17dff630c4e89372367e693202e87c4262"
const MODEL_PATH = normpath(joinpath(@__DIR__, "..", "model",
    "N6StatisticsModel.jl"))
const MODEL_TEST_PATH = normpath(joinpath(@__DIR__, "..", "model",
    "runtests.jl"))

bytes2hex(open(sha256, MODEL_PATH)) == MODEL_SHA256 ||
    error("independent model hash mismatch")
include(MODEL_PATH)
const Model = N6StatisticsModel

function parsenothing(token::String, parser)
    token == "-" && return nothing
    return parser(token)
end

function parsehex(token::String)
    token == "-" && return nothing
    iseven(ncodeunits(token)) || throw(ArgumentError("hex token has odd length"))
    occursin(r"^[0-9a-f]*$", token) ||
        throw(ArgumentError("hex token is not lowercase hexadecimal"))
    return hex2bytes(token)
end

function parsebool(token::String)
    token == "0" && return false
    token == "1" && return true
    throw(ArgumentError("Boolean token is invalid"))
end

function physicaltype(token::String)
    values = Dict(
        "BOOLEAN" => Model.PHYSICAL_BOOLEAN,
        "INT32" => Model.PHYSICAL_INT32,
        "INT64" => Model.PHYSICAL_INT64,
        "INT96" => Model.PHYSICAL_INT96,
        "FLOAT" => Model.PHYSICAL_FLOAT,
        "DOUBLE" => Model.PHYSICAL_DOUBLE,
        "BYTE_ARRAY" => Model.PHYSICAL_BYTE_ARRAY,
        "FIXED_LEN_BYTE_ARRAY" => Model.PHYSICAL_FIXED_LEN_BYTE_ARRAY,
    )
    haskey(values, token) || throw(ArgumentError("physical type is invalid"))
    return values[token]
end

function logicaltype(token::String, signed::Union{Nothing,Bool})
    if token == "INTEGER"
        signed === nothing && throw(ArgumentError("INTEGER lacks signedness"))
        return signed ? Model.LOGICAL_SIGNED_INTEGER :
            Model.LOGICAL_UNSIGNED_INTEGER
    end
    values = Dict(
        "NONE" => Model.LOGICAL_NONE,
        "STRING" => Model.LOGICAL_STRING,
        "ENUM" => Model.LOGICAL_ENUM,
        "JSON" => Model.LOGICAL_JSON,
        "BSON" => Model.LOGICAL_BSON,
        "UUID" => Model.LOGICAL_UUID,
        "DECIMAL" => Model.LOGICAL_DECIMAL,
        "DATE" => Model.LOGICAL_DATE,
        "TIME" => Model.LOGICAL_TIME,
        "TIMESTAMP" => Model.LOGICAL_TIMESTAMP,
        "FLOAT16" => Model.LOGICAL_FLOAT16,
        "INTERVAL" => Model.LOGICAL_INTERVAL,
        "UNKNOWN" => Model.LOGICAL_UNKNOWN,
        "VARIANT" => Model.LOGICAL_VARIANT,
        "GEOMETRY" => Model.LOGICAL_GEOMETRY,
        "GEOGRAPHY" => Model.LOGICAL_GEOGRAPHY,
        "LIST" => Model.LOGICAL_LIST,
        "MAP" => Model.LOGICAL_MAP,
    )
    haskey(values, token) || throw(ArgumentError("logical type is invalid"))
    return values[token]
end

function timeunit(token::String)
    token == "-" && return nothing
    token == "MILLIS" && return Model.TIME_MILLIS
    token == "MICROS" && return Model.TIME_MICROS
    token == "NANOS" && return Model.TIME_NANOS
    throw(ArgumentError("time unit is invalid"))
end

function declaredorders(token::String, leaf::Int, count::Int)
    token == "ABSENT" && return nothing
    0 <= leaf < count || throw(ArgumentError("leaf ordinal is invalid"))
    orders = fill(Model.ORDER_FUTURE, count)
    orders[leaf + 1] = token == "TYPE_ORDER" ? Model.ORDER_TYPE :
        token == "IEEE_754_TOTAL_ORDER" ? Model.ORDER_IEEE : Model.ORDER_FUTURE
    return orders
end

function modelvalue(value)
    value === nothing && return "NONE"
    value isa Model.SignedValue && return "SIGNED:" * string(value.value)
    value isa Model.UnsignedValue && return "UNSIGNED:" * string(value.value)
    value isa Model.BooleanValue && return "BOOLEAN:" * (value.value ? "1" : "0")
    value isa Model.ByteValue && return "BYTES:" * bytes2hex(value.value)
    value isa Model.DecimalValue && return "DECIMAL:" * bytes2hex(value.value)
    if value isa Model.FloatValue
        width = Int(value.width)
        digits = width ÷ 4
        return "FLOAT:" * string(width) * ":" *
            string(value.bits; base=16, pad=digits)
    end
    throw(ArgumentError("model returned an unknown value kind"))
end

function boundtokens(bound::Model.BoundFact)
    return String[
        string(bound.state),
        string(bound.reason),
        string(bound.exactness),
        modelvalue(bound.value),
    ]
end

function resulttokens(index::String, result::Model.StatisticsResult)
    output = String[index, "OK"]
    append!(output, boundtokens(result.lower))
    append!(output, boundtokens(result.upper))
    append!(output, String[
        result.null_count.known ? "1" : "0",
        string(result.null_count.value),
        result.nan_count.known ? "1" : "0",
        string(result.nan_count.value),
        result.distinct_count.known ? "1" : "0",
        string(result.distinct_count.value),
        string(result.occupancy),
        string(result.family),
        string(result.comparator),
        string(result.trust.state),
        string(result.trust.reason),
    ])
    return output
end

function createdby(present::Bool, token::String)
    !present && token == "-" && return nothing
    present || throw(ArgumentError("absent created_by carries bytes"))
    bytes = parsehex(token)
    bytes === nothing && throw(ArgumentError("created_by bytes are absent"))
    return String(bytes)
end

function interpret(fields::Vector{SubString{String}})
    length(fields) == 25 || throw(ArgumentError("bridge input field count is invalid"))
    index = String(fields[1])
    leafindex = parse(Int, fields[4])
    leafcount = parse(Int, fields[5])
    signed = parsenothing(String(fields[10]), parsebool)
    leaf = Model.LeafSpec(
        physicaltype(String(fields[6]));
        logical=logicaltype(String(fields[7]), signed),
        type_length=parsenothing(String(fields[8]), token -> parse(Int, token)),
        bit_width=parsenothing(String(fields[9]), token -> parse(Int, token)),
        precision=parsenothing(String(fields[11]), token -> parse(Int, token)),
        time_unit=timeunit(String(fields[12])),
    )
    stats = Model.RawStatistics(
        modern_lower=parsehex(String(fields[17])),
        modern_upper=parsehex(String(fields[18])),
        deprecated_lower=parsehex(String(fields[19])),
        deprecated_upper=parsehex(String(fields[20])),
        null_count=parsenothing(String(fields[21]), token -> parse(Int64, token)),
        nan_count=parsenothing(String(fields[22]), token -> parse(Int64, token)),
        distinct_count=parsenothing(String(fields[23]), token -> parse(Int64, token)),
        lower_exact=parsenothing(String(fields[24]), parsebool),
        upper_exact=parsenothing(String(fields[25]), parsebool),
    )
    result = Model.interpret_statistics(leaf, parse(Int64, fields[13]), stats,
        declaredorders(String(fields[14]), leafindex, leafcount);
        leaf_index=leafindex + 1, leaf_count=leafcount,
        created_by=createdby(parsebool(String(fields[15])), String(fields[16])))
    return resulttokens(index, result)
end

function errorline(index::String, kind::String, error)
    message = sprint(showerror, error)
    return join(String[index, kind, bytes2hex(codeunits(message))], '\t')
end

function runmodelsuite()
    suite = Module(:N6FrozenModelSuite)
    Core.eval(suite, :(include(path) = Base.include($suite, path)))
    redirect_stdout(devnull) do
        redirect_stderr(devnull) do
            Base.include(suite, MODEL_TEST_PATH)
            return
        end
        return
    end
    return
end

function runbridge()
    isempty(ARGS) || ARGS == ["--run-suite"] ||
        error("model bridge arguments are invalid")
    input = read(stdin, INPUT_LIMIT + 1)
    length(input) <= INPUT_LIMIT || error("bridge input exceeds its byte limit")
    isempty(input) && error("bridge input is empty")
    last(input) == 0x0a || error("bridge input lacks its final newline")
    ARGS == ["--run-suite"] && runmodelsuite()
    executable = Base.julia_cmd().exec[1]
    println("TOOLCHAIN\t", VERSION, "\t", bytes2hex(open(sha256, executable)))
    text = String(input)
    lines = split(chop(text; tail=1), '\n'; keepempty=false)
    length(lines) <= 313 || error("bridge input exceeds its record limit")
    for line in lines
        fields = split(line, '\t'; keepempty=true)
        index = isempty(fields) ? "" : String(fields[1])
        try
            println(join(interpret(fields), '\t'))
        catch error
            if error isa Model.ModelFormatError
                println(errorline(index, "FORMAT_ERROR", error))
            elseif error isa ArgumentError
                println(errorline(index, "ARGUMENT_ERROR", error))
            else
                rethrow()
            end
        end
    end
    return
end

runbridge()
