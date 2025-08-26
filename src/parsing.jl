#=
- `dt::Float64`: The offset in seconds of the current point measurement from the `timestamp`
- `lat::Float64`: The latitude measured from the ADS-B signal in degrees
- `lon::Float64`: The longitude measured from the ADS-B signal in degrees
- `alt::Float64`: The altitude measured from the ADS-B signal in feet. This is `0.0` if the original entry had `ground` as a value and `NaN` if the original entry had `null` as value.
=#
const TRACE_ENTRY_TYPE = @NamedTuple{dt::Float64, lat::Float64, lon::Float64, alt::Float64}
const TABLE_ROW_TYPE = @NamedTuple{icao::String, timestamp::DateTime, trace::Vector{TRACE_ENTRY_TYPE}}

# This function assumes the buffer is already either at the beginning or within the `trace` entry in the json file and just reads a single row, extracting the time offset, position and altitude of the entry of the trace row. It directly puts the row entry inside the vector `out`
function read_trace_row(rbuf, out; min_dt = 300)
    seekuntil(rbuf, "[") # We find the beginning of the trace entry, it is always inside an array of various parameters. We are actually only interested in the first 4, which are time offset, lat, lon and altitude
    rbuf.idx > 0 || return nothing # If the seeking actually didn't reach the target delimiter we exit early
    dt = parseuntil(Float64, rbuf, ",")::Float64
    last_dt = isempty(out) ? -Inf : last(out).dt
    if dt - last_dt > min_dt
        lat = parseuntil(Float64, rbuf, ",")::Float64
        lon = parseuntil(Float64, rbuf, ",")::Float64
        alt = if startswith(rbuf, "\"ground")
            0.0
        elseif startswith(rbuf, "null")
            NaN
        else
            parseuntil(Float64, rbuf, ",")::Float64
        end
        # We add the extracted values to the vector
        push!(out, (; dt, lat, lon, alt))
    end
    seekuntil(rbuf, "\n") # We move towards the end of the line, which marks the end of the entry. We do so because the other entry elements we are skipping might also include arrays which would break parsing (as we'd have a '[' appearing before the actual beginning of the following trace entry)
    return nothing
end

function read_json_entry(jsonpath, rbuf::RawBuffer)
    jsio = read(jsonpath) # We read the file contents
    gzip_decompress!(rbuf, jsio) # The adsblol json files are gzip compressed so we uncompress the data, which will be stored inside the `rbuf` object
    seekuntil(rbuf, ":")
    seekuntil(rbuf, "\"")
    icao = parseuntil(String, rbuf, "\"")::String # Parse the icao 24-bit hex code
    seekuntil(rbuf, "timestamp\":")
    timestamp = parseuntil(Float64, rbuf, ",")::Float64 # Parse the timestamp in seconds since unix epoch
    seekuntil(rbuf, "[") # Look for the beginning of the traces array
    trace = TRACE_ENTRY_TYPE[]
    while rbuf.idx > 0
        try
            read_trace_row(rbuf, trace)
        catch
            @info "Error while processing file at $jsonpath"
            rethrow()
        end
    end
    return (; icao, timestamp=unix2datetime(timestamp), trace)
end

function process_json_subfolder(subfold; nthreads=4, min_length = 1e6)
    fnames = readdir(subfold)
    buf_ch = Channel{RawBuffer}(nthreads)
    out_ch = Channel{Vector{TABLE_ROW_TYPE}}(nthreads)
    for n in 1:nthreads
        put!(buf_ch, RawBuffer())
        put!(out_ch, TABLE_ROW_TYPE[])
    end
    tforeach(fnames) do fn
        contains(fn, "~") && return # We skip files with an invalid ICAO number
        jsonpath = joinpath(subfold, fn)
        try
            rbuf = take!(buf_ch)
            obj = read_json_entry(jsonpath, rbuf)
            put!(buf_ch, rbuf)
            if trace_length(obj) > min_length
                out = take!(out_ch)
                push!(out, obj)
                put!(out_ch, out)
            end
        catch
            rethrow()
        end
    end
    close(buf_ch)
    close(out_ch)
    return vcat(out_ch...)
end

function extract_and_process_subfolder(tarpath, subfold_path)
    return mktempdir() do dir
        files = joinpath(subfold_path, "*")
        cmd = `$(p7zip()) e $tarpath $(files) -o$dir`
        # subdir
        run(pipeline(cmd; stdout=devnull))
        process_json_subfolder(dir)
    end
end

function extract_traces_subfolders(tarpath)
    cmd = `$(p7zip()) l $tarpath`
    out = IOBuffer()
    pipeline(cmd; stdout=out) |> run
    readuntil(seekstart(out), "-----") # The first part of the output is header with some p7zip information. After a line with ---- we start having the list of files
    psep = normpath("/")
    delim = "." * psep
    readuntil(out, delim) # We read till the first path entry
    nms = String[]
    while !eof(out)
        push!(nms, readline(out))
        readuntil(out, delim)
    end
    map(filter(endswith("json"), nms)) do nm
        dirname(delim * nm)
    end |> unique
end

function process_tarfile(tarpath, out = TABLE_ROW_TYPE[]; max_folders = nothing)
    subfolds = extract_traces_subfolders(tarpath)
    max_folders = @something max_folders length(subfolds)
    for i in 1:max_folders
        @info "Processing folder $i of $max_folders"
        subfold = subfolds[i]
        subout = extract_and_process_subfolder(tarpath, subfold)
        append!(out, subout)
    end
    return out
end

function trace_length(entry::TABLE_ROW_TYPE)
    (; trace) = entry
    length(trace) > 1 || return 0.0
    sum(eachindex(trace)[1:end-1]) do i
        p1 = trace[i].lon, trace[i].lat
        p2 = trace[i+1].lon, trace[i+1].lat
        haversine(p1, p2)
    end
end