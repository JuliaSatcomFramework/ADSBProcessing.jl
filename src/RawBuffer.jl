"""
    RawBuffer

Structure used to directly parse ADS-B data from the raw vector of UInt8 extracted from the gzipped jsons of adsblol history data.
"""
@kwdef mutable struct RawBuffer
    const buf::Vector{UInt8} = UInt8[]
    const dec::Decompressor = Decompressor()
    idx::Int = 1
end

# This will take an input stream and decompress it into the buffer of `rbuf`. It will also reset the mark of rbuf to point to the beginning
function LibDeflate.gzip_decompress!(rbuf::RawBuffer, args...; kwargs...)
    gzip_decompress!(rbuf.dec, rbuf.buf, args...; kwargs...)
    rbuf.idx = 1
    return rbuf
end

# This expands the findnext working directly on UInt8 vectors
function Base.findnext(pattern::AbstractString, rbuf::RawBuffer, from::Int=rbuf.idx)
    return findnext(codeunits(pattern), rbuf.buf, from)
end

# Check if the rawbuffer (from the mark it's set at) starts with the provided `prefix`
function Base.startswith(rbuf, prefix::AbstractString)
    matchrange = findnext(prefix, rbuf)
    if isnothing(matchrange) || first(matchrange) != rbuf.idx
        return false
    else
        return true
    end
end

function Base.seekstart(rbuf::RawBuffer)
    rbuf.idx = 1
    return nothing
end

function seekuntil(rbuf::RawBuffer, delim::AbstractString; keep=false)
    matchr = findnext(delim, rbuf)
    if isnothing(matchr)
        rbuf.idx = 0
    else
        rbuf.idx = keep ? first(matchr) : (last(matchr) + 1)
    end
    return nothing
end

function parseuntil(::Type{T}, rbuf, delim::AbstractString; keep=false, move=true) where T
    0 < rbuf.idx <= length(rbuf.buf) || return nothing
    matchr = findnext(delim, rbuf)
    isnothing(matchr) && return nothing
    subrange = range(rbuf.idx, first(matchr)-1; step = 1)
    sbuf = @views(rbuf.buf[subrange])
    out = if T === String
        String(sbuf)
    else
        Parsers.parse(T, sbuf)
    end
    move || return out
    rbuf.idx = keep ? first(subrange) : (last(subrange) + 1 + ncodeunits(delim)) 
    return out
end