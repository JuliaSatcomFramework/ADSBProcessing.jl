# ADSBProcessing

This is a package used to parse the daily ADS-B historical data from https://github.com/adsblol/globe_history_2025.

It has functions to take the tar file for a single day and go through all of the contained json files and reconstruct a reduced vector of trace objects with just the following fields:
- `icao::String`: The 24-bit hex code of the aircraft
- `timestamp::DateTime`: The datetime this trace is referred to
- `trace::Vector`: A vector of `NamedTuple`, one per downsampled (to maximum 1 every 2 minutes) trace entry, where each NamedTuple element has the following fields:
  - `dt::Float64`: The offset in seconds of the current point measurement from the `timestamp`
  - `lat::Float64`: The latitude measured from the ADS-B signal in degrees
  - `lon::Float64`: The longitude measured from the ADS-B signal in degrees
  - `alt::Float64`: The altitude measured from the ADS-B signal in feet. This is `0.-` if the original entry had `ground` as a value and `NaN` if the original entry had `null` as value.