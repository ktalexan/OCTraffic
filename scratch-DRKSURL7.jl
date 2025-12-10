# Import the original.toml file from the codebook parent directory to a dictionary
using TOML
#using OrderedCollections

cb_path = joinpath(@__DIR__, "codebook", "cb.toml")
cb = TOML.parsefile(cb_path)

# Print the keys of the dictionary to verify successful import
println(keys(cb))