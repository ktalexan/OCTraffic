# Import the original.toml file from the codebook parent directory to a dictionary
using TOML
#using OrderedCollections

cb_path = joinpath(@__DIR__, "codebook", "cb.toml")
cb = TOML.parsefile(cb_path)

# Print the keys of the dictionary to verify successful import
println(keys(cb))




# Include the oct.jl file to access the projectmetadata function
include(joinpath(@__DIR__, "oct.jl"))

# Call the projectmetadata function with sample arguments
md = projectmetadata(0, 2025.03, true)

exportmetadata(md)

prjdirs = projectdirectories(pwd(), false)

println(sort!(collect(keys(prjdirs))))

for year in prjyears
    print("Checking year $year in project metadata...")
    if !(year in prjmd["years"])
        println("Year $year is missing from project metadata!")
    else
        println("Year $year is present in project metadata.")
    end
end



for year in prjmd["years"]
    println("Processing year: $year")
end




for year in collect(prjmd["years"])
    # check if year is integer or string
    println("Year: $year, Type: $(typeof(year))")
end

for year in excludedyears
    println("Excluded Year: $year, Type: $(typeof(year))")
end



for year in collect(prjmd["years"])
    year = parse(Int, string(year))
    if year in excludedyears
        prjmd["tims"][string(year)]["status"] = "final"
        prjmd["tims"][string(year)]["notes"] = "Data removed from TIMS (updated on $currentdate)"
    elseif year in finalyears
        prjmd["tims"][string(year)]["status"] = "final"
        prjmd["tims"][string(year)]["notes"] = "Final data in TIMS (updated on $currentdate)"
    elseif year in provisionalyears
        prjmd["tims"][string(year)]["status"] = "provisional"
        prjmd["tims"][string(year)]["notes"] = "Provisional data in TIMS (updated on $currentdate)"
    else
        prjmd["tims"][string(year)]["status"] = "unknown"
        prjmd["tims"][string(year)]["notes"] = "Unknown data in TIMS (updated on $currentdate)"
    end
end

for year in collect(prjmd["years"])
    crashespath = joinpath(prjdirs["dataraw"], prjmd["tims"][year]["files"]["crashes"])
    partiespath = joinpath(prjdirs["dataraw"], prjmd["tims"][year]["files"]["parties"])
    vehiclespath = joinpath(prjdirs["dataraw"], prjmd["tims"][year]["files"]["victims"])
    println("Year: $year")
    println("  Crashes Path: $crashespath")
    println("  Parties Path: $partiespath")
    println("  Vehicles Path: $vehiclespath")
end

# Count the rows of the crashespath file for each year
using CSV
for year in collect(prjmd["years"])
    crashespath = joinpath(prjdirs["dataraw"], prjmd["tims"][year]["files"]["crashes"])
    if isfile(crashespath)
        df = CSV.read(crashespath, DataFrame)
        nrows = nrow(df)
        println("Year: $year, Crashes File: $crashespath, Rows: $nrows")
    else
        println("Year: $year, Crashes File: $crashespath does not exist.")
    end
end

for year in prjyears
    if !("$year" in prjmd["years"])
        println("Year $year is missing from project metadata!")
    else
        println("Year $year is present in project metadata.")
    end
end

for year in collect(prjmd["years"])
    println("Processing year: $year")
end

for year in collect(prjmd["years"])
    year = parse(Int, string(year))
    if year in excludedyears
        println("Year $year is excluded.")
    elseif year in finalyears
        println("Year $year is final.")
    elseif year in provisionalyears
        println("Year $year is provisional.")
    else
        println("Year $year is unknown.")
    end
end
