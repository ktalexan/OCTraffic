#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Analysis
# Title: Part 0 - Update Metadata for OCSWITRS Datasets
# File: p00updatemd.jl
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.02, Date: December 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

println("\nOCTraffic Data Processing - Part 0: Update Metadata for OCSWITRS Datasets\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("1. Preliminaries")

# Importing necessary packages
using TOML
using DotEnv
using Dates
using CSV

# Include the oct.jl file to access the projectmetadata function
include(joinpath(pwd(), "oct.jl"))


# Load the .env file to access environment variables
DotEnv.load!()

# Part and Version information
part = 0
version = 2025.02


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Data Definition variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n1.1. Data Definition variables")

# Define the last year of the data and the cutoff between final and provisional data
firstyear = 2012
timsstartyear = 2013
timsfinalyear = 2023
timsendyear = 2025

# Create a string with the current month and year. The month is the mont's name
currentdate = Dates.format(Dates.now(), dateformat"U yyyy")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2. Project and Workspace variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n1.2. Project and Workspace variables")

# Create a dictionary with the project metadata
println("- Creating project metadata...")
prjmd = projectmetadata(part, version, false)

# Create a dictionary with the project directories
println("- Creating project directories...")
prjdirs = projectdirectories(pwd(), false)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Update TIMS Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2. Update TIMS Metadata")

# Get a range of years for the project
prjyears = collect(firstyear:timsendyear)
excludedyears = collect(firstyear:timsstartyear-1)
finalyears = collect(timsstartyear:timsfinalyear)
provisionalyears = collect(timsfinalyear+1:timsendyear)

# Create an empty list to hold new years to be added to prjmd["years"]
missingyears = []

# Create a list of the years in prjyears that are not in prjmd["years"]
for year in prjyears
    if !("$year" in prjmd["years"])
        # Add the year to the missingyears list
        push!(missingyears, year)
        
        # Append the prjmd["tims"] with a new template for that year
        prjmd["tims"]["$year"] = Dict(
            "datestart" => "$year-01-01",
            "dateend" => "$year-03-31",
            "dateupdated" => currentdate,
            "dategp" => "",
            "status" => "provisional"
        )
        # Append the files sub-dictionary
        prjmd["tims"]["$year"]["files"]["crashes"] = "crashes_$year.csv"
        prjmd["tims"]["$year"]["files"]["parties"] = "parties_$year.csv"
        prjmd["tims"]["$year"]["files"]["victims"] = "victims_$year.csv"
    end
end

# For each year key in prjmd["tims"], update the metadata
for year in collect(prjmd["years"])
    year = parse(Int, string(year))
    if year in excludedyears
        prjmd["tims"]["$year"]["status"] = "final"
        prjmd["tims"]["$year"]["notes"] = "Data removed from TIMS (updated on $currentdate)"
    elseif year in finalyears
        prjmd["tims"]["$year"]["status"] = "final"
        prjmd["tims"]["$year"]["notes"] = "Final data in TIMS (updated on $currentdate)"
    elseif year in provisionalyears
        prjmd["tims"]["$year"]["status"] = "provisional"
        prjmd["tims"]["$year"]["notes"] = "Provisional data in TIMS (updated on $currentdate)"
    else
        prjmd["tims"]["$year"]["status"] = "unknown"
        prjmd["tims"]["$year"]["notes"] = "Unknown data in TIMS (updated on $currentdate)"
    end
end

# Update the counts from the data files
println("- Updating counts in metadata...")
for year in collect(prjmd["years"])
    # Define the file paths
    crashespath = joinpath(prjdirs["dataraw"], prjmd["tims"]["$year"]["files"]["crashes"])
    partiespath = joinpath(prjdirs["dataraw"], prjmd["tims"]["$year"]["files"]["parties"])
    victimspath = joinpath(prjdirs["dataraw"], prjmd["tims"]["$year"]["files"]["victims"])
    # Read the CSV files and count the number of rows
    # Update crashes count
    if isfile(crashespath)
        crashesdf = CSV.read(crashespath, DataFrame)
        prjmd["tims"]["$year"]["reported"]["crashes"] = nrow(crashesdf)
        println("  - Year $year: Crashes count updated to $(nrow(crashesdf))")
    else
        prjmd["tims"][year]["reported"]["crashes"] = 0
        println("  - Year $year: Crashes file not found, count set to 0")
    end
    # Update parties count
    if isfile(partiespath)
        partiesdf = CSV.read(partiespath, DataFrame)
        prjmd["tims"][year]["reported"]["parties"] = nrow(partiesdf)
        println("  - Year $year: Parties count updated to $(nrow(partiesdf))")
    else
        prjmd["tims"][year]["reported"]["parties"] = 0
        println("  - Year $year: Parties file not found, count set to 0")
    end
    # Update victims count
    if isfile(victimspath)
        victimsdf = CSV.read(victimspath, DataFrame)
        prjmd["tims"][year]["reported"]["victims"] = nrow(victimsdf)
        println("  - Year $year: Victims count updated to $(nrow(victimsdf))")
    else
        prjmd["tims"][year]["reported"]["victims"] = 0
        println("  - Year $year: Victims file not found, count set to 0")
    end
end

# Print the prjmd["tims"] dictionary to verify updates
println("\nUpdated TIMS Metadata:")
for (year, data) in prjmd["tims"]
    # Print each year's metadata with proper formatting
    println("Year $year:")
    for (key, value) in data
        println("  $key: $value")
    end
end

# Update the project metadata TOML file
mdfile = joinpath(prjdirs["metadata"], "timsmd.toml")
println("\n- Writing updated metadata to $mdfile...")
open(mdfile, "w") do io
    TOML.print(io, prjmd["tims"])
end
println("\nMetadata update completed successfully.\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of Script ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("End of Part 0 - Update Metadata for OCSWITRS Datasets")
println("Last updated on $(Dates.format(Dates.now(), dateformat"U dd, yyyy"))")
