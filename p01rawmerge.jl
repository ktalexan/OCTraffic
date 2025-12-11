#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Analysis
# Title: Part 1 - Merging Raw Data Files
# File: p01rawmerge.jl
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.02, Date: December 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

println("\nOCTraffic Data Processing - Part 1: Merging Raw Data Files\n")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("1. Preliminaries")

# Importing necessary packages
using TOML
using DotEnv
using Dates
using CSV
using Formatting
using Arrow, DataFrames

# Include the oct.jl file to access the projectmetadata function
include(joinpath(pwd(), "oct.jl"))


# Load the .env file to access environment variables
DotEnv.load!()

# Part and Version information
part = 1
version = 2025.02


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1.1. Project and Workspace variables
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n1.1. Project and Workspace variables")

# Create a dictionary with the project metadata
println("\n- Creating project metadata...")
prjmd = projectmetadata(part, version, false)

# Create a dictionary with the project directories
println("\n- Creating project directories...")
prjdirs = projectdirectories(pwd(), false)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Import Raw Data (initialization)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2. Import Raw Data (initialization)")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.1. Importing Raw Data Files from disk
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.1. Importing Raw Data Files from disk")

# Creating a new data frame to hold the merged raw data
println("\n- Creating a new data frame to hold the merged raw data...")
dfdata = DataFrame()
# Adding column names to the merged data frame
println("\n- Adding column names to the merged data frame...")
dfdata[!, "year"] = String[]
dfdata[!, "datestart"] = Date[]
dfdata[!, "dateend"] = Date[]
dfdata[!, "cntcrashes"] = Int64[]
dfdata[!, "cntparties"] = Int64[]
dfdata[!, "cntvictims"] = Int64[]

# Set a new index for the data frame
println("\n- Setting a new index for the data frame...")
i = 1

# Init  ialize variables
println("\n- Initializing variables...")
datestart = Date(prjmd["datestart"], dateformat"yyyy-mm-dd")
dateend = Date(prjmd["dateend"], dateformat"yyyy-mm-dd")

# Loop through the years in the project metadata
println("\n- Looping through the years in the project metadata...")
for year in collect(prjmd["years"])
    println("\n- Processing year $year...")
    # Add a new row to the dataframe for the current year
    push!(dfdata, (year=year, datestart=datestart, dateend=dateend, cntcrashes=0, cntparties=0, cntvictims=0))

    # Loop through the levels of the data frame
    println("  - Looping through the levels of the data frame...")
    for level in ["crashes", "parties", "victims"]
        println("    - Processing level $level...")
        # Read the data from the CSV file
        println("    - Reading data from CSV file...")
        rawdata = CSV.read(joinpath(prjdirs["dataraw"], prjmd["tims"][year]["files"][level]), DataFrame)

        # If the level is "crashes"
        if level == "crashes"
            println("    - Processing crashes...")
            # Safely parse COLLISION_DATE to datetime and handle missing/invalid values
            dstart = datestart
            dend = dateend

            if "COLLISION_DATE" in names(rawdata)
                # Helper function to parse dates safely
                function parse_date_safe(x)
                    if x isa Date
                        return x
                    elseif x isa AbstractString
                        return tryparse(Date, x, dateformat"yyyy-mm-dd")
                    else
                        return tryparse(Date, string(x), dateformat"yyyy-mm-dd")
                    end
                end

                # Extract and parse dates
                dates = parse_date_safe.(rawdata[!, "COLLISION_DATE"])
                # Filter valid dates (not nothing and not missing)
                valid_dates = [d for d in dates if d isa Date]

                if !isempty(valid_dates)
                    dstart = minimum(valid_dates)
                    dend = maximum(valid_dates)
                end
            end

            dfdata[i, "datestart"] = dstart
            dfdata[i, "dateend"] = dend
            dfdata[i, "cntcrashes"] = nrow(rawdata)

            # If the level is "parties"
        elseif level == "parties"
            println("    - Processing parties...")
            dfdata[i, "cntparties"] = nrow(rawdata)

            # If the level is "victims"
        elseif level == "victims"
            println("    - Processing victims...")
            dfdata[i, "cntvictims"] = nrow(rawdata)
        end
    end

    # Update the tims metadata using the updatetimsmetadata function
    println("  - Updating tims metadata...")
    updatetimsmetadata(parse(Int, year); type="reported", datacounts=[dfdata[i, "cntcrashes"], dfdata[i, "cntparties"], dfdata[i, "cntvictims"]])

    # Increment the row index
    println("  - Incrementing row index $(i+1)...")
    global i += 1
end


# Add data dictionary attributes
println("- Adding data dictionary attributes...")
metadata!(dfdata, "name", "dfdata", style=:note)
metadata!(dfdata, "label", "Data Dictionary", style=:note)
metadata!(dfdata, "description", "A data dictionary containing metadata for the raw data files.", style=:note)
metadata!(dfdata, "version", prjmd["version"], style=:note)
metadata!(dfdata, "author", prjmd["author"], style=:note)
metadata!(dfdata, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.2. Merging Temporal Raw Data Files
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.2. Merging Temporal Raw Data Files")

# Crashes Data Frame
#~~~~~~~~~~~~~~~~~~~

println("\n- Merging the raw crashes data files...")

# Create a new data frame to store the crashes data
crashes = DataFrame()

# Loop through the years in the project metadata
println("\n- Looping through the years in the project metadata...")
for year in collect(prjmd["years"])
    # Read the data from the CSV file
    println("  - Reading data from $year CSV file...")
    rawcrashes = CSV.read(joinpath(prjdirs["dataraw"], prjmd["tims"][year]["files"]["crashes"]), DataFrame)
    # Add the data to the crashes data frame
    println("  - Adding data to the $year crashes data frame...")
    append!(crashes, rawcrashes; cols=:union, promote=true)
    # Remove the temporary data frame
    rawcrashes = nothing
end

# Add data dictionary attributes
println("- Adding crashes data dictionary attributes...")
metadata!(crashes, "name", "crashes", style=:note)
metadata!(crashes, "label", "OCTraffic Crashes Data", style=:note)
metadata!(crashes, "description", "A data frame containing the crashes data from $datestart to $dateend.", style=:note)
metadata!(crashes, "version", prjmd["version"], style=:note)
metadata!(crashes, "author", prjmd["author"], style=:note)
metadata!(crashes, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)


# Parties Data Frame
#~~~~~~~~~~~~~~~~~~~

println("\n- Merging the raw parties data files...")

# Create a new data frame to store the parties data
parties = DataFrame()

# Loop through the years in the project metadata
println("\n- Looping through the years in the project metadata...")
for year in collect(prjmd["years"])
    # Read the data from the CSV file
    println("  - Reading data from $year CSV file...")
    rawparties = CSV.read(joinpath(prjdirs["dataraw"], prjmd["tims"][year]["files"]["parties"]), DataFrame)
    # Add the data to the end of the parties data frame
    println("  - Adding data to the $year parties data frame...")
    append!(parties, rawparties; cols=:union, promote=true)
    # Remove the temporary data frame
    rawparties = nothing
end

# Add data dictionary attributes
println("- Adding parties data dictionary attributes...")
metadata!(parties, "name", "parties", style=:note)
metadata!(parties, "label", "OCTraffic Parties Data", style=:note)
metadata!(parties, "description", "A data frame containing the parties data from $datestart to $dateend.", style=:note)
metadata!(parties, "version", prjmd["version"], style=:note)
metadata!(parties, "author", prjmd["author"], style=:note)
metadata!(parties, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)


# Victims Data Frame
#~~~~~~~~~~~~~~~~~~~

println("\n- Merging the raw victims data files...")

# Create a new data frame to store the victims data
victims = DataFrame()

# Loop through the years in the project metadata
println("\n- Looping through the years in the project metadata...")
for year in collect(prjmd["years"])
    # Read the data from the CSV file
    println("  - Reading data from $year CSV file...")
    rawvictims = CSV.read(joinpath(prjdirs["dataraw"], prjmd["tims"][year]["files"]["victims"]), DataFrame)
    # Add the data to the end of the victims data frame
    println("  - Adding data to the $year victims data frame...")
    append!(victims, rawvictims; cols=:union, promote=true)
    # Remove the temporary data frame
    rawvictims = nothing
end

# Add data dictionary attributes
println("- Adding victims data dictionary attributes...")
metadata!(victims, "name", "victims", style=:note)
metadata!(victims, "label", "OCTraffic Victims Data", style=:note)
metadata!(victims, "description", "A data frame containing the victims data from $datestart to $dateend.", style=:note)
metadata!(victims, "version", prjmd["version"], style=:note)
metadata!(victims, "author", prjmd["author"], style=:note)
metadata!(victims, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)


# Wrap-up
#~~~~~~~~

println("\n- Counting the rows in each of the data frames...")

# Count the rows in each of the data frames
cntcrashes = nrow(crashes)
cntparties = nrow(parties)
cntvictims = nrow(victims)

# Print the number of rows in each of the data frames, showing the counts in a comma separated list
println("  - Crashes: $(format(cntcrashes, commas=true)) rows")
println("  - Parties: $(format(cntparties, commas=true)) rows")
println("  - Victims: $(format(cntvictims, commas=true)) rows")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.3. Saving the Data Frames
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.3. Saving the Data Frames\n")

# Saving the crashes, parties, and victims data frames to disk as arrow files
println("- Saving the crashes data frame to disk as arrow file...")
Arrow.write(joinpath(prjdirs["dataprocessed"], "crashes.arrow"), crashes)
println("- Saving the parties data frame to disk as arrow file...")
Arrow.write(joinpath(prjdirs["dataprocessed"], "parties.arrow"), parties)
println("- Saving the victims data frame to disk as arrow file...")
Arrow.write(joinpath(prjdirs["dataprocessed"], "victims.arrow"), victims)

# Saving the dfdata data frame to disk as arrow file
println("\n- Saving the dfdata data frame to disk as arrow file...")
Arrow.write(joinpath(prjdirs["dataprocessed"], "dfdata.arrow"), dfdata)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.4. Create LaTeX Variable Dictionary
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.4. Create LaTeX Variable Dictionary")

# Define number of months between the start and end dates
println("- Defining number of months between the start and end dates...")
months = (year(dateend) - year(datestart)) * 12 + month(dateend) - month(datestart)
if day(dateend) >= day(datestart)
    months = months + 1
end

# Define LaTeX variables
println("- Defining LaTeX variables...")
latexvars = Dict(
    "prjversion" => prjmd["version"],
    "prjupdated" => Dates.format(Dates.today(), "U dd, yyyy"),
    "prjdatewhole" => Dates.format(dateend, "mm/dd/yyyy"),
    "prjdateus" => Dates.format(dateend, "mm/dd/yyyy"),
    "prjmonths" => months,
    "crashesoriginal" => cntcrashes,
    "partiesoriginal" => cntparties,
    "victimsoriginal" => cntvictims,
    "collisionsoriginal" => 0,
    "crashescount" => 0,
    "partiescount" => 0,
    "victimscount" => 0,
    "collisionscount" => 0,
    "crashesfatalities" => 0,
    "partiesfatalities" => 0,
    "victimsfatalities" => 0
)

# Define the path to save the LaTeX variable dictionary
println("- Defining the path to save the LaTeX variable dictionary...")
latexvarpath = joinpath(prjdirs["metadata"], "latexvars.toml")

# Save the LaTeX variable dictionary to disk as a TOML file
println("- Saving the LaTeX variable dictionary to disk as a TOML file...")
open(latexvarpath, "w") do io
    TOML.print(io, latexvars; sorted=true)
end

# Print a message to the console
println("- LaTeX variable dictionary saved to $latexvarpath")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of p01rawmerge.jl script
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\nEnd of Part 1 - Raw Data Merge")
println("Last updated on $(Dates.format(Dates.now(), dateformat"U dd, yyyy"))")
