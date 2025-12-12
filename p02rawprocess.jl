#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Analysis
# Title: Part 2 - Processing Raw Data Files
# File: p02rawprocess.jl
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.02, Date: December 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

println("\nOCTraffic Data Processing - Part 2: Processing Raw Data Files\n")

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
using GeoDataFrames
using ArchGDAL

# Include the oct.jl file to access the projectmetadata function
include(joinpath(pwd(), "oct.jl"))


# Load the .env file to access environment variables
DotEnv.load!()

# Part and Version information
part = 2
version = 2025.02


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1.1. Project and Workspace variables
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n1.1. Project and Workspace variables")

# Create a dictionary with the project metadata
println("- Creating project metadata...")
prjmd = projectmetadata(part, version, false)

# Create a dictionary with the project directories
println("- Creating project directories...")
prjdirs = projectdirectories(pwd(), false)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Raw Data Import (initialization)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2. Raw Data Import (initialization)")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.1. Importing Raw Data Files from disk
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.1. Importing Raw Data Files from disk")

# load the crashes arrow file
println("- Loading the crashes arrow file...")
crashespath = joinpath(prjdirs["dataprocessed"], "crashes.arrow")
crashes = Arrow.Table(crashespath) |> DataFrame

# load the parties arrow file
println("- Loading the parties arrow file...")
partiespath = joinpath(prjdirs["dataprocessed"], "parties.arrow")
parties = Arrow.Table(partiespath) |> DataFrame

# load the victims arrow file
println("- Loading the victims arrow file...")
victimspath = joinpath(prjdirs["dataprocessed"], "victims.arrow")
victims = Arrow.Table(victimspath) |> DataFrame

# load the data dictionary arrow file
println("- Loading the data dictionary arrow file...")
dfdatapath = joinpath(prjdirs["dataprocessed"], "dfdata.arrow")
dfdata = Arrow.Table(dfdatapath) |> DataFrame


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.2. Supporting GIS Data from Geodatabase
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.2. Supporting GIS Data from Geodatabase")

# Import the supporting feature classes (boundaries, cities, roads, blocks) from the OCSWITRS Geodatabase as spatial dataframes

# Path to the project geodatabase, and the supporting feature dataset
gdbpath = prjdirs["agpgdb"]
gdbsupporting = prjdirs["agpgdbsupporting"]

# Boundaries data frame
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("- Loading the boundaries data frame...")

# Read the boundaries data frame from the geodatabase
boundaries = GeoDataFrames.read(gdbpath, "boundaries")

# Add metadata attributes to the boundaries data frame
metadata!(boundaries, "name", "boundaries", style=:note)
metadata!(boundaries, "label", "OCSWITRS Boundaries", style=:note)
metadata!(boundaries, "description", "Spatially enabled dataframe containing the Orange County boundaries for the OCSWITRS dataset.", style=:note)
metadata!(boundaries, "version", prjmd["version"], style=:note)
metadata!(boundaries, "author", prjmd["author"], style=:note)
metadata!(boundaries, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)

# Print the metadata of the boundaries data frame
println("\nMetadata of the boundaries data frame:")
printmetadata(boundaries)

# Cities data frame
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("- Loading the cities data frame...")

# Read the cities data frame from the geodatabase
cities = GeoDataFrames.read(gdbpath, "cities")

# Add metadata attributes to the cities data frame
metadata!(cities, "name", "cities", style=:note)
metadata!(cities, "label", "OCSWITRS Cities", style=:note)
metadata!(cities, "description", "Spatially enabled dataframe containing the Orange County cities for the OCSWITRS dataset.", style=:note)
metadata!(cities, "version", prjmd["version"], style=:note)
metadata!(cities, "author", prjmd["author"], style=:note)
metadata!(cities, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)

# Print the metadata of the cities data frame
println("\nMetadata of the cities data frame:")
printmetadata(cities)

# Roads data frame
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("- Loading the roads data frame...")

# Read the roads data frame from the geodatabase
roads = GeoDataFrames.read(gdbpath, "roads")

# Add metadata attributes to the roads data frame
metadata!(roads, "name", "roads", style=:note)
metadata!(roads, "label", "OCSWITRS Roads", style=:note)
metadata!(roads, "description", "Spatially enabled dataframe containing the Orange County roads for the OCSWITRS dataset.", style=:note)
metadata!(roads, "version", prjmd["version"], style=:note)
metadata!(roads, "author", prjmd["author"], style=:note)
metadata!(roads, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)

# Print the metadata of the roads data frame
println("\nMetadata of the roads data frame:")
printmetadata(roads)


# Blocks data frame
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("- Loading the blocks data frame...")

# Read the blocks data frame from the geodatabase
blocks = GeoDataFrames.read(gdbpath, "blocks")

# Add metadata attributes to the blocks data frame
metadata!(blocks, "name", "blocks", style=:note)
metadata!(blocks, "label", "OCSWITRS Census Blocks", style=:note)
metadata!(blocks, "description", "Spatially enabled dataframe containing the Orange County census blocks for the OCSWITRS dataset.", style=:note)
metadata!(blocks, "version", prjmd["version"], style=:note)
metadata!(blocks, "author", prjmd["author"], style=:note)
metadata!(blocks, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)

# Print the metadata of the blocks data frame
println("\nMetadata of the blocks data frame:")
printmetadata(blocks)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.3. Statistics and Data Processing
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.3. Statistics and Data Processing")

