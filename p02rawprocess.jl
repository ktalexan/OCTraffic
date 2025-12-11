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

