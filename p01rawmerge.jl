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

# Include the oct.jl file to access the projectmetadata function
include(joinpath(pwd(), "oct.jl"))


# Load the .env file to access environment variables
DotEnv.load!()

# Part and Version information
part = 0
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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Import Raw Data (initialization)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2. Import Raw Data (initialization)")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.1. Importing Raw Data Files from disk
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.1. Importing Raw Data Files from disk")

# Creating a new data frame to hold the merged raw data
dfdata = DataFrame()
# Adding column names to the merged data frame
dfdata[!, "year"] = String[]
dfdata[!, "datestart"] = Date[]
dfdata[!, "dateend"] = Date[]
dfdata[!, "cntcrashes"] = Int64[]
dfdata[!, "cntparties"] = Int64[]
dfdata[!, "cntvictims"] = Int64[]

# Set a new index for the data frame
i = 1

# Initialize variables
data = nothing
datestart = prjmd["datestart"]
dateend = prjmd["dateend"]
cntcrashes = 0
cntparties = 0
cntvictims = 0

# Loop through the years in the project metadata
for year in collect(prjmd["years"])
    # set the index for the data frame year
    dfdata[i, "year"] = year
    # Loop through the levels of the data frame
    for level in ["Crashes", "Parties", "Victims"]
        # Read the data from the CSV file
        
