#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Analysis
# Title: Part 2 - Processing Raw Data Files
# File: p02rawprocess.jl
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.02, Date: December 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

println("\nOCTraffic Data Processing - Part 2: Processing Raw Data Files\n")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
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
## 1.1. Project and Workspace variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n1.1. Project and Workspace variables")

# Create a dictionary with the project metadata
println("- Creating project metadata...")
prjmd = projectmetadata(part, version, false)

# Create a dictionary with the project directories
println("- Creating project directories...")
prjdirs = projectdirectories(pwd(), false)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Raw Data Import (initialization) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2. Raw Data Import (initialization)")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1. Importing Raw Data Files from disk ----
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
## 2.2. Supporting GIS Data from Geodatabase ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.2. Supporting GIS Data from Geodatabase")

# Import the supporting feature classes (boundaries, cities, roads, blocks) from the OCSWITRS Geodatabase as spatial dataframes

# Path to the project geodatabase, and the supporting feature dataset
gdbpath = prjdirs["agpgdb"]
gdbsupporting = prjdirs["agpgdbsupporting"]

### Boundaries data frame ----------------------------------------
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

### Cities data frame ----------------------------------------
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

### Roads data frame ----------------------------------------
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


### Blocks data frame ----------------------------------------
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
## 2.3. Statistics and Data Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.3. Statistics and Data Processing")

# Compile a list of the data frames to be processed
println("- Compiling a list of the data frames to be processed...")
dataframes = [crashes, parties, victims, boundaries, cities, roads, blocks]

# Get basic statistics of the imported data frames
println(
    "- Basic Data Frame Statistics\n" *
    "  Raw Data Frames:\n" *
    "  - crashes: $(format(nrow(crashes), commas=true)) rows x $(ncol(crashes)) columns\n" *
    "  - parties: $(format(nrow(parties), commas=true)) rows x $(ncol(parties)) columns\n" *
    "  - victims: $(format(nrow(victims), commas=true)) rows x $(ncol(victims)) columns\n" *
    "  Supporting Data Frames:\n" *
    "  - boundaries: $(format(nrow(boundaries), commas=true)) rows x $(ncol(boundaries)) columns\n" *
    "  - cities: $(format(nrow(cities), commas=true)) rows x $(ncol(cities)) columns\n" *
    "  - roads: $(format(nrow(roads), commas=true)) rows x $(ncol(roads)) columns\n" *
    "  - blocks: $(format(nrow(blocks), commas=true)) rows x $(ncol(blocks)) columns"
)

# List of columns for each of the raw data frames ensuring that (a) the columns are in the correct order, and (b) the columns are not duplicated (apart from the case_id column which is common in all data frames, and the party number which is common between the parties and victims data frames).
rawcols = Dict(
    "crashes" => [
        "CASE_ID",
        "CITY",
        "COLLISION_DATE",
        "COLLISION_TIME",
        "ACCIDENT_YEAR",
        "DAY_OF_WEEK",
        "PROC_DATE",
        "COLLISION_SEVERITY",
        "PARTY_COUNT",
        "NUMBER_KILLED",
        "NUMBER_INJURED",
        "COUNT_SEVERE_INJ",
        "COUNT_VISIBLE_INJ",
        "COUNT_COMPLAINT_PAIN",
        "COUNT_PED_KILLED",
        "COUNT_PED_INJURED",
        "COUNT_BICYCLIST_KILLED",
        "COUNT_BICYCLIST_INJURED",
        "COUNT_MC_KILLED",
        "COUNT_MC_INJURED",
        "PRIMARY_COLL_FACTOR",
        "TYPE_OF_COLLISION",
        "PEDESTRIAN_ACCIDENT",
        "BICYCLE_ACCIDENT",
        "MOTORCYCLE_ACCIDENT",
        "TRUCK_ACCIDENT",
        "HIT_AND_RUN",
        "ALCOHOL_INVOLVED",
        "JURIS",
        "OFFICER_ID",
        "REPORTING_DISTRICT",
        "CHP_SHIFT",
        "CNTY_CITY_LOC",
        "SPECIAL_COND",
        "BEAT_TYPE",
        "CHP_BEAT_TYPE",
        "CHP_BEAT_CLASS",
        "BEAT_NUMBER",
        "PRIMARY_RD",
        "SECONDARY_RD",
        "DISTANCE",
        "DIRECTION",
        "INTERSECTION",
        "WEATHER_1",
        "WEATHER_2",
        "ROAD_SURFACE",
        "ROAD_COND_1",
        "ROAD_COND_2",
        "LIGHTING",
        "CONTROL_DEVICE",
        "STATE_HWY_IND",
        "SIDE_OF_HWY",
        "TOW_AWAY",
        "PCF_CODE_OF_VIOL",
        "PCF_VIOL_CATEGORY",
        "PCF_VIOLATION",
        "PCF_VIOL_SUBSECTION",
        "MVIW",
        "PED_ACTION",
        "NOT_PRIVATE_PROPERTY",
        "STWD_VEHTYPE_AT_FAULT",
        "CHP_VEHTYPE_AT_FAULT",
        "PRIMARY_RAMP",
        "SECONDARY_RAMP",
        "LATITUDE",
        "LONGITUDE",
        "POINT_X",
        "POINT_Y",
        "POPULATION",
        "CITY_DIVISION_LAPD",
        "CALTRANS_COUNTY",
        "CALTRANS_DISTRICT",
        "STATE_ROUTE",
        "ROUTE_SUFFIX",
        "POSTMILE_PREFIX",
        "POSTMILE",
        "LOCATION_TYPE",
        "RAMP_INTERSECTION",
        "CHP_ROAD_TYPE",
        "COUNTY"
    ],
    "parties" => [
        "CASE_ID",
        "PARTY_NUMBER",
        "PARTY_TYPE",
        "AT_FAULT",
        "PARTY_SEX",
        "PARTY_AGE",
        "RACE",
        "PARTY_NUMBER_KILLED",
        "PARTY_NUMBER_INJURED",
        "INATTENTION",
        "PARTY_SOBRIETY",
        "PARTY_DRUG_PHYSICAL",
        "DIR_OF_TRAVEL",
        "PARTY_SAFETY_EQUIP_1",
        "PARTY_SAFETY_EQUIP_2",
        "FINAN_RESPONS",
        "SP_INFO_1",
        "SP_INFO_2",
        "SP_INFO_3",
        "OAF_VIOLATION_CODE",
        "OAF_VIOL_CAT",
        "OAF_VIOL_SECTION",
        "OAF_VIOLATION_SUFFIX",
        "OAF_1",
        "OAF_2",
        "MOVE_PRE_ACC",
        "VEHICLE_YEAR",
        "VEHICLE_MAKE",
        "STWD_VEHICLE_TYPE",
        "CHP_VEH_TYPE_TOWING",
        "CHP_VEH_TYPE_TOWED",
        "SPECIAL_INFO_F",
        "SPECIAL_INFO_G"
    ],
    "victims" => [
        "CASE_ID",
        "PARTY_NUMBER",
        "VICTIM_NUMBER",
        "VICTIM_ROLE",
        "VICTIM_SEX",
        "VICTIM_AGE",
        "VICTIM_DEGREE_OF_INJURY",
        "VICTIM_SEATING_POSITION",
        "VICTIM_SAFETY_EQUIP_1",
        "VICTIM_SAFETY_EQUIP_2",
        "VICTIM_EJECTED"
    ]
)

# Export the rawcols dictionary to disk as a TOML file
rawcolspath = joinpath(prjdirs["codebook"], "rawcols.toml")
open(rawcolspath, "w") do io
    TOML.print(io, rawcols; sorted=true)
end

# Reorder the columns in the crashes, parties, and victims data frames using the rawcols["crashes"], rawcols["parties"], and rawcols["victims"] dictionaries
crashes = crashes[:, rawcols["crashes"]]
parties = parties[:, rawcols["parties"]]
victims = victims[:, rawcols["victims"]]

# Make sure that the columns in the data frames are matching the raw_cols dictionary
if ncol(crashes) != length(rawcols["crashes"]) || ncol(parties) != length(rawcols["parties"]) || ncol(victims) != length(rawcols["victims"])
    println("Error: The number of columns in the data frames does not match the raw_cols dictionary.")
    exit()
elseif ncol(crashes) == length(rawcols["crashes"]) && ncol(parties) == length(rawcols["parties"]) && ncol(victims) == length(rawcols["victims"])
    println("The number of columns in the data frames matches the raw_cols dictionary.")
end


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.4. Import codebook ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n2.4. Importing codebook")

# Import the codebook from disk as a TOML file
println("- Importing the codebook TOML file...")
cbpath = joinpath(prjdirs["codebook"], "cb.toml")
cb = TOML.parsefile(cbpath)

# Create a data frame from the cb TOML file by transposing the data
rows = [merge(Dict("Field" => k), v) for (k, v) in cb]
allkeys = unique(reduce(vcat, collect(keys(r)) for r in rows))
cbdf = DataFrame()
for key in allkeys
    cbdf[!, key] = [get(row, key, missing) for row in rows]
end
select!(cbdf, "Field", Not("Field"))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Raw Data operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n3. Raw Data operations")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.1. Variable Name and columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n3.1. Variable Name and columns")

# For each of the data frames below, we will process their data by:
# 1. Creating a list of names for the data frame (contains new_name as name, and old_name as value).
# 2. Renaming the columns of the data frame using the new_names from the codebook list.
# 3. Removing all the deprecated and unused columns from the data frame.

### Crashes Data Frame ----------------------------------------
println("- Crashes Data Frame")

# from the cb dictionary, find all the keys that have rawdata = true, and their fcinclude contains crases = true
crasheskeys = [k for (k, v) in cb if get(v, "rawdata", false) && get(get(v, "fcinclude", Dict()), "crashes", false)]
println("Found $(length(crasheskeys)) keys for crashes raw data.")

# Initialize the old and new names for the crashes data frame
oldname = Nothing
newname = Nothing

# Loop through the list of selected names and rename the columns in the crashes data frame
for newname in crasheskeys
    oldname = cb[newname]["rawvar"]
    println("Renaming $oldname to $newname")
    # Check if the oldname exists in the crashes data frame
    if oldname in names(crashes)
        # Rename the column in place
        rename!(crashes, oldname => newname)
    end
end

# Remove all the columns in the crashes data frame that are not in crasheskeys
select!(crashes, intersect(names(crashes), crasheskeys))

# Add metadata attributes to the crashes data frame
metadata!(crashes, "name", "crashes", style=:note)
metadata!(crashes, "label", "OCSWITRS Crashes", style=:note)
metadata!(crashes, "description", "Spatially enabled dataframe containing the Orange County crashes for the OCSWITRS dataset.", style=:note)
metadata!(crashes, "version", prjmd["version"], style=:note)
metadata!(crashes, "author", prjmd["author"], style=:note)
metadata!(crashes, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)

# Print the metadata of the crashes data frame
println("\nMetadata of the crashes data frame:")
printmetadata(crashes)

### Parties Data Frame ----------------------------------------
println("- Parties Data Frame")

# from the cb dictionary, find all the keys that have rawdata = true, and their fcinclude contains parties = true
partieskeys = [k for (k, v) in cb if get(v, "rawdata", false) && get(get(v, "fcinclude", Dict()), "parties", false)]
println("Found $(length(partieskeys)) keys for parties raw data.")

# Initialize the old and new names for the parties data frame
oldname = Nothing
newname = Nothing

# Loop through the list of selected names and rename the columns in the parties data frame
for newname in partieskeys
    oldname = cb[newname]["rawvar"]
    println("Renaming $oldname to $newname")
    # Check if the oldname exists in the parties data frame
    if oldname in names(parties)
        # Rename the column in place
        rename!(parties, oldname => newname)
    end
end

# Remove all the columns in the parties data frame that are not in partieskeys
select!(parties, intersect(names(parties), partieskeys))

# Add metadata attributes to the parties data frame
metadata!(parties, "name", "parties", style=:note)
metadata!(parties, "label", "OCSWITRS Parties", style=:note)
metadata!(parties, "description", "Spatially enabled dataframe containing the Orange County parties for the OCSWITRS dataset.", style=:note)
metadata!(parties, "version", prjmd["version"], style=:note)
metadata!(parties, "author", prjmd["author"], style=:note)
metadata!(parties, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)

# Print the metadata of the parties data frame
println("\nMetadata of the parties data frame:")
printmetadata(parties)

### Victims Data Frame ----------------------------------------
println("- Victims Data Frame")

# from the cb dictionary, find all the keys that have rawdata = true, and their fcinclude contains victims = true
victimskeys = [k for (k, v) in cb if get(v, "rawdata", false) && get(get(v, "fcinclude", Dict()), "victims", false)]
println("Found $(length(victimskeys)) keys for victims raw data.")

# Initialize the old and new names for the victims data frame
oldname = Nothing
newname = Nothing

# Loop through the list of selected names and rename the columns in the victims data frame
for newname in victimskeys
    oldname = cb[newname]["rawvar"]
    println("Renaming $oldname to $newname")
    # Check if the oldname exists in the victims data frame
    if oldname in names(victims)
        # Rename the column in place
        rename!(victims, oldname => newname)
    end
end

# Remove all the columns in the victims data frame that are not in victimskeys
select!(victims, intersect(names(victims), victimskeys))

# Add metadata attributes to the victims data frame
metadata!(victims, "name", "victims", style=:note)
metadata!(victims, "label", "OCSWITRS Victims", style=:note)
metadata!(victims, "description", "Spatially enabled dataframe containing the Orange County victims for the OCSWITRS dataset.", style=:note)
metadata!(victims, "version", prjmd["version"], style=:note)
metadata!(victims, "author", prjmd["author"], style=:note)
metadata!(victims, "updated", Dates.format(Dates.today(), "mm/dd/yyyy"), style=:note)

# Print the metadata of the victims data frame
println("\nMetadata of the victims data frame:")
printmetadata(victims)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.2. Remove Leading and Trailing Spaces ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n3.2. Remove Leading and Trailing Spaces")

# Remove leading and trailing spaces from all columns in the crashes data frame
for df in [crashes, parties, victims]
    println("\n- Removing leading and trailing spaces from $(metadata(df, "name")) data frame...")
    # Loop through the columns of the data frame and remove leading and trailing spaces from the content of each column
    for col in names(df)
        println("  - Removing leading and trailing spaces from $col column...")
        df[!, col] = strip.(string.(df[!, col]))
    end
end


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.3. Add CID, PID and VID Columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n3.3. Add CID, PID and VID Columns")

### Add CID Columns ----
println("- Adding CID columns...")

# Generate CID column for the data frames by converting the case_id column to a string and adding them to the data frame
crashes.cid = string.(crashes.caseid)
parties.cid = string.(parties.caseid)
victims.cid = string.(victims.caseid)

# Move the cid column after the caseid column in the data frames
select!(crashes, "caseid", "cid", Not(["caseid", "cid"]))
select!(parties, "caseid", "cid", Not(["caseid", "cid"]))
select!(victims, "caseid", "cid", Not(["caseid", "cid"]))


### Add PID Columns ----
println("- Adding PID columns...")

# Generate PID column for the data frames by converting the caseid column to a string and for each caseid on the same row, adding the partynumber as a string, separated by a dash
parties.pid = string.(parties.caseid) .* "-" .* string.(parties.partynumber)
victims.pid = string.(victims.caseid) .* "-" .* string.(victims.partynumber)

# Move the pid column after the cid column in the data frames
select!(parties, 1:columnindex(parties, :cid), :pid, Not(1:columnindex(parties, :cid), :pid))
select!(victims, 1:columnindex(victims, :cid), :pid, Not(1:columnindex(victims, :cid), :pid))


### Add VID Columns ----
println("- Adding VID columns...")

# Generate VID column for the data frames by converting the caseid column to a string and for each caseid on the same row, adding the partynumber and victimnumber as a string, separated by a dash
victims.vid = string.(victims.caseid) .* "-" .* string.(victims.partynumber) .* "-" .* string.(victims.victimnumber)

# Move the vid column after the pid column in the data frame
select!(victims, 1:columnindex(victims, :pid), :vid, Not(1:columnindex(victims, :pid), :vid))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.4. Add totalcrashes, totalparties, and totalvictims columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n3.4. Add totalcrashes, totalparties, and totalvictims columns")

### Crashes: totalcrashes ----
println("- Adding totalcrashes column to the crashes data frame...")

# Add count of unique cid to the crashes data frame
transform!(groupby(crashes, :cid), :cid => length => :crashescidcount)

# Relocate the crashescidcount column after the cid column in the data frame
select!(crashes, 1:columnindex(crashes, :cid), :crashescidcount, Not(1:columnindex(crashes, :cid), :crashescidcount))


### Parties: totalparties ----
println("- Adding totalparties column to the parties data frame...")

# Add count of unique cid to the parties data frame
transform!(groupby(parties, :cid), :cid => length => :partiescidcount)
transform!(groupby(parties, :pid), :pid => length => :partiespidcount)

# Relocate the partiescidcount and partiespidcount columns after the pid column in the data frame
select!(parties, 1:columnindex(parties, :pid), :partiescidcount, :partiespidcount, Not(1:columnindex(parties, :pid), :partiescidcount, :partiespidcount))

### Victims: totalvictims ----
println("- Adding totalvictims column to the victims data frame...")

# Add count of unique cid to the victims data frame
transform!(groupby(victims, :cid), :cid => length => :victimscidcount)
transform!(groupby(victims, :pid), :pid => length => :victimspidcount)
transform!(groupby(victims, :vid), :vid => length => :victimsvidcount)

# Relocate the victimscidcount, victimspidcount, and victimsvidcount columns after the vid column in the data frame
select!(victims, 1:columnindex(victims, :vid), :victimscidcount, :victimspidcount, :victimsvidcount, Not(1:columnindex(victims, :vid), :victimscidcount, :victimspidcount, :victimsvidcount))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.5. Additional Column Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
println("\n3.5. Additional Column Processing")

### CIty Names Title Case ----
println("- Converting city names to title case...")

# Convert city names to title case in the crashes data frame
for df in [crashes, parties, victims]
    println("\n- Converting city names to title case in $(metadata(df, "name")) data frame...")
    # Loop through the columns of the data frame and convert the city names to title case
    for col in names(df)
        if col == "city"
            df[!, col] = titlecase.(string.(df[!, col]))
        end
    end
end 


# list all the unique city names in the crashes data frame
uniquecities = unique(crashes.city)

# print the unique city names
println("\n  - Unique city names in the crashes data frame: $uniquecities")

# If needed, convert the city names to title case in the roads data frame