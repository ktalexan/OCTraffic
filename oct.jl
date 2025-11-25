# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OC Traffic Data Analysis
# Title: Create Project Functions
# Description: Julia functions to create and manage the OC Traffic Project.
# Language: Julia
# Author: Kostas Alexandridis, PhD, GISP
# Version: 2025-04
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# region 1. Import Necessary Packages
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

using Pkg
using JSON
using Dates
using DataFrames
using Match


# endregion 1


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# region 2. Project Metadata Function
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
    create_project_metadata(project_name::String, author::String, description::String)
Create a dictionary containing metadata for an OCSWITRS project.
# Arguments
- `project_name::String`: Name of the project.
- `author::String`: Author of the project.
- `description::String`: Description of the project.
# Returns
- `Dict`: A dictionary containing project metadata.
"""

function projectmetadata(part::Int64, version::Float64, silent::Bool=false)


    # Check if the TIMS metadata file exists
    metadatafile = joinpath(pwd(), "metadata", "tims_metadata.json")

    # Check if the metadatafile exists, otherwise raise an error
    if !isfile(metadatafile)
        error("TIMS metadata file not found at $metadatafile")
    end

    # Read the metadata file to a dictionary
    try
        jsontext = read(metadatafile, String)
        timsmetadata = Dict(JSON.parse(jsontext))
    catch e
        error("Error reading TIMS metadata file: $e")
    end

    # keys of the timsmetadata dictionary
    keysvec = collect(keys(timsmetadata))
    # Sort the keysvec to ensure chronological order
    sort!(keysvec)

    # Get the first and the last dates from the tims metadata
    startdate = Date(string(timsmetadata[first(keysvec)]["date_start"]), dateformat"yyyy-mm-dd")
    enddate = Date(string(timsmetadata[last(keysvec)]["date_end"]), dateformat"yyyy-mm-dd")

    # Check if the part variable is an integer  
    if !(typeof(part) <: Integer)
        error("The 'part' variable must be an integer.")
    end

    # Check if the version is numeric
    if !(typeof(version) <: Real)
        error("The 'version' variable must be numeric.")
    end

    # Set dateupdated to the current date
    dateupdated = Dates.format(Dates.now(), dateformat"yyyy-mm-dd")

    # Match part to value and define step and description
    step, desc = @match part begin
        0 => ("Part 0: TIMS Metadata Update", "Updating the TIMS metadata for the OCSWITRS data processing object")
        1 => ("Part 1: Raw Data Merging", "Merging and verifying the raw SWITRS annual data files into single files.")
        2 => ("Part 2: Raw Data Processing", "Processing the raw data files into data frames and generating additional variables.")
        3 => ("Part 3: Time Series Processing", "Generating and processing time series data from OCSWITRS collision data.")
        4 => ("Part 4: Collision Data Analysis", "Analyzing the collision data and generating graphs and statistics.")
        5 => ("Part 5: Time Series Analysis", "Analyzing the OCSWITRS time series data and generating graphs and statistics.")
        6 => ("Part 6: GIS Feature Class Processing", "Processing data frame tabular data into GIS feature classes, and performing geoprocessing operations and analysis.")
        7 => ("Part 7: GIS Map Processing", "Processing GIS feature classes into GIS Maps.")
        8 => ("Part 8: GIS Layout Processing", "Processing GIS Maps and layers into GIS Layouts.")
        9 => ("Part 9: ArcGIS Online Feature Sharing", "Sharing GIS feature classes to ArcGIS Online.")
        10 => ("Part 10: ArcGIS Online Metadata Update", "Updating the metadata of the shared ArcGIS Online feature classes.")
        _ => (error("Invalid part number. Must be 0, or 1 through 10."), error("Invalid part number. Must be 0, or 1 through 10."))
    end

    # Create a dictionary to hold the project metadata
    metadata = Dict{String, Any}()
    metadata["name"] = "OC Traffic Data Analysis"
    metadata["title"] = step
    metadata["description"] = desc
    metadata["version"] = version
    metadata["author"] = "Dr. Kostas Alexandridis, PhD, GISP"
    metadata["years"] = keysvec
    metadata["datestart"] = string(startdate)
    metadata["dateend"] = string(enddate)
    metadata["dateupdated"] = sting(dateupdated)
    metadata["tims"] = timsmetadata

    # Format the date strings to monthname day, year
    metadata["datestart"] = Dates.format(startdate, dateformat"yyyy-mm-dd")
    metadata["dateend"] = Dates.format(enddate, dateformat"yyyy-mm-dd")

    
    # If the silent flag is false, print the metadata
    if !silent
        println("Project Metadata:\n- Name: $(metadata["name"])\n- Title: $(metadata["title"])\n- Description: $(metadata["description"])\n- Version: $(metadata["version"])\n- Author: $(metadata["author"])\n- Data Years: $(join(metadata["years"], ", "))\n- Data Start Date: $(Dates.format(startdate, dateformat"U dd, yyyy"))\n- Data End Date: $(Dates.format(enddate, dateformat"U dd, yyyy"))")
        for (key, value) in metadata
            println(" - $key: $value")
        end
    end
end
    get_project_metadata() -> Dict{String, Any}