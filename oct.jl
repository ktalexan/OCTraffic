# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OC Traffic Data Analysis
# Title: Create Project Functions
# Description: Julia functions to create and manage the OC Traffic Project.
# Language: Julia
# Author: Kostas Alexandridis, PhD, GISP
# Version: 2025-04
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Import Necessary Packages
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

using Pkg
using JSON
using Dates
using DataFrames
using Match
using TOML
using Logging

export projectmetadata, exportmetadata, projectdirectories


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Project Metadata Function
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
    mdpath = joinpath(pwd(), "metadata", "timsmd.toml")

    # Check if the metadatafile exists, otherwise raise an error
    if !isfile(mdpath)
        error("TIMS metadata file not found at $mdpath")
    else
        println("TIMS metadata file found at $mdpath")
    end

    # Read the metadata file to a dictionary
    md = Dict{String, Any}()
    try
        md = TOML.parsefile(mdpath)
    catch e
        error("Error reading TIMS metadata file: $e")
    end

    # keys of the timsmetadata dictionary
    keysvec = collect(keys(md))
    # Sort the keysvec to ensure chronological order
    sort!(keysvec)

    # Get the first and the last dates from the tims metadata
    startdate = Date(string(md[first(keysvec)]["datestart"]), dateformat"yyyy-mm-dd")
    enddate = Date(string(md[last(keysvec)]["dateend"]), dateformat"yyyy-mm-dd")

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
    metadata["dateupdated"] = dateupdated
    metadata["tims"] = md

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
    return metadata
end


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Export TIMS Metadata Function
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
Export TIMS metadata to a TOML file.

Args:
    metadata (Dict): The metadata dictionary to export. Expected to contain
        either the string key "tims" or the symbol :tims.

Returns:
    Nothing: Writes the TIMS metadata to metadata/timsmd.toml and
    returns `nothing`.

Raises:
    ArgumentError: If `metadata` is not a Dict or does not contain a `tims`
        key, or if the metadata directory does not exist.
    Any underlying I/O exception is rethrown after logging.

Examples:
    julia> exportmetadata(Dict("tims" => Dict("foo" => 1)))
"""
function exportmetadata(metadata::Dict)
    if !(isa(metadata, Dict))
        throw(ArgumentError("- Metadata must be a dictionary."))
    end

    # Accept string or symbol key for 'tims'
    timskey = if haskey(metadata, "tims")
        "tims"
    elseif haskey(metadata, :tims)
        :tims
    else
        throw(ArgumentError("- Metadata does not contain 'tims' key."))
    end

    timsmd = metadata[timskey]

    # Define the path to the metadata directory and file
    metadatadir = joinpath(pwd(), "metadata")
    if !isdir(metadatadir)
        throw(ArgumentError("- Metadata directory does not exist: $metadatadir"))
    end

    timspath = joinpath(metadatadir, "timsmd.toml")

    try
        open(timspath, "w") do io
            TOML.print(io, timsmd)
        end
    catch err
        @warn "Failed to export TIMS metadata" error = err path = timspath
        rethrow()
    end

    println("- TIMS metadata exported to disk successfully.")
    return nothing
end


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 4. Project Directories Function
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
    projectdirectories(basepath::String, silent::Bool=false) -> Dict{String,String}

Generate project directories for the OCSWITRS data processing project.

Args:
    basepath (String): The base path of the project.
    silent (Bool): If false, prints the directories to stdout.

Returns:
    Dict{String,String}: A mapping of directory keys to their full paths.

Example:
    prjdirs = projectdirectories("C:\\Projects\\OCTraffic")
"""
function projectdirectories(basepath::String, silent::Bool=false)::Dict{String,String}
    prjdirs = Dict{String,String}(
        "root" => basepath,
        "admin" => joinpath(basepath, "admin"),
        "agp" => joinpath(basepath, "AGPSWITRS"),
        "agpaprx" => joinpath(basepath, "AGPSWITRS", "AGPSWITRS.aprx"),
        "agpgdb" => joinpath(basepath, "AGPSWITRS", "AGPSWITRS.gdb"),
        "agpgdbraw" => joinpath(basepath, "AGPSWITRS", "AGPSWITRS.gdb", "raw"),
        "agpgdbsupporting" => joinpath(basepath, "AGPSWITRS", "AGPSWITRS.gdb", "supporting"),
        "agpgdbanalysis" => joinpath(basepath, "AGPSWITRS", "AGPSWITRS.gdb", "analysis"),
        "agpgdbhotspots" => joinpath(basepath, "AGPSWITRS", "AGPSWITRS.gdb", "hotspots"),
        "analysys" => joinpath(basepath, "analysis"),
        "codebook" => joinpath(basepath, "codebook"),
        "data" => joinpath(basepath, "data"),
        "dataago" => joinpath(basepath, "data", "ago"),
        "dataarchived" => joinpath(basepath, "data", "archived"),
        "datagis" => joinpath(basepath, "data", "gis"),
        "dataprocessed" => joinpath(basepath, "data", "processed"),
        "dataraw" => joinpath(basepath, "data", "raw"),
        "gis" => joinpath(basepath, "gis"),
        "gislayers" => joinpath(basepath, "gis", "layers"),
        "gislayerstemplates" => joinpath(basepath, "gis", "layers", "templates"),
        "gislayouts" => joinpath(basepath, "gis", "layouts"),
        "gismaps" => joinpath(basepath, "gis", "maps"),
        "gisstyles" => joinpath(basepath, "gis", "styles"),
        "graphics" => joinpath(basepath, "graphics"),
        "graphicsgis" => joinpath(basepath, "graphics", "gis"),
        "graphicsoverleaf" => joinpath(basepath, "graphics", "overleaf"),
        "graphicspresentations" => joinpath(basepath, "graphics", "presentations"),
        "metadata" => joinpath(basepath, "metadata"),
        "notebooks" => joinpath(basepath, "notebooks"),
        "scripts" => joinpath(basepath, "scripts"),
        "metadata" => joinpath(basepath, "metadata"),
        "notebooks" => joinpath(basepath, "notebooks"),
        "notebooksarchived" => joinpath(basepath, "notebooks", "archived"),
        "scripts" => joinpath(basepath, "scripts")
    )

    if !silent
        println("Project Directories:")
        for (key, value) in prjdirs
            println(" - $key: $value")
        end
    end

    return prjdirs
end


