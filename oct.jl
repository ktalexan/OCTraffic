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

export projectmetadata, exportmetadata, projectdirectories, updatetimsmetadata, relocatecolumn!


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
    md = Dict{String,Any}()
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
    metadata = Dict{String,Any}()
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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 5. Update TIMS Metadata Function
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
    updatetimsmetadata(year::Int; type::String="reported", datacounts::Vector{Int}=Int[0, 0, 0])

Update the TIMS metadata file with the counts of crashes, parties, and victims for a given year and type.

# Arguments
- `year::Int`: The year for which the metadata is being updated.
- `type::String`: The type of data being updated. Valid values are "reported", "geocoded", or "excluded".
- `datacounts::Vector{Int}`: A list containing the counts of crashes, parties, and victims. Defaults to `[0, 0, 0]`.

# Returns
- `Nothing`: Updates the `metadata/timsmd.toml` file.

# Raises
- `ArgumentError`: If the type is not valid.
- `ErrorException`: If the metadata file does not exist.
"""
function updatetimsmetadata(year::Int64; type::String="reported", datacounts::Vector{Int}=Int[0, 0, 0])
    # Types definition
    valid_types = ["reported", "geocoded", "excluded"]
    if !(type in valid_types)
        throw(ArgumentError("Invalid type '$type'. Valid types are: $(join(valid_types, ", "))"))
    end

    # Check if datacounts is a list of three integers
    if length(datacounts) != 3
        throw(ArgumentError("datacounts must be a list of three integers: [crashes, parties, victims]"))
    end

    # Get the counts from the datacounts list
    cntcrashes = datacounts[1]
    cntparties = datacounts[2]
    cntvictims = datacounts[3]

    # Check if the TIMS metadata file exists
    mdfile = joinpath(pwd(), "metadata", "timsmd.toml")
    if !isfile(mdfile)
        error("Metadata file $mdfile does not exist.")
    end

    # Load the TIMS metadata
    timsmd = TOML.parsefile(mdfile)

    # Convert year to string for dictionary key access
    yearstring = string(year)

    # Note: TOML parsing results in Dict{String, Any}.
    # We need to make sure the structure exists.
    if !haskey(timsmd, yearstring)
        # If year doesn't exist, we might want to create it or error. 
        # For now, let's assume it should exist or we create a basic structure.
        timsmd[yearstring] = Dict{String,Any}()
    end

    if !haskey(timsmd[yearstring], type)
        timsmd[yearstring][type] = Dict{String,Any}()
    end

    timsmd[yearstring][type]["crashes"] = cntcrashes
    timsmd[yearstring][type]["parties"] = cntparties
    timsmd[yearstring][type]["victims"] = cntvictims

    # Save the updated metadata back to the file
    open(mdfile, "w") do io
        TOML.print(io, timsmd; sorted=true)
    end

    return nothing
end





#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 6. Relocate Data Frame Column Function
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
    relocatecolumn!(df::DataFrame, col_name::Union{String, Vector{String}}, ref_col_name::String, position::String="after") -> DataFrame

Relocates a column in a DataFrame to a new position relative to another column.

# Arguments
- `df::DataFrame`: The DataFrame to modify.
- `col_name::Union{String, Vector{String}}`: The name of the column to relocate.
- `ref_col_name::String`: The name of the reference column.
- `position::String`: "before" or "after" the reference column. Default is "after".

# Returns
- `DataFrame`: The modified DataFrame with the relocated column.
"""
function relocatecolumn!(df::DataFrame, col_name::Union{String,Vector{String}}, ref_col_name::String, position::String="after")

    # Make sure the ref_col_name exists in the DataFrame
    if !(ref_col_name in names(df))
        throw(ArgumentError("Reference column '\$ref_col_name' does not exist in the DataFrame."))
    end

    # Normalize col_name to a vector of strings
    cols_to_move = isa(col_name, String) ? [col_name] : col_name

    # Check if all columns to move exist
    for c in cols_to_move
        if !(c in names(df))
            throw(ArgumentError("Column '\$c' does not exist in the DataFrame."))
        end
    end

    if !(position in ["before", "after"])
        throw(ArgumentError("Position must be 'before' or 'after'."))
    end

    # Get all column names
    all_cols = names(df)

    # Remove the columns to be moved from the list of columns
    remaining_cols = filter(c -> !(c in cols_to_move), all_cols)

    # Find the index of the reference column in the remaining columns
    ref_idx = findfirst(==(ref_col_name), remaining_cols)

    if isnothing(ref_idx)
        throw(ArgumentError("Reference column cannot be one of the columns being moved."))
    end

    # Determine insertion index
    if position == "before"
        insert_idx = ref_idx
    else # position == "after"
        insert_idx = ref_idx + 1
    end

    # Construct the new column order
    new_order = vcat(remaining_cols[1:insert_idx-1], cols_to_move, remaining_cols[insert_idx:end])

    # Reorder the DataFrame
    select!(df, new_order)

    return df
end

