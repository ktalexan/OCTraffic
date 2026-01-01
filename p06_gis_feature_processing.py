# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Part 6 - GIS Feature Processing ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.3, Date: December 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOCTraffic GIS Data Processing - Part 6 - GIS Feature Processing\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1. Preliminaries")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Referencing Libraries and Initialization ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.1. Referencing Libraries and Initialization")

# Import Python libraries
import os
import datetime as dt
import pandas as pd
import pytz
from dotenv import load_dotenv
import arcpy
from arcpy import metadata as md

from octraffic import OCTraffic

# Initialize the OCTraffic object
ocs = OCTraffic(part = 6, version = 2025.3)

# Load environment variables from .env file
load_dotenv()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2. Project and Workspace Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.2. Project and Workspace Variables")


### Project and Geodatabase Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Project and Geodatabase Paths")

# Create a dictionary with the project metadata
print("\nCreating project metadata")
prj_meta = ocs.project_metadata(silent = False)

# Create a dictionary with the project directories
print("\nCreating project directories")
prj_dirs = ocs.project_directories(silent = False)

# Set the current working directory to the project root
os.chdir(prj_dirs["root"])

# Define the project folder (parent directory of the current working directory)
prj_folder = os.getcwd()

# Get the new current working directory
print(f"\nCurrent working directory: {prj_folder}\n")


### ArcGIS Pro Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- ArcGIS Pro Paths")

# Get the paths for the ArcGIS Pro project and geodatabase
aprx_path: str = prj_dirs.get("agp_aprx", "")
gdb_path: str = prj_dirs.get("agp_gdb", "")
# ArcGIS Pro Project object
aprx, workspace = ocs.load_aprx(aprx_path = aprx_path, gdb_path = gdb_path, add_to_map = False)
# Close all map views
aprx.closeViews()
# Current ArcGIS workspace (arcpy)
#arcpy.env.workspace = gdb_path
#workspace = arcpy.env.workspace
# Enable overwriting existing outputs
#arcpy.env.overwriteOutput = True
# Disable adding outputs to map
#arcpy.env.addOutputsToMap = False


### Data Folder Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Data Folder Paths")

# The most current raw data files cover the periods from 01/01/2013 to 09/30/2024. The data files are already processed in the R scripts and imported into the project's geodatabase.
# Add the start date of the raw data to a new python datetime object
date_start = prj_meta["date_start"]
# Add the end date of the raw data to a new python datetime object
date_end = prj_meta["date_end"]
# Define time and date variables
time_zone = pytz.timezone("US/Pacific")
# Add today's day
today = dt.datetime.now(time_zone)
date_updated = today.strftime("%B %d, %Y")
time_updated = today.strftime("%I:%M %p")

# Define date strings for metadata
# String defining the years of the raw data
md_years = f"{date_start.year}-{date_end.year}"
# String defining the start and end dates of the raw data
md_dates = f"Data from {date_start.strftime('%B %d, %Y')} to {date_end.strftime('%B %d, %Y')}"


### Codebook ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Codebook")

# Load the codebook from the project codebook directory
print("- Loading the codebook from the project codebook directory")
cb = ocs.load_cb()

# create a data frame from the codebook
print("- Creating a data frame from the codebook")
df_cb = pd.DataFrame(cb).transpose()
# Add attributes to the codebook data frame
df_cb.attrs["name"] = "Codebook"
print(df_cb.head())


### JSON CIM Exports ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- JSON CIM Exports")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.3. Map and Layout Lists ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.3. Map and Layout Lists")

### Project Maps ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Project Maps")

# List of maps to be created for the project
map_list = [
    "collisions",
    "crashes",
    "parties",
    "victims",
    "injuries",
    "fatalities",
    "fhs100m1km",
    "fhs150m2km",
    "fhs100m5km",
    "fhsRoads500ft",
    "ohsRoads500ft",
    "roadCrashes",
    "roadHotspots",
    "roadBuffers",
    "roadSegments",
    "roads",
    "pointFhs",
    "pointOhs",
    "popDens",
    "houDens",
    "areaCities",
    "areaBlocks",
    "summaries",
    "analysis",
    "regression",
]


### Project Layouts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Project Layouts")

# List or layouts to be created for the project
layout_list = ["maps", "injuries", "hotspots", "roads", "points", "densities", "areas"]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.4. Clean Up Data ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.4. Clean Up Data")


### Delete Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Delete Feature Classes")

# Clean up the feature classes in the geodatabase for the Analysis and Hotspot Feature Datasets
for d in ["analysis", "hotspots"]:
    print(f"Dataset: {d}")
    for f in arcpy.ListFeatureClasses(feature_dataset = d):
        print(f"- Removing {f} feature class from the project...")
        arcpy.management.Delete(f)


### Delete Maps ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Delete Maps")

# Clean up the maps in the project structure
for m in aprx.listMaps():
    print(f"- Removing {m.name} map from the project...")
    aprx.deleteItem(m)


### Delete Layouts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Delete Layouts")

# Clean up the layouts in the project structure
for l in aprx.listLayouts():
    print(f"- Removing {l.name} layout from the project...")
    aprx.deleteItem(l)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Geodatabase Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2. Geodatabase Operations")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1. Raw Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.1. Raw Data Feature Classes")


### Feature Class Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Feature Class Paths")

# Paths to raw data feature classes
victims = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "victims")
parties = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "parties")
crashes = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "crashes")
collisions = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "collisions")


### Feature Class Fields ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Feature Class Fields")

# Fields for the raw data feature classes
victims_fields = [f.name for f in arcpy.ListFields(victims)]  # victims field list
parties_fields = [f.name for f in arcpy.ListFields(parties)]  # parties field list
crashes_fields = [f.name for f in arcpy.ListFields(crashes)]  # crashes field list
collisions_fields = [f.name for f in arcpy.ListFields(collisions)]  # collisions field list


### Raw Counts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Raw Counts")

# Get the count for the raw data feature classes
victims_count = int(arcpy.management.GetCount(victims)[0])
parties_count = int(arcpy.management.GetCount(parties)[0])
crashes_count = int(arcpy.management.GetCount(crashes)[0])
collisions_count = int(arcpy.management.GetCount(collisions)[0])
print(
    f"\nRaw Data Counts:\n- Victims: {victims_count:,}\n- Parties: {parties_count:,}\n- Crashes: {crashes_count:,}\n- Collisions: {collisions_count:,}"
)


### Feature Class Aliases ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Feature Class Aliases")

# Collisions feature class alias
collisions_alias = "OCTraffic Collisions"
# Collisions feature class
arcpy.AlterAliasName(collisions, collisions_alias)
print(f"Collisions: {arcpy.GetMessages()}")

# Collisions field aliases
for f in collisions_fields:
    if f in list(cb.keys()):
        print(f"\tMatch {cb[f]['order']}: {f} ({cb[f]['var_alias']})")
        arcpy.management.AlterField(
            in_table = collisions, field = f, new_field_alias = cb[f]["var_alias"]
        )
print(arcpy.GetMessages())


### Crashes Feature Class Aliases ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Crashes Feature Class Aliases")

# Crashes feature class alias
crashes_alias = "OCTraffic Crashes"
# Crashes feature class
arcpy.AlterAliasName(crashes, crashes_alias)
print(f"Crashes: {arcpy.GetMessages()}")
# Crashes field aliases
for f in crashes_fields:
    if f in list(cb.keys()):
        print(f"\tMatch {cb[f]['order']}: {f} ({cb[f]['var_alias']})")
        arcpy.management.AlterField(
            in_table = crashes, field = f, new_field_alias = cb[f]["var_alias"]
        )
print(arcpy.GetMessages())


### Parties Feature Class Aliases ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Parties Feature Class Aliases")

# Parties feature class alias
parties_alias = "OCTraffic Parties"
# Parties feature class
arcpy.AlterAliasName(parties, parties_alias)
print(f"Parties: {arcpy.GetMessages()}")
# Parties field aliases
for f in parties_fields:
    if f in list(cb.keys()):
        print(f"\tMatch {cb[f]['order']}: {f} ({cb[f]['var_alias']})")
        arcpy.management.AlterField(
            in_table = parties, field = f, new_field_alias = cb[f]["var_alias"]
        )
print(arcpy.GetMessages())


### Victims Feature Class Aliases ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Victims Feature Class Aliases")

# Victims feature class alias
victims_alias = "OCTraffic Victims"
# Victims feature class
arcpy.AlterAliasName(victims, victims_alias)
print(f"Victims: {arcpy.GetMessages()}")
# Victims field aliases
for f in victims_fields:
    if f in list(cb.keys()):
        print(f"\tMatch {cb[f]['order']}: {f} ({cb[f]['var_alias']})")
        arcpy.management.AlterField(
            in_table = victims, field = f, new_field_alias = cb[f]["var_alias"]
        )
print(arcpy.GetMessages())


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2. Supporting Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.2. Supporting Data Feature Classes")


### Feature Class Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Feature Class Paths")

# Paths to supporting data feature classes
boundaries = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "boundaries")
cities = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "cities")
blocks = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "blocks")
roads = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "roads")


### Feature Class Fields ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Feature Class Fields")

# Fields for the supporting data feature classes
boundaries_fields = [f.name for f in arcpy.ListFields(boundaries)] # boundaries field list
cities_fields = [f.name for f in arcpy.ListFields(cities)]  # cities field list
blocks_fields = [f.name for f in arcpy.ListFields(blocks)]  # censusBlocks field list
roads_fields = [f.name for f in arcpy.ListFields(roads)]  # roads field list


### Row Counts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Row Counts")

# Get the count for the supporting data feature classes
boundaries_count = int(arcpy.management.GetCount(boundaries)[0])
cities_count = int(arcpy.management.GetCount(cities)[0])
blocks_count = int(arcpy.management.GetCount(blocks)[0])
roads_count = int(arcpy.management.GetCount(roads)[0])
# Print the counts
print(
    f"Supporting Data Counts:\n- Boundaries: {boundaries_count:,}\n- Cities: {cities_count:,}\n- Census Blocks: {blocks_count:,}\n- Roads: {roads_count:,}"
)


### Roads Feature Class ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Roads Feature Class")

# Roads feature class alias
roads_alias = "OCTraffic Roads"
# Roads feature class
arcpy.AlterAliasName(roads, roads_alias)
print(f"Roads: {arcpy.GetMessages()}")
# Roads field aliases
for f in roads_fields:
    if f in list(cb.keys()):
        print(f"\tMatch {cb[f]['order']}: {f} ({cb[f]['var_alias']})")
        arcpy.management.AlterField(
            in_table = roads, field = f, new_field_alias = cb[f]["var_alias"]
        )
print(arcpy.GetMessages())


### Census Blocks Feature Class ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Census Blocks Feature Class")

# Census Blocks feature class alias
blocks_alias = "OCTraffic Census Blocks"
# Census Blocks feature class
arcpy.AlterAliasName(blocks, blocks_alias)
print(f"USC 2020 Census Blocks: {arcpy.GetMessages()}")
# Census Blocks field aliases
for f in blocks_fields:
    if f in list(cb.keys()):
        print(f"\tMatch {cb[f]['order']}: {f} ({cb[f]['var_alias']})")
        arcpy.management.AlterField(
            in_table = blocks, field = f, new_field_alias = cb[f]["var_alias"]
        )
print(arcpy.GetMessages())


### Cities Feature Class ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Cities Feature Class")

# Cities feature class alias
cities_alias = "OCTraffic Cities"
# Cities feature class
arcpy.AlterAliasName(cities, cities_alias)
print(f"Cities: {arcpy.GetMessages()}")
# Cities field aliases
for f in cities_fields:
    if f in list(cb.keys()):
        print(f"\tMatch {cb[f]['order']}: {f} ({cb[f]['var_alias']})")
        arcpy.management.AlterField(
            in_table = cities, field = f, new_field_alias = cb[f]["var_alias"]
        )
print(arcpy.GetMessages())


### Boundaries Feature Class ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Boundaries Feature Class")

# Boundaries feature class alias
boundaries_alias = "OCTraffic Boundaries"
# Boundaries feature class
arcpy.AlterAliasName(boundaries, boundaries_alias)
print(f"Boundaries: {arcpy.GetMessages()}")


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.3. Data Enrichment Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.3. Data Enrichment Feature Classes")


### Feature Class Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Feature Class Paths")

# Join the collisions feature class with the censusBlocks feature class
collisions1 = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "collisions1")


### Feature Class Joins ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Feature Class Joins")

# Join the collisions feature class with the censusBlocks feature class
arcpy.analysis.SpatialJoin(
    target_features = collisions,
    join_features = blocks,
    out_feature_class = collisions1,
    join_operation = "JOIN_ONE_TO_ONE",
    join_type = "KEEP_ALL",
    match_option = "INTERSECT",
    search_radius = None,
    distance_field_name = None,
    match_fields = None,
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.4. Analysis Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.4. Analysis Data Feature Classes")


# region Delete all Old Analysis Feature Classes
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Delete all Old Analysis Feature Classes")

# Loop through all analysis data feature dataset and delete all feature classes
for f in arcpy.ListFeatureClasses(feature_dataset = "analysis"):
    print(f"Deleting {f}...")
    arcpy.Delete_management(f)
    print(arcpy.GetMessages())


### Create Major Roads ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create Major Roads")

# Separate the primary and secondary roads from the local roads
# Output feature class for the major roads
roads_major = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major")
# Select the major (primary and secondary) roads from the roads feature class
arcpy.analysis.Select(
    in_features = roads,
    out_feature_class = roads_major,
    where_clause = "road_cat = 'Primary' Or road_cat = 'Secondary'",
)
print(arcpy.GetMessages())

# Add feature class alias for the major roads feature class
# Define the major roads layer alias and modify the feature class alias
roads_major_alias = "OCTraffic Major Roads"
arcpy.AlterAliasName(roads_major, roads_major_alias)
print(arcpy.GetMessages())
# Obtain the list of fields for the major roads feature class
roads_majorFields = [
    f.name for f in arcpy.ListFields(roads_major)
]  # roads_major field list
# Field Aliases for the major roads feature class
for f in arcpy.ListFields(roads_major):
    print(f"{f.name} ({f.aliasName})")


### Create Major Roads Buffers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create Major Roads Buffers")

# Create road buffers for the primary and secondary roads
# Output feature class for the major roads buffers
roads_major_buffers = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_buffers")
# Buffer the major roads feature class by 250 meters (on each side)
arcpy.analysis.Buffer(
    in_features = roads_major,
    out_feature_class = roads_major_buffers,
    buffer_distance_or_field = "250 Meters",
    line_side = "FULL",
    line_end_type = "FLAT",
    dissolve_option = "NONE",
    dissolve_field = None,
    method = "PLANAR",
)
print(arcpy.GetMessages())

# Add feature class alias for the major road buffers feature class
# Define the major roads buffers layer alias and modify the feature class alias
roads_major_buffers_alias = "OCTraffic Major Roads Buffers"
arcpy.AlterAliasName(roads_major_buffers, roads_major_buffers_alias)
print(arcpy.GetMessages())
# Obtain the list of fields for the major road buffers feature class
roads_major_buffers_fields = [
    f.name for f in arcpy.ListFields(roads_major_buffers)
]  # roads_major_buffers field list
# Field Aliases for the major roads buffers feature class
for f in arcpy.ListFields(roads_major_buffers):
    print(f"{f.name} ({f.aliasName})")


### Summarize Major Road Buffers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Summarize Major Road Buffers")

# Create a summary for each of the road buffers that contains statistics and counts of crash collision data
# Output feature class for the summarized major roads buffers
roads_major_buffers_sum = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_buffers_sum")
# Summarize the major roads buffers feature class by key crashes attributes
arcpy.analysis.SummarizeWithin(
    in_polygons = roads_major_buffers,
    in_sum_features = crashes,
    out_feature_class = roads_major_buffers_sum,
    keep_all_polygons = "KEEP_ALL",
    sum_fields = [
        ["crash_tag", "Sum"],
        ["party_count", "Sum"],
        ["victim_count", "Sum"],
        ["number_killed", "Sum"],
        ["number_inj", "Sum"],
        ["count_severe_inj", "Sum"],
        ["count_visible_inj", "Sum"],
        ["count_complaint_pain", "Sum"],
        ["count_car_killed", "Sum"],
        ["count_car_inj", "Sum"],
        ["count_ped_killed", "Sum"],
        ["count_ped_inj", "Sum"],
        ["count_bic_killed", "Sum"],
        ["count_bic_inj", "Sum"],
        ["count_mc_killed", "Sum"],
        ["count_mc_inj", "Sum"],
        ["coll_severity_num", "Mean"],
        ["coll_severity_rank_num", "Mean"],
        ["coll_severity_hs", "Mean"]
    ],
)
print(arcpy.GetMessages())

# Add feature class alias for the summarized major road buffers feature class
# Define the major roads buffers summary layer alias and modify the feature class alias
roads_major_buffers_sum_alias = "OCTraffic Major Roads Buffers Summary"
arcpy.AlterAliasName(roads_major_buffers_sum, roads_major_buffers_sum_alias)
print(arcpy.GetMessages())
# Obtain the fields for the summarized major road buffers feature class
roads_major_buffers_sum_fields = [
    f.name for f in arcpy.ListFields(roads_major_buffers_sum)
]  # roads_major_buffers_sum field list
# Field Aliases for the major roads buffers summary feature class
for f in arcpy.ListFields(roads_major_buffers_sum):
    print(f"{f.name} ({f.aliasName})")


### Points 1,000 ft along major road lines ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Points 1,000 ft along major road lines")

# Generate points every 1,000 feet along the major road lines
# Create a path for the new summarized major road buffers feature class
roads_major_points_along_lines = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_points_along_lines")
arcpy.management.GeneratePointsAlongLines(
    Input_Features = roads_major,
    Output_Feature_Class = roads_major_points_along_lines,
    Point_Placement = "DISTANCE",
    Distance = "1000 Feet",
    Percentage = None,
    Include_End_Points = "NO_END_POINTS",
    Add_Chainage_Fields = "NO_CHAINAGE",
    Distance_Field = None,
    Distance_Method = "PLANAR",
)

# Add feature class alias for the points along major road lines feature class
# Define the major roads points along lines layer alias and modify the feature class alias
roads_major_points_along_lines_alias = "OCTraffic Major Roads Points Along Lines"
arcpy.AlterAliasName(roads_major_points_along_lines, roads_major_points_along_lines_alias)
print(arcpy.GetMessages())
# Obtain the fields for the points along major road lines feature class
roads_major_points_along_lines_fields = [
    f.name for f in arcpy.ListFields(roads_major_points_along_lines)
]  # roads_major_points_along_lines field list
# Field Aliases for the major roads points along lines feature class
for f in arcpy.ListFields(roads_major_points_along_lines):
    print(f"{f.name} ({f.aliasName})")


### Split Road Segments 1,000 ft apart ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Split Road Segments 1,000 ft apart")

# Split road segments at the points (1,000 feet apart)
# Create a path for the new split major roads feature class
roads_major_split = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_split")
# Split the major roads at the points along the lines
arcpy.management.SplitLineAtPoint(
    in_features = roads_major,
    point_features = roads_major_points_along_lines,
    out_feature_class = roads_major_split,
    search_radius = "1000 Feet",
)

# Add feature class alias for the split road segments feature class
# Define the major roads split layer alias and modify the feature class alias
roads_major_split_alias = "OCTraffic Major Roads Split"
arcpy.AlterAliasName(roads_major_split, roads_major_split_alias)
print(arcpy.GetMessages())
# Obtain the fields for the split road segments feature class
roads_major_split_fields = [
    f.name for f in arcpy.ListFields(roads_major_split)
]  # roads_major_split field list
# Field Aliases for the major roads split feature class
for f in arcpy.ListFields(roads_major_split):
    print(f"{f.name} ({f.aliasName})")


### Buffers 500ft around road segments ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Buffers 500ft around road segments")

# Create buffers (500 ft) around the road segments (1,000 feet)
# Create a path for the new split major roads feature class
roads_major_split_buffer = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_split_buffer")
# Buffer the split major roads by 500 feet
arcpy.analysis.Buffer(
    in_features = roads_major_split,
    out_feature_class = roads_major_split_buffer,
    buffer_distance_or_field = "500 Feet",
    line_side = "FULL",
    line_end_type = "FLAT",
    dissolve_option = "NONE",
    dissolve_field = None,
    method = "PLANAR",
)

# Add feature class alias for the road segment buffers feature class
# Define the major roads split buffer layer alias and modify the feature class alias
roads_major_split_buffer_alias = "OCTraffic Major Roads Split Buffer"
arcpy.AlterAliasName(roads_major_split_buffer, roads_major_split_buffer_alias)
print(arcpy.GetMessages())
# Obtain the fields for the road segment buffers feature class
roads_major_split_buffer_fields = [
    f.name for f in arcpy.ListFields(roads_major_split_buffer)
]  # roads_major_split_buffer field list
# Field Aliases for the major roads split buffer feature class
for f in arcpy.ListFields(roads_major_split_buffer):
    print(f"{f.name} ({f.aliasName})")


### Summarize road segments buffers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Summarize road segments buffers")

# Summarize the crash collision data for each of the road segments
# Create a path for the new summarized major road buffers feature class
roads_major_split_buffer_sum = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_split_buffer_sum")
# Summarize the data within the major road buffers from the crashes data
arcpy.analysis.SummarizeWithin(
    in_polygons = roads_major_split_buffer,
    in_sum_features = crashes,
    out_feature_class = roads_major_split_buffer_sum,
    keep_all_polygons = "KEEP_ALL",
    sum_fields = [
        ["crash_tag", "Sum"],
        ["party_count", "Sum"],
        ["victim_count", "Sum"],
        ["number_killed", "Sum"],
        ["number_inj", "Sum"],
        ["count_severe_inj", "Sum"],
        ["count_visible_inj", "Sum"],
        ["count_complaint_pain", "Sum"],
        ["count_car_killed", "Sum"],
        ["count_car_inj", "Sum"],
        ["count_ped_killed", "Sum"],
        ["count_ped_inj", "Sum"],
        ["count_bic_killed", "Sum"],
        ["count_bic_inj", "Sum"],
        ["count_mc_killed", "Sum"],
        ["count_mc_inj", "Sum"],
        ["coll_severity_num", "Mean"],
        ["coll_severity_rank_num", "Mean"],
        ["coll_severity_hs", "Mean"]
    ],
)

# Add feature class alias for the summarized road segments buffers feature class
# Define the major roads split buffer summary layer alias and modify the feature class alias
roads_major_split_buffer_sum_alias = "OCTraffic Major Roads Split Buffer Summary"
arcpy.AlterAliasName(roads_major_split_buffer_sum, roads_major_split_buffer_sum_alias)
print(arcpy.GetMessages())
# Obtain the fields for the summarized road segments buffers feature class
roads_major_split_buffer_sum_fields = [
    f.name for f in arcpy.ListFields(roads_major_split_buffer_sum)
]  # roads_major_split_buffer_sum field list
# Field Aliases for the major roads split buffer summary feature class
for f in arcpy.ListFields(roads_major_split_buffer_sum):
    print(f"{f.name} ({f.aliasName})")


### Summarize Census Blocks ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Summarize Census Blocks")


# Create a summary for each of the Census blocks that contains statistics and counts of crash collision data
# Create a path for the new summarized US 2020 Census Blocks feature class
blocks_sum = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "blocks_sum")
arcpy.analysis.SummarizeWithin(
    in_polygons = blocks,
    in_sum_features = crashes,
    out_feature_class = blocks_sum,
    keep_all_polygons = "KEEP_ALL",
    sum_fields = [
        ["crash_tag", "Sum"],
        ["party_count", "Sum"],
        ["victim_count", "Sum"],
        ["number_killed", "Sum"],
        ["number_inj", "Sum"],
        ["count_severe_inj", "Sum"],
        ["count_visible_inj", "Sum"],
        ["count_complaint_pain", "Sum"],
        ["count_car_killed", "Sum"],
        ["count_car_inj", "Sum"],
        ["count_ped_killed", "Sum"],
        ["count_ped_inj", "Sum"],
        ["count_bic_killed", "Sum"],
        ["count_bic_inj", "Sum"],
        ["count_mc_killed", "Sum"],
        ["count_mc_inj", "Sum"],
        ["coll_severity_num", "Mean"],
        ["coll_severity_rank_num", "Mean"],
        ["coll_severity_hs", "Mean"]
    ],
)

# Add feature class alias for the summarized Census blocks feature class
# Define the US 2020 Census Blocks summary layer alias and modify the feature class alias
blocks_sum_alias = "OCTraffic Census Blocks Summary"
arcpy.AlterAliasName(blocks_sum, blocks_sum_alias)
print(arcpy.GetMessages())
# Obtain the fields for the summarized Census blocks feature class
blocks_sum_fields = [
    f.name for f in arcpy.ListFields(blocks_sum)
]  # cenBlocks_sum field list
# Field Aliases for the US 2020 Census Blocks summary feature class
for f in arcpy.ListFields(blocks_sum):
    print(f"{f.name} ({f.aliasName})")


### Summarize Cities ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Summarize Cities")

# Create a summary for each of the cities that contains statistics and counts of crash collision data
# Create a path for the new summarized US 2020 Census Blocks feature class
cities_sum = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "cities_sum")
# Summarize the data within the cities from the crashes data
arcpy.analysis.SummarizeWithin(
    in_polygons = cities,
    in_sum_features = crashes,
    out_feature_class = cities_sum,
    keep_all_polygons = "KEEP_ALL",
    sum_fields = [
        ["crash_tag", "Sum"],
        ["party_count", "Sum"],
        ["victim_count", "Sum"],
        ["number_killed", "Sum"],
        ["number_inj", "Sum"],
        ["count_severe_inj", "Sum"],
        ["count_visible_inj", "Sum"],
        ["count_complaint_pain", "Sum"],
        ["count_car_killed", "Sum"],
        ["count_car_inj", "Sum"],
        ["count_ped_killed", "Sum"],
        ["count_ped_inj", "Sum"],
        ["count_bic_killed", "Sum"],
        ["count_bic_inj", "Sum"],
        ["count_mc_killed", "Sum"],
        ["count_mc_inj", "Sum"],
        ["coll_severity_num", "Mean"],
        ["coll_severity_rank_num", "Mean"],
        ["coll_severity_hs", "Mean"]
    ],
)

# Add feature class alias for the summarized cities feature class
# Define the cities summary layer alias and modify the feature class alias
cities_sum_alias = "OCTraffic Cities Summary"
arcpy.AlterAliasName(cities_sum, cities_sum_alias)
print(arcpy.GetMessages())
# Obtain the fields for the summarized cities feature class
cities_sum_fields = [f.name for f in arcpy.ListFields(cities_sum)]  # cities_sum field list
# Field Aliases for the cities summary feature class
for f in arcpy.ListFields(cities_sum):
    print(f"{f.name} ({f.aliasName})")


### Crashes within 500ft from Major Roads ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Crashes within 500ft from Major Roads")

# Select all crashes that are within 500 ft of the major roads
# Create a path for the new feature class
crashes_500ft_from_major_roads = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "crashes_500ft_from_major_roads")
# Select the crashes within 500 feet of the major roads and store it in a temporary layer
tempLyr = arcpy.management.SelectLayerByLocation(
    in_layer = crashes,
    select_features = roads_major,
    search_distance = "500 Feet",
    selection_type = "NEW_SELECTION",
    invert_spatial_relationship = "NOT_INVERT",
)
# Export the selected crashes to a new feature class
arcpy.conversion.ExportFeatures(
    in_features = tempLyr,
    out_features = crashes_500ft_from_major_roads,
    where_clause = "",
    use_field_alias_as_name = "NOT_USE_ALIAS",
)
print(arcpy.GetMessages())
# Delete the temporary layer
arcpy.management.Delete(tempLyr)

# Add feature class alias for the crashes within 500 ft from major roads feature class
# Define the crashes 500 feet from major roads layer alias and modify the feature class alias
crashes_500ft_from_major_roads_alias = "OCTraffic Crashes 500 Feet from Major Roads"
arcpy.AlterAliasName(crashes_500ft_from_major_roads, crashes_500ft_from_major_roads_alias)
print(arcpy.GetMessages())
# Obtain the fields for the crashes within 500 ft from major roads feature class
crashes_500ft_from_major_roads_fields = [
    f.name for f in arcpy.ListFields(crashes_500ft_from_major_roads)
]  # crashes_500ft_from_major_roads field list
# Field Aliases for the crashes 500 feet from major roads feature class
for f in arcpy.ListFields(crashes_500ft_from_major_roads):
    print(f"{f.name} ({f.aliasName})")


### Crashes (Collision Severity) Exploratory Regression ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Crashes (Collision Severity) Exploratory Regression")

# Generate a collision severity binary indicator to crashes dataset
# Add collision severity binary indicator to crashes
arcpy.management.CalculateField(
    in_table = crashes,
    field = "severity_bin",
    expression = "sevbin(!coll_severity_bin!)",
    expression_type = "PYTHON3",
    code_block = """def sevbin(x):
    if x == "Severe or fatal":
        return 1
    elif x == "None, minor or pain":
        return 0""",
    field_type = "SHORT",
    enforce_domains = "NO_ENFORCE_DOMAINS",
)
# Add a field alias for the collision severity binary indicator
arcpy.management.AlterField(
    in_table = crashes, field = "severity_bin", new_field_alias = "Severity Binary"
)

# Define outputs for the exploratory regression
regression_report_file = os.path.join(prj_dirs["agp"], "regression_report.txt")
regression_results_table = os.path.join(prj_dirs["agp_gdb"], "regression_results_table")

# Perform exploratory regression to predict the binary severity bin
arcpy.stats.ExploratoryRegression(
    Input_Features = crashes,
    Dependent_Variable = "severity_bin",
    Candidate_Explanatory_Variables = "accident_year;coll_severity_num;coll_severity_rank_num;party_count;victim_count;number_killed;number_inj;count_severe_inj;count_visible_inj;count_complaint_pain;count_car_killed;count_car_inj;count_ped_killed;count_ped_inj;count_bic_killed;count_bic_inj;count_mc_killed;count_mc_inj",
    Weights_Matrix_File = None,
    Output_Report_File = regression_report_file,
    Output_Results_Table = regression_results_table,
    Maximum_Number_of_Explanatory_Variables = 5,
    Minimum_Number_of_Explanatory_Variables = 1,
    Minimum_Acceptable_Adj_R_Squared = 0.5,
    Maximum_Coefficient_p_value_Cutoff = 0.05,
    Maximum_VIF_Value_Cutoff = 7.5,
    Minimum_Acceptable_Jarque_Bera_p_value = 0.1,
    Minimum_Acceptable_Spatial_Autocorrelation_p_value = 0.1,
)
print(arcpy.GetMessages())


### Geographically Weighted Regression (GWR) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Geographically Weighted Regression (GWR)")

# Define the output of the geographically weighted regression
crashes_gwr = os.path.join(prj_dirs["agp_gdb_analysis"], "crashes_gwr")

# Perform geographically weighted regression to predict the collision severity after (and using the results from) the exploratory regression
arcpy.stats.GWR(
    in_features = crashes,
    dependent_variable = "severity_bin",
    model_type = "COUNT",
    explanatory_variables = "coll_severity_num;number_killed;count_severe_inj;count_visible_inj",
    output_features = crashes_gwr,
    neighborhood_type = "NUMBER_OF_NEIGHBORS",
    neighborhood_selection_method = "GOLDEN_SEARCH",
    minimum_number_of_neighbors = 80,
    maximum_number_of_neighbors = 1000,
    minimum_search_distance = None,
    maximum_search_distance = None,
    number_of_neighbors_increment = None,
    search_distance_increment = None,
    number_of_increments = None,
    number_of_neighbors = None,
    distance_band = None,
    prediction_locations = None,
    explanatory_variables_to_match = None,
    output_predicted_features = None,
    robust_prediction = "NON_ROBUST",
    local_weighting_scheme = "BISQUARE",
    coefficient_raster_workspace = None,
    scale = "NO_SCALE_DATA"
)
print(arcpy.GetMessages())


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.5. Hotspot Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.5. Hotspot Data Feature Classes")


### Delete all Old Hotspot Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Delete all Old Hotspot Feature Classes")

# Loop through all hotspot data feature dataset and delete all feature classes
for f in arcpy.ListFeatureClasses(feature_dataset = "hotspots"):
    print(f"Deleting {f}...")
    arcpy.Delete_management(f)
    print(arcpy.GetMessages())


### Create Hot Spots (Crashes, Collision Severity) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create Hot Spots (Crashes, Collision Severity)")

# Create a hot spot analysis for the crash collision data (collision severity)
# Create a path for the new crashes hot spots feature class
crashes_hotspots = os.path.join(prj_dirs["agp_gdb_hotspots"], "crashes_hotspots")
# Create hot spots points
arcpy.stats.HotSpots(
    Input_Feature_Class = crashes,
    Input_Field = "coll_severity_hs",
    Output_Feature_Class = crashes_hotspots,
    Conceptualization_of_Spatial_Relationships = "FIXED_DISTANCE_BAND",
    Distance_Method = "EUCLIDEAN_DISTANCE",
    Standardization = "ROW",
    Distance_Band_or_Threshold_Distance = None,
    Self_Potential_Field = None,
    Weights_Matrix_File = None,
    Apply_False_Discovery_Rate__FDR__Correction = "NO_FDR",
    number_of_neighbors = None,
)
print(arcpy.GetMessages())

# Add feature class alias for the hot spots (crashes, collision severity) feature class
# Define the crashes hot spots layer alias and modify the feature class alias
crashes_hotspots_alias = "OCTraffic Crashes Hot Spots"
arcpy.AlterAliasName(crashes_hotspots, crashes_hotspots_alias)
print(arcpy.GetMessages())
# Obtain the fields for the hot spots (crashes, collision severity) feature class
crashes_hotspots_fields = [
    f.name for f in arcpy.ListFields(crashes_hotspots)
]  # crashes_hotspots field list
# Field Aliases for the crashes hot spots feature class
for f in arcpy.ListFields(crashes_hotspots):
    print(f"{f.name} ({f.aliasName})")


### Optimized Hot Spots (Crashes, Collision Severity, 1,000m) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Optimized Hot Spots (Crashes, Collision Severity, 1,000m)")

# Optimized hot spot analysis for the crash collision data (collision severity)
# Create a path for the new optimized crashes hot spots feature class
crashes_optimized_hotspots = os.path.join(prj_dirs["agp_gdb_hotspots"], "crashes_optimized_hotspots")
# Perform Optimized Hot Spot Analysis on the crashes data
arcpy.stats.OptimizedHotSpotAnalysis(
    Input_Features = crashes,
    Output_Features = crashes_optimized_hotspots,
    Analysis_Field = "coll_severity_hs",
    Incident_Data_Aggregation_Method = "COUNT_INCIDENTS_WITHIN_FISHNET_POLYGONS",
    Bounding_Polygons_Defining_Where_Incidents_Are_Possible = None,
    Polygons_For_Aggregating_Incidents_Into_Counts = None,
    Density_Surface = None,
    Cell_Size = None,
    Distance_Band = "1000 Meters",
)
print(arcpy.GetMessages())

# Add feature class alias for the optimized hot spots (crashes, collision severity, 1,000 m) feature class
# Define the optimized crashes hot spots layer alias and modify the feature class alias
crashes_optimized_hotspots_alias = "OCTraffic Crashes Optimized Hot Spots"
arcpy.AlterAliasName(crashes_optimized_hotspots, crashes_optimized_hotspots_alias)
print(arcpy.GetMessages())
# Obtain the fields for the optimized hot spots (crashes, collision severity, 1,000 m) feature class
crashes_optimized_hotspots_fields = [
    f.name for f in arcpy.ListFields(crashes_optimized_hotspots)
]  # crashes_optimized_hotspots field list
# Field Aliases for the optimized crashes hot spots feature class
for f in arcpy.ListFields(crashes_optimized_hotspots):
    print(f"{f.name} ({f.aliasName})")


### Find Hot Spots (Crashes, 100m bins, 1km neighbors) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Find Hot Spots (Crashes, 100m bins, 1km neighbors)")

# Find hot spots for the crash collision data using 100 m bins and 1 km neighborhood radius (328 ft/ 0.621 mi)
# Create a path for the new crashes find hot spots feature class
crashes_find_hotspots_100m1km = os.path.join(prj_dirs["agp_gdb_hotspots"], "crashes_find_hotspots_100m1km")
# Find the hot spots within the crashes data
arcpy.gapro.FindHotSpots(
    point_layer = crashes,
    out_feature_class = crashes_find_hotspots_100m1km,
    bin_size = "100 Meters",
    neighborhood_size = "1 Kilometers",
    time_step_interval = None,
    time_step_alignment = "START_TIME",
    time_step_reference = None,
)
print(arcpy.GetMessages())

# Add feature class alias for the hot spots (crashes, 100m bins, 1km neighbors) feature class
# Define the crashes find hot spots layer alias and modify the feature class alias
crashes_find_hotspots_100m1km_alias = "OCTraffic Crashes Find Hot Spots 100m 1km"
arcpy.AlterAliasName(crashes_find_hotspots_100m1km, crashes_find_hotspots_100m1km_alias)
print(arcpy.GetMessages())
# Obtain the fields for the hot spots (crashes, 100m bins, 1km neighbors) feature class
crashes_find_hotspots_100m1km_fields = [
    f.name for f in arcpy.ListFields(crashes_find_hotspots_100m1km)
]  # crashes_find_hotspots_100m1km field list
# Field Aliases for the crashes find hot spots feature class
for f in arcpy.ListFields(crashes_find_hotspots_100m1km):
    print(f"{f.name} ({f.aliasName})")


### Find Hot Spots (Crashes, 150m bins, 2km neighbors) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Find Hot Spots (Crashes, 150m bins, 2km neighbors)")

# Find hot spots for the crash collision data using 150 m bins and 2 km neighborhood radius (492 ft/ 1.24 mi)
# Create a path for the new crashes find hot spots feature class
crashes_find_hotspots_150m2km = os.path.join(prj_dirs["agp_gdb_hotspots"], "crashes_find_hotspots_150m2km")
# Find the hot spots within the crashes data
arcpy.gapro.FindHotSpots(
    point_layer = crashes,
    out_feature_class = crashes_find_hotspots_150m2km,
    bin_size = "150 Meters",
    neighborhood_size = "2 Kilometers",
    time_step_interval = None,
    time_step_alignment = "START_TIME",
    time_step_reference = None,
)
print(arcpy.GetMessages())

# Add feature class alias for the hot spots (crashes, 150m bins, 2km neighbors) feature class
# Define the crashes find hot spots layer alias and modify the feature class alias
crashes_find_hotspots_150m2km_alias = "OCTraffic Crashes Find Hot Spots 150m 2km"
arcpy.AlterAliasName(crashes_find_hotspots_150m2km, crashes_find_hotspots_150m2km_alias)
print(arcpy.GetMessages())
# Obtain the fields for the hot spots (crashes, 150m bins, 2km neighbors) feature class
crashes_find_hotspots_150m2km_fields = [
    f.name for f in arcpy.ListFields(crashes_find_hotspots_150m2km)
]  # crashes_find_hotspots_150m2km field list
# Field Aliases for the crashes find hot spots feature class
for f in arcpy.ListFields(crashes_find_hotspots_150m2km):
    print(f"{f.name} ({f.aliasName})")


### Find Hot Spots (Crashes, 100m bins, 5km neighbors) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Find Hot Spots (Crashes, 100m bins, 5km neighbors)")

# Find hot spots for the crash collision data using 100 m bins and 5 km neighborhood radius (328 ft/ 3.11 mi)
# Create a path for the new crashes find hot spots feature class
crashes_find_hotspots_100m5km = os.path.join(prj_dirs["agp_gdb_hotspots"], "crashes_find_hotspots_100m5km")
# Find the hot spots within the crashes data
arcpy.gapro.FindHotSpots(
    point_layer = crashes,
    out_feature_class = crashes_find_hotspots_100m5km,
    bin_size = "100 Meters",
    neighborhood_size = "5 Kilometers",
    time_step_interval = None,
    time_step_alignment = "START_TIME",
    time_step_reference = None,
)
print(arcpy.GetMessages())

# Add feature class alias for the hot spots (crashes, 100m bins, 5km neighbors) feature class
# Define the crashes find hot spots layer alias and modify the feature class alias
crashes_find_hotspots_100m5km_alias = "OCTraffic Crashes Find Hot Spots 100m 5km"
arcpy.AlterAliasName(crashes_find_hotspots_100m5km, crashes_find_hotspots_100m5km_alias)
print(arcpy.GetMessages())

# Obtain the fields for the hot spots (crashes, 100m bins, 5km neighbors) feature class
crashes_find_hotspots_100m5km_fields = [
    f.name for f in arcpy.ListFields(crashes_find_hotspots_100m5km)
]  # crashes_find_hotspots_100m5km field list
# Field Aliases for the crashes find hot spots feature class
for f in arcpy.ListFields(crashes_find_hotspots_100m5km):
    print(f"{f.name} ({f.aliasName})")


### Hot Spots (Proximity to Major Roads, 500ft) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Hot Spots (Proximity to Major Roads, 500ft)")

# Hot spot points within 500 feet of major roads
# Create a path for the new hot spots within 500 mt from major roads feature class
crashes_hotspots_500ft_from_major_roads = os.path.join(prj_dirs["agp_gdb_hotspots"], "crashes_hotspots_500ft_from_major_roads")
arcpy.stats.HotSpots(
    Input_Feature_Class = crashes_500ft_from_major_roads,
    Input_Field = "coll_severity_hs",
    Output_Feature_Class = crashes_hotspots_500ft_from_major_roads,
    Conceptualization_of_Spatial_Relationships = "FIXED_DISTANCE_BAND",
    Distance_Method = "EUCLIDEAN_DISTANCE",
    Standardization = "ROW",
    Distance_Band_or_Threshold_Distance = None,
    Self_Potential_Field = None,
    Weights_Matrix_File = None,
    Apply_False_Discovery_Rate__FDR__Correction = "NO_FDR",
    number_of_neighbors = None,
)
print(arcpy.GetMessages())

# Add feature class alias for the hot spots (proximity to major roads, 500ft) feature class
# Define the crashes hot spots 500 feet from major roads layer alias and modify the feature class alias
crashes_hotspots_500ft_from_major_roads_alias = (
    "OCTraffic Crashes Hot Spots 500 Feet from Major Roads"
)
arcpy.AlterAliasName(
    crashes_hotspots_500ft_from_major_roads, crashes_hotspots_500ft_from_major_roads_alias
)
print(arcpy.GetMessages())
# Obtain the fields for the hot spots (proximity to major roads, 500ft) feature class
crashes_hotspots_500ft_from_major_roads_fields = [
    f.name for f in arcpy.ListFields(crashes_hotspots_500ft_from_major_roads)
]  # crashes_hotspots_500ft_from_major_roads field list
# Field Aliases for the crashes hot spots 500 feet from major roads feature class
for f in arcpy.ListFields(crashes_hotspots_500ft_from_major_roads):
    print(f"{f.name} ({f.aliasName})")


### Find Hot Spots (Proximity to Major Roads, 500ft) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Find Hot Spots (Proximity to Major Roads, 500ft)")

# Find hot spots within 500 feet from major roads
# Create a path for the new hot spots within 500 ft from major roads feature class
crashes_find_hotspots_500ft_major_roads_500ft1mi = os.path.join(prj_dirs["agp_gdb_hotspots"], "crashes_find_hotspots_500ft_major_roads_500ft1mi")
arcpy.gapro.FindHotSpots(
    point_layer = crashes_500ft_from_major_roads,
    out_feature_class = crashes_find_hotspots_500ft_major_roads_500ft1mi,
    bin_size = "500 Feet",
    neighborhood_size = "1 Miles",
    time_step_interval = None,
    time_step_alignment = "START_TIME",
    time_step_reference = None,
)
print(arcpy.GetMessages())

# Add feature class alias for the hot spots (proximity to major roads, 500ft) feature class
# Define the crashes find hot spots 500 feet from major roads layer alias and modify the feature class alias
crashes_find_hotspots_500ft_major_roads_500ft1mi_alias = (
    "OCTraffic Crashes Find Hot Spots 500 Feet from Major Roads 500ft 1mi"
)
arcpy.AlterAliasName(
    crashes_find_hotspots_500ft_major_roads_500ft1mi,
    crashes_find_hotspots_500ft_major_roads_500ft1mi_alias,
)
print(arcpy.GetMessages())
# Obtain the fields for the hot spots (proximity to major roads, 500ft) feature class
crashes_find_hotspots_500ft_major_roads_500ft1mi_fields = [
    f.name for f in arcpy.ListFields(crashes_find_hotspots_500ft_major_roads_500ft1mi)
]  # crashes_find_hotspots_500ft_major_roads_500ft1mi field list
# Field Aliases for the crashes find hot spots 500 feet from major roads feature class
for f in arcpy.ListFields(crashes_find_hotspots_500ft_major_roads_500ft1mi):
    print(f"{f.name} ({f.aliasName})")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Geodatabase, Feature Dataset and Feature Class Metadata Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3. Geodatabase, Feature Dataset and Feature Class Metadata Processing")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.1. Project Geodatabase Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.1. Project Geodatabase Metadata")

# Define key metadata attributes for the AGPSWITRS geodatabase
mdo_gdb = md.Metadata()
mdo_gdb.title = "OCTraffic Historical Traffic Collisions"
mdo_gdb.tags = "Orange County, California, OCTraffic, Traffic, Traffic Conditions, Crashes, Collisions, Parties, Victims, Injuries, Fatalities, Hot Spots, Road Safety, Accidents, SWITRS, Transportation"
mdo_gdb.summary = f"Statewide Integrated Traffic Records System (SWITRS) Combined Collisions Data for Orange County, California ({md_years})"
mdo_gdb.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">combined reports on collision crashes, parties, and victims</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_gdb.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_gdb.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_gdb.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata object to the project geodatabase
md_gdb = md.Metadata(gdb_path)
if not md_gdb.isReadOnly:
    md_gdb.copy(mdo_gdb)
    md_gdb.save()
    print("Metadata updated for project geodatabase.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.2. Feature Dataset Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.2. Analysis Data Feature Dataset Metadata")


### Analysis Feature Dataset Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Analysis Feature Dataset Metadata")

# Create a new metadata object for the analysis feature dataset
mdo_analysis = md.Metadata()
mdo_analysis.title = "OCTraffic Traffic Collisions Analysis Dataset"
mdo_analysis.tags = "Orange County, California, OCTraffic, Traffic, Traffic Conditions, Crashes, Collisions, Parties, Victims, Injuries, Fatalities, Hot Spots, Road Safety, Accidents, SWITRS, Transportation"
mdo_analysis.summary = f"Statewide Integrated Traffic Records System (SWITRS) Combined Collisions Data for Orange County, California ({md_years})"
mdo_analysis.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">combined reports on collision crashes, parties, and victims</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_analysis.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_analysis.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_analysis.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata object to the analysis feature dataset
md_analysis = md.Metadata(prj_dirs.get("agp_gdb_analysis", ""))
if not md_analysis.isReadOnly:
    md_analysis.copy(mdo_analysis)
    md_analysis.save()
    print("Metadata updated for the analysis feature dataset.")


### HotSpots Feature Dataset Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- HotSpots Feature Dataset Metadata")

# Create a new metadata object for the hotspots feature dataset
mdo_hotspots = md.Metadata()
mdo_hotspots.title = "OCTraffic Traffic Collisions Hotspots Dataset"
mdo_hotspots.tags = "Orange County, California, OCTraffic, Traffic, Traffic Conditions, Crashes, Collisions, Parties, Victims, Injuries, Fatalities, Hot Spots, Road Safety, Accidents, SWITRS, Transportation"
mdo_hotspots.summary = f"Statewide Integrated Traffic Records System (SWITRS) Combined Collisions Data for Orange County, California ({md_years})"
mdo_hotspots.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">combined reports on collision crashes, parties, and victims</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_hotspots.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_hotspots.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_hotspots.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata object to the hotspots feature dataset
md_hotspots = md.Metadata(prj_dirs.get("agp_gdb_hotspots", ""))
if not md_hotspots.isReadOnly:
    md_hotspots.copy(mdo_hotspots)
    md_hotspots.save()
    print("Metadata updated for the hotspots feature dataset.")


### Raw Feature Dataset Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Raw Feature Dataset Metadata")

# Create a new metadata object for the raw feature dataset
mdo_raw = md.Metadata()
mdo_raw.title = "OCTraffic Traffic Collisions Raw Data Dataset"
mdo_raw.tags = "Orange County, California, OCTraffic, Traffic, Traffic Conditions, Crashes, Collisions, Parties, Victims, Injuries, Fatalities, Hot Spots, Road Safety, Accidents, SWITRS, Transportation"
mdo_raw.summary = f"Statewide Integrated Traffic Records System (SWITRS) Combined Collisions Data for Orange County, California ({md_years})"
mdo_raw.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">combined reports on collision crashes, parties, and victims</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_raw.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_raw.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_raw.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata object to the raw data feature dataset
md_raw = md.Metadata(prj_dirs.get("agp_gdb_raw", ""))
if not md_raw.isReadOnly:
    md_raw.copy(mdo_raw)
    md_raw.save()
    print("Metadata updated for the raw data feature dataset.")


### Supporting Feature Dataset Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Supporting Feature Dataset Metadata")

# Create a new metadata object for the supporting feature dataset
mdo_supporting = md.Metadata()
mdo_supporting.title = "OCTraffic Traffic Collisions Supporting Data Dataset"
mdo_supporting.tags = "Orange County, California, OCTraffic, Traffic, Traffic Conditions, Crashes, Collisions, Parties, Victims, Injuries, Fatalities, Hot Spots, Road Safety, Accidents, SWITRS, Transportation"
mdo_supporting.summary = f"Statewide Integrated Traffic Records System (SWITRS) Combined Collisions Data for Orange County, California ({md_years})"
mdo_supporting.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">combined reports on collision crashes, parties, and victims</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_supporting.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_supporting.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_supporting.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata object to the supporting data feature dataset
md_supporting = md.Metadata(prj_dirs.get("agp_gdb_supporting", ""))
if not md_supporting.isReadOnly:
    md_supporting.copy(mdo_supporting)
    md_supporting.save()
    print("Metadata updated for the supporting data feature dataset.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.3. Feature Class Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.3. Feature Class Metadata")


### Collisions Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Collisions Metadata")

# Define key metadata attributes for the Collisions feature class
mdo_collisions = md.Metadata()
mdo_collisions.title = "OCTraffic Combined Collisions Points"
mdo_collisions.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_collisions.summary = f"Statewide Integrated Traffic Records System (SWITRS) Combined Collisions Data for Orange County, California ({md_years})"
mdo_collisions.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">combined reports on collision crashes, parties, and victims</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_collisions.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_collisions.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_collisions.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata object to the collisions feature class
md_collisions = md.Metadata(collisions)
if not md_collisions.isReadOnly:
    md_collisions.copy(mdo_collisions)
    md_collisions.save()
    print(f"Metadata updated for {collisions_alias} feature class.")


### Crashes Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Crashes Metadata")

# Define key metadata attributes for the Crashes feature class
mdo_crashes = md.Metadata()
mdo_crashes.title = "OCTraffic Crashes Points"
mdo_crashes.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_crashes.summary = f"Statewide Integrated Traffic Records System (SWITRS) Crash Data for Orange County, California ({md_years})"
mdo_crashes.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">reports on crashes</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_crashes.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_crashes.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_crashes.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata object to the crashes feature class
md_crashes = md.Metadata(crashes)
if not md_crashes.isReadOnly:
    md_crashes.copy(mdo_crashes)
    md_crashes.save()
    print(f"Metadata updated for {crashes_alias} feature class.")


### Parties Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Parties Metadata")

# Define key metadata attributes for the Parties feature class
mdo_parties = md.Metadata()
mdo_parties.title = "OCTraffic Parties Points"
mdo_parties.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Parties, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_parties.summary = f"Statewide Integrated Traffic Records System (SWITRS) Incident-Involved Parties Data for Orange County, California ({md_years})"
mdo_parties.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">reports on parties involved in crash incidents</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_parties.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_parties.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_parties.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/1e07bb1002f9457fa6fd3540fdb08e29/data"

# Apply the metadata object to the parties feature class
md_parties = md.Metadata(parties)
if not md_parties.isReadOnly:
    md_parties.copy(mdo_parties)
    md_parties.save()
    print(f"Metadata updated for {parties_alias} feature class.")


### Victims Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Victims Metadata")

# Define key metadata attributes for the Victims feature class
mdo_victims = md.Metadata()
mdo_victims.title = "OCTraffic Victims Points"
mdo_victims.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Victims, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_victims.summary = f"Statewide Integrated Traffic Records System (SWITRS) Incident-Involved Victims Data for Orange County, California ({md_years})"
mdo_victims.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">reports on victims/persons involved in crash incidents</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_victims.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_victims.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_victims.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/78682395df4744009c58625f1db0c25b/data"

# Apply the metadata object to the victims feature class
md_victims = md.Metadata(victims)
if not md_victims.isReadOnly:
    md_victims.copy(mdo_victims)
    md_victims.save()
    print(f"Metadata updated for {victims_alias} feature class.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.4. Supporting Features Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.10. Roads Metadata")


### Roads Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Roads Metadata")

# Define key metadata attributes for the Roads feature class
mdo_roads = md.Metadata()
mdo_roads.title = "OCTraffic Roads Network"
mdo_roads.tags = "Orange County, California, Roads, Traffic, Road Safety, Transportation, Collisions, Crashes, SWITRS, OCTraffic"
mdo_roads.summary = "All roads for Orange County, California (Primary roads and highways, secondary roads, and local roads)"
mdo_roads.description = """<div style = "text-align:Left;"><div><div><p><span>The Orange County Roads Network is a comprehensive representation of all roads in the area, including primary roads and highways, secondary roads, and local roads. The data are sourced from the Orange County Department of Public Works and are updated regularly to reflect the most current road network configuration.</span></p></div></div></div>"""
mdo_roads.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_roads.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_roads.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/76f6fbe9acbb482c9684307854d6352b/data"

# Apply the metadata object to the roads feature class
md_roads = md.Metadata(roads)
if not md_roads.isReadOnly:
    md_roads.copy(mdo_roads)
    md_roads.save()
    print(f"Metadata updated for {roads_alias} feature class.")


### Census Blocks Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Census Blocks Metadata")

# Define key metadata attributes for the US Census 2020 Blocks feature class
mdo_blocks = md.Metadata()
mdo_blocks.title = "OCTraffic US Census 2020 Blocks"
mdo_blocks.tags = "Orange County, California, US Census 2020, Blocks, Census, Demographics, Population"
mdo_blocks.summary = "US Census 2020 Blocks for Orange County, California"
mdo_blocks.description = """<div style = "text-align:Left;"><div><div><p><span>The US Census 2020 Blocks feature class provides a comprehensive representation of the 2020 Census Blocks for Orange County, California. The data are sourced from the US Census Bureau and are updated regularly to reflect the most current demographic and population data.</span></p></div></div></div>"""
mdo_blocks.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_blocks.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_blocks.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/e2c4cd39783a4d1bb0925ead15a23cdc/data"

# Apply the metadata object to the US Census 2020 Blocks feature class
md_blocks = md.Metadata(blocks)
if not md_blocks.isReadOnly:
    md_blocks.copy(mdo_blocks)
    md_blocks.save()
    print(f"Metadata updated for {blocks_alias} feature class.")


### Cities Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Cities Metadata")

# Define key metadata attributes for the Cities feature class
mdo_cities = md.Metadata()
mdo_cities.title = "OCTraffic Cities Boundaries"
mdo_cities.tags = "Orange County, California, Cities, Traffic, Road Safety, Transportation, Collisions, Crashes, SWITRS, OCTraffic"
mdo_cities.summary = "Orange County City and Unincorporated Areas Land Boundaries, enriched with geodemographic characteristics"
mdo_cities.description = """<div style = "text-align:Left;"><div><div><p><span>The Orange County City and Unincorporated Areas Land Boundaries are enriched with a comprehensive set of geodemographic characteristics from OC ACS 2021 data. These characteristics span across demographic, housing, economic, and social aspects, providing a holistic view of the area. </span></p><p><span>The geodemographic data originate from the US Census American Community Survey (ACS) 2021, a 5-year estimate of the key Characteristics of Cities' geographic level in Orange County, California. The data contains:</span></p><ul><li><span>Total population and housing counts for each area;</span></li><li><span>Population and housing density measurements (per square mile);</span></li><li><span>Race counts for Asian, Black or African American, Hispanic and White groups;</span></li><li><span>Aggregate values for the number of vehicles commuting and travel time to work;</span></li></ul></div></div></div>"""
mdo_cities.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_cities.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_cities.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/ffe4a73307a245eda7dc7eaffe1db6d2/data"

# Apply the metadata object to the Cities feature class
md_cities = md.Metadata(cities)
if not md_cities.isReadOnly:
    md_cities.copy(mdo_cities)
    md_cities.save()
    print(f"Metadata updated for {cities_alias} feature class.")


### Boundaries Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Boundaries Metadata")

# Define key metadata attributes for the Boundaries feature class
mdo_boundaries = md.Metadata()
mdo_boundaries.title = "OC Land Boundaries"
mdo_boundaries.tags = "Orange County, California, Boundaries, Traffic, Road Safety, Transportation, Collisions, Crashes, SWITRS, OCTraffic"
mdo_boundaries.summary = (
    "Land boundaries for Orange County, cities, and unincorporated areas"
)
mdo_boundaries.description = """<div style = "text-align:Left;"><div><div><p><span>Land boundaries for Orange County, cities, and unincorporated areas (based on the five supervisorial districts). Contains additional geodemographic data on population and housing from the US Census 2021 American Community Survey (ACS).</span></p></div></div></div>"""
mdo_boundaries.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_boundaries.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_boundaries.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/4041c4b1f4234218a4ce654e5d22f176/data"

# Apply the metadata object to the Boundaries feature class
md_boundaries = md.Metadata(boundaries)
if not md_boundaries.isReadOnly:
    md_boundaries.copy(mdo_boundaries)
    md_boundaries.save()
    print(f"Metadata updated for {boundaries_alias} feature class.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.5. Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.5. Save Project")
# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of Script ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\nLast Execution:", dt.datetime.now().strftime("%Y-%m-%d"))
print("\nEnd of Script")
# Last Executed: 2025-12-31
