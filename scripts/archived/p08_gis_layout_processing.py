# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCSWITRS Data Processing
# Title: Part 8 - GIS Layout Processing
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.2, Date: September 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOC SWITRS GIS Data Processing - Part 8 - GIS Layout Processing\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1. Preliminaries")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Referencing Libraries and Initialization ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.1. Referencing Libraries and Initialization")

# Import Python libraries
import os, sys, time, math
import datetime as dt
import json, pickle, pprint, textwrap
from dateutil.parser import parse
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
from scipy import stats
from scipy.stats import gaussian_kde
from scipy.interpolate import make_interp_spline
import statsmodels.api as sm
from statsmodels.nonparametric.smoothers_lowess import lowess
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MultipleLocator
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle, Patch
import seaborn as sns
import pytz
from dotenv import load_dotenv
import arcpy, arcgis
from arcpy import metadata as md

# important as it "enhances" Pandas by importing these classes (from ArcGIS API for Python)
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import ocswitrs as ocs
import codebook.cbl as cbl


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2. Project and Workspace Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.2. Project and Workspace Variables")


### Project and Geodatabase Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Project and Geodatabase Paths")

# Create a dictionary with the project metadata
print("\nCreating project metadata")
prj_meta = ocs.project_metadata(part = 8, version = 2025.2, silent = False)

# Create a dictionary with the project directories
print("\nCreating project directories")
prj_dirs = ocs.project_directories(base_path = os.getcwd(), silent = False)

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
aprx = arcpy.mp.ArcGISProject(aprx_path)
# Close all map views
aprx.closeViews()
# Current ArcGIS workspace (arcpy)
arcpy.env.workspace = gdb_path
workspace = arcpy.env.workspace
# Enable overwriting existing outputs
arcpy.env.overwriteOutput = True
# Disable adding outputs to map
arcpy.env.addOutputsToMap = False


### Data Folder Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Data Folder Paths")

# The most current raw data files cover the periods from 01/01/2013 to 09/30/2024. The data files are already processed in the R s_cripts and imported into the project's geodatabase.
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
md_dates = (f"Data from {date_start.strftime('%B %d, %Y')} to {date_end.strftime('%B %d, %Y')}")

# Load the graphics_list pickle data file
print("- Loading the graphics_list pickle data file")
graphics_list = pd.read_pickle(os.path.join(prj_dirs["data_python"], "graphics_list.pkl"))


### Codebook ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Codebook")

# Load the JSON file from directory and store it in a variable
print("- Loading the codebook JSON file")
cb_path = os.path.join(prj_dirs["codebook"], "cb.json")
with open(cb_path, encoding = "utf-8") as json_file:
    cb = json.load(json_file)

# create a data frame from the codebook
print("- Creating a data frame from the codebook")
df_cb = pd.DataFrame(cb).transpose()
# Add attributes to the codebook data frame
df_cb.attrs["name"] = "Codebook"
print(df_cb.head())


### JSON CIM Exports ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- JSON CIM Exports")

# Creating a function to export the CIM JSON files to disk.
def export_cim(cim_type, cim_object, cim_name):
    """Export a CIM object to a file in both native (MAPX, PAGX, LYRX) and JSON CIM formats."""
    match cim_type:
        # When the CIM object is a map
        case "map":
            # Export the CIM object to a MAPX file
            print(f"Exporting {cim_name} map to MAPX...")
            cim_object.exportToMAPX(os.path.join(prj_dirs.get("maps"), cim_name + ".mapx"))
            print(arcpy.GetMessages())
            # Export the CIM object to a JSON file
            print(f"Exporting {cim_name} map to JSON...\n")
            with open(os.path.join(prj_dirs.get("maps"), cim_name + ".mapx"), "r", encoding = "utf-8") as f:
                data = f.read()
            with open(os.path.join(prj_dirs.get("maps"), cim_name + ".json"), "w", encoding = "utf-8") as f:
                f.write(data)
        # When the CIM object is a layout
        case "layout":
            # Export the CIM object to a PAGX file
            print(f"Exporting {cim_name} layout to PAGX...")
            cim_object.exportToPAGX(os.path.join(prj_dirs.get("layouts"), cim_name + ".pagx"))
            print(arcpy.GetMessages())
            # Export the CIM object to a JSON file
            print(f"Exporting {cim_name} layout to JSON...\n")
            with open(os.path.join(prj_dirs.get("layouts"), cim_name + ".pagx"), "r", encoding = "utf-8") as f:
                data = f.read()
            with open(os.path.join(prj_dirs.get("layouts"), cim_name + ".json"), "w", encoding = "utf-8") as f:
                f.write(data)
        # When the CIM object is a layer
        case "layer":
            # Export the CIM object to a LYRX file
            print(f"Exporting {cim_name} layer to LYRX...")
            # Reformat the name of the output file
            cim_new_name = "default_layer_name"  # Initialize cim_new_name with a default value
            for m in aprx.listMaps():
                for l in m.listLayers():
                    if l == cim_object:
                        cim_new_name = (
                            m.name.title() + "Map-" + l.name.replace("OCSWITRS ", "")
                        )
            # Save the layer to a LYRX file
            arcpy.management.SaveToLayerFile(
                cim_object, os.path.join(prj_dirs.get("layers"), cim_new_name + ".lyrx")
            )
            print(arcpy.GetMessages())
            # Export the CIM object to a JSON file
            print(f"Exporting {cim_name} layer to JSON...\n")
            with open(os.path.join(prj_dirs.get("layers"), cim_new_name + ".lyrx"), "r", encoding = "utf-8") as f:
                data = f.read()
            with open(os.path.join(prj_dirs.get("layers"), cim_new_name + ".json"), "w", encoding = "utf-8") as f:
                f.write(data)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.3. ArcGIS Pro Workspace ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.3. ArcGIS Pro Workspace")

# Set the workspace and environment to the root of the project geodatabase
arcpy.env.workspace = gdb_path
workspace = arcpy.env.workspace

# Enable overwriting existing outputs
arcpy.env.overwriteOutput = True

# Disable adding outputs to map
arcpy.env.addOutputsToMap = False


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.4. Map and Layout Lists ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.4. Map and Layout Lists")

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
    "fhs_100m1km",
    "fhs_150m2km",
    "fhs_100m5km",
    "fhs_roads_500ft",
    "ohs_roads_500ft",
    "road_crashes",
    "road_hotspots",
    "road_buffers",
    "road_segments",
    "roads",
    "point_fhs",
    "point_ohs",
    "pop_dens",
    "hou_dens",
    "area_cities",
    "area_blocks",
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
## 1.5. Feature Class Definitions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.5. Feature Class Definitions")


### Raw Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Raw Data Feature Classes")

# Paths to raw data feature classes
victims = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "victims")
parties = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "parties")
crashes = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "crashes")
collisions = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "collisions")


### Processed Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Supporting Data Feature Classes")

# Define paths for the feature classes in the supporting data feature dateset of the geodatabase
boundaries = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "boundaries")
cities = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "cities")
blocks = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "blocks")
roads = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "roads")


### Analysis Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Analysis Data Feature Classes")

# Define paths for the feature classes in the analysis data feature dateset of the geodatabase
roads_major = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major")
roads_major_buffers = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_buffers")
roads_major_buffers_sum = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_buffers_sum")
roads_major_points_along_lines = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_points_along_lines")
roads_major_split = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_split")
roads_major_split_buffer = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_split_buffer")
roads_major_split_buffer_sum = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "roads_major_split_buffer_sum")
blocks_sum = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "blocks_sum")
cities_sum = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "cities_sum")
crashes_500ft_from_major_roads = os.path.join(prj_dirs.get("agp_gdb_analysis", ""), "crashes_500ft_from_major_roads")


### Hot Spot Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hot Spot Data Feature Classes")

# Define paths for the feature classes in the hot spot data feature dateset of the geodatabase
crashes_hotspots = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_hotspots")
crashes_optimized_hotspots = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_optimized_hotspots")
crashes_find_hotspots_100m1km = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_find_hotspots_100m1km")
crashes_find_hotspots_150m2km = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_find_hotspots_150m2km")
crashesFindHotspots_100m5km = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashesFindHotspots_100m5km")
crashes_hotspots_500ft_from_major_roads = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_hotspots_500ft_from_major_roads")
crashes_find_hotspots_500ft_major_roads_500ft1mi = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_find_hotspots_500ft_major_roads_500ft1mi")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.6. Map Layer Definitions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.6. Map Layer Definitions")


### Project Maps ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Project Maps")

# Define the maps of the project

# OCSWITRS Data Maps
map_collisions = aprx.listMaps("collisions")[0]
map_crashes = aprx.listMaps("crashes")[0]
map_parties = aprx.listMaps("parties")[0]
map_victims = aprx.listMaps("victims")[0]
map_injuries = aprx.listMaps("injuries")[0]
map_fatalities = aprx.listMaps("fatalities")[0]
map_road_crashes = aprx.listMaps("road_crashes")[0]

# OCSWITRS Hotspot Maps
map_fhs_100m1km = aprx.listMaps("fhs_100m1km")[0]
map_fhs_150m2km = aprx.listMaps("fhs_150m2km")[0]
map_fhs_100m5km = aprx.listMaps("fhs_100m5km")[0]
map_fhs_roads_500ft = aprx.listMaps("fhs_roads_500ft")[0]
map_ohs_roads_500ft = aprx.listMaps("ohs_roads_500ft")[0]
map_road_hotspots = aprx.listMaps("road_hotspots")[0]
map_point_fhs = aprx.listMaps("point_fhs")[0]
map_point_ohs = aprx.listMaps("point_ohs")[0]

# OCSWITRS Supporting Data Maps
map_road_buffers = aprx.listMaps("road_buffers")[0]
map_road_segments = aprx.listMaps("road_segments")[0]
map_roads = aprx.listMaps("roads")[0]
map_pop_dens = aprx.listMaps("pop_dens")[0]
map_hou_dens = aprx.listMaps("hou_dens")[0]
map_area_cities = aprx.listMaps("area_cities")[0]
map_area_blocks = aprx.listMaps("area_blocks")[0]

# OCSWITRS Analysis and Processing Maps
map_summaries = aprx.listMaps("summaries")[0]
map_analysis = aprx.listMaps("analysis")[0]
map_regression = aprx.listMaps("regression")[0]


### Collisions Map 1 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Collisions Map 1 Layers")

# Define the layers for the collisions map

# Define map layers
map_collisions_lyr_boundaries = map_collisions.listLayers("OCSWITRS Boundaries")[0]
map_collisions_lyr_cities = map_collisions.listLayers("OCSWITRS Cities")[0]
map_collisions_lyr_blocks = map_collisions.listLayers("OCSWITRS Census Blocks")[0]
map_collisions_lyr_roads = map_collisions.listLayers("OCSWITRS Roads")[0]
map_collisions_lyr_collisions = map_collisions.listLayers("OCSWITRS Collisions")[0]

# List layers in map
print("Collisions Map Layers:")
for l in map_collisions.listLayers():
    print(f"- {l.name}")

# Count Collisions
count_collisions = int(arcpy.management.GetCount(collisions)[0])
print(f"Count of Collisions: {count_collisions:,}")


### Crashes Map 2 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Crashes Map 2 Layers")

# Define the layers for the crashes map

# Define map layers
map_crashes_lyr_boundaries = map_crashes.listLayers("OCSWITRS Boundaries")[0]
map_crashes_lyr_cities = map_crashes.listLayers("OCSWITRS Cities")[0]
map_crashes_lyr_blocks = map_crashes.listLayers("OCSWITRS Census Blocks")[0]
map_crashes_lyr_roads = map_crashes.listLayers("OCSWITRS Roads")[0]
map_crashes_lyr_crashes = map_crashes.listLayers("OCSWITRS Crashes")[0]

# List layers in map
print("Crashes Map Layers:")
for l in map_crashes.listLayers():
    print(f"- {l.name}")
    
# Count Crashes
count_crashes = int(arcpy.management.GetCount(crashes)[0])
print(f"Count of Crashes: {count_crashes:,}")


### Parties Map 3 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Parties Map 3 Layers")

# Define map layers
map_parties_lyr_boundaries = map_parties.listLayers("OCSWITRS Boundaries")[0]
map_parties_lyr_cities = map_parties.listLayers("OCSWITRS Cities")[0]
map_parties_lyr_blocks = map_parties.listLayers("OCSWITRS Census Blocks")[0]
map_parties_lyr_roads = map_parties.listLayers("OCSWITRS Roads")[0]
map_parties_lyr_parties = map_parties.listLayers("OCSWITRS Parties")[0]

# List layers in map
print("Parties Map Layers:")
for l in map_parties.listLayers():
    print(f"- {l.name}")
    
# Count Parties
count_parties = int(arcpy.management.GetCount(parties)[0])
print(f"Count of Parties: {count_parties:,}")


### Victims Map 4 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Victims Map 4 Layers")

# Define map layers
map_victims_lyr_boundaries = map_victims.listLayers("OCSWITRS Boundaries")[0]
map_victims_lyr_cities = map_victims.listLayers("OCSWITRS Cities")[0]
map_victims_lyr_blocks = map_victims.listLayers("OCSWITRS Census Blocks")[0]
map_victims_lyr_roads = map_victims.listLayers("OCSWITRS Roads")[0]
map_victims_lyr_victims = map_victims.listLayers("OCSWITRS Victims")[0]

# List layers in map
print("Victims Map Layers:")
for l in map_victims.listLayers():
    print(f"- {l.name}")

# Count Victims
countVictims = int(arcpy.management.GetCount(victims)[0])
print(f"Count of Victims: {countVictims:,}")


### Injuries Map 5 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Injuries Map 5 Layers")

# Define map layers
map_injuries_lyr_boundaries = map_injuries.listLayers("OCSWITRS Boundaries")[0]
map_injuries_lyr_cities = map_injuries.listLayers("OCSWITRS Cities")[0]
map_injuries_lyr_blocks = map_injuries.listLayers("OCSWITRS Census Blocks")[0]
map_injuries_lyr_victims = map_injuries.listLayers("OCSWITRS Victims")[0]

# List layers in map
print("Injuries Map Layers:")
for l in map_injuries.listLayers():
    print(f"- {l.name}")


### Fatalities Map 6 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Fatalities Map 6 Layers")

# Define map layers
map_fatalities_lyr_boundaries = map_fatalities.listLayers("OCSWITRS Boundaries")[0]
map_fatalities_lyr_roads = map_fatalities.listLayers("OCSWITRS Roads")[0]
map_fatalities_lyr_roads_major_buffers = map_fatalities.listLayers(
    "OCSWITRS Major Roads Buffers"
)[0]
map_fatalities_lyr_fatalities = map_fatalities.listLayers("OCSWITRS Crashes")[0]

# List layers in map
print("Fatalities Map Layers:")
for l in map_fatalities.listLayers():
    print(f"- {l.name}")


### Hot Spots (100m, 1km) Map 7 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hot Spots (100m, 1km) Map 7 Layers")

# Define map layers
map_fhs_100m1km_lyr_boundaries = map_fhs_100m1km.listLayers("OCSWITRS Boundaries")[0]
map_fhs_100m1km_lyr_cities = map_fhs_100m1km.listLayers("OCSWITRS Cities")[0]
map_fhs_100m1km_lyr_blocks = map_fhs_100m1km.listLayers("OCSWITRS Census Blocks")[0]
map_fhs_100m1km_lyr_roads = map_fhs_100m1km.listLayers("OCSWITRS Roads")[0]
map_fhs_100m1km_lyr_fhs_100m1km = map_fhs_100m1km.listLayers(
    "OCSWITRS Crashes Find Hot Spots 100m 1km"
)[0]

# List layers in map
print("Find Hotspots 100m 1km Map Layers:")
for l in map_fhs_100m1km.listLayers():
    print(f"- {l.name}")


### Hot Spots (150m, 2km) Map 8 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hot Spots (150m, 2km) Map 8 Layers")

# Define map layers
map_fhs_150m2km_lyr_boundaries = map_fhs_150m2km.listLayers("OCSWITRS Boundaries")[0]
map_fhs_150m2km_lyr_cities = map_fhs_150m2km.listLayers("OCSWITRS Cities")[0]
map_fhs_150m2km_lyr_blocks = map_fhs_150m2km.listLayers("OCSWITRS Census Blocks")[0]
map_fhs_150m2km_lyr_roads = map_fhs_150m2km.listLayers("OCSWITRS Roads")[0]
map_fhs_150m2km_lyr_fhs_150m2km = map_fhs_150m2km.listLayers(
    "OCSWITRS Crashes Find Hot Spots 150m 2km"
)[0]

# List layers in map
print("Find Hotspots 150m 2km Map Layers:")
for l in map_fhs_150m2km.listLayers():
    print(f"- {l.name}")


### Hot Spots (100m, 5km) Map 9 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hot Spots (100m, 5km) Map 9 Layers")

# Define map layers
map_fhs_100m5km_lyr_boundaries = map_fhs_100m5km.listLayers("OCSWITRS Boundaries")[0]
map_fhs_100m5km_lyr_cities = map_fhs_100m5km.listLayers("OCSWITRS Cities")[0]
map_fhs_100m5km_lyr_blocks = map_fhs_100m5km.listLayers("OCSWITRS Census Blocks")[0]
map_fhs_100m5km_lyr_roads = map_fhs_100m5km.listLayers("OCSWITRS Roads")[0]
map_fhs_100m5km_lyr_fhs_100m5km = map_fhs_100m5km.listLayers(
    "OCSWITRS Crashes Find Hot Spots 100m 5km"
)[0]

# List layers in map
print("Find Hotspots 100m 5km Map Layers:")
for l in map_fhs_100m5km.listLayers():
    print(f"- {l.name}")


### Hot Spots 500ft from Major Roads Map 10 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hot Spots 500ft from Major Roads Map 10 Layers")

# Define map layers
map_fhs_roads_500ft_lyr_boundaries = map_fhs_roads_500ft.listLayers("OCSWITRS Boundaries")[0]
map_fhs_roads_500ft_lyr_cities = map_fhs_roads_500ft.listLayers("OCSWITRS Cities")[0]
map_fhs_roads_500ft_lyr_blocks = map_fhs_roads_500ft.listLayers("OCSWITRS Census Blocks")[0]
map_fhs_roads_500ft_lyr_roads = map_fhs_roads_500ft.listLayers("OCSWITRS Roads")[0]
map_fhs_roads_500ft_lyr_fhs_roads_500ft = map_fhs_roads_500ft.listLayers(
    "OCSWITRS Crashes Find Hot Spots 500 Feet from Major Roads 500ft 1mi"
)[0]

# List layers in map
print("Find Hotspots 500 Feet from Major Roads Map Layers:")
for l in map_fhs_roads_500ft.listLayers():
    print(f"- {l.name}")


### Optimized Hot Spots 500ft from Major Roads Map 11 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Optimized Hot Spots 500ft from Major Roads Map 11 Layers")

# Define map layers
map_ohs_roads_500ft_lyr_boundaries = map_ohs_roads_500ft.listLayers("OCSWITRS Boundaries")[0]
map_ohs_roads_500ft_lyr_cities = map_ohs_roads_500ft.listLayers("OCSWITRS Cities")[0]
map_ohs_roads_500ft_lyr_blocks = map_ohs_roads_500ft.listLayers("OCSWITRS Census Blocks")[0]
map_ohs_roads_500ft_lyr_roads = map_ohs_roads_500ft.listLayers("OCSWITRS Roads")[0]
map_ohs_roads_500ft_lyr_ohs_roads_500ft = map_ohs_roads_500ft.listLayers(
    "OCSWITRS Crashes Find Hot Spots 500 Feet from Major Roads 500ft 1mi"
)[0]

# List layers in map
print("Optimized Hotspots 500 Feet from Major Roads Map Layers:")
for l in map_ohs_roads_500ft.listLayers():
    print(f"- {l.name}")


### Major Road Crashes Map 12 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Major Road Crashes Map 12 Layers")

# Define map layers
map_road_crashes_lyr_boundaries = map_road_crashes.listLayers("OCSWITRS Boundaries")[0]
map_road_crashes_lyr_blocks = map_road_crashes.listLayers("OCSWITRS Census Blocks")[0]
map_road_crashes_lyr_roads_major = map_road_crashes.listLayers("OCSWITRS Major Roads")[0]
map_road_crashes_lyr_crashes_500ft_roads = map_road_crashes.listLayers(
    "OCSWITRS Crashes 500 Feet from Major Roads"
)[0]

# List layers in map
print("Major Road Crasjes Map Layers:")
for l in map_road_crashes.listLayers():
    print(f"- {l.name}")


### Major Road Hotspots Map 13 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Major Road Hotspots Map 13 Layers")

# Define map layers
map_road_hotspots_lyr_boundaries = map_road_hotspots.listLayers("OCSWITRS Boundaries")[0]
map_road_hotspots_lyr_blocks = map_road_hotspots.listLayers("OCSWITRS Census Blocks")[0]
map_road_hotspots_lyr_roads_major = map_road_hotspots.listLayers("OCSWITRS Major Roads")[0]
map_road_hotspots_lyr_crashes_hotspots = map_road_hotspots.listLayers(
    "OCSWITRS Crashes Hot Spots 500 Feet from Major Roads"
)

# List layers in map
print("Major Road Hotspots Map Layers:")
for l in map_road_hotspots.listLayers():
    print(f"- {l.name}")


### Major Road Buffers Map 14 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Major Road Buffers Map 14 Layers")

# Define map layers
map_road_buffers_lyr_boundaries = map_road_buffers.listLayers("OCSWITRS Boundaries")[0]
map_road_buffers_lyr_blocks = map_road_buffers.listLayers("OCSWITRS Census Blocks")[0]
map_road_buffers_lyr_roads_major = map_road_buffers.listLayers("OCSWITRS Major Roads")[0]
map_road_buffers_lyr_road_buffers = map_road_buffers.listLayers("OCSWITRS Major Roads Buffers Summary")[0]

# List layers in map
print("Major Road Buffers Map Layers:")
for l in map_road_buffers.listLayers():
    print(f"- {l.name}")


### Major Road Segments Map 15 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Major Road Segments Map 15 Layers")

# Define map layers
map_road_segments_lyr_boundaries = map_road_segments.listLayers("OCSWITRS Boundaries")[0]
map_road_segments_lyr_blocks = map_road_segments.listLayers("OCSWITRS Census Blocks")[0]
map_road_segments_lyr_roads_major = map_road_segments.listLayers("OCSWITRS Major Roads")[0]
map_road_segments_lyr_roads_major_split = map_road_segments.listLayers(
    "OCSWITRS Major Roads Split Buffer Summary"
)[0]

# List layers in map
print("Major Road Segments Map Layers:")
for l in map_road_segments.listLayers():
    print(f"- {l.name}")


### Major Road Segments Map 16 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Major Road Segments Map 16 Layers")

# Define map layers
map_roads_lyr_roads_major = map_roads.listLayers("OCSWITRS Major Roads")[0]
map_roads_lyr_roads_major_buffers = map_roads.listLayers("OCSWITRS Major Roads Buffers")[0]
map_roads_lyr_roads_major_buffers_sum = map_roads.listLayers(
    "OCSWITRS Major Roads Buffers Summary"
)[0]
map_roads_lyr_roads_major_points_along_lines = map_roads.listLayers(
    "OCSWITRS Major Roads Points Along Lines"
)[0]
map_roads_lyr_roads_major_split = map_roads.listLayers("OCSWITRS Major Roads Split")[0]
map_roads_lyr_roads_major_split_buffer = map_roads.listLayers(
    "OCSWITRS Major Roads Split Buffer"
)[0]
map_roads_lyr_roads_major_split_buffer_sum = map_roads.listLayers(
    "OCSWITRS Major Roads Split Buffer Summary"
)[0]

# List layers in map
print("Roads Map Layers:")
for l in map_roads.listLayers():
    print(f"- {l.name}")


### Population Density Map 17 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Population Density Map 17 Layers")

# Define map layers
map_point_fhs_lyr_boundaries = map_point_fhs.listLayers("OCSWITRS Boundaries")[0]
map_point_fhs_lyr_roads_major = map_point_fhs.listLayers("OCSWITRS Major Roads")[0]
map_point_fhs_lyr_fhs = map_point_fhs.listLayers("OCSWITRS Crashes Hot Spots")[0]

# List layers in map
print("Hotspot Points Map Layers:")
for l in map_point_fhs.listLayers():
    print(f"- {l.name}")


### Optimized Hotspot Points Map 18 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Optimized Hotspot Points Map 18 Layers")

# Define map layers
map_point_ohs_lyr_boundaries = map_point_ohs.listLayers("OCSWITRS Boundaries")[0]
map_point_ohs_lyr_roads_major = map_point_ohs.listLayers("OCSWITRS Major Roads")[0]
map_point_ohs_lyr_ohs = map_point_ohs.listLayers("OCSWITRS Crashes Optimized Hot Spots")[0]

# List layers in map
print("Optimized Hotspot Points Map Layers:")
for l in map_point_ohs.listLayers():
    print(f"- {l.name}")


### Population Density Map 19 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Population Density Map 19 Layers")

# Define map layers
map_pop_dens_lyr_boundaries = map_pop_dens.listLayers("OCSWITRS Boundaries")[0]
map_pop_dens_lyr_roads_major = map_pop_dens.listLayers("OCSWITRS Major Roads")[0]
map_pop_dens_lyr_pop_dens = map_pop_dens.listLayers("OCSWITRS Population Density")[0]

# List layers in map
print("Population Density Map Layers:")
for l in map_pop_dens.listLayers():
    print(f"- {l.name}")


### Housing Density Map 20 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Housing Density Map 20 Layers")

# Define map layers
map_hou_dens_lyr_boundaries = map_hou_dens.listLayers("OCSWITRS Boundaries")[0]
map_hou_dens_lyr_roads_major = map_hou_dens.listLayers("OCSWITRS Major Roads")[0]
map_hou_dens_lyr_hou_dens = map_hou_dens.listLayers("OCSWITRS Housing Density")[0]

# List layers in map
print("Housing Density Map Layers:")
for l in map_hou_dens.listLayers():
    print(f"- {l.name}")


### Victims by City Areas Map 21 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Victims by City Areas Map 21 Layers")

# Define map layers
map_area_cities_lyr_boundaries = map_area_cities.listLayers("OCSWITRS Boundaries")[0]
map_area_cities_lyr_roads_major = map_area_cities.listLayers("OCSWITRS Major Roads")[0]
map_area_cities_lyr_cities = map_area_cities.listLayers("OCSWITRS Cities Summary")[0]

# List layers in map
print("Victims by City Areas Map Layers:")
for l in map_area_cities.listLayers():
    print(f"- {l.name}")


### Victims by Census Blocks Map 22 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Victims by Census Blocks Map 22 Layers")

# Define map layers
map_area_blocks_lyr_boundaries = map_area_blocks.listLayers("OCSWITRS Boundaries")[0]
map_area_blocks_lyr_roads_major = map_area_blocks.listLayers("OCSWITRS Major Roads")[0]
map_area_blocks_lyr_blocks = map_area_blocks.listLayers("OCSWITRS Census Blocks Summary")[0]

# List layers in map
print("Victims by Census Block Areas Map Layers:")
for l in map_area_blocks.listLayers():
    print(f"- {l.name}")


### Summaries Map 23 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Summaries Map 23 Layers")

# Define map layers
map_summaries_lyr_blocksSum = map_summaries.listLayers("OCSWITRS Census Blocks Summary")[0]
map_summaries_lyr_citiesSum = map_summaries.listLayers("OCSWITRS Cities Summary")[0]
map_summaries_lyr_crashes_500ft_from_major_roads = map_summaries.listLayers(
    "OCSWITRS Crashes 500 Feet from Major Roads"
)[0]

# List layers in map
print("Summaries Map Layers:")
for l in map_summaries.listLayers():
    print(f"- {l.name}")


### Analysis Map 24 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Analysis Map 24 Layers")

# Define map layers
map_analysis_lyr_crashes_hotspots = map_analysis.listLayers("OCSWITRS Crashes Hot Spots")[0]
map_analysis_lyr_crashes_optimized_hotspots = map_analysis.listLayers(
    "OCSWITRS Crashes Optimized Hot Spots"
)[0]

# List layers in map
print("Analysis Map Layers:")
for l in map_analysis.listLayers():
    print(f"- {l.name}")


### Regression Map 25 Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Regression Map 25 Layers")

# Define map layers
map_regression_lyr_boundaries = map_regression.listLayers("OCSWITRS Boundaries")[0]
map_regression_lyr_cities = map_regression.listLayers("OCSWITRS Cities")[0]
map_regression_lyr_blocks = map_regression.listLayers("OCSWITRS Census Blocks")[0]
map_regression_lyr_roads = map_regression.listLayers("OCSWITRS Roads")[0]

# List layers in map
print("Regression Map Layers:")
for l in map_regression.listLayers():
    print(f"- {l.name}")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.7. Project and Map Extent ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.7. Project and Map Extent")

# Define and create the project and map extent

# Define the map extent coordinates
xmin = -13150753.258299999
ymin = 3942787.8856000006
xmax = -13069273.7991
ymax = 4029458.1212000027

# Get the spatial reference of the boundaries layer
ref = arcpy.Describe(boundaries).spatialReference

# Set the map extent
prj_extent = arcpy.Extent(xmin, ymin, xmax, ymax, spatial_reference = ref)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Project Layouts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2. Project Layouts")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1. Setup Map Layouts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.1. Setup Map Layouts")


### Layout Configuration ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout Configuration")

# Setting up layout configuration variables. Options are:
# - Single map frame: 6 x 4 inches (landscape)
# - Dual map frames: 12 x 4 inches (landscape) (two 6 x 4 inches frames)
# - Four map frames: 12 x 8 inches (landscape) (four 6 x 4 inches frames)

# Function to setup layout configuration
def layout_configuration(nmf):
    # Match the number of map frames in layout
    lyt_config = {}
    # Set the layout configuration based on the number of map frames
    match nmf:
        case 1:
            lyt_config = {
                "page_width": 11.0,
                "page_height": 8.5,
                "page_units": "INCH",
                "rows": 1,
                "cols": 1,
                "nmf": 1,
                "mf1": {
                    "coords": [(0.0, 8.5), (11.0, 8.5), (0.0, 0.0), (11.0, 0.0)],
                    "width": 11.0,
                    "height": 8.5,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.0,
                    "coordY": 0.0,
                    "geometry": arcpy.Point(0.0, 0.0),
                },
                "t1": {
                    "width": 1.9184,
                    "height": 0.3414,
                    "anchor": "TOP_LEFT_CORNER",
                    "coordX": 0.25,
                    "coordY": 8.25,
                    "geometry": arcpy.Point(0.25, 8.25),
                },
                "na": {
                    "width": 0.3606,
                    "height": 0.75,
                    "anchor": "BOTTOM_RIGHT_CORNER",
                    "coordX": 10.75,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(10.75, 0.25),
                },
                "sb": {
                    "width": 4.5,
                    "height": 0.5,
                    "anchor": "BOTTOM_MID_POINT",
                    "coordX": 5.5,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(5.5, 0.25),
                },
                "cr": {
                    "width": 0.0,
                    "height": 0.0,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.0,
                    "coordY": 0.0,
                    "geometry": arcpy.Point(0.0, 0.0),
                },
                "lg1": {
                    "width": 4.5,
                    "height": 2.0,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.25,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(0.25, 0.25),
                },
            }
        case 2:
            lyt_config = {
                "page_width": 22.0,
                "page_height": 8.5,
                "page_units": "INCH",
                "rows": 1,
                "cols": 2,
                "nmf": 2,
                "mf1": {
                    "coords": [(0.0, 8.5), (11.0, 8.5), (0.0, 0.0), (11.0, 0.0)],
                    "width": 11.0,
                    "height": 8.5,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.0,
                    "coordY": 0.0,
                    "geometry": arcpy.Point(0.0, 0.0),
                },
                "mf2": {
                    "coords": [(11.0, 8.5), (22.0, 8.5), (11.0, 0.0), (22.0, 0.0)],
                    "width": 11.0,
                    "height": 8.5,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 11.0,
                    "coordY": 0.0,
                    "geometry": arcpy.Point(11.0, 0.0),
                },
                "t1": {
                    "width": 1.9184,
                    "height": 0.3414,
                    "anchor": "TOP_LEFT_CORNER",
                    "coordX": 0.25,
                    "coordY": 8.25,
                    "geometry": arcpy.Point(0.25, 8.25),
                },
                "t2": {
                    "width": 1.9184,
                    "height": 0.3414,
                    "anchor": "TOP_LEFT_CORNER",
                    "coordX": 11.25,
                    "coordY": 8.25,
                    "geometry": arcpy.Point(11.25, 8.25),
                },
                "na": {
                    "width": 0.3606,
                    "height": 0.75,
                    "anchor": "BOTTOM_RIGHT_CORNER",
                    "coordX": 21.75,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(21.75, 0.25),
                },
                "sb": {
                    "width": 4.5,
                    "height": 0.5,
                    "anchor": "BOTTOM_MID_POINT",
                    "coordX": 16.5,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(16.5, 0.25),
                },
                "cr": {
                    "width": 0.0,
                    "height": 0.0,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.0,
                    "coordY": 0.0,
                    "geometry": arcpy.Point(0.0, 0.0),
                },
                "lg1": {
                    "width": 4.5,
                    "height": 2.0,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.25,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(0.25, 0.25),
                },
                "lg2": {
                    "width": 4.5,
                    "height": 2.0,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 11.25,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(11.25, 0.25),
                },
            }
        case 4:
            lyt_config = {
                "page_width": 22.0,
                "page_height": 17.0,
                "page_units": "INCH",
                "rows": 2,
                "cols": 2,
                "nmf": 4,
                "mf1": {
                    "coords": [(0.0, 17.0), (11.0, 17.0), (0.0, 8.5), (11.0, 8.5)],
                    "width": 11.0,
                    "height": 8.5,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.0,
                    "coordY": 8.5,
                    "geometry": arcpy.Point(0.0, 8.5),
                },
                "mf2": {
                    "coords": [(11.0, 17.0), (22.0, 17.0), (11.0, 8.5), (22.0, 8.5)],
                    "width": 11.0,
                    "height": 8.5,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 11.0,
                    "coordY": 8.5,
                    "geometry": arcpy.Point(11.0, 8.5),
                },
                "mf3": {
                    "coords": [(0.0, 8.5), (11.0, 8.5), (0.0, 0.0), (11.0, 0.0)],
                    "width": 11.0,
                    "height": 8.5,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.0,
                    "coordY": 0.0,
                    "geometry": arcpy.Point(0.0, 0.0),
                },
                "mf4": {
                    "coords": [(11.0, 8.5), (22.0, 8.5), (11.0, 0.0), (22.0, 0.0)],
                    "width": 11.0,
                    "height": 8.5,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 11.0,
                    "coordY": 0.0,
                    "geometry": arcpy.Point(11.0, 0.0),
                },
                "t1": {
                    "width": 1.9184,
                    "height": 0.3414,
                    "anchor": "TOP_LEFT_CORNER",
                    "coordX": 0.25,
                    "coordY": 16.75,
                    "geometry": arcpy.Point(0.25, 16.75),
                },
                "t2": {
                    "width": 1.9184,
                    "height": 0.3414,
                    "anchor": "TOP_LEFT_CORNER",
                    "coordX": 11.25,
                    "coordY": 16.75,
                    "geometry": arcpy.Point(11.25, 16.75),
                },
                "t3": {
                    "width": 1.9184,
                    "height": 0.3414,
                    "anchor": "TOP_LEFT_CORNER",
                    "coordX": 0.25,
                    "coordY": 8.25,
                    "geometry": arcpy.Point(0.25, 8.25),
                },
                "t4": {
                    "width": 1.9184,
                    "height": 0.3414,
                    "anchor": "TOP_LEFT_CORNER",
                    "coordX": 11.25,
                    "coordY": 8.25,
                    "geometry": arcpy.Point(11.25, 8.25),
                },
                "na": {
                    "width": 0.3606,
                    "height": 0.75,
                    "anchor": "BOTTOM_RIGHT_CORNER",
                    "coordX": 21.75,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(21.75, 0.25),
                },
                "sb": {
                    "width": 4.5,
                    "height": 0.5,
                    "anchor": "BOTTOM_MID_POINT",
                    "coordX": 16.75,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(16.75, 0.25),
                },
                "cr": {
                    "width": 0.5,
                    "height": 0.5,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.0,
                    "coordY": 0.0,
                    "geometry": arcpy.Point(0.0, 0.0),
                },
                "lg1": {
                    "width": 4.5,
                    "height": 2.0,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.25,
                    "coordY": 8.75,
                    "geometry": arcpy.Point(0.25, 8.75),
                },
                "lg2": {
                    "width": 4.5,
                    "height": 2.0,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 11.25,
                    "coordY": 8.75,
                    "geometry": arcpy.Point(11.25, 8.75),
                },
                "lg3": {
                    "width": 4.5,
                    "height": 2.0,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 0.25,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(0.25, 0.25),
                },
                "lg4": {
                    "width": 4.5,
                    "height": 2.0,
                    "anchor": "BOTTOM_LEFT_CORNER",
                    "coordX": 11.25,
                    "coordY": 0.25,
                    "geometry": arcpy.Point(11.25, 0.25),
                },
            }
    return lyt_config

# Apply the layout configuration to all layouts

# Define layout configurations for each map
maps_lyt_config = layout_configuration(4)
injuries_lyt_config = layout_configuration(2)
hotspots_lyt_config = layout_configuration(4)
roads_lyt_config = layout_configuration(4)
points_lyt_config = layout_configuration(2)
densities_lyt_config = layout_configuration(2)
areas_lyt_config = layout_configuration(2)

# Add the layout configurations to a new dictionary
lyt_dict = {
    "maps": maps_lyt_config,
    "injuries": injuries_lyt_config,
    "hotspots": hotspots_lyt_config,
    "roads": roads_lyt_config,
    "points": points_lyt_config,
    "densities": densities_lyt_config,
    "areas": areas_lyt_config,
}


### Remove Old Layouts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Layouts")

# Remove all old layouts from the ArcGIS Pro project
if aprx.listLayouts():
    for l in aprx.listLayouts():
        print(f"Deleting layout: {l.name}")
        aprx.deleteItem(l)
else:
    print("No layouts to delete.")


### Create New Layouts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Create New Layouts")

# Create new layouts in the ArcGIS Pro project

# for each of the layouts in the list, if it exists, delete it
for l in layout_list:
    for i in aprx.listLayouts():
        if i.name == l:
            print(f"Deleting layout: {l}")
            aprx.deleteItem(i)
    # Create new layouts
    print(f"Creating layout: {l}")
    aprx.createLayout(
        page_width = lyt_dict[l]["page_width"],
        page_height = lyt_dict[l]["page_height"],
        page_units = lyt_dict[l]["page_units"],
        name = l,
    )

# List all the newly created layouts
print("\nNewly created layouts:")
for l in aprx.listLayouts():
    print(f"- {l.name}")

# Store the layout objects in variables
maps_layout = aprx.listLayouts("maps")[0]  # maps layout
injuries_layout = aprx.listLayouts("injuries")[0]  # injuries layout
hotspots_layout = aprx.listLayouts("hotspots")[0]  # hotspots layout
roads_layout = aprx.listLayouts("roads")[0]  # road hotspots layout
points_layout = aprx.listLayouts("points")[0]  # point hotspots layout
densities_layout = aprx.listLayouts("densities")[0]  # densities layout
areas_layout = aprx.listLayouts("areas")[0]  # areas layout


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2. Layout Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.2. Layout Metadata")


### Maps Layout Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Maps Layout Metadata")

# Create a new metadata object for the maps layout and assign it to the layout
mdo_maps_layout = md.Metadata()
mdo_maps_layout.title = "OCSWITRS Maps Layout"
mdo_maps_layout.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCSWITRS, Transportation"
mdo_maps_layout.summary = "Layout for the OCSWITRS Project Maps"
mdo_maps_layout.description = """<div style = "text-align:Left;"><div><div><p><span>Layout for the OCSWITRS Project Maps</span></p></div></div></div>"""
mdo_maps_layout.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_maps_layout.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_maps_layout.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the layout
maps_layout.metadata = mdo_maps_layout


### Injuries Layout Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Injuries Layout Metadata")

# Create a new metadata object for the injuries layout and assign it to the layout
mdo_injuries_layout = md.Metadata()
mdo_injuries_layout.title = "OCSWITRS Injuries Layout"
mdo_injuries_layout.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCSWITRS, Transportation"
mdo_injuries_layout.summary = "Layout for the OCSWITRS Project Injuries"
mdo_injuries_layout.description = """<div style = "text-align:Left;"><div><div><p><span>Layout for the OCSWITRS Project Injuries</span></p></div></div></div>"""
mdo_injuries_layout.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_injuries_layout.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_injuries_layout.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the layout
injuries_layout.metadata = mdo_injuries_layout


### Hotspots Layout Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hotspots Layout Metadata")

# Create a new metadata object for the hotspots layout and assign it to the layout
mdo_hotspots_layout = md.Metadata()
mdo_hotspots_layout.title = "OCSWITRS Hot Spots Layout"
mdo_hotspots_layout.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCSWITRS, Transportation"
mdo_hotspots_layout.summary = "Layout for the OCSWITRS Project Hot Spots"
mdo_hotspots_layout.description = """<div style = "text-align:Left;"><div><div><p><span>Layout for the OCSWITRS Project Hot Spots</span></p></div></div></div>"""
mdo_hotspots_layout.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_hotspots_layout.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_hotspots_layout.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the layout
hotspots_layout.metadata = mdo_hotspots_layout


### Roads Layout Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Roads Layout Metadata")

# Create a new metadata object for the roads layout and assign it to the layout
mdo_roads_layout = md.Metadata()
mdo_roads_layout.title = "OCSWITRS Road Hot Spots Layout"
mdo_roads_layout.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCSWITRS, Transportation"
mdo_roads_layout.summary = "Layout for the OCSWITRS Project Roads Hot Spots"
mdo_roads_layout.description = """<div style = "text-align:Left;"><div><div><p><span>Layout for the OCSWITRS Project Hot Spots</span></p></div></div></div>"""
mdo_roads_layout.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_roads_layout.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_roads_layout.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the layout
roads_layout.metadata = mdo_roads_layout


### Points Layout Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Points Layout Metadata")

# Create a new metadata object for the points hotspots layout and assign it to the layout
mdo_points_layout = md.Metadata()
mdo_points_layout.title = "OCSWITRS Optimized Hot Spots Layout"
mdo_points_layout.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCSWITRS, Transportation"
mdo_points_layout.summary = "Layout for the OCSWITRS Project Optimized Hot Spots"
mdo_points_layout.description = """<div style = "text-align:Left;"><div><div><p><span>Layout for the OCSWITRS Project Optimized Hot Spots</span></p></div></div></div>"""
mdo_points_layout.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_points_layout.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_points_layout.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the layout
points_layout.metadata = mdo_points_layout


### Density Layout Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Density Layout Metadata")

# Create a new metadata object for the densities layout and assign it to the layout
mdo_densities_layout = md.Metadata()
mdo_densities_layout.title = "OCSWITRS Densities Layout"
mdo_densities_layout.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCSWITRS, Transportation"
mdo_densities_layout.summary = "Layout for the OCSWITRS Project Densities"
mdo_densities_layout.description = """<div style = "text-align:Left;"><div><div><p><span>Layout for the OCSWITRS Project Densities</span></p></div></div></div>"""
mdo_densities_layout.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_densities_layout.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_densities_layout.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the layout
densities_layout.metadata = mdo_densities_layout


### Areas Layout Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Areas Layout Metadata")

# Create a new metadata object for the areas layout and assign it to the layout
mdo_areas_layout = md.Metadata()
mdo_areas_layout.title = "OCSWITRS Areas Layout"
mdo_areas_layout.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCSWITRS, Transportation"
mdo_areas_layout.summary = "Layout for the OCSWITRS Project Areas"
mdo_areas_layout.description = """<div style = "text-align:Left;"><div><div><p><span>Layout for the OCSWITRS Project Areas</span></p></div></div></div>"""
mdo_areas_layout.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_areas_layout.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_areas_layout.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the layout
areas_layout.metadata = mdo_areas_layout


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Maps Layout Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3. Maps Layout Processing")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.1. Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.1. Layout View")


### Set Maps Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Set Maps Layout View")

# Close all views and open the map layout
# Close all previous views
aprx.closeViews()
# Open the maps layout view
maps_layout.openView()
# set the main layout as active view
layout = aprx.activeView


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.2. Add Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.2. Add Map Frames")


### Remove Old Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Map Frames")

# Delete all map frames from the layout
for el in maps_layout.listElements():
    if el.type == "MAPFRAME_ELEMENT":
        print(f"Deleting map frame: {el.name}")
        maps_layout.deleteElement(el)


### Map Frame Definitions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame Definitions")

# List of maps to be added as map frames to the layout
maps_mf_list = [map_collisions, map_crashes, map_parties, map_victims]

# Number of map frames
maps_mf_count = len(maps_mf_list)

# Number of rows and columns for the map frames
maps_mf_cols = 2
maps_mf_rows = math.ceil(maps_mf_count / maps_mf_cols)

# Map frame page dimensions
maps_mf_page_width = lyt_dict["maps"]["page_width"]
maps_mf_page_height = lyt_dict["maps"]["page_height"]

# Map frame names
maps_mf_names = [f"mf{i}" for i in range(1, maps_mf_count + 1)]


### Map Frame 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 1")

# Add collisions map frame (map frame 1) to the layout
maps_mf1 = maps_layout.createMapFrame(
    geometry = lyt_dict["maps"]["mf1"]["geometry"],
    map = maps_mf_list[0],
    name = "mf1",
)

# Set up map frame properties
maps_mf1.name = "mf1"
maps_mf1.setAnchor(lyt_dict["maps"]["mf1"]["anchor"])
maps_mf1.elementWidth = lyt_dict["maps"]["mf1"]["width"]
maps_mf1.elementHeight = lyt_dict["maps"]["mf1"]["height"]
maps_mf1.elementPositionX = lyt_dict["maps"]["mf1"]["coordX"]
maps_mf1.elementPositionY = lyt_dict["maps"]["mf1"]["coordY"]
maps_mf1.elementRotation = 0
maps_mf1.visible = True
maps_mf1.map = maps_mf_list[0]
maps_mf1_cim = maps_mf1.getDefinition("V3")
maps_mf1_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
maps_mf1.setDefinition(maps_mf1_cim)


### Map Frame 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 2")

# Add crashes map frame (map frame 2) to the layout
maps_mf2 = maps_layout.createMapFrame(
    geometry = lyt_dict["maps"]["mf2"]["geometry"],
    map = maps_mf_list[1],
    name = "mf2",
)

# Set up map frame properties
maps_mf2.name = "mf2"
maps_mf2.setAnchor(lyt_dict["maps"]["mf2"]["anchor"])
maps_mf2.elementWidth = lyt_dict["maps"]["mf2"]["width"]
maps_mf2.elementHeight = lyt_dict["maps"]["mf2"]["height"]
maps_mf2.elementPositionX = lyt_dict["maps"]["mf2"]["coordX"]
maps_mf2.elementPositionY = lyt_dict["maps"]["mf2"]["coordY"]
maps_mf2.elementRotation = 0
maps_mf2.visible = True
maps_mf2.map = maps_mf_list[1]
maps_mf2_cim = maps_mf2.getDefinition("V3")
maps_mf2_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
maps_mf2.setDefinition(maps_mf2_cim)


### Map Frame 3 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 3")

# Add parties map frame (map frame 3) to the layout
maps_mf3 = maps_layout.createMapFrame(
    geometry = lyt_dict["maps"]["mf3"]["geometry"],
    map = maps_mf_list[2],
    name = "mf3",
)

# Set up map frame properties
maps_mf3.name = "mf3"
maps_mf3.setAnchor(lyt_dict["maps"]["mf3"]["anchor"])
maps_mf3.elementWidth = lyt_dict["maps"]["mf3"]["width"]
maps_mf3.elementHeight = lyt_dict["maps"]["mf3"]["height"]
maps_mf3.elementPositionX = lyt_dict["maps"]["mf3"]["coordX"]
maps_mf3.elementPositionY = lyt_dict["maps"]["mf3"]["coordY"]
maps_mf3.elementRotation = 0
maps_mf3.visible = True
maps_mf3.map = maps_mf_list[2]
maps_mf3_cim = maps_mf3.getDefinition("V3")
maps_mf3_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
maps_mf3.setDefinition(maps_mf3_cim)


### Map Frame 4 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 4")

# Add victims map frame (map frame 4) to the layout
maps_mf4 = maps_layout.createMapFrame(
    geometry = lyt_dict["maps"]["mf4"]["geometry"],
    map = maps_mf_list[3],
    name = "mf4",
)

# Set up map frame properties
maps_mf4.name = "mf4"
maps_mf4.setAnchor(lyt_dict["maps"]["mf4"]["anchor"])
maps_mf4.elementWidth = lyt_dict["maps"]["mf4"]["width"]
maps_mf4.elementHeight = lyt_dict["maps"]["mf4"]["height"]
maps_mf4.elementPositionX = lyt_dict["maps"]["mf4"]["coordX"]
maps_mf4.elementPositionY = lyt_dict["maps"]["mf4"]["coordY"]
maps_mf4.elementRotation = 0
maps_mf4.visible = True
maps_mf4.map = maps_mf_list[3]
maps_mf4_cim = maps_mf4.getDefinition("V3")
maps_mf4_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
maps_mf4.setDefinition(maps_mf4_cim)


### Adjust Map Frame Layer Visibility ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adjust Map Frame Layer Visibility")

# Set extent for the map views (zoom to layers)
maps_mf1.camera.setExtent(prj_extent)
maps_mf2.camera.setExtent(prj_extent)
maps_mf3.camera.setExtent(prj_extent)
maps_mf4.camera.setExtent(prj_extent)

# Turn on the visibility of the appropriate layers for each map frame
# Loop through map frames and turn on appropriate layers
for mf in maps_layout.listElements("MAPFRAME_ELEMENT"):
    for l in mf.map.listLayers():
        if l.name in [
            "OCSWITRS Collisions",
            "OCSWITRS Crashes",
            "OCSWITRS Parties",
            "OCSWITRS Victims",
            "OCSWITRS Roads",
            "OCSWITRS Cities",
            "OCSWITRS Boundaries",
            "Light Gray Base",
        ]:
            l.visible = True
        else:
            l.visible = False


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.3. Add North Arrow ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.3. Add North Arrow")

# Add the North Arrow to the layout
maps_na = maps_layout.createMapSurroundElement(
    geometry = lyt_dict["maps"]["na"]["geometry"],
    mapsurround_type = "NORTH_ARROW",
    mapframe = maps_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "North_Arrow", "ArcGIS North 1")[0],
    name = "na",
)

# Set up north arrow properties
maps_na.name = "na"
maps_na.setAnchor(lyt_dict["maps"]["na"]["anchor"])
maps_na.elementWidth = lyt_dict["maps"]["na"]["width"]
maps_na.elementHeight = lyt_dict["maps"]["na"]["height"]
maps_na.elementPositionX = lyt_dict["maps"]["na"]["coordX"]
maps_na.elementPositionY = lyt_dict["maps"]["na"]["coordY"]
maps_na.elementRotation = 0
maps_na.visible = True
maps_na_cim = maps_na.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.4. Add Scale Bar ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.4. Add Scale Bar")

# Add the Scale Bar to the layout
maps_sb = maps_layout.createMapSurroundElement(
    geometry = lyt_dict["maps"]["sb"]["geometry"],
    mapsurround_type = "SCALE_BAR",
    mapframe = maps_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "SCALE_BAR", "Scale Line 1")[0],
    name = "sb",
)

# Set up scale bar properties
maps_sb.name = "sb"
maps_sb.setAnchor(lyt_dict["maps"]["sb"]["anchor"])
maps_sb.elementWidth = lyt_dict["maps"]["sb"]["width"]
maps_sb.elementHeight = lyt_dict["maps"]["sb"]["height"]
maps_sb.elementPositionX = lyt_dict["maps"]["sb"]["coordX"]
maps_sb.elementPositionY = lyt_dict["maps"]["sb"]["coordY"]
maps_sb.elementRotation = 0
maps_sb.visible = True
maps_sb_cim = maps_sb.getDefinition("V3")
maps_sb_cim.labelSymbol.symbol.height = 14
maps_sb.setDefinition(maps_sb_cim)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.5. Add Dynamic Text (Service Layer Credits) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.5. Add Dynamic Text (Service Layer Credits)")

# Add dy_namic text element for the service layer credits
if maps_layout.listElements("TEXT_ELEMENT", "cr"):
    maps_layout.deleteElement("cr")

# Add the credits text to the layout
maps_cr = aprx.createTextElement(
    container = maps_layout,
    geometry = lyt_dict["maps"]["cr"]["geometry"],
    text_size = 6,
    font_family_name = "Inter 9pt Regular",
    style_item = None,
    name = "cr",
    text_type = "POINT",
    text = "<dyn type = 'layout' name = 'maps' property = 'serviceLayerCredits'/>",
)

# Set up credits text properties
maps_cr.name = "cr"
maps_cr.setAnchor(lyt_dict["maps"]["cr"]["anchor"])
maps_cr.elementPositionX = 0
maps_cr.elementPositionY = 0
maps_cr.elementRotation = 0
maps_cr.visible = False
maps_cr_cim = maps_cr.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.6. Add New Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.6. Add New Legends")


### Remove Old Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Legends")

# Remove all old legends from the layout
for el in maps_layout.listElements():
    if el.type == "LEGEND_ELEMENT":
        print(f"Deleting legend: {el.name}")
        maps_layout.deleteElement(el)


### Legend 1: Severity Legend (Collisions Severity) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 1: Severity Legend (Collisions Severity)")

# Adding severity Legend (legend 1) to the layout
maps_lg1 = maps_layout.createMapSurroundElement(
    geometry = lyt_dict["maps"]["lg1"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = maps_mf1,
    name = "lg1",
)

# Set density legend properties
# Obtain the CIM object of the legend
maps_lg1_cim = maps_lg1.getDefinition("V3")
# Disable the legend title
maps_lg1_cim.showTitle = False
# Adjust fitting of the legend frame
maps_lg1_cim.fittingStrategy = "AdjustFrame"
# Turn on the collisions layer, and turn off the rest of the layers
for i in maps_lg1_cim.items:
    if i.name == "OCSWITRS Collisions":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
        i.showFeatureCount = True
    else:
        i.isVisible = False

# Update the legend CIM definitions
maps_lg1.setDefinition(maps_lg1_cim)

# Adjust overall legend properties
maps_lg1.name = "lg1"
maps_lg1.setAnchor(lyt_dict["maps"]["lg1"]["anchor"])
maps_lg1.elementPositionX = lyt_dict["maps"]["lg1"]["coordX"]
maps_lg1.elementPositionY = lyt_dict["maps"]["lg1"]["coordY"]
maps_lg1.elementRotation = 0
maps_lg1.visible = True
maps_lg1.mapFrame = maps_mf1
maps_lg1_cim = maps_lg1.getDefinition("V3")


### Legend 2: Roads Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 2: Roads Legend")

# Adding Roads Legend (legend 2) to the layout
maps_lg2 = maps_layout.createMapSurroundElement(
    geometry = lyt_dict["maps"]["lg2"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = maps_mf1,
    name = "lg2",
)

# Set roads legend properties
# Obtain the CIM object of the legend
maps_lg2_cim = maps_lg2.getDefinition("V3")
# Disable the legend title
maps_lg2_cim.showTitle = False
# Adjust fitting of the legend frame
maps_lg2_cim.fittingStrategy = "AdjustFrame"
# Turn on the roads layer, and turn off the rest of the layers
for i in maps_lg2_cim.items:
    if i.name == "OCSWITRS Roads":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
        i.showFeatureCount = True
    else:
        i.isVisible = False

# Update the legend CIM definitions
maps_lg2.setDefinition(maps_lg2_cim)

# Adjust overall legend properties
maps_lg2.name = "lg2"
maps_lg2.setAnchor(lyt_dict["maps"]["lg2"]["anchor"])
maps_lg2.elementPositionX = lyt_dict["maps"]["lg2"]["coordX"]
maps_lg2.elementPositionY = lyt_dict["maps"]["lg2"]["coordY"]
maps_lg2.elementRotation = 0
maps_lg2.visible = True
maps_lg2.mapFrame = maps_mf1
maps_lg2_cim = maps_lg2.getDefinition("V3")


### Legend 3: Density Legend (Cities) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 3: Density Legend (Cities)")

# Adding Severity Legend (legend 3) to the layout
maps_lg3 = maps_layout.createMapSurroundElement(
    geometry = lyt_dict["maps"]["lg3"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = maps_mf1,
    name = "lg3",
)

# Set severity legend properties
# Obtain the CIM object of the legend
maps_lg3_cim = maps_lg3.getDefinition("V3")
# Disable the legend title
maps_lg3_cim.showTitle = False
# Adjust fitting of the legend frame
maps_lg3_cim.fittingStrategy = "AdjustFrame"
# Turn on the cities and boundaries layers, and turn off the rest of the layers
for i in maps_lg3_cim.items:
    if i.name == "OCSWITRS Cities":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    elif i.name == "OCSWITRS Boundaries":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
maps_lg3.setDefinition(maps_lg3_cim)

# Adjust overall legend properties
maps_lg3.name = "lg3"
maps_lg3.setAnchor(lyt_dict["maps"]["lg3"]["anchor"])
maps_lg3.elementPositionX = lyt_dict["maps"]["lg3"]["coordX"]
maps_lg3.elementPositionY = lyt_dict["maps"]["lg3"]["coordY"]
maps_lg3.elementRotation = 0
maps_lg3.visible = True
maps_lg3.mapFrame = maps_mf1
maps_lg3_cim = maps_lg3.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.7. Add Titles for Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.7. Add Titles for Map Frames")


### Title 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 1")

# Add title for for the collisions map frame (title 1)
# Check if the collisions title already exist and if it is, delete it
if maps_layout.listElements("TEXT_ELEMENT", "t1"):
    maps_layout.deleteElement("t1")
# Add the title to the layout
maps_t1 = aprx.createTextElement(
    container = maps_layout,
    geometry = lyt_dict["maps"]["t1"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t1",
    text_type = "POINT",
    text = f"(a) Collisions (Count: {count_collisions:,})",
)

# Set up title properties
maps_t1.name = "t1"
maps_t1.setAnchor(lyt_dict["maps"]["t1"]["anchor"])
maps_t1.elementPositionX = lyt_dict["maps"]["t1"]["coordX"]
maps_t1.elementPositionY = lyt_dict["maps"]["t1"]["coordY"]
maps_t1.elementRotation = 0
maps_t1.visible = True
maps_t1.text = f"(a) Collisions (Count: {count_collisions:,})"
maps_t1.textSize = 20
maps_t1_cim = maps_t1.getDefinition("V3")


### Title 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 2")

# Add title for for the crashes map frame (title 2)
# Check if the crashes title already exist and if it is, delete it
if maps_layout.listElements("TEXT_ELEMENT", "t2"):
    maps_layout.deleteElement("t2")
# Add the title to the layout
maps_t2 = aprx.createTextElement(
    container = maps_layout,
    geometry = lyt_dict["maps"]["t2"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t2",
    text_type = "POINT",
    text = f"(b) Crashes (Count: {count_crashes:,})",
)

# Set up title properties
maps_t2.name = "t2"
maps_t2.setAnchor(lyt_dict["maps"]["t2"]["anchor"])
maps_t2.elementPositionX = lyt_dict["maps"]["t2"]["coordX"]
maps_t2.elementPositionY = lyt_dict["maps"]["t2"]["coordY"]
maps_t2.elementRotation = 0
maps_t2.visible = True
maps_t2.text = f"(b) Crashes (Count: {count_crashes:,})"
maps_t2.textSize = 20
maps_t2_cim = maps_t2.getDefinition("V3")


### Title 3 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 3")

# Add title for for the parties map frame (title 3)
# Check if the parties title already exist and if it is, delete it
if maps_layout.listElements("TEXT_ELEMENT", "t3"):
    maps_layout.deleteElement("t3")
# Add the title to the layout
maps_t3 = aprx.createTextElement(
    container = maps_layout,
    geometry = lyt_dict["maps"]["t3"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t3",
    text_type = "POINT",
    text = f"(c) Parties (Count: {count_parties:,})",
)

# Set up title properties
maps_t3.name = "t3"
maps_t3.setAnchor(lyt_dict["maps"]["t3"]["anchor"])
maps_t3.elementPositionX = lyt_dict["maps"]["t3"]["coordX"]
maps_t3.elementPositionY = lyt_dict["maps"]["t3"]["coordY"]
maps_t3.elementRotation = 0
maps_t3.visible = True
maps_t3.text = f"(c) Parties (Count: {count_parties:,})"
maps_t3.textSize = 20
maps_t3_cim = maps_t3.getDefinition("V3")


### Title 4 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 4")

# Add title for for the victims map frame (title 4)
# Check if the victims title already exist and if it is, delete it
if maps_layout.listElements("TEXT_ELEMENT", "t4"):
    maps_layout.deleteElement("t4")
# Add the title to the layout
maps_t4 = aprx.createTextElement(
    container = maps_layout,
    geometry = lyt_dict["maps"]["t4"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t4",
    text_type = "POINT",
    text = f"(d) Victims (Count: {countVictims:,})",
)

# Set up title properties
maps_t4.name = "t4"
maps_t4.setAnchor(lyt_dict["maps"]["t4"]["anchor"])
maps_t4.elementPositionX = lyt_dict["maps"]["t4"]["coordX"]
maps_t4.elementPositionY = lyt_dict["maps"]["t4"]["coordY"]
maps_t4.elementRotation = 0
maps_t4.visible = True
maps_t4.text = f"(d) Victims (Count: {countVictims:,})"
maps_t4.textSize = 20
maps_t4_cim = maps_t4.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.8. Export Maps Layout ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.8. Export Maps Layout")

# Get maps layout CIM
maps_layout_cim = maps_layout.getDefinition("V3")  # Maps layout CIM
# Export maps layout to disk
export_cim("layout", maps_layout, maps_layout.name)


### Export Layout Image ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Layout Image")

# Add graphics metadata for Figure 9
graphics_list = ocs.graphics_entry(
    gr_type=2,
    gr_id=10,
    gr_attr={
        "category": "GIS Map Layout",
        "name": "OCSWITRS Collisions Maps Layout",
        "description": "Map layout for the OCSWITRS project, visualizing collisions, crashes, parties, and victims data.",
        "caption": f"Visual mapping of the OCSWITRS collision data between {date_start.year} and {date_end.year}. The top left subgraph (a) displays all the collision data in the dataset. The remaining subgraphs (b), (c) and (d) display the crashes, parties, and victims sub-datasets. The visual display of points is color-coded by the collision severity classification.",
        "type": "GIS Layout",
        "method": "geoprocessing",
        "file_format": ".png",
        "file": "Maps Layout",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)

# Export maps layout to PNG (Figure 10)
maps_layout.exportToPNG(
    out_png = graphics_list["graphics"]["fig10"]["path"],
    resolution = graphics_list["graphics"]["fig10"]["resolution"],
    color_mode = "24-BIT_TRUE_COLOR",
    transparent_background = False,
    embed_color_profile = False,
    clip_to_elements = True,
)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 4. Injuries Layout Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4. Injuries Layout Processing")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.1. Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.1. Layout View")


### Set Injuries Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Set Injuries Layout View")

# Close all views and open the injuries layout
# Close all previous views
aprx.closeViews()
# Open the injuries layout view
injuries_layout.openView()
# set the main layout as active view
layout = aprx.activeView


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.2. Add Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.2. Add Map Frames")


### Remove Old Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Map Frames")

# Delete all layout data frames and elements

# Delete all map frames from the layout
for el in injuries_layout.listElements():
    if el.type == "MAPFRAME_ELEMENT":
        print(f"Deleting map frame: {el.name}")
        injuries_layout.deleteElement(el)


### Map Frame Definitions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame Definitions")

# List of maps to be added as map frames to the layout
injuries_mf_list = [map_injuries, map_fatalities]

# Number of map frames
injuries_mf_count = len(injuries_mf_list)

# Number of rows and columns for the map frames
injuries_mf_cols = 2
injuries_mf_rows = math.ceil(injuries_mf_count / injuries_mf_cols)

# Map frame page dimensions
injuries_mf_page_width = lyt_dict["injuries"]["page_width"]
injuries_mf_page_height = lyt_dict["injuries"]["page_height"]

# Map frame names
injuries_mf_names = [f"mf{i}" for i in range(1, injuries_mf_count + 1)]


### Map Frame 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 1")

# Add Victims map frame (map frame 1)

# Add the mapframe to the layout
injuries_mf1 = injuries_layout.createMapFrame(
    geometry = lyt_dict["injuries"]["mf1"]["geometry"],
    map = injuries_mf_list[0],
    name = "mf1",
)

# Set up map frame properties
injuries_mf1.name = "mf1"
injuries_mf1.setAnchor(lyt_dict["injuries"]["mf1"]["anchor"])
injuries_mf1.elementWidth = lyt_dict["injuries"]["mf1"]["width"]
injuries_mf1.elementHeight = lyt_dict["injuries"]["mf1"]["height"]
injuries_mf1.elementPositionX = lyt_dict["injuries"]["mf1"]["coordX"]
injuries_mf1.elementPositionY = lyt_dict["injuries"]["mf1"]["coordY"]
injuries_mf1.elementRotation = 0
injuries_mf1.visible = True
injuries_mf1.map = injuries_mf_list[0]
injuries_mf1_cim = injuries_mf1.getDefinition("V3")
injuries_mf1_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
injuries_mf1.setDefinition(injuries_mf1_cim)


### Map Frame 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 2")

# Add Fatalities map frame (map frame 2) to the layout
injuries_mf2 = injuries_layout.createMapFrame(
    geometry = lyt_dict["injuries"]["mf2"]["geometry"],
    map = injuries_mf_list[1],
    name = "mf2",
)

# Set up map frame properties
injuries_mf2.name = "mf2"
injuries_mf2.setAnchor(lyt_dict["injuries"]["mf2"]["anchor"])
injuries_mf2.elementWidth = lyt_dict["injuries"]["mf2"]["width"]
injuries_mf2.elementHeight = lyt_dict["injuries"]["mf2"]["height"]
injuries_mf2.elementPositionX = lyt_dict["injuries"]["mf2"]["coordX"]
injuries_mf2.elementPositionY = lyt_dict["injuries"]["mf2"]["coordY"]
injuries_mf2.elementRotation = 0
injuries_mf2.visible = True
injuries_mf2.map = injuries_mf_list[1]
injuries_mf2_cim = injuries_mf2.getDefinition("V3")
injuries_mf2_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
injuries_mf2.setDefinition(injuries_mf2_cim)


### Adjust Map Frame Layer Visibility ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adjust Map Frame Layer Visibility")

# Set extent for the map frame
injuries_mf1.camera.setExtent(prj_extent)
injuries_mf2.camera.setExtent(prj_extent)

# Loop through map frames and turn on appropriate layers
for mf in injuries_layout.listElements("MAPFRAME_ELEMENT"):
    for l in mf.map.listLayers():
        if l.name in [
            "OCSWITRS Victims",
            "OCSWITRS Crashes",
            "OCSWITRS Major Roads Buffers",
            "OCSWITRS Roads",
            "OCSWITRS Cities",
            "OCSWITRS Boundaries",
            "Light Gray Base",
        ]:
            l.visible = True
        else:
            l.visible = False


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.3. Add North Arrow ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.3. Add North Arrow")

# Add the North Arrow to the layout
injuries_na = injuries_layout.createMapSurroundElement(
    geometry = lyt_dict["injuries"]["na"]["geometry"],
    mapsurround_type = "NORTH_ARROW",
    mapframe = injuries_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "North_Arrow", "ArcGIS North 1")[0],
    name = "na",
)

# Set up north arrow properties
injuries_na.name = "na"
injuries_na.setAnchor(lyt_dict["injuries"]["na"]["anchor"])
injuries_na.elementWidth = lyt_dict["injuries"]["na"]["width"]
injuries_na.elementHeight = lyt_dict["injuries"]["na"]["height"]
injuries_na.elementPositionX = lyt_dict["injuries"]["na"]["coordX"]
injuries_na.elementPositionY = lyt_dict["injuries"]["na"]["coordY"]
injuries_na.elementRotation = 0
injuries_na.visible = True
injuries_na_cim = injuries_na.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.4. Add Scale Bar ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.4. Add Scale Bar")

# Add the Scale Bar to the layout
injuries_sb = injuries_layout.createMapSurroundElement(
    geometry = lyt_dict["injuries"]["sb"]["geometry"],
    mapsurround_type = "SCALE_BAR",
    mapframe = injuries_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "SCALE_BAR", "Scale Line 1")[0],
    name = "sb",
)

# Set up scale bar properties
injuries_sb.name = "sb"
injuries_sb.setAnchor(lyt_dict["injuries"]["sb"]["anchor"])
injuries_sb.elementWidth = lyt_dict["injuries"]["sb"]["width"]
injuries_sb.elementHeight = lyt_dict["injuries"]["sb"]["height"]
injuries_sb.elementPositionX = lyt_dict["injuries"]["sb"]["coordX"]
injuries_sb.elementPositionY = lyt_dict["injuries"]["sb"]["coordY"]
injuries_sb.elementRotation = 0
injuries_sb.visible = True
injuries_sb_cim = injuries_sb.getDefinition("V3")
injuries_sb_cim.labelSymbol.symbol.height = 14
injuries_sb.setDefinition(injuries_sb_cim)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.5. Add Dynamic Text (Service Layer Credits) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.5. Add Dynamic Text (Service Layer Credits)")

# Add dy_namic text element for the service layer credits
if injuries_layout.listElements("TEXT_ELEMENT", "cr"):
    injuries_layout.deleteElements("cr")

# Add the credits text to the layout
injuries_cr = aprx.createTextElement(
    container = injuries_layout,
    geometry = lyt_dict["injuries"]["cr"]["geometry"],
    text_size = 6,
    font_family_name = "Inter 9pt Regular",
    style_item = None,
    name = "cr",
    text_type = "POINT",
    text = "<dyn type = 'layout' name = 'injuries' property = 'serviceLayerCredits'/>",
)

# Set up credits text properties
injuries_cr.name = "cr"
injuries_cr.setAnchor(lyt_dict["injuries"]["cr"]["anchor"])
injuries_cr.elementPositionX = 0
injuries_cr.elementPositionY = 0
injuries_cr.elementRotation = 0
injuries_cr.visible = False
injuries_cr_cim = injuries_cr.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.6. Add New Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.6. Add New Legends")


### Remove Old Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Legends")

# Remove all old legends from the layout
for el in injuries_layout.listElements():
    if el.type == "LEGEND_ELEMENT":
        print(f"Deleting legend: {el.name}")
        injuries_layout.deleteElement(el)


### Legend 1: Severity Legend (Injuries Severity) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 1: Severity Legend (Injuries Severity)")

# Adding Severity Legend (legend 1) to the layout
injuries_lg1 = injuries_layout.createMapSurroundElement(
    geometry = lyt_dict["injuries"]["lg1"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = injuries_mf1,
    name = "lg1",
)

# Set severity legend properties
# Obtain the CIM object of the legend
injuries_lg1_cim = injuries_lg1.getDefinition("V3")
# Disable the legend title
injuries_lg1_cim.showTitle = False
# Adjust fitting of the legend frame
injuries_lg1_cim.fittingStrategy = "AdjustFrame"
# Turn on the victims layer, and turn off the rest of the layers
for i in injuries_lg1_cim.items:
    if i.name == "OCSWITRS Victims":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
        i.showFeatureCount = True
    else:
        i.isVisible = False

# Update the legend CIM definitions
injuries_lg1.setDefinition(injuries_lg1_cim)

# Set up legend properties
injuries_lg1.name = "lg1"
injuries_lg1.setAnchor(lyt_dict["injuries"]["lg1"]["anchor"])
injuries_lg1.elementPositionX = lyt_dict["injuries"]["lg1"]["coordX"]
injuries_lg1.elementPositionY = lyt_dict["injuries"]["lg1"]["coordY"]
injuries_lg1.elementRotation = 0
injuries_lg1.visible = True
injuries_lg1.mapFrame = injuries_mf1
injuries_lg1_cim = injuries_lg1.getDefinition("V3")


### Legend 2: Fatalities and Roads Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 2: Fatalities and Roads Legend")

# Adding Fatalities and Roads Legend (legend 2) to the layout
injuries_lg2 = injuries_layout.createMapSurroundElement(
    geometry = lyt_dict["injuries"]["lg2"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = injuries_mf2,
    name = "lg2",
)

# Set fatalities and roads legend properties
# Obtain the CIM object of the legend
injuries_lg2_cim = injuries_lg2.getDefinition("V3")
# Disable the legend title
injuries_lg2_cim.showTitle = False
# Adjust fitting of the legend frame
injuries_lg2_cim.fittingStrategy = "AdjustFrame"

# Turn on the fatalities layer, and turn off the rest of the layers
for i in injuries_lg2_cim.items:
    if i.name == "OCSWITRS Crashes":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    elif i.name == "OCSWITRS Major Roads Buffers":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    elif i.name == "OCSWITRS Roads":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
injuries_lg2.setDefinition(injuries_lg2_cim)

# Set up legend properties
injuries_lg2.name = "lg2"
injuries_lg2.setAnchor(lyt_dict["injuries"]["lg2"]["anchor"])
injuries_lg2.elementPositionX = lyt_dict["injuries"]["lg2"]["coordX"]
injuries_lg2.elementPositionY = lyt_dict["injuries"]["lg2"]["coordY"]
injuries_lg2.elementRotation = 0
injuries_lg2.visible = True
injuries_lg2.mapFrame = injuries_mf2
injuries_lg2_cim = injuries_lg2.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.7. Add Titles for Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.7. Add Titles for Map Frames")


### Title 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 1")

# Add title for for the victims map frame (title 1)
# Check if the victims title already exist and if it is, delete it
if injuries_layout.listElements("TEXT_ELEMENT", "t1"):
    injuries_layout.deleteElement("t1")
# Add the title to the layout
injuries_t1 = aprx.createTextElement(
    container = injuries_layout,
    geometry = lyt_dict["injuries"]["t1"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t1",
    text_type = "POINT",
    text = f"(a) Victim Injuries",
)

# Set up title properties
injuries_t1.name = "t1"
injuries_t1.setAnchor(lyt_dict["injuries"]["t1"]["anchor"])
injuries_t1.elementPositionX = lyt_dict["injuries"]["t1"]["coordX"]
injuries_t1.elementPositionY = lyt_dict["injuries"]["t1"]["coordY"]
injuries_t1.elementRotation = 0
injuries_t1.visible = True
injuries_t1.text = f"(a) Victim Injuries"
injuries_t1.textSize = 20
injuries_t1_cim = injuries_t1.getDefinition("V3")


### Title 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 2")

# Add title for for the fatalities map frame (title 2)
# Check if the crashes title already exist and if it is, delete it
if injuries_layout.listElements("TEXT_ELEMENT", "t2"):
    injuries_layout.deleteElement("t2")
# Add the title to the layout
injuries_t2 = aprx.createTextElement(
    container = injuries_layout,
    geometry = lyt_dict["injuries"]["t2"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t2",
    text_type = "POINT",
    text = f"(b) Victim Fatalities",
)

# Set up title properties
injuries_t2.name = "t2"
injuries_t2.setAnchor(lyt_dict["injuries"]["t2"]["anchor"])
injuries_t2.elementPositionX = lyt_dict["injuries"]["t2"]["coordX"]
injuries_t2.elementPositionY = lyt_dict["injuries"]["t2"]["coordY"]
injuries_t2.elementRotation = 0
injuries_t2.visible = True
injuries_t2.text = f"(b) Victim Fatalities"
injuries_t2.textSize = 20
injuries_t2_cim = injuries_t2.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.8. Export Injuries Layout ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.8. Export Injuries Layout")

# Get injuries layout CIM
injuries_layout_cim = injuries_layout.getDefinition("V3")  # Injuries layout CIM

# Export injuries layout to disk
export_cim("layout", injuries_layout, injuries_layout.name)


### Export Layout Image ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Layout Image")

# Add graphics metadata for Figure 9
graphics_list = ocs.graphics_entry(
    gr_type=2,
    gr_id=11,
    gr_attr={
        "category": "GIS Injuries Layout",
        "name": "OCSWITRS Injuries Layout",
        "description": "Map layout for the OCSWITRS project, visualizing victim injuries and fatalities data.",
        "caption": f"Visual mapping of the OCSWITRS collision data between {date_start.year} and {date_end.year}. The left subgraph displays all the victim injuries reported during the time period, color-coded by the level of severity (fatal, severe, minor, or pain). The right subgraph shows only collision incidents involving victims killed as a result of the crash (from single to multiple fatalities).",
        "type": "GIS Layout",
        "method": "geoprocessing",
        "file_format": ".png",
        "file": "Injuries Layout",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)

# Export maps layout to PNG (Figure 11)
injuries_layout.exportToPNG(
    out_png = graphics_list["graphics"]["fig11"]["path"],
    resolution = graphics_list["graphics"]["fig11"]["resolution"],
    color_mode = "24-BIT_TRUE_COLOR",
    transparent_background = False,
    embed_color_profile = False,
    clip_to_elements = True,
)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 5. Hotspots Layout Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5. Hotspots Layout Processing")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.1. Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.1. Layout View")


### Set Hotspots Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Set Hotspots Layout View")

# Close all views and open the hotspots layout
# Close all previous views
aprx.closeViews()
# Open the hotspots layout view
hotspots_layout.openView()
# set the main layout as active view
layout = aprx.activeView


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.2. Add Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.2. Add Map Frames")


### Remove Old Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Map Frames")

# Delete all layout data frames and elements

# Delete all map frames from the layout
for el in hotspots_layout.listElements():
    if el.type == "MAPFRAME_ELEMENT":
        print(f"Deleting map frame: {el.name}")
        hotspots_layout.deleteElement(el)


### Map Frame Definitions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame Definitions")

# Map frames definitions and calculations
# List of maps to be added as map frames to the layout
hotspots_mf_list = [map_fhs_100m1km, map_fhs_150m2km, map_fhs_100m5km, map_fhs_roads_500ft]
# Number of map frames
hotspots_mf_count = len(hotspots_mf_list)
# Number of rows and columns for the map frames
hotspots_mf_cols = 2
hotspots_mf_rows = math.ceil(hotspots_mf_count / hotspots_mf_cols)
# Map frame page dimensions
hotspots_mf_page_width = lyt_dict["hotspots"]["page_width"]
hotspots_mf_page_height = lyt_dict["hotspots"]["page_height"]
# Map frame names
hotspots_mf_names = [f"mf{i}" for i in range(1, hotspots_mf_count + 1)]


### Map Frame 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 1")

# Add Hot Spots (100m, 1km) map frame (map frame 1) to the layout
hotspots_mf1 = hotspots_layout.createMapFrame(
    geometry = lyt_dict["hotspots"]["mf1"]["geometry"],
    map = hotspots_mf_list[0],
    name = "mf1",
)

# Set up map frame properties
hotspots_mf1.name = "mf1"
hotspots_mf1.setAnchor(lyt_dict["hotspots"]["mf1"]["anchor"])
hotspots_mf1.elementWidth = lyt_dict["hotspots"]["mf1"]["width"]
hotspots_mf1.elementHeight = lyt_dict["hotspots"]["mf1"]["height"]
hotspots_mf1.elementPositionX = lyt_dict["hotspots"]["mf1"]["coordX"]
hotspots_mf1.elementPositionY = lyt_dict["hotspots"]["mf1"]["coordY"]
hotspots_mf1.elementRotation = 0
hotspots_mf1.visible = True
hotspots_mf1.map = hotspots_mf_list[0]
hotspots_mf1_cim = hotspots_mf1.getDefinition("V3")
hotspots_mf1_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
hotspots_mf1.setDefinition(hotspots_mf1_cim)


### Map Frame 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 2")

# Add Hot Spots (150m, 2km) map frame (map frame 2) to the layout
hotspots_mf2 = hotspots_layout.createMapFrame(
    geometry = lyt_dict["hotspots"]["mf2"]["geometry"],
    map = hotspots_mf_list[1],
    name = "mf2",
)

# Set up map frame properties
hotspots_mf2.name = "mf2"
hotspots_mf2.setAnchor(lyt_dict["hotspots"]["mf2"]["anchor"])
hotspots_mf2.elementWidth = lyt_dict["hotspots"]["mf2"]["width"]
hotspots_mf2.elementHeight = lyt_dict["hotspots"]["mf2"]["height"]
hotspots_mf2.elementPositionX = lyt_dict["hotspots"]["mf2"]["coordX"]
hotspots_mf2.elementPositionY = lyt_dict["hotspots"]["mf2"]["coordY"]
hotspots_mf2.elementRotation = 0
hotspots_mf2.visible = True
hotspots_mf2.map = hotspots_mf_list[1]
hotspots_mf2_cim = hotspots_mf2.getDefinition("V3")
hotspots_mf2_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
hotspots_mf2.setDefinition(hotspots_mf2_cim)


### Map Frame 3 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 3")

# Add Hot Spots (100m, 5km) map frame (map frame 3) to the layout
hotspots_mf3 = hotspots_layout.createMapFrame(
    geometry = lyt_dict["hotspots"]["mf3"]["geometry"],
    map = hotspots_mf_list[2],
    name = "mf3",
)

# Set up map frame properties
hotspots_mf3.name = "mf3"
hotspots_mf3.setAnchor(lyt_dict["hotspots"]["mf3"]["anchor"])
hotspots_mf3.elementWidth = lyt_dict["hotspots"]["mf3"]["width"]
hotspots_mf3.elementHeight = lyt_dict["hotspots"]["mf3"]["height"]
hotspots_mf3.elementPositionX = lyt_dict["hotspots"]["mf3"]["coordX"]
hotspots_mf3.elementPositionY = lyt_dict["hotspots"]["mf3"]["coordY"]
hotspots_mf3.elementRotation = 0
hotspots_mf3.visible = True
hotspots_mf3.map = hotspots_mf_list[2]
hotspots_mf3_cim = hotspots_mf3.getDefinition("V3")
hotspots_mf3_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
hotspots_mf3.setDefinition(hotspots_mf3_cim)


### Map Frame 4 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 4")

# Add Hot Spots (500ft from Major Roads) map frame (map frame 4) to the layout
hotspots_mf4 = hotspots_layout.createMapFrame(
    geometry = lyt_dict["hotspots"]["mf4"]["geometry"],
    map = hotspots_mf_list[3],
    name = "mf4",
)

# Set up map frame properties
hotspots_mf4.name = "mf4"
hotspots_mf4.setAnchor(lyt_dict["hotspots"]["mf4"]["anchor"])
hotspots_mf4.elementWidth = lyt_dict["hotspots"]["mf4"]["width"]
hotspots_mf4.elementHeight = lyt_dict["hotspots"]["mf4"]["height"]
hotspots_mf4.elementPositionX = lyt_dict["hotspots"]["mf4"]["coordX"]
hotspots_mf4.elementPositionY = lyt_dict["hotspots"]["mf4"]["coordY"]
hotspots_mf4.elementRotation = 0
hotspots_mf4.visible = True
hotspots_mf4.map = hotspots_mf_list[3]
hotspots_mf4_cim = hotspots_mf4.getDefinition("V3")
hotspots_mf4_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
hotspots_mf4.setDefinition(hotspots_mf4_cim)


### Adjust Map Frame Layer Visibility ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adjust Map Frame Layer Visibility")

# Set extent for the map frame
hotspots_mf1.camera.setExtent(prj_extent)
hotspots_mf2.camera.setExtent(prj_extent)
hotspots_mf3.camera.setExtent(prj_extent)
hotspots_mf4.camera.setExtent(prj_extent)

# Adjust visibility of the layers
# Loop through map frames and turn on appropriate layers
for mf in hotspots_layout.listElements("MAPFRAME_ELEMENT"):
    for l in mf.map.listLayers():
        if l.name in [
            "OCSWITRS Crashes Find Hot Spots 100m 1km",
            "OCSWITRS Crashes Find Hot Spots 150m 2km",
            "OCSWITRS Crashes Find Hot Spots 100m 5km",
            "OCSWITRS Crashes Find Hot Spots 500 Feet from Major Roads 500ft 1mi",
            "OCSWITRS Boundaries",
            "OCSWITRS Roads",
            "OCSWITRS Census Blocks",
            "Light Gray Base",
        ]:
            l.visible = True
            if l.name in ["OCSWITRS Census Blocks", "OCSWITRS Roads"]:
                l.transparency = 65
        else:
            l.visible = False


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.3. Add North Arrow ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.3. Add North Arrow")

# Add the North Arrow to the layout
hotspots_na = hotspots_layout.createMapSurroundElement(
    geometry = lyt_dict["hotspots"]["na"]["geometry"],
    mapsurround_type = "NORTH_ARROW",
    mapframe = hotspots_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "North_Arrow", "ArcGIS North 1")[0],
    name = "na",
)

# Set up north arrow properties
hotspots_na.name = "na"
hotspots_na.setAnchor(lyt_dict["hotspots"]["na"]["anchor"])
hotspots_na.elementWidth = lyt_dict["hotspots"]["na"]["width"]
hotspots_na.elementHeight = lyt_dict["hotspots"]["na"]["height"]
hotspots_na.elementPositionX = lyt_dict["hotspots"]["na"]["coordX"]
hotspots_na.elementPositionY = lyt_dict["hotspots"]["na"]["coordY"]
hotspots_na.elementRotation = 0
hotspots_na.visible = True
hotspots_na_cim = hotspots_na.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.4. Add Scale Bar ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.4. Add Scale Bar")

# Add the Scale Bar to the layout
hotspots_sb = hotspots_layout.createMapSurroundElement(
    geometry = lyt_dict["hotspots"]["sb"]["geometry"],
    mapsurround_type = "SCALE_BAR",
    mapframe = hotspots_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "SCALE_BAR", "Scale Line 1")[0],
    name = "sb",
)

# Set up scale bar properties
hotspots_sb.name = "sb"
hotspots_sb.setAnchor(lyt_dict["hotspots"]["sb"]["anchor"])
hotspots_sb.elementWidth = lyt_dict["hotspots"]["sb"]["width"]
hotspots_sb.elementHeight = lyt_dict["hotspots"]["sb"]["height"]
hotspots_sb.elementPositionX = lyt_dict["hotspots"]["sb"]["coordX"]
hotspots_sb.elementPositionY = lyt_dict["hotspots"]["sb"]["coordY"]
hotspots_sb.elementRotation = 0
hotspots_sb.visible = True
hotspots_sb_cim = hotspots_sb.getDefinition("V3")
hotspots_sb_cim.labelSymbol.symbol.height = 14
hotspots_sb.setDefinition(hotspots_sb_cim)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.5. Add Dynamic Text (Service Layer Credits) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.5. Add Dynamic Text (Service Layer Credits)")

# Add dy_namic text element for the service layer credits
if hotspots_layout.listElements("TEXT_ELEMENT", "cr"):
    hotspots_layout.deleteElement("cr")

# Add the credits text to the layout
hotspots_cr = aprx.createTextElement(
    container = hotspots_layout,
    geometry = lyt_dict["hotspots"]["cr"]["geometry"],
    text_size = 6,
    font_family_name = "Inter 9pt Regular",
    style_item = None,
    name = "cr",
    text_type = "POINT",
    text = "<dyn type = 'layout' name = 'hotspots' property = 'serviceLayerCredits'/>",
)

# Set up credits text properties
hotspots_cr.name = "cr"
hotspots_cr.setAnchor(lyt_dict["hotspots"]["cr"]["anchor"])
hotspots_cr.elementPositionX = 0
hotspots_cr.elementPositionY = 0
hotspots_cr.elementRotation = 0
hotspots_cr.visible = False
hotspots_cr_cim = hotspots_cr.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.6. Add New Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.6. Add New Legends")


### Remove Old Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Legends")

# Remove all old legends from the layout
for el in hotspots_layout.listElements():
    if el.type == "LEGEND_ELEMENT":
        print(f"Deleting legend: {el.name}")
        hotspots_layout.deleteElement(el)


### Legend 1: Hotspots Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 1: Hotspots Legend")

# Adding Hotspots Legend (legend 1) to the layout
hotspots_lg1 = hotspots_layout.createMapSurroundElement(
    geometry = lyt_dict["hotspots"]["lg1"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = hotspots_mf1,
    name = "lg1",
)

# Set hotspots legend properties
# Obtain the CIM object of the legend
hotspots_lg1_cim = hotspots_lg1.getDefinition("V3")
# Disable the legend title
hotspots_lg1_cim.showTitle = False
# Adjust fitting of the legend frame
hotspots_lg1_cim.fittingStrategy = "AdjustFrame"
# Turn on the hotspots layer, and turn off the rest of the layers
for i in hotspots_lg1_cim.items:
    if i.name == "OCSWITRS Crashes Find Hot Spots 100m 1km":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
hotspots_lg1.setDefinition(hotspots_lg1_cim)

# Set up legend properties
hotspots_lg1.name = "lg1"
hotspots_lg1.setAnchor(lyt_dict["hotspots"]["lg1"]["anchor"])
hotspots_lg1.elementPositionX = lyt_dict["hotspots"]["lg1"]["coordX"]
hotspots_lg1.elementPositionY = lyt_dict["hotspots"]["lg1"]["coordY"]
hotspots_lg1.elementRotation = 0
hotspots_lg1.visible = True
hotspots_lg1.mapFrame = hotspots_mf1
hotspots_lg1_cim = hotspots_lg1.getDefinition("V3")


### Legend 2: Roads Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 2: Roads Legend")

# Adding Roads Legend (legend 2) to the layout
hotspots_lg2 = hotspots_layout.createMapSurroundElement(
    geometry = lyt_dict["hotspots"]["lg2"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = hotspots_mf1,
    name = "lg2",
)

# Set roads legend properties
# Obtain the CIM object of the legend
hotspots_lg2_cim = hotspots_lg2.getDefinition("V3")
# Disable the legend title
hotspots_lg2_cim.showTitle = False
# Adjust fitting of the legend frame
hotspots_lg2_cim.fittingStrategy = "AdjustFrame"
# Turn on the roads layer, and turn off the rest of the layers
for i in hotspots_lg2_cim.items:
    if i.name == "OCSWITRS Roads":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
hotspots_lg2.setDefinition(hotspots_lg2_cim)

# Set up legend properties
hotspots_lg2.name = "lg2"
hotspots_lg2.setAnchor(lyt_dict["hotspots"]["lg2"]["anchor"])
hotspots_lg2.elementPositionX = lyt_dict["hotspots"]["lg2"]["coordX"]
hotspots_lg2.elementPositionY = lyt_dict["hotspots"]["lg2"]["coordY"]
hotspots_lg2.elementRotation = 0
hotspots_lg2.visible = True
hotspots_lg2.mapFrame = hotspots_mf1
hotspots_lg2_cim = hotspots_lg2.getDefinition("V3")


### Legend 3: Density Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 3: Density Legend")

# Adding Density (Census Blocks) Legend (legend 3) to the layout
hotspots_lg3 = hotspots_layout.createMapSurroundElement(
    geometry = lyt_dict["hotspots"]["lg3"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = hotspots_mf1,
    name = "lg3",
)

# Set density legend properties
# Obtain the CIM object of the legend
hotspots_lg3_cim = hotspots_lg3.getDefinition("V3")
# Disable the legend title
hotspots_lg3_cim.showTitle = False
# Adjust fitting of the legend frame
hotspots_lg3_cim.fittingStrategy = "AdjustFrame"
# Turn on the fhs_100m1km layer, and turn off the rest of the layers
for i in hotspots_lg3_cim.items:
    if i.name == "OCSWITRS Census Blocks":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
hotspots_lg3.setDefinition(hotspots_lg3_cim)

# Set up legend properties
hotspots_lg3.name = "lg3"
hotspots_lg3.setAnchor(lyt_dict["hotspots"]["lg3"]["anchor"])
hotspots_lg3.elementPositionX = lyt_dict["hotspots"]["lg3"]["coordX"]
hotspots_lg3.elementPositionY = lyt_dict["hotspots"]["lg3"]["coordY"]
hotspots_lg3.elementRotation = 0
hotspots_lg3.visible = True
hotspots_lg3.mapFrame = hotspots_mf1
hotspots_lg3_cim = hotspots_lg3.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.7. Add Titles for Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.7. Add Titles for Map Frames")


### Title 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 1")

# Add title for for the hotspots (100m, 1km) map frame (title 1) to the layout
# Check if the fhs_100m1km title already exist and if it is, delete it
if hotspots_layout.listElements("TEXT_ELEMENT", "t1"):
    hotspots_layout.deleteElement("t1")
# Add the title to the layout
hotspots_t1 = aprx.createTextElement(
    container = hotspots_layout,
    geometry = lyt_dict["hotspots"]["t1"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t1",
    text_type = "POINT",
    text = f"(a) Crashes Hot Spots (100m Bins, 1km NN)",
)

# Set up title properties
hotspots_t1.name = "t1"
hotspots_t1.setAnchor(lyt_dict["hotspots"]["t1"]["anchor"])
hotspots_t1.elementPositionX = lyt_dict["hotspots"]["t1"]["coordX"]
hotspots_t1.elementPositionY = lyt_dict["hotspots"]["t1"]["coordY"]
hotspots_t1.elementRotation = 0
hotspots_t1.visible = True
hotspots_t1.text = f"(a) Crashes Hot Spots (100m Bins, 1km NN)"
hotspots_t1.textSize = 20
findHotspots_t1_cim = hotspots_t1.getDefinition("V3")


### Title 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 2")

# Add title for for the hotspots (150m, 2km) map frame (title 2) to the layout
# Check if the fhs_150m2km title already exist and if it is, delete it
if hotspots_layout.listElements("TEXT_ELEMENT", "t2"):
    hotspots_layout.deleteElement("t2")
# Add the title to the layout
hotspots_t2 = aprx.createTextElement(
    container = hotspots_layout,
    geometry = lyt_dict["hotspots"]["t2"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t2",
    text_type = "POINT",
    text = f"(b) Crashes Hot Spots (150m Bins, 2km NN)",
)

# Set up title properties
hotspots_t2.name = "t2"
hotspots_t2.setAnchor(lyt_dict["hotspots"]["t2"]["anchor"])
hotspots_t2.elementPositionX = lyt_dict["hotspots"]["t2"]["coordX"]
hotspots_t2.elementPositionY = lyt_dict["hotspots"]["t2"]["coordY"]
hotspots_t2.elementRotation = 0
hotspots_t2.visible = True
hotspots_t2.text = f"(b) Crashes Hot Spots (150m Bins, 2km NN)"
hotspots_t2.textSize = 20
findHotspots_t2_cim = hotspots_t2.getDefinition("V3")


### Title 3 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 3")

# Add title for for the hotspots (100m, 5km) map frame (title 3) to the layout
# Check if the fhs_100m5km title already exist and if it is, delete it
if hotspots_layout.listElements("TEXT_ELEMENT", "t3"):
    hotspots_layout.deleteElement("t3")
# Add the title to the layout
hotspots_t3 = aprx.createTextElement(
    container = hotspots_layout,
    geometry = lyt_dict["hotspots"]["t3"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t3",
    text_type = "POINT",
    text = f"(c) Crashes Hot Spots (100m Bins, 5km NN)",
)

# Set up title properties
hotspots_t3.name = "t3"
hotspots_t3.setAnchor(lyt_dict["hotspots"]["t3"]["anchor"])
hotspots_t3.elementPositionX = lyt_dict["hotspots"]["t3"]["coordX"]
hotspots_t3.elementPositionY = lyt_dict["hotspots"]["t3"]["coordY"]
hotspots_t3.elementRotation = 0
hotspots_t3.visible = True
hotspots_t3.text = f"(c) Crashes Hot Spots (100m Bins, 5km NN)"
hotspots_t3.textSize = 20
hotspots_t3_cim = hotspots_t3.getDefinition("V3")


### Title 4 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 4")

# Add title for for the hotspots 500ft from major roads map frame (title 4) to the layout
# Check if the fhs_roads_500ft title already exist and if it is, delete it
if hotspots_layout.listElements("TEXT_ELEMENT", "t4"):
    hotspots_layout.deleteElement("t4")
# Add the title to the layout
hotspots_t4 = aprx.createTextElement(
    container = hotspots_layout,
    geometry = lyt_dict["hotspots"]["t4"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t4",
    text_type = "POINT",
    text = f"(d) Crashes Hot Spots (500ft from Major Roads)",
)

# Set up title properties
hotspots_t4.name = "t4"
hotspots_t4.setAnchor(lyt_dict["hotspots"]["t4"]["anchor"])
hotspots_t4.elementPositionX = lyt_dict["hotspots"]["t4"]["coordX"]
hotspots_t4.elementPositionY = lyt_dict["hotspots"]["t4"]["coordY"]
hotspots_t4.elementRotation = 0
hotspots_t4.visible = True
hotspots_t4.text = f"(d) Crashes Hot Spots (500ft from Major Roads)"
hotspots_t4.textSize = 20
hotspots_t4_cim = hotspots_t4.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.8. Export Hotspots Layout ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.8. Export Hotspots Layout")

# Get hotspots layout CIM
hotspots_layout_cim = hotspots_layout.getDefinition("V3")  # Find Hotspots layout CIM
# Export hotspots layout to disk
export_cim("layout", hotspots_layout, hotspots_layout.name)


### Export Layout Image ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Layout Image")

# Add graphics metadata for Figure 12
graphics_list = ocs.graphics_entry(
    gr_type=2,
    gr_id=12,
    gr_attr={
        "category": "GIS Hotspots Layout",
        "name": "OCSWITRS Hotspots Layout",
        "description": "Map layout for the OCSWITRS project, visualizing crash hotspots data.",
        "caption": "Visual display and mapping of hotspot cluster analysis configurations and parameterizations. Specifically, from top left to bottom right, the images display hotspot collision areas involving fatal or severe crashes, and reports their confidence intervals for their Getis-Ord $Gi^*$ z-scores, considering the following parameters: (a) 100 meter binned crash points, over 1 square kilometer wide neighborhood areas; (b) 150 meter binned collision points over 2 square kilometer neighborhood areas; (c) 100 meter binned crash points, over a wider, 5 square kilometers neighborhood areas, and; (d) only crash incidents within 500 feet from major roads or highways (250 on each side), 500 feet binned points, and 1 square mile neighborhood areas.",
        "type": "GIS Layout",
        "method": "geoprocessing",
        "file_format": ".png",
        "file": "Hotspots Layout",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)

# Export maps layout to PNG (Figure 12)
hotspots_layout.exportToPNG(
    out_png = graphics_list["graphics"]["fig12"]["path"],
    resolution = graphics_list["graphics"]["fig12"]["resolution"],
    color_mode = "24-BIT_TRUE_COLOR",
    transparent_background = False,
    embed_color_profile = False,
    clip_to_elements = True,
)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 6. Roads Layout Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6. Roads Layout Processing")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.1. Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.1. Layout View")


### Set Road Hotspots Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Set Road Hotspots Layout View")

# Close all views and open the map layout
# Close all previous views
aprx.closeViews()
# Open the roads layout view
roads_layout.openView()
# set the main layout as active view
layout = aprx.activeView


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.2. Add Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.2. Add Map Frames")


### Remove Old Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Map Frames")

# Delete all map frames from the layout
for el in roads_layout.listElements():
    if el.type == "MAPFRAME_ELEMENT":
        print(f"Deleting map frame: {el.name}")
        roads_layout.deleteElement(el)


### Map Frame Definitions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame Definitions")

# Map frames definitions and calculations
# List of maps to be added as map frames to the layout
roads_mf_list = [map_road_crashes, map_road_hotspots, map_road_buffers, map_road_segments]
# Number of map frames
roads_mf_count = len(roads_mf_list)
# Number of rows and columns for the map frames
roads_mf_cols = 2
roads_mf_rows = math.ceil(roads_mf_count / roads_mf_cols)
# Map frame page dimensions
roads_mf_page_width = lyt_dict["roads"]["page_width"]
roads_mf_page_height = lyt_dict["roads"]["page_height"]
# Map frame names
roads_mf_names = [f"mf{i}" for i in range(1, roads_mf_count + 1)]


### Map Frame 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 1")

# Add Crashes within 500ft of Major Roads map frame (map frame 1) to the layout
roads_mf1 = roads_layout.createMapFrame(
    geometry = lyt_dict["roads"]["mf1"]["geometry"],
    map = roads_mf_list[0],
    name = "mf1",
)

# Set up map frame properties
roads_mf1.name = "mf1"
roads_mf1.setAnchor(lyt_dict["roads"]["mf1"]["anchor"])
roads_mf1.elementWidth = lyt_dict["roads"]["mf1"]["width"]
roads_mf1.elementHeight = lyt_dict["roads"]["mf1"]["height"]
roads_mf1.elementPositionX = lyt_dict["roads"]["mf1"]["coordX"]
roads_mf1.elementPositionY = lyt_dict["roads"]["mf1"]["coordY"]
roads_mf1.elementRotation = 0
roads_mf1.visible = True
roads_mf1.map = roads_mf_list[0]
roads_mf1_cim = roads_mf1.getDefinition("V3")
roads_mf1_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
roads_mf1.setDefinition(roads_mf1_cim)


### Map Frame 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 2")

# Add Crashes Hotspots 500ft of Major Roads map frame (map frame 2) to the layout
roads_mf2 = roads_layout.createMapFrame(
    geometry = lyt_dict["roads"]["mf2"]["geometry"],
    map = roads_mf_list[1],
    name = "mf2",
)

# Set up map frame properties
roads_mf2.name = "mf2"
roads_mf2.setAnchor(lyt_dict["roads"]["mf2"]["anchor"])
roads_mf2.elementWidth = lyt_dict["roads"]["mf2"]["width"]
roads_mf2.elementHeight = lyt_dict["roads"]["mf2"]["height"]
roads_mf2.elementPositionX = lyt_dict["roads"]["mf2"]["coordX"]
roads_mf2.elementPositionY = lyt_dict["roads"]["mf2"]["coordY"]
roads_mf2.elementRotation = 0
roads_mf2.visible = True
roads_mf2.map = roads_mf_list[1]
roads_mf2_cim = roads_mf2.getDefinition("V3")
roads_mf2_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
roads_mf2.setDefinition(roads_mf2_cim)


### Map Frame 3 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 3")

# Add Major Road and Highway Buffers map frame (map frame 3) to the layout
roads_mf3 = roads_layout.createMapFrame(
    geometry = lyt_dict["roads"]["mf3"]["geometry"],
    map = roads_mf_list[2],
    name = "mf3",
)

# Set up map frame properties
roads_mf3.name = "mf3"
roads_mf3.setAnchor(lyt_dict["roads"]["mf3"]["anchor"])
roads_mf3.elementWidth = lyt_dict["roads"]["mf3"]["width"]
roads_mf3.elementHeight = lyt_dict["roads"]["mf3"]["height"]
roads_mf3.elementPositionX = lyt_dict["roads"]["mf3"]["coordX"]
roads_mf3.elementPositionY = lyt_dict["roads"]["mf3"]["coordY"]
roads_mf3.elementRotation = 0
roads_mf3.visible = True
roads_mf3.map = roads_mf_list[2]
roads_mf3_cim = roads_mf3.getDefinition("V3")
roads_mf3_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
roads_mf3.setDefinition(roads_mf3_cim)


### Map Frame 4 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 4")

# Add Major Road Segments (1,000ft along lines) map frame (map frame 4) to the layout
roads_mf4 = roads_layout.createMapFrame(
    geometry = lyt_dict["roads"]["mf4"]["geometry"],
    map = roads_mf_list[3],
    name = "mf4",
)

# Set up map frame properties
roads_mf4.name = "mf4"
roads_mf4.setAnchor(lyt_dict["roads"]["mf4"]["anchor"])
roads_mf4.elementWidth = lyt_dict["roads"]["mf4"]["width"]
roads_mf4.elementHeight = lyt_dict["roads"]["mf4"]["height"]
roads_mf4.elementPositionX = lyt_dict["roads"]["mf4"]["coordX"]
roads_mf4.elementPositionY = lyt_dict["roads"]["mf4"]["coordY"]
roads_mf4.elementRotation = 0
roads_mf4.visible = True
roads_mf4.map = roads_mf_list[3]
roads_mf4_cim = roads_mf4.getDefinition("V3")
roads_mf4_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
roads_mf4.setDefinition(roads_mf4_cim)


### Adjust Map Frame Layer Visibility ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adjust Map Frame Layer Visibility")

# Set extent for the map frame
roads_mf1.camera.setExtent(prj_extent)
roads_mf2.camera.setExtent(prj_extent)
roads_mf3.camera.setExtent(prj_extent)
roads_mf4.camera.setExtent(prj_extent)

# Adjust visibility of the layers
# Loop through map frames and turn on appropriate layers
for mf in roads_layout.listElements("MAPFRAME_ELEMENT"):
    for l in mf.map.listLayers():
        if l.name in [
            "OCSWITRS Crashes 500 Feet from Major Roads",
            "OCSWITRS Crashes Hot Spots 500 Feet from Major Roads",
            "OCSWITRS Major Roads Buffers Summary",
            "OCSWITRS Major Roads Split Buffer Summary",
            "OCSWITRS Boundaries",
            "Light Gray Base",
        ]:
            l.visible = True
            l.transparency = 0
        else:
            l.visible = False


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.3. Add North Arrow ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.3. Add North Arrow")

# Add the North Arrow to the layout
roads_na = roads_layout.createMapSurroundElement(
    geometry = lyt_dict["roads"]["na"]["geometry"],
    mapsurround_type = "NORTH_ARROW",
    mapframe = roads_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "North_Arrow", "ArcGIS North 1")[0],
    name = "na",
)

# Set up north arrow properties
roads_na.name = "na"
roads_na.setAnchor(lyt_dict["roads"]["na"]["anchor"])
roads_na.elementWidth = lyt_dict["roads"]["na"]["width"]
roads_na.elementHeight = lyt_dict["roads"]["na"]["height"]
roads_na.elementPositionX = lyt_dict["roads"]["na"]["coordX"]
roads_na.elementPositionY = lyt_dict["roads"]["na"]["coordY"]
roads_na.elementRotation = 0
roads_na.visible = True
roads_na_cim = roads_na.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.4. Add Scale Bar ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.4. Add Scale Bar")

# Add the Scale Bar to the layout
roads_sb = roads_layout.createMapSurroundElement(
    geometry = lyt_dict["roads"]["sb"]["geometry"],
    mapsurround_type = "SCALE_BAR",
    mapframe = roads_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "SCALE_BAR", "Scale Line 1")[0],
    name = "sb",
)

# Set up scale bar properties
roads_sb.name = "sb"
roads_sb.setAnchor(lyt_dict["roads"]["sb"]["anchor"])
roads_sb.elementWidth = lyt_dict["roads"]["sb"]["width"]
roads_sb.elementHeight = lyt_dict["roads"]["sb"]["height"]
roads_sb.elementPositionX = lyt_dict["roads"]["sb"]["coordX"]
roads_sb.elementPositionY = lyt_dict["roads"]["sb"]["coordY"]
roads_sb.elementRotation = 0
roads_sb.visible = True
roads_sb_cim = roads_sb.getDefinition("V3")
roads_sb_cim.labelSymbol.symbol.height = 14
roads_sb.setDefinition(roads_sb_cim)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.5. Add Dynamic Text (Service Layer Credits) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.5. Add Dynamic Text (Service Layer Credits)")

# Add dy_namic text element for the service layer credits
if roads_layout.listElements("TEXT_ELEMENT", "cr"):
    roads_layout.deleteElement("cr")

# Add the credits text to the layout
roads_cr = aprx.createTextElement(
    container = roads_layout,
    geometry = lyt_dict["roads"]["cr"]["geometry"],
    text_size = 6,
    font_family_name = "Inter 9pt Regular",
    style_item = None,
    name = "cr",
    text_type = "POINT",
    text = "<dyn type = 'layout' name = 'roads' property = 'serviceLayerCredits'/>",
)

# Set up credits text properties
roads_cr.name = "cr"
roads_cr.setAnchor(lyt_dict["roads"]["cr"]["anchor"])
roads_cr.elementPositionX = 0
roads_cr.elementPositionY = 0
roads_cr.elementRotation = 0
roads_cr.visible = False
roads_cr_cim = roads_cr.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.6. Add New Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.6. Add New Legends")


### Remove Old Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Legends")

# Remove all old legends from the layout
for el in roads_layout.listElements():
    if el.type == "LEGEND_ELEMENT":
        print(f"Deleting legend: {el.name}")
        roads_layout.deleteElement(el)


### Legend 1: Road Crashes Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 1: Road Crashes Legend")

# Adding Road Crashes Legend (legend 1) to the layout
roads_lg1 = roads_layout.createMapSurroundElement(
    geometry = lyt_dict["roads"]["lg1"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = roads_mf1,
    name = "lg1",
)

# Set road crashes legend properties
# Obtain the CIM object of the legend
roads_lg1_cim = roads_lg1.getDefinition("V3")
# Disable the legend title
roads_lg1_cim.showTitle = False
# Adjust fitting of the legend frame
roads_lg1_cim.fittingStrategy = "AdjustFrame"
# Turn on the roads layer, and turn off the rest of the layers
for i in roads_lg1_cim.items:
    if i.name == "OCSWITRS Crashes 500 Feet from Major Roads":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
roads_lg1.setDefinition(roads_lg1_cim)

# Set up legend properties
roads_lg1.name = "lg1"
roads_lg1.setAnchor(lyt_dict["roads"]["lg1"]["anchor"])
roads_lg1.elementPositionX = lyt_dict["roads"]["lg1"]["coordX"]
roads_lg1.elementPositionY = lyt_dict["roads"]["lg1"]["coordY"]
roads_lg1.elementRotation = 0
roads_lg1.visible = True
roads_lg1.mapFrame = roads_mf1
roads_lg1_cim = roads_lg1.getDefinition("V3")


### Legend 2: Road Hotspots Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 2: Road Hotspots Legend")

# Adding Road Hotspots Legend (legend 2) to the layout
roads_lg2 = roads_layout.createMapSurroundElement(
    geometry = lyt_dict["roads"]["lg2"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = roads_mf2,
    name = "lg2",
)

# Set road hotspots legend properties
# Obtain the CIM object of the legend
roads_lg2_cim = roads_lg2.getDefinition("V3")
# Disable the legend title
roads_lg2_cim.showTitle = False
# Adjust fitting of the legend frame
roads_lg2_cim.fittingStrategy = "AdjustFrame"
# Turn on the roads layer, and turn off the rest of the layers
for i in roads_lg2_cim.items:
    if i.name == "OCSWITRS Crashes Hot Spots 500 Feet from Major Roads":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
roads_lg2.setDefinition(roads_lg2_cim)

# Set up legend properties
roads_lg2.name = "lg2"
roads_lg2.setAnchor(lyt_dict["roads"]["lg2"]["anchor"])
roads_lg2.elementPositionX = lyt_dict["roads"]["lg2"]["coordX"]
roads_lg2.elementPositionY = lyt_dict["roads"]["lg2"]["coordY"]
roads_lg2.elementRotation = 0
roads_lg2.visible = True
roads_lg2.mapFrame = roads_mf2
roads_lg2_cim = roads_lg2.getDefinition("V3")


# region Legend 3: Road Buffers Legend
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 3: Road Buffers Legend")

# Adding Road Buffers Legend (legend 3) to the layout
roads_lg3 = roads_layout.createMapSurroundElement(
    geometry = lyt_dict["roads"]["lg3"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = roads_mf3,
    name = "lg3",
)

# Set road buffers legend properties
# Obtain the CIM object of the legend
roads_lg3_cim = roads_lg3.getDefinition("V3")
# Disable the legend title
roads_lg3_cim.showTitle = False
# Adjust fitting of the legend frame
roads_lg3_cim.fittingStrategy = "AdjustFrame"
# Turn on the roads layer, and turn off the rest of the layers
for i in roads_lg3_cim.items:
    if i.name == "OCSWITRS Major Roads Buffers Summary":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
roads_lg3.setDefinition(roads_lg3_cim)

# Set up legend properties
roads_lg3.name = "lg3"
roads_lg3.setAnchor(lyt_dict["roads"]["lg3"]["anchor"])
roads_lg3.elementPositionX = lyt_dict["roads"]["lg3"]["coordX"]
roads_lg3.elementPositionY = lyt_dict["roads"]["lg3"]["coordY"]
roads_lg3.elementRotation = 0
roads_lg3.visible = True
roads_lg3.mapFrame = roads_mf3
roads_lg3_cim = roads_lg3.getDefinition("V3")


### Legend 4: Road Segments Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 4: Road Segments Legend")

# Adding Road Segments Legend (legend 4) to the layout
roads_lg4 = roads_layout.createMapSurroundElement(
    geometry = lyt_dict["roads"]["lg4"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = roads_mf4,
    name = "lg4",
)

# Set road segments legend properties
# Obtain the CIM object of the legend
roads_lg4_cim = roads_lg4.getDefinition("V3")
# Disable the legend title
roads_lg4_cim.showTitle = False
# Adjust fitting of the legend frame
roads_lg4_cim.fittingStrategy = "AdjustFrame"
# Turn on the roads layer, and turn off the rest of the layers
for i in roads_lg4_cim.items:
    if i.name == "OCSWITRS Major Roads Split Buffer Summary":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
roads_lg4.setDefinition(roads_lg4_cim)

# Set up legend properties
roads_lg4.name = "lg4"
roads_lg4.setAnchor(lyt_dict["roads"]["lg4"]["anchor"])
roads_lg4.elementPositionX = lyt_dict["roads"]["lg4"]["coordX"]
roads_lg4.elementPositionY = lyt_dict["roads"]["lg4"]["coordY"]
roads_lg4.elementRotation = 0
roads_lg4.visible = True
roads_lg4.mapFrame = roads_mf4
roads_lg4_cim = roads_lg4.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.7. Add Titles for Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.7. Add Titles for Map Frames")


### Title 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 1")

# Add title for for the crashes within 500ft of major roads map frame (title 1) to the layout
# Check if the fhs_roads_500ft title already exist and if it is, delete it
if roads_layout.listElements("TEXT_ELEMENT", "t1"):
    roads_layout.deleteElement("t1")
# Add the title to the layout
roads_t1 = aprx.createTextElement(
    container = roads_layout,
    geometry = lyt_dict["roads"]["t1"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t1",
    text_type = "POINT",
    text = f"(a) Crashes (500ft from Major Roads)",
)

# Set up title properties
roads_t1.name = "t1"
roads_t1.setAnchor(lyt_dict["roads"]["t1"]["anchor"])
roads_t1.elementPositionX = lyt_dict["roads"]["t1"]["coordX"]
roads_t1.elementPositionY = lyt_dict["roads"]["t1"]["coordY"]
roads_t1.elementRotation = 0
roads_t1.visible = True
roads_t1.text = f"(a) Crashes (500ft from Major Roads)"
roads_t1.textSize = 20
roads_t1_cim = roads_t1.getDefinition("V3")


### Title 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 2")

# Add title for the OCSWITRS Crashes Hot Spots 500 Feet from Major Roads map frame (title 2) tpo the layout
# Check if the fhs_roads_500ft title already exist and if it is, delete it
if roads_layout.listElements("TEXT_ELEMENT", "t2"):
    roads_layout.deleteElement("t2")
# Add the title to the layout
roads_t2 = aprx.createTextElement(
    container = roads_layout,
    geometry = lyt_dict["roads"]["t2"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t2",
    text_type = "POINT",
    text = f"(b) Crashes Hot Spots (500ft from Major Roads)",
)

# Set up title properties
roads_t2.name = "t2"
roads_t2.setAnchor(lyt_dict["roads"]["t2"]["anchor"])
roads_t2.elementPositionX = lyt_dict["roads"]["t2"]["coordX"]
roads_t2.elementPositionY = lyt_dict["roads"]["t2"]["coordY"]
roads_t2.elementRotation = 0
roads_t2.visible = True
roads_t2.text = f"(b) Crashes Hot Spots (500ft from Major Roads)"
roads_t2.textSize = 20
roads_t2_cim = roads_t2.getDefinition("V3")


### Title 3 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 3")

# Add title for the OCSWITRS Major Roads Buffers Summary map frame (title 3) to the layout
# Check if the fhs_roads_500ft title already exist and if it is, delete it
if roads_layout.listElements("TEXT_ELEMENT", "t3"):
    roads_layout.deleteElement("t3")
# Add the title to the layout
roads_t3 = aprx.createTextElement(
    container = roads_layout,
    geometry = lyt_dict["roads"]["t3"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t3",
    text_type = "POINT",
    text = f"(c) Major Roads Buffers",
)

# Set up title properties
roads_t3.name = "t3"
roads_t3.setAnchor(lyt_dict["roads"]["t3"]["anchor"])
roads_t3.elementPositionX = lyt_dict["roads"]["t3"]["coordX"]
roads_t3.elementPositionY = lyt_dict["roads"]["t3"]["coordY"]
roads_t3.elementRotation = 0
roads_t3.visible = True
roads_t3.text = f"(c) Major Roads Buffers"
roads_t3.textSize = 20
roads_t3_cim = roads_t3.getDefinition("V3")


### Title 4 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 4")

# Add title for the OCSWITRS Major Roads Split Buffer Summary map frame (title 4) to the layout
# Check if the fhs_roads_500ft title already exist and if it is, delete it
if roads_layout.listElements("TEXT_ELEMENT", "t4"):
    roads_layout.deleteElement("t4")
# Add the title to the layout
roads_t4 = aprx.createTextElement(
    container = roads_layout,
    geometry = lyt_dict["roads"]["t4"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t4",
    text_type = "POINT",
    text = f"(d) Major Road Segments (1,000ft length)",
)

# Set up title properties
roads_t4.name = "t4"
roads_t4.setAnchor(lyt_dict["roads"]["t4"]["anchor"])
roads_t4.elementPositionX = lyt_dict["roads"]["t4"]["coordX"]
roads_t4.elementPositionY = lyt_dict["roads"]["t4"]["coordY"]
roads_t4.elementRotation = 0
roads_t4.visible = True
roads_t4.text = f"(d) Major Road Segments (1,000ft length)"
roads_t4.textSize = 20
roads_t4_cim = roads_t4.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.8. Export Road Hotspots Layout ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.8. Export Road Hotspots Layout")

# Get roads layout CIM
roads_layout_cim = roads_layout.getDefinition("V3")  # Roads layout CIM

# Export roads layout to disk
export_cim("layout", roads_layout, roads_layout.name)


### Export Layout Image ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Layout Image")

# Add graphics metadata for Figure 13
graphics_list = ocs.graphics_entry(
    gr_type=2,
    gr_id=13,
    gr_attr={
        "category": "GIS Roads Layout",
        "name": "OCSWITRS Roads Layout",
        "description": "Map layout for the OCSWITRS project, visualizing road hotspots data.",
        "caption": "Visual display and mapping of road hotspots analysis configurations and parameterizations. Specifically, from top left to bottom right, the images display crash incidents within 500 feet from major roads, and reports their confidence intervals for their Getis-Ord $Gi^*$ z-scores, considering the following parameters: (a) Crashes (500ft from Major Roads); (b) Crashes Hot Spots (500ft from Major Roads); (c) Major Roads Buffers, and; (d) Major Road Segments (1,000ft length).",
        "type": "GIS Layout",
        "method": "geoprocessing",
        "file_format": ".png",
        "file": "Roads Layout",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)

# Export maps layout to PNG (Figure 12)
roads_layout.exportToPNG(
    out_png = graphics_list["graphics"]["fig13"]["path"],
    resolution = graphics_list["graphics"]["fig13"]["resolution"],
    color_mode = "24-BIT_TRUE_COLOR",
    transparent_background = False,
    embed_color_profile = False,
    clip_to_elements = True,
)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 7. Points Layout Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7. Points Layout Processing")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.1. Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.1. Layout View")


### Set Optimized Hotspots Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Set Optimized Hotspots Layout View")

# Close all previous views
aprx.closeViews()
# Open the points layout view
points_layout.openView()
# set the main layout as active view
layout = aprx.activeView


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.2. Add Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.2. Add Map Frames")


### Remove Old Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Map Frames")

# Delete all map frames from the layout
for el in points_layout.listElements():
    if el.type == "MAPFRAME_ELEMENT":
        print(f"Deleting map frame: {el.name}")
        points_layout.deleteElement(el)


### Map Frame Definitions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame Definitions")

# Map frames definitions and calculations
# List of maps to be added as map frames to the layout
points_mf_list = [map_point_fhs, map_point_ohs]
# Number of map frames
points_mf_count = len(points_mf_list)
# Number of rows and columns for the map frames
points_mf_cols = 2
points_mf_rows = math.ceil(points_mf_count / points_mf_cols)
# Map frame page dimensions
points_mf_page_width = lyt_dict["points"]["page_width"]
points_mf_page_height = lyt_dict["points"]["page_height"]
# Map frame names
points_mf_names = [f"mf{i}" for i in range(1, points_mf_count + 1)]


### Map Frame 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 1")

# Add Hotspot Points map frame (map frame 1) to the layout
points_mf1 = points_layout.createMapFrame(
    geometry = lyt_dict["points"]["mf1"]["geometry"],
    map = points_mf_list[0],
    name = "mf1",
)

# Set up map frame properties
points_mf1.name = "mf1"
points_mf1.setAnchor(lyt_dict["points"]["mf1"]["anchor"])
points_mf1.elementWidth = lyt_dict["points"]["mf1"]["width"]
points_mf1.elementHeight = lyt_dict["points"]["mf1"]["height"]
points_mf1.elementPositionX = lyt_dict["points"]["mf1"]["coordX"]
points_mf1.elementPositionY = lyt_dict["points"]["mf1"]["coordY"]
points_mf1.elementRotation = 0
points_mf1.visible = True
points_mf1.map = points_mf_list[0]
points_mf1_cim = points_mf1.getDefinition("V3")
points_mf1_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
points_mf1.setDefinition(points_mf1_cim)


### Map Frame 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 2")

# Add Optimized Hotspot Points map frame (map frame 2) to the layout
points_mf2 = points_layout.createMapFrame(
    geometry = lyt_dict["points"]["mf2"]["geometry"],
    map = points_mf_list[1],
    name = "mf2",
)

# Set up map frame properties
points_mf2.name = "mf2"
points_mf2.setAnchor(lyt_dict["points"]["mf2"]["anchor"])
points_mf2.elementWidth = lyt_dict["points"]["mf2"]["width"]
points_mf2.elementHeight = lyt_dict["points"]["mf2"]["height"]
points_mf2.elementPositionX = lyt_dict["points"]["mf2"]["coordX"]
points_mf2.elementPositionY = lyt_dict["points"]["mf2"]["coordY"]
points_mf2.elementRotation = 0
points_mf2.visible = True
points_mf2.map = points_mf_list[1]
points_mf2_cim = points_mf2.getDefinition("V3")
points_mf2_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
points_mf2.setDefinition(points_mf2_cim)


### Adjust Map Frame Layer Visibility ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adjust Map Frame Layer Visibility")

# Set extent for the map frame
points_mf1.camera.setExtent(prj_extent)
points_mf2.camera.setExtent(prj_extent)

# Adjust visibility of the layers
# Loop through map frames and turn on appropriate layers
for mf in points_layout.listElements("MAPFRAME_ELEMENT"):
    for l in mf.map.listLayers():
        if l.name in [
            "OCSWITRS Crashes Hot Spots",
            "OCSWITRS Crashes Optimized Hot Spots",
            "OCSWITRS Roads",
            "OCSWITRS Boundaries",
            "Light Gray Base",
        ]:
            l.visible = True
            l.transparency = 0
        else:
            l.visible = False


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.3. Add North Arrow ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.3. Add North Arrow")

# Add the North Arrow to the layout
points_na = points_layout.createMapSurroundElement(
    geometry = lyt_dict["points"]["na"]["geometry"],
    mapsurround_type = "NORTH_ARROW",
    mapframe = points_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "North_Arrow", "ArcGIS North 1")[0],
    name = "na",
)

# Set up north arrow properties
points_na.name = "na"
points_na.setAnchor(lyt_dict["points"]["na"]["anchor"])
points_na.elementWidth = lyt_dict["points"]["na"]["width"]
points_na.elementHeight = lyt_dict["points"]["na"]["height"]
points_na.elementPositionX = lyt_dict["points"]["na"]["coordX"]
points_na.elementPositionY = lyt_dict["points"]["na"]["coordY"]
points_na.elementRotation = 0
points_na.visible = True
points_na_cim = points_na.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.4. Add Scale Bar ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.4. Add Scale Bar")

# Add the Scale Bar to the layout
points_sb = points_layout.createMapSurroundElement(
    geometry = lyt_dict["points"]["sb"]["geometry"],
    mapsurround_type = "SCALE_BAR",
    mapframe = points_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "SCALE_BAR", "Scale Line 1")[0],
    name = "sb",
)

# Set up scale bar properties
points_sb.name = "sb"
points_sb.setAnchor(lyt_dict["points"]["sb"]["anchor"])
points_sb.elementWidth = lyt_dict["points"]["sb"]["width"]
points_sb.elementHeight = lyt_dict["points"]["sb"]["height"]
points_sb.elementPositionX = lyt_dict["points"]["sb"]["coordX"]
points_sb.elementPositionY = lyt_dict["points"]["sb"]["coordY"]
points_sb.elementRotation = 0
points_sb.visible = True
points_sb_cim = points_sb.getDefinition("V3")
points_sb_cim.labelSymbol.symbol.height = 14
points_sb.setDefinition(points_sb_cim)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.5. Add Dynamic Text (Service Layer Credits) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.5. Add Dynamic Text (Service Layer Credits)")

# Add dynamic text element for the service layer credits
if points_layout.listElements("TEXT_ELEMENT", "cr"):
    points_layout.deleteElement("cr")

# Add the credits text to the layout
points_cr = aprx.createTextElement(
    container = points_layout,
    geometry = lyt_dict["points"]["cr"]["geometry"],
    text_size = 6,
    font_family_name = "Inter 9pt Regular",
    style_item = None,
    name = "cr",
    text_type = "POINT",
    text = "<dyn type = 'layout' name = 'points' property = 'serviceLayerCredits'/>",
)

# Set up credits text properties
points_cr.name = "cr"
points_cr.setAnchor(lyt_dict["points"]["cr"]["anchor"])
points_cr.elementPositionX = 0
points_cr.elementPositionY = 0
points_cr.elementRotation = 0
points_cr.visible = False
points_cr_cim = points_cr.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.6. Add New Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.6. Add New Legends")


### Remove Old Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Legends")

# Remove all old legends from the layout
for el in points_layout.listElements():
    if el.type == "LEGEND_ELEMENT":
        print(f"Deleting legend: {el.name}")
        points_layout.deleteElement(el)


### Legend 1: Hotspots Points Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 1: Hotspots Points Legend")

# Adding Hotspots Points Legend (legend 1) to the layout
points_lg1 = points_layout.createMapSurroundElement(
    geometry = lyt_dict["points"]["lg1"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = points_mf1,
    name = "lg1",
)

# Set hotspots points legend properties
# Obtain the CIM object of the legend
points_lg1_cim = points_lg1.getDefinition("V3")
# Disable the legend title
points_lg1_cim.showTitle = False
# Adjust fitting of the legend frame
points_lg1_cim.fittingStrategy = "AdjustFrame"
# Turn on the hotspots point layer, and turn off the rest of the layers
for i in points_lg1_cim.items:
    if i.name == "OCSWITRS Crashes Hot Spots":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
points_lg1.setDefinition(points_lg1_cim)

# Set up legend properties
points_lg1.name = "lg1"
points_lg1.setAnchor(lyt_dict["points"]["lg1"]["anchor"])
points_lg1.elementPositionX = lyt_dict["points"]["lg1"]["coordX"]
points_lg1.elementPositionY = lyt_dict["points"]["lg1"]["coordY"]
points_lg1.elementRotation = 0
points_lg1.visible = True
points_lg1.mapFrame = points_mf1
points_lg1_cim = points_lg1.getDefinition("V3")


### Legend 2: Optimized Hotspots Points Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 2: Optimized Hotspots Points Legend")

# Adding Optimized Hotspots Points Legend (legend 2) to the layout
points_lg2 = points_layout.createMapSurroundElement(
    geometry = lyt_dict["points"]["lg2"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = points_mf2,
    name = "lg2",
)

# Set optimized hotspots points legend properties
# Obtain the CIM object of the legend
points_lg2_cim = points_lg2.getDefinition("V3")
# Disable the legend title
points_lg2_cim.showTitle = False
# Adjust fitting of the legend frame
points_lg2_cim.fittingStrategy = "AdjustFrame"
# Turn on the optimized hotspots point layer, and turn off the rest of the layers
for i in points_lg2_cim.items:
    if i.name == "OCSWITRS Crashes Optimized Hot Spots":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
points_lg2.setDefinition(points_lg2_cim)

# Set up legend properties
points_lg2.name = "lg2"
points_lg2.setAnchor(lyt_dict["points"]["lg2"]["anchor"])
points_lg2.elementPositionX = lyt_dict["points"]["lg2"]["coordX"]
points_lg2.elementPositionY = lyt_dict["points"]["lg2"]["coordY"]
points_lg2.elementRotation = 0
points_lg2.visible = True
points_lg2.mapFrame = points_mf2
points_lg2_cim = points_lg2.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.7. Add Titles for Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.7. Add Titles for Map Frames")


### Title 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 1")

# Add title for for the hotspots points map frame (title 1) to the layout
# Check if the title already exist and if it is, delete it
if points_layout.listElements("TEXT_ELEMENT", "t1"):
    points_layout.deleteElement("t1")
# Add the title to the layout
points_t1 = aprx.createTextElement(
    container = points_layout,
    geometry = lyt_dict["points"]["t1"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t1",
    text_type = "POINT",
    text = f"(a) Crashes Hot Spots",
)

# Set up title properties
points_t1.name = "t1"
points_t1.setAnchor(lyt_dict["points"]["t1"]["anchor"])
points_t1.elementPositionX = lyt_dict["points"]["t1"]["coordX"]
points_t1.elementPositionY = lyt_dict["points"]["t1"]["coordY"]
points_t1.elementRotation = 0
points_t1.visible = True
points_t1.text = f"(a) Crashes Hot Spots"
points_t1.textSize = 20
points_t1_cim = points_t1.getDefinition("V3")


### Title 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 2")

# Add title for for the optimized hotspots points map frame (title 2) to the layout
# Check if the title already exist and if it is, delete it
if points_layout.listElements("TEXT_ELEMENT", "t2"):
    points_layout.deleteElement("t2")
# Add the title to the layout
points_t2 = aprx.createTextElement(
    container = points_layout,
    geometry = lyt_dict["points"]["t2"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t2",
    text_type = "POINT",
    text = f"(b) Optimized Crashes Hot Spots",
)

# Set up title properties
points_t2.name = "t2"
points_t2.setAnchor(lyt_dict["points"]["t2"]["anchor"])
points_t2.elementPositionX = lyt_dict["points"]["t2"]["coordX"]
points_t2.elementPositionY = lyt_dict["points"]["t2"]["coordY"]
points_t2.elementRotation = 0
points_t2.visible = True
points_t2.text = f"(b) Optimized Crashes Hot Spots"
points_t2.textSize = 20
points_t2_cim = points_t2.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.8. Export Points Hotspots Layout ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.8. Export Points Hotspots Layout")

# Get points layout CIM
points_layout_cim = points_layout.getDefinition("V3")  # Points layout CIM

# Export points layout to disk
export_cim("layout", points_layout, points_layout.name)


### Export Layout Image ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Layout Image")

# Add graphics metadata for Figure 14
graphics_list = ocs.graphics_entry(
    gr_type=2,
    gr_id=14,
    gr_attr={
        "category": "GIS Points Layout",
        "name": "OCSWITRS Points Layout",
        "description": "Map layout for the OCSWITRS project, visualizing points hotspots data.",
        "caption": "Visual display and mapping of points hotspots analysis configurations and parameterizations. Specifically, from top left to bottom right, the images display crash incidents within 500 feet from major roads, and reports their confidence intervals for their Getis-Ord $Gi^*$ z-scores, considering the following parameters: (a) Crashes Hot Spots; (b) Optimized Crashes Hot Spots.",
        "type": "GIS Layout",
        "method": "geoprocessing",
        "file_format": ".png",
        "file": "Points Layout",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)

# Export maps layout to PNG (Figure 14)
points_layout.exportToPNG(
    out_png = graphics_list["graphics"]["fig14"]["path"],
    resolution = graphics_list["graphics"]["fig14"]["resolution"],
    color_mode = "24-BIT_TRUE_COLOR",
    transparent_background = False,
    embed_color_profile = False,
    clip_to_elements = True,
)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 8. Densities Layout Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8. Densities Layout Processing")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.1. Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.1. Layout View")


### Set Densities Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Set Densities Layout View")

# Close all previous views
aprx.closeViews()
# Open the densities layout view
densities_layout.openView()
# set the main layout as active view
layout = aprx.activeView


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.2. Add Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.2. Add Map Frames")


### Remove Old Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Map Frames")

# Delete all map frames from the layout
for el in densities_layout.listElements():
    if el.type == "MAPFRAME_ELEMENT":
        print(f"Deleting map frame: {el.name}")
        densities_layout.deleteElement(el)


### Map Frame Definitions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame Definitions")

# Map frames definitions and calculations
# List of maps to be added as map frames to the layout
densities_mf_list = [map_pop_dens, map_hou_dens]
# Number of map frames
densities_mf_count = len(densities_mf_list)
# Number of rows and columns for the map frames
densities_mf_cols = 2
densities_mf_rows = math.ceil(densities_mf_count / densities_mf_cols)
# Map frame page dimensions
densities_mf_page_width = lyt_dict["densities"]["page_width"]
densities_mf_page_height = lyt_dict["densities"]["page_height"]
# Map frame names
densities_mf_names = [f"mf{i}" for i in range(1, densities_mf_count + 1)]


### Map Frame 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 1")

# Add Population Density map frame (map frame 1) to the layout
densities_mf1 = densities_layout.createMapFrame(
    geometry = lyt_dict["densities"]["mf1"]["geometry"],
    map = densities_mf_list[0],
    name = "mf1",
)

# Set up map frame properties
densities_mf1.name = "mf1"
densities_mf1.setAnchor(lyt_dict["densities"]["mf1"]["anchor"])
densities_mf1.elementWidth = lyt_dict["densities"]["mf1"]["width"]
densities_mf1.elementHeight = lyt_dict["densities"]["mf1"]["height"]
densities_mf1.elementPositionX = lyt_dict["densities"]["mf1"]["coordX"]
densities_mf1.elementPositionY = lyt_dict["densities"]["mf1"]["coordY"]
densities_mf1.elementRotation = 0
densities_mf1.visible = True
densities_mf1.map = densities_mf_list[0]
densities_mf1_cim = densities_mf1.getDefinition("V3")
densities_mf1_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
densities_mf1.setDefinition(densities_mf1_cim)


### Map Frame 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 2")

# Add Housing Density map frame (map frame 2) to the layout
densities_mf2 = densities_layout.createMapFrame(
    geometry = lyt_dict["densities"]["mf2"]["geometry"],
    map = densities_mf_list[1],
    name = "mf2",
)

# Set up map frame properties
densities_mf2.name = "mf2"
densities_mf2.setAnchor(lyt_dict["densities"]["mf2"]["anchor"])
densities_mf2.elementWidth = lyt_dict["densities"]["mf2"]["width"]
densities_mf2.elementHeight = lyt_dict["densities"]["mf2"]["height"]
densities_mf2.elementPositionX = lyt_dict["densities"]["mf2"]["coordX"]
densities_mf2.elementPositionY = lyt_dict["densities"]["mf2"]["coordY"]
densities_mf2.elementRotation = 0
densities_mf2.visible = True
densities_mf2.map = densities_mf_list[1]
densities_mf2_cim = densities_mf2.getDefinition("V3")
densities_mf2_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
densities_mf2.setDefinition(densities_mf2_cim)


### Adjust Map Frame Layer Visibility ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adjust Map Frame Layer Visibility")

# Set extent for the map frame
densities_mf1.camera.setExtent(prj_extent)
densities_mf2.camera.setExtent(prj_extent)

# Adjust visibility of the layers
# Loop through map frames and turn on appropriate layers
for mf in densities_layout.listElements("MAPFRAME_ELEMENT"):
    for l in mf.map.listLayers():
        if l.name in [
            "OCSWITRS Population Density",
            "OCSWITRS Housing Density",
            "OCSWITRS Boundaries",
            "Light Gray Base",
        ]:
            l.visible = True
            l.transparency = 0
        else:
            l.visible = False


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.3. Add North Arrow ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.3. Add North Arrow")

# Add the North Arrow to the layout
densities_na = densities_layout.createMapSurroundElement(
    geometry = lyt_dict["densities"]["na"]["geometry"],
    mapsurround_type = "NORTH_ARROW",
    mapframe = densities_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "North_Arrow", "ArcGIS North 1")[0],
    name = "na",
)

# Set up north arrow properties
densities_na.name = "na"
densities_na.setAnchor(lyt_dict["densities"]["na"]["anchor"])
densities_na.elementWidth = lyt_dict["densities"]["na"]["width"]
densities_na.elementHeight = lyt_dict["densities"]["na"]["height"]
densities_na.elementPositionX = lyt_dict["densities"]["na"]["coordX"]
densities_na.elementPositionY = lyt_dict["densities"]["na"]["coordY"]
densities_na.elementRotation = 0
densities_na.visible = True
densities_na_cim = densities_na.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.4. Add Scale Bar ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.4. Add Scale Bar")

# Add the Scale Bar to the layout
densities_sb = densities_layout.createMapSurroundElement(
    geometry = lyt_dict["densities"]["sb"]["geometry"],
    mapsurround_type = "SCALE_BAR",
    mapframe = densities_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "SCALE_BAR", "Scale Line 1")[0],
    name = "sb",
)

# Set up scale bar properties
densities_sb.name = "sb"
densities_sb.setAnchor(lyt_dict["densities"]["sb"]["anchor"])
densities_sb.elementWidth = lyt_dict["densities"]["sb"]["width"]
densities_sb.elementHeight = lyt_dict["densities"]["sb"]["height"]
densities_sb.elementPositionX = lyt_dict["densities"]["sb"]["coordX"]
densities_sb.elementPositionY = lyt_dict["densities"]["sb"]["coordY"]
densities_sb.elementRotation = 0
densities_sb.visible = True
densities_sb_cim = densities_sb.getDefinition("V3")
densities_sb_cim.labelSymbol.symbol.height = 14
densities_sb.setDefinition(densities_sb_cim)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.5. Add Dynamic Text (Service Layer Credits) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.5. Add Dynamic Text (Service Layer Credits)")

# Add dy_namic text element for the service layer credits
if densities_layout.listElements("TEXT_ELEMENT", "cr"):
    densities_layout.deleteElement("cr")

# Add the credits text to the layout
densities_cr = aprx.createTextElement(
    container = densities_layout,
    geometry = lyt_dict["densities"]["cr"]["geometry"],
    text_size = 6,
    font_family_name = "Inter 9pt Regular",
    style_item = None,
    name = "cr",
    text_type = "POINT",
    text = "<dyn type = 'layout' name = 'density' property = 'serviceLayerCredits'/>",
)

# Set up credits text properties
densities_cr.name = "cr"
densities_cr.setAnchor(lyt_dict["densities"]["cr"]["anchor"])
densities_cr.elementPositionX = 0
densities_cr.elementPositionY = 0
densities_cr.elementRotation = 0
densities_cr.visible = False
densities_cr_cim = densities_cr.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.6. Add New Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.6. Add New Legends")


### Remove Old Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Legends")

# Remove all old legends from the layout
for el in densities_layout.listElements():
    if el.type == "LEGEND_ELEMENT":
        print(f"Deleting legend: {el.name}")
        densities_layout.deleteElement(el)


### Legend 1: Population Density Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 1: Population Density Legend")

# Adding Population Density Legend (legend 1) to the layout
densities_lg1 = densities_layout.createMapSurroundElement(
    geometry = lyt_dict["densities"]["lg1"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = densities_mf1,
    name = "lg1",
)

# Set population density legend properties
# Obtain the CIM object of the legend
densities_lg1_cim = densities_lg1.getDefinition("V3")
# Disable the legend title
densities_lg1_cim.showTitle = False
# Adjust fitting of the legend frame
densities_lg1_cim.fittingStrategy = "AdjustFrame"
# Turn on the population density layer, and turn off the rest of the layers
for i in densities_lg1_cim.items:
    if i.name == "OCSWITRS Population Density":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
densities_lg1.setDefinition(densities_lg1_cim)

# Set up legend properties
densities_lg1.name = "lg1"
densities_lg1.setAnchor(lyt_dict["densities"]["lg1"]["anchor"])
densities_lg1.elementPositionX = lyt_dict["densities"]["lg1"]["coordX"]
densities_lg1.elementPositionY = lyt_dict["densities"]["lg1"]["coordY"]
densities_lg1.elementRotation = 0
densities_lg1.visible = True
densities_lg1.mapFrame = densities_mf1
densities_lg1_cim = densities_lg1.getDefinition("V3")


### Legend 2: Housing Density Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 2: Housing Density Legend")

# Adding Housing Density Legend (legend 2) to the layout
densities_lg2 = densities_layout.createMapSurroundElement(
    geometry = lyt_dict["densities"]["lg2"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = densities_mf2,
    name = "lg2",
)

# Set housing density legend properties
# Obtain the CIM object of the legend
densities_lg2_cim = densities_lg2.getDefinition("V3")
# Disable the legend title
densities_lg2_cim.showTitle = False
# Adjust fitting of the legend frame
densities_lg2_cim.fittingStrategy = "AdjustFrame"
# Turn on the housing density layer, and turn off the rest of the layers
for i in densities_lg2_cim.items:
    if i.name == "OCSWITRS Housing Density":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
densities_lg2.setDefinition(densities_lg2_cim)

# Set up legend properties
densities_lg2.name = "lg2"
densities_lg2.setAnchor(lyt_dict["densities"]["lg2"]["anchor"])
densities_lg2.elementPositionX = lyt_dict["densities"]["lg2"]["coordX"]
densities_lg2.elementPositionY = lyt_dict["densities"]["lg2"]["coordY"]
densities_lg2.elementRotation = 0
densities_lg2.visible = True
densities_lg2.mapFrame = densities_mf2
densities_lg2_cim = densities_lg2.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.7. Add Titles for Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.7. Add Titles for Map Frames")


### Title 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 1")

# Add title for for the population density map frame
# Check if the title already exist and if it is, delete it
if densities_layout.listElements("TEXT_ELEMENT", "t1"):
    densities_layout.deleteElement("t1")
# Add the title to the layout
densities_t1 = aprx.createTextElement(
    container = densities_layout,
    geometry = lyt_dict["densities"]["t1"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t1",
    text_type = "POINT",
    text = f"(a) Population Density",
)

# Set up title properties
densities_t1.name = "t1"
densities_t1.setAnchor(lyt_dict["densities"]["t1"]["anchor"])
densities_t1.elementPositionX = lyt_dict["densities"]["t1"]["coordX"]
densities_t1.elementPositionY = lyt_dict["densities"]["t1"]["coordY"]
densities_t1.elementRotation = 0
densities_t1.visible = True
densities_t1.text = f"(a) Population Density"
densities_t1.textSize = 20
densities_t1_cim = densities_t1.getDefinition("V3")


### Title 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 2")

# Add title for for the housing density map frame
# Check if the title already exist and if it is, delete it
if densities_layout.listElements("TEXT_ELEMENT", "t2"):
    densities_layout.deleteElement("t2")
# Add the title to the layout
densities_t2 = aprx.createTextElement(
    container = densities_layout,
    geometry = lyt_dict["densities"]["t2"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t2",
    text_type = "POINT",
    text = f"(b) Housing Density",
)

# Set up title properties
densities_t2.name = "t2"
densities_t2.setAnchor(lyt_dict["densities"]["t2"]["anchor"])
densities_t2.elementPositionX = lyt_dict["densities"]["t2"]["coordX"]
densities_t2.elementPositionY = lyt_dict["densities"]["t2"]["coordY"]
densities_t2.elementRotation = 0
densities_t2.visible = True
densities_t2.text = f"(b) Housing Density"
densities_t2.textSize = 20
densities_t2_cim = densities_t2.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.8. Export Densities Layout ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.8. Export Densities Layout")

# Get densities layout CIM
densities_layout_cim = densities_layout.getDefinition("V3")  # Density layout CIM

# Export densities layout to disk
export_cim("layout", densities_layout, densities_layout.name)


### Export Layout Image ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Layout Image")

# Add graphics metadata for Figure 15
graphics_list = ocs.graphics_entry(
    gr_type=2,
    gr_id=15,
    gr_attr={
        "category": "GIS Densities Layout",
        "name": "OCSWITRS Densities Layout",
        "description": "Map layout for the OCSWITRS project, visualizing densities data.",
        "caption": "Density mapping of US Census data in Orange County. The unit area of analysis and display is the Census 2020 Blocks. The left subgraph shows the population density (block area total population per square mile). The right subgraph shows the housing density (block area total housing units per square mile).",
        "type": "GIS Layout",
        "method": "geoprocessing",
        "file_format": ".png",
        "file": "Densities Layout",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)

# Export maps layout to PNG (Figure 15)
densities_layout.exportToPNG(
    out_png = graphics_list["graphics"]["fig15"]["path"],
    resolution = graphics_list["graphics"]["fig15"]["resolution"],
    color_mode = "24-BIT_TRUE_COLOR",
    transparent_background = False,
    embed_color_profile = False,
    clip_to_elements = True,
)

### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 9. Areas Layout Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9. Areas Layout Processing")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.1. Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.1. Layout View")


### Set City Areas Layout View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Set City Areas Layout View")

# Close all previous views
aprx.closeViews()
# Open the city areas layout view
areas_layout.openView()
# set the main layout as active view
layout = aprx.activeView


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.2. Add Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.2. Add Map Frames")


### Remove Old Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Map Frames")

# Delete all map frames from the layout
for el in areas_layout.listElements():
    if el.type == "MAPFRAME_ELEMENT":
        print(f"Deleting map frame: {el.name}")
        areas_layout.deleteElement(el)


### Map Frame Definitions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame Definitions")

# Map frames definitions and calculations
# List of maps to be added as map frames to the layout
areas_mf_list = [map_area_cities, map_area_blocks]
# Number of map frames
areas_mf_count = len(areas_mf_list)
# Number of rows and columns for the map frames
areas_mf_cols = 2
areas_mf_rows = math.ceil(areas_mf_count / areas_mf_cols)
# Map frame page dimensions
areas_mf_page_width = lyt_dict["areas"]["page_width"]
areas_mf_page_height = lyt_dict["areas"]["page_height"]
# Map frame names
areas_mf_names = [f"mf{i}" for i in range(1, areas_mf_count + 1)]


### Map Frame 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 1")

# Add Victims by City Areas map frame (map frame 1) to the layout
areas_mf1 = areas_layout.createMapFrame(
    geometry = lyt_dict["areas"]["mf1"]["geometry"],
    map = areas_mf_list[0],
    name = "mf1",
)

# Set up map frame properties
areas_mf1.name = "mf1"
areas_mf1.setAnchor(lyt_dict["areas"]["mf1"]["anchor"])
areas_mf1.elementWidth = lyt_dict["areas"]["mf1"]["width"]
areas_mf1.elementHeight = lyt_dict["areas"]["mf1"]["height"]
areas_mf1.elementPositionX = lyt_dict["areas"]["mf1"]["coordX"]
areas_mf1.elementPositionY = lyt_dict["areas"]["mf1"]["coordY"]
areas_mf1.elementRotation = 0
areas_mf1.visible = True
areas_mf1.map = areas_mf_list[0]
areas_mf1_cim = areas_mf1.getDefinition("V3")
areas_mf1_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
areas_mf1.setDefinition(areas_mf1_cim)


### Map Frame 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frame 2")

# Add Victims by Census Blocks map frame (map frame 2) to the layout
areas_mf2 = areas_layout.createMapFrame(
    geometry = lyt_dict["areas"]["mf2"]["geometry"],
    map = areas_mf_list[1],
    name = "mf2",
)

# Set up map frame properties
areas_mf2.name = "mf2"
areas_mf2.setAnchor(lyt_dict["areas"]["mf2"]["anchor"])
areas_mf2.elementWidth = lyt_dict["areas"]["mf2"]["width"]
areas_mf2.elementHeight = lyt_dict["areas"]["mf2"]["height"]
areas_mf2.elementPositionX = lyt_dict["areas"]["mf2"]["coordX"]
areas_mf2.elementPositionY = lyt_dict["areas"]["mf2"]["coordY"]
areas_mf2.elementRotation = 0
areas_mf2.visible = True
areas_mf2.map = areas_mf_list[1]
areas_mf2_cim = areas_mf2.getDefinition("V3")
areas_mf2_cim.graphicFrame.borderSymbol.symbol.symbolLayers[0].enable = False
areas_mf2.setDefinition(areas_mf2_cim)


### Adjust Map Frame Layer Visibility ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adjust Map Frame Layer Visibility")

# Set extent for the map frame
areas_mf1.camera.setExtent(prj_extent)
areas_mf2.camera.setExtent(prj_extent)

# Adjust visibility of the layers
# Loop through map frames and turn on appropriate layers
for mf in areas_layout.listElements("MAPFRAME_ELEMENT"):
    for l in mf.map.listLayers():
        if l.name in [
            "OCSWITRS Cities Summary",
            "OCSWITRS Census Blocks Summary",
            "OCSWITRS Boundaries",
            "Light Gray Base",
        ]:
            l.visible = True
            l.transparency = 0
        else:
            l.visible = False


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.3. Add North Arrow ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.3. Add North Arrow")

# Add the North Arrow to the layout
areas_na = areas_layout.createMapSurroundElement(
    geometry = lyt_dict["areas"]["na"]["geometry"],
    mapsurround_type = "NORTH_ARROW",
    mapframe = areas_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "North_Arrow", "ArcGIS North 1")[0],
    name = "na",
)

# Set up north arrow properties
areas_na.name = "na"
areas_na.setAnchor(lyt_dict["areas"]["na"]["anchor"])
areas_na.elementWidth = lyt_dict["areas"]["na"]["width"]
areas_na.elementHeight = lyt_dict["areas"]["na"]["height"]
areas_na.elementPositionX = lyt_dict["areas"]["na"]["coordX"]
areas_na.elementPositionY = lyt_dict["areas"]["na"]["coordY"]
areas_na.elementRotation = 0
areas_na.visible = True
areas_na_cim = areas_na.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.4. Add Scale Bar ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.4. Add Scale Bar")

# Add the Scale Bar to the layout
areas_sb = areas_layout.createMapSurroundElement(
    geometry = lyt_dict["areas"]["sb"]["geometry"],
    mapsurround_type = "SCALE_BAR",
    mapframe = areas_mf1,
    style_item = aprx.listStyleItems("ArcGIS 2D", "SCALE_BAR", "Scale Line 1")[0],
    name = "sb",
)

# Set up scale bar properties
areas_sb.name = "sb"
areas_sb.setAnchor(lyt_dict["areas"]["sb"]["anchor"])
areas_sb.elementWidth = lyt_dict["areas"]["sb"]["width"]
areas_sb.elementHeight = lyt_dict["areas"]["sb"]["height"]
areas_sb.elementPositionX = lyt_dict["areas"]["sb"]["coordX"]
areas_sb.elementPositionY = lyt_dict["areas"]["sb"]["coordY"]
areas_sb.elementRotation = 0
areas_sb.visible = True
areas_sb_cim = areas_sb.getDefinition("V3")
areas_sb_cim.labelSymbol.symbol.height = 14
areas_sb.setDefinition(areas_sb_cim)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.5. Add Dynamic Text (Service Layer Credits) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.5. Add Dynamic Text (Service Layer Credits)")

# Add dy_namic text element for the service layer credits
if areas_layout.listElements("TEXT_ELEMENT", "cr"):
    areas_layout.deleteElement("cr")

# Add the credits text to the layout
areas_cr = aprx.createTextElement(
    container = areas_layout,
    geometry = lyt_dict["areas"]["cr"]["geometry"],
    text_size = 6,
    font_family_name = "Inter 9pt Regular",
    style_item = None,
    name = "cr",
    text_type = "POINT",
    text = "<dyn type = 'layout' name = 'areas' property = 'serviceLayerCredits'/>",
)

# Set up credits text properties
areas_cr.name = "cr"
areas_cr.setAnchor(lyt_dict["areas"]["cr"]["anchor"])
areas_cr.elementPositionX = 0
areas_cr.elementPositionY = 0
areas_cr.elementRotation = 0
areas_cr.visible = False
areas_cr_cim = areas_cr.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.6. Add New Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.6. Add New Legends")


### Remove Old Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Legends")

# Remove all old legends from the layout
for el in areas_layout.listElements():
    if el.type == "LEGEND_ELEMENT":
        print(f"Deleting legend: {el.name}")
        areas_layout.deleteElement(el)


### Legend 1: Victims by City Areas Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 1: Victims by City Areas Legend")

# Adding Victims by City Areas Legend (legend 1) to the layout
areas_lg1 = areas_layout.createMapSurroundElement(
    geometry = lyt_dict["areas"]["lg1"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = areas_mf1,
    name = "lg1",
)

# Set victims by city areas legend properties
# Obtain the CIM object of the legend
areas_lg1_cim = areas_lg1.getDefinition("V3")
# Disable the legend title
areas_lg1_cim.showTitle = False
# Adjust fitting of the legend frame
areas_lg1_cim.fittingStrategy = "AdjustFrame"
# Turn on the area cities layer, and turn off the rest of the layers
for i in areas_lg1_cim.items:
    if i.name == "OCSWITRS Cities Summary":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
areas_lg1.setDefinition(areas_lg1_cim)

# Set up legend properties
areas_lg1.name = "lg1"
areas_lg1.setAnchor(lyt_dict["areas"]["lg1"]["anchor"])
areas_lg1.elementPositionX = lyt_dict["areas"]["lg1"]["coordX"]
areas_lg1.elementPositionY = lyt_dict["areas"]["lg1"]["coordY"]
areas_lg1.elementRotation = 0
areas_lg1.visible = True
areas_lg1.mapFrame = areas_mf1
areas_lg1_cim = areas_lg1.getDefinition("V3")


### Legend 2: Victims by Census Blocks Legend ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legend 2: Victims by Census Blocks Legend")

# Adding Victims by Census Blocks Legend (legend 2) to the layout
areas_lg2 = areas_layout.createMapSurroundElement(
    geometry = lyt_dict["areas"]["lg2"]["geometry"],
    mapsurround_type = "LEGEND",
    mapframe = areas_mf2,
    name = "lg2",
)

# Set victims by census blocks legend properties
# Obtain the CIM object of the legend
areas_lg2_cim = areas_lg2.getDefinition("V3")
# Disable the legend title
areas_lg2_cim.showTitle = False
# Adjust fitting of the legend frame
areas_lg2_cim.fittingStrategy = "AdjustFrame"
# Turn on the area blocks layer, and turn off the rest of the layers
for i in areas_lg2_cim.items:
    if i.name == "OCSWITRS Census Blocks Summary":
        i.isVisible = True
        i.showLayerName = False
        i.autoVisibility = True
        i.keepTogetherOption = "Items"
        i.scaleToPatch = False
        i.headingSymbol.symbol.height = 16
        i.labelSymbol.symbol.height = 14
    else:
        i.isVisible = False

# Update the legend CIM definitions
areas_lg2.setDefinition(areas_lg2_cim)

# Set up legend properties
areas_lg2.name = "lg2"
areas_lg2.setAnchor(lyt_dict["areas"]["lg2"]["anchor"])
areas_lg2.elementPositionX = lyt_dict["areas"]["lg2"]["coordX"]
areas_lg2.elementPositionY = lyt_dict["areas"]["lg2"]["coordY"]
areas_lg2.elementRotation = 0
areas_lg2.visible = True
areas_lg2.mapFrame = areas_mf2
areas_lg2_cim = areas_lg2.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.7. Add Titles for Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.7. Add Titles for Map Frames")


### Title 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 1")

# Add title for for the victims by city areas map frame
# Check if the title already exist and if it is, delete it
if areas_layout.listElements("TEXT_ELEMENT", "t1"):
    areas_layout.deleteElement("t1")
# Add the title to the layout
areas_t1 = aprx.createTextElement(
    container = areas_layout,
    geometry = lyt_dict["areas"]["t1"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t1",
    text_type = "POINT",
    text = f"(a) Area Cities",
)

# Set up title properties
areas_t1.name = "t1"
areas_t1.setAnchor(lyt_dict["areas"]["t1"]["anchor"])
areas_t1.elementPositionX = lyt_dict["areas"]["t1"]["coordX"]
areas_t1.elementPositionY = lyt_dict["areas"]["t1"]["coordY"]
areas_t1.elementRotation = 0
areas_t1.visible = True
areas_t1.text = f"(a) Area Cities"
areas_t1.textSize = 20
areas_t1_cim = areas_t1.getDefinition("V3")


### Title 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Title 2")

# Add title for for the victims by census blocks map frame
# Check if the title already exist and if it is, delete it
if areas_layout.listElements("TEXT_ELEMENT", "t2"):
    areas_layout.deleteElement("t2")
# Add the title to the layout
areas_t2 = aprx.createTextElement(
    container = areas_layout,
    geometry = lyt_dict["areas"]["t2"]["geometry"],
    text_size = 20,
    font_family_name = "Inter 18pt Medium",
    style_item = None,
    name = "t2",
    text_type = "POINT",
    text = f"(b) Area Blocks",
)

# Set up title properties
areas_t2.name = "t2"
areas_t2.setAnchor(lyt_dict["areas"]["t2"]["anchor"])
areas_t2.elementPositionX = lyt_dict["areas"]["t2"]["coordX"]
areas_t2.elementPositionY = lyt_dict["areas"]["t2"]["coordY"]
areas_t2.elementRotation = 0
areas_t2.visible = True
areas_t2.text = f"(b) Area Blocks"
areas_t2.textSize = 20
areas_t2_cim = areas_t2.getDefinition("V3")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.8. Export City Areas Layout ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.8. Export City Areas Layout")

# Get city areas layout CIM
areas_layout_cim = areas_layout.getDefinition("V3")  # Areas layout CIM

# Export city areas layout to disk
export_cim("layout", areas_layout, areas_layout.name)


### Export Layout Image ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Layout Image")

# Add graphics metadata for Figure 16
graphics_list = ocs.graphics_entry(
    gr_type=2,
    gr_id=16,
    gr_attr={
        "category": "GIS Areas Layout",
        "name": "OCSWITRS Areas Layout",
        "description": "Map layout for the OCSWITRS project, visualizing areas data.",
        "caption": "Summary collision incident distribution of the OCSWITRS geocoded crashes, by city area membership (left subgraph), and by membership in relevant US Census 2020 block area).",
        "type": "GIS Layout",
        "method": "geoprocessing",
        "file_format": ".png",
        "file": "Areas Layout",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)

# Export maps layout to PNG (Figure 16)
areas_layout.exportToPNG(
    out_png = graphics_list["graphics"]["fig16"]["path"],
    resolution = graphics_list["graphics"]["fig16"]["resolution"],
    color_mode = "24-BIT_TRUE_COLOR",
    transparent_background = False,
    embed_color_profile = False,
    clip_to_elements = True,
)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 10. Layout Elements and CIM ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10. Layout Elements and CIM")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.1. Maps Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.1. Maps Layout Elements")


### Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout Elements")

# Get all the elements of the layout by element type and functionality
# Layout element lists
maps_layout_map_frames = maps_layout.listElements(element_type = "MAPFRAME_ELEMENT")
maps_layout_legend_set = maps_layout.listElements(element_type = "LEGEND_ELEMENT")
maps_layout_scale_bars = maps_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)
maps_layout_north_arrows = maps_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)
maps_layout_titles = maps_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t*")
maps_layout_text = maps_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "cr")


### Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frames")

# Get the map frames of the layout elements
# List map frames
print(f"Map Frames:")
for i in maps_layout_map_frames:
    print(f"- {i.name}")
# Get map frames
maps_layout_mf1 = maps_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf1"
)[0]
maps_layout_mf2 = maps_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf2"
)[0]
maps_layout_mf3 = maps_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf3"
)[0]
maps_layout_mf4 = maps_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf4"
)[0]

# Get the map frame CIM objects for each map frame
# get map frame CIMs
maps_layout_mf1_cim = maps_layout_mf1.getDefinition("V3")
maps_layout_mf2_cim = maps_layout_mf2.getDefinition("V3")
maps_layout_mf3_cim = maps_layout_mf3.getDefinition("V3")
maps_layout_mf4_cim = maps_layout_mf4.getDefinition("V3")


### Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legends")

# Get the legends of the layout
# List legends
print(f"Legends:")
for i in maps_layout_legend_set:
    print(f"- {i.name}")
# Get legends
maps_layout_lg1 = maps_layout.listElements(element_type = "LEGEND_ELEMENT", wildcard = "lg1")[0]
maps_layout_lg2 = maps_layout.listElements(element_type = "LEGEND_ELEMENT", wildcard = "lg2")[0]
maps_layout_lg3 = maps_layout.listElements(element_type = "LEGEND_ELEMENT", wildcard = "lg3")[0]

# Get legend CIMs
maps_layout_lg1_cim = maps_layout_lg1.getDefinition("V3")
maps_layout_lg2_cim = maps_layout_lg2.getDefinition("V3")
maps_layout_lg3_cim = maps_layout_lg3.getDefinition("V3")


### Scale Bars and North Arrows ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Scale Bars and North Arrows")

# Get the scale bar and north arrow of the layout
# List scale bars
print(f"Scale Bars:")
for i in maps_layout_scale_bars:
    print(f"- {i.name}")
# List north arrows
print(f"North Arrows:")
for i in maps_layout_north_arrows:
    print(f"- {i.name}")

# Get scale bars and north arrows
maps_layout_sb = maps_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)[0]
maps_layout_na = maps_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)[0]


# Get scale bar and north arrow CIMs
maps_layout_sb_cim = maps_layout_sb.getDefinition("V3")
maps_layout_na_cim = maps_layout_na.getDefinition("V3")


### Titles and Text Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Titles and Text Elements")

# Get the titles and text elements of the layout
# List titles
print(f"Titles:")
for i in maps_layout_titles:
    print(f"- {i.name}")
# List text
print(f"Text:")
for i in maps_layout_text:
    print(f"- {i.name}")
# Get titles and text
maps_layout_t1 = maps_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t1")[0]
maps_layout_t2 = maps_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t2")[0]
maps_layout_t3 = maps_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t3")[0]
maps_layout_t4 = maps_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t4")[0]
maps_layout_cr = maps_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "cr")[0]

# Get title and text CIMs
maps_layout_t1_cim = maps_layout_t1.getDefinition("V3")
maps_layout_t2_cim = maps_layout_t2.getDefinition("V3")
maps_layout_t3_cim = maps_layout_t3.getDefinition("V3")
maps_layout_t4_cim = maps_layout_t4.getDefinition("V3")
maps_layout_cr_cim = maps_layout_cr.getDefinition("V3")


### Layout CIM ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout CIM")

# Get the layout CIM object
maps_layout_cim = maps_layout.getDefinition("V3")

# Export the layout CIM to disk
export_cim("layout", maps_layout, maps_layout.name)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.2. Injuries Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.2. Injuries Layout Elements")


### Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout Elements")

# Get all the elements of the layout by element type and functionality
# Layout element lists
injuries_layout_map_frames = injuries_layout.listElements(element_type = "MAPFRAME_ELEMENT")
injuries_layout_legend_set = injuries_layout.listElements(element_type = "LEGEND_ELEMENT")
injuries_layout_scale_bars = injuries_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)
injuries_layout_north_arrows = injuries_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)
injuries_layout_titles = injuries_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t*"
)
injuries_layout_text = injuries_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "cr"
)


### Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frames")

# Get the map frames of the layout elements
# List map frames
print(f"Map Frames:")
for i in injuries_layout_map_frames:
    print(f"- {i.name}")
# Get map frames
injuries_layout_mf1 = injuries_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf1"
)[0]
injuries_layout_mf2 = injuries_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf2"
)[0]

# Get the map frame CIM objects for each map frame
injuries_layout_mf1_cim = injuries_layout_mf1.getDefinition("V3")
injuries_layout_mf2_cim = injuries_layout_mf2.getDefinition("V3")


### Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legends")

# Get the legends of the layout
# List legends
print(f"Legends:")
for i in injuries_layout_legend_set:
    print(f"- {i.name}")
# Get legends
injuries_layout_lg1 = injuries_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg1"
)[0]
injuries_layout_lg2 = injuries_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg2"
)[0]

# Get the legend CIM objects for each legend
injuries_layout_lg1_cim = injuries_layout_lg1.getDefinition("V3")
injuries_layout_lg2_cim = injuries_layout_lg2.getDefinition("V3")


### Scale Bars and North Arrows ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Scale Bars and North Arrows")

# Get the scale bar and north arrow of the layout
# List scale bars
print(f"Scale Bars:")
for i in injuries_layout_scale_bars:
    print(f"- {i.name}")
# List north arrows
print(f"North Arrows:")
for i in injuries_layout_north_arrows:
    print(f"- {i.name}")
# Get scale bars and north arrows
injuries_layout_sb = injuries_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)[0]
injuries_layout_na = injuries_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)[0]

# Get the scale bar and north arrow CIM objects
injuries_layout_sb_cim = injuries_layout_sb.getDefinition("V3")
injuries_layout_na_cim = injuries_layout_na.getDefinition("V3")


### Titles and Text Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Titles and Text Elements")

# Get the titles and text elements of the layout
# List titles
print(f"Titles:")
for i in injuries_layout_titles:
    print(f"- {i.name}")
# List text
print(f"Text:")
for i in injuries_layout_text:
    print(f"- {i.name}")
# Get titles and text
injuries_layout_t1 = injuries_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t1"
)[0]
injuries_layout_t2 = injuries_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t2"
)[0]
injuries_layout_cr = injuries_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "cr"
)[0]

# Get the title and text CIM objects
injuries_layout_t1_cim = injuries_layout_t1.getDefinition("V3")
injuries_layout_t2_cim = injuries_layout_t2.getDefinition("V3")
injuries_layout_cr_cim = injuries_layout_cr.getDefinition("V3")


### Layout CIM ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout CIM")

# Get the layout CIM object
injuries_layout_cim = injuries_layout.getDefinition("V3")

# Export the layout CIM to disk
export_cim("layout", injuries_layout, injuries_layout.name)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.3. Hotspots Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.3. Hotspots Layout Elements")


### Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout Elements")

# Get all the elements of the layout by element type and functionality
# Layout element lists
hotspots_layout_map_frames = hotspots_layout.listElements(element_type = "MAPFRAME_ELEMENT")
hotspots_layout_legend_set = hotspots_layout.listElements(element_type = "LEGEND_ELEMENT")
hotspots_layout_scale_bars = hotspots_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)
hotspots_layout_north_arrows = hotspots_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)
hotspots_layout_titles = hotspots_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t*"
)
hotspots_layout_text = hotspots_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "cr"
)


### Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frames")

# Get the map frames of the layout elements
# List map frames
print(f"Map Frames:")
for i in hotspots_layout_map_frames:
    print(f"- {i.name}")
# Get map frames
hotspots_layout_mf1 = hotspots_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf1"
)[0]
hotspots_layout_mf2 = hotspots_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf2"
)[0]
hotspots_layout_mf3 = hotspots_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf3"
)[0]
hotspots_layout_mf4 = hotspots_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf4"
)[0]

# Get the map frame CIM objects for each map frame
hotspots_layout_mf1_cim = hotspots_layout_mf1.getDefinition("V3")
hotspots_layout_mf2_cim = hotspots_layout_mf2.getDefinition("V3")
hotspots_layout_mf3_cim = hotspots_layout_mf3.getDefinition("V3")
hotspots_layout_mf4_cim = hotspots_layout_mf4.getDefinition("V3")


### Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legends")

# Get the legends of the layout
# List legends
print(f"Legends:")
for i in hotspots_layout_legend_set:
    print(f"- {i.name}")
# Get legends
hotspots_layout_lg1 = hotspots_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg1"
)[0]
hotspots_layout_lg2 = hotspots_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg2"
)[0]
hotspots_layout_lg3 = hotspots_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg3"
)[0]

# Get the legend CIM objects for each legend
hotspots_layout_lg1_cim = hotspots_layout_lg1.getDefinition("V3")
hotspots_layout_lg2_cim = hotspots_layout_lg2.getDefinition("V3")
hotspots_layout_lg3_cim = hotspots_layout_lg3.getDefinition("V3")


### Scale Bars and North Arrows ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Scale Bars and North Arrows")

# Get the scale bar and north arrow of the layout
# List scale bars
print(f"Scale Bars:")
for i in hotspots_layout_scale_bars:
    print(f"- {i.name}")
# List north arrows
print(f"North Arrows:")
for i in hotspots_layout_north_arrows:
    print(f"- {i.name}")
# Get scale bars and north arrows
hotspots_layout_sb = hotspots_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)[0]
hotspots_layout_na = hotspots_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)[0]

# Get the scale bar and north arrow CIM objects
hotspots_layout_sb_cim = hotspots_layout_sb.getDefinition("V3")
hotspots_layout_na_cim = hotspots_layout_na.getDefinition("V3")


### Titles and Text Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Titles and Text Elements")

# Get the titles and text elements of the layout
# List titles
print(f"Titles:")
for i in hotspots_layout_titles:
    print(f"- {i.name}")
# List text
print(f"Text:")
for i in hotspots_layout_text:
    print(f"- {i.name}")
# Get titles and text
hotspots_layout_t1 = hotspots_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t1"
)[0]
hotspots_layout_t2 = hotspots_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t2"
)[0]
hotspots_layout_t3 = hotspots_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t3"
)[0]
hotspots_layout_t4 = hotspots_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t4"
)[0]
hotspots_layout_cr = hotspots_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "cr"
)[0]

# Get the title and text CIM objects
hotspots_layout_t1_cim = hotspots_layout_t1.getDefinition("V3")
hotspots_layout_t2_cim = hotspots_layout_t2.getDefinition("V3")
hotspots_layout_t3_cim = hotspots_layout_t3.getDefinition("V3")
hotspots_layout_t4_cim = hotspots_layout_t4.getDefinition("V3")
hotspots_layout_cr_cim = hotspots_layout_cr.getDefinition("V3")


### Layout CIM ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout CIM")

# Get the layout CIM object
hotspots_layout_cim = hotspots_layout.getDefinition("V3")

# Export the layout CIM to disk
export_cim("layout", hotspots_layout, hotspots_layout.name)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.4. Roads Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.4. Roads Layout Elements")


### Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout Elements")

# Get all the elements of the layout by element type and functionality
# Layout element lists
roads_layout_map_frames = roads_layout.listElements(element_type = "MAPFRAME_ELEMENT")
roads_layout_legend_set = roads_layout.listElements(element_type = "LEGEND_ELEMENT")
roads_layout_scale_bars = roads_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)
roads_layout_north_arrows = roads_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)
roads_layout_titles = roads_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t*")
roads_layout_text = roads_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "cr")


### Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frames")

# Get the map frames of the layout elements
# List map frames
print(f"Map Frames:")
for i in roads_layout_map_frames:
    print(f"- {i.name}")
# Get map frames
roads_layout_mf1 = roads_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf1"
)[0]
roads_layout_mf2 = roads_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf2"
)[0]
roads_layout_mf3 = roads_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf3"
)[0]
roads_layout_mf4 = roads_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf4"
)[0]

# Get the map frame CIM objects for each map frame
roads_layout_mf1_cim = roads_layout_mf1.getDefinition("V3")
roads_layout_mf2_cim = roads_layout_mf2.getDefinition("V3")
roads_layout_mf3_cim = roads_layout_mf3.getDefinition("V3")
roads_layout_mf4_cim = roads_layout_mf4.getDefinition("V3")


### Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legends")

# Get the legends of the layout
# List legends
print(f"Legends:")
for i in roads_layout_legend_set:
    print(f"- {i.name}")
# Get legends
roads_layout_lg1 = roads_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg1"
)[0]
roads_layout_lg2 = roads_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg2"
)[0]
roads_layout_lg3 = roads_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg3"
)[0]
roads_layout_lg4 = roads_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg4"
)[0]

# Get the legend CIM objects for each legend
roads_layout_lg1_cim = roads_layout_lg1.getDefinition("V3")
roads_layout_lg2_cim = roads_layout_lg2.getDefinition("V3")
roads_layout_lg3_cim = roads_layout_lg3.getDefinition("V3")
roads_layout_lg4_cim = roads_layout_lg4.getDefinition("V3")


### Scale Bars and North Arrows ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Scale Bars and North Arrows")

# Get the scale bar and north arrow of the layout
# List scale bars
print(f"Scale Bars:")
for i in roads_layout_scale_bars:
    print(f"- {i.name}")
# List north arrows
print(f"North Arrows:")
for i in roads_layout_north_arrows:
    print(f"- {i.name}")
# Get scale bars and north arrows
roads_layout_sb = roads_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)[0]
roads_layout_na = roads_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)[0]

# Get the scale bar and north arrow CIM objects
roads_layout_sb_cim = roads_layout_sb.getDefinition("V3")
roads_layout_na_cim = roads_layout_na.getDefinition("V3")


### Titles and Text Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Titles and Text Elements")

# Get the titles and text elements of the layout
# List titles
print(f"Titles:")
for i in roads_layout_titles:
    print(f"- {i.name}")
# List text
print(f"Text:")
for i in roads_layout_text:
    print(f"- {i.name}")
# Get titles and text
roads_layout_t1 = roads_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t1")[0]
roads_layout_t2 = roads_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t2")[0]
roads_layout_t3 = roads_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t3")[0]
roads_layout_t4 = roads_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t4")[0]
roads_layout_cr = roads_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "cr")[0]

# Get the title and text CIM objects
roads_layout_t1_cim = roads_layout_t1.getDefinition("V3")
roads_layout_t2_cim = roads_layout_t2.getDefinition("V3")
roads_layout_t3_cim = roads_layout_t3.getDefinition("V3")
roads_layout_t4_cim = roads_layout_t4.getDefinition("V3")
roads_layout_cr_cim = roads_layout_cr.getDefinition("V3")


### Layout CIM ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout CIM")

# Get the layout CIM object
# layout CIM
roads_layout_cim = roads_layout.getDefinition("V3")

# Export the layout CIM to disk
export_cim("layout", roads_layout, roads_layout.name)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.5. Points Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.5. Points Layout Elements")


### Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout Elements")

# Get all the elements of the layout by element type and functionality
# Layout element lists
points_layout_map_frames = points_layout.listElements(element_type = "MAPFRAME_ELEMENT")
points_layout_legend_set = points_layout.listElements(element_type = "LEGEND_ELEMENT")
points_layout_scale_bars = points_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)
points_layout_north_arrows = points_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)
points_layout_titles = points_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t*"
)
points_layout_text = points_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "cr")


### Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frames")

# Get the map frames of the layout elements
# List map frames
print(f"Map Frames:")
for i in points_layout_map_frames:
    print(f"- {i.name}")
# Get map frames
points_layout_mf1 = points_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf1"
)[0]
points_layout_mf2 = points_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf2"
)[0]

# Get the map frame CIM objects for each map frame
points_layout_mf1_cim = points_layout_mf1.getDefinition("V3")
points_layout_mf2_cim = points_layout_mf2.getDefinition("V3")


### Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legends")

# Get the legends of the layout
# List legends
print(f"Legends:")
for i in points_layout_legend_set:
    print(f"- {i.name}")
# Get legends
points_layout_lg1 = points_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg1"
)[0]
points_layout_lg2 = points_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg2"
)[0]

# Get the legend CIM objects for each legend
points_layout_lg1_cim = points_layout_lg1.getDefinition("V3")
points_layout_lg2_cim = points_layout_lg2.getDefinition("V3")


### Scale Bars and North Arrows ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Scale Bars and North Arrows")

# Get the scale bar and north arrow of the layout
# List scale bars
print(f"Scale Bars:")
for i in points_layout_scale_bars:
    print(f"- {i.name}")
# List north arrows
print(f"North Arrows:")
for i in points_layout_north_arrows:
    print(f"- {i.name}")
# Get scale bars and north arrows
points_layout_sb = points_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)[0]
points_layout_na = points_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)[0]

# Get the scale bar and north arrow CIM objects
points_layout_sb_cim = points_layout_sb.getDefinition("V3")
points_layout_na_cim = points_layout_na.getDefinition("V3")


### Titles and Text Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Titles and Text Elements")

# Get the titles and text elements of the layout
# List titles
print(f"Titles:")
for i in points_layout_titles:
    print(f"- {i.name}")
# List text
print(f"Text:")
for i in points_layout_text:
    print(f"- {i.name}")
# Get titles and text
points_layout_t1 = points_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t1")[
    0
]
points_layout_t2 = points_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t2")[
    0
]
points_layout_cr = points_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "cr")[
    0
]

# Get the title and text CIM objects
points_layout_t1_cim = points_layout_t1.getDefinition("V3")
points_layout_t2_cim = points_layout_t2.getDefinition("V3")
points_layout_cr_cim = points_layout_cr.getDefinition("V3")


### Layout CIM ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout CIM")

# Get the layout CIM object
points_layout_cim = points_layout.getDefinition("V3")

# Export the layout CIM to disk
export_cim("layout", points_layout, points_layout.name)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.6. Density Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.6. Density Layout Elements")


### Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout Elements")

# Get all the elements of the layout by element type and functionality
# Layout element lists
densities_layout_map_frames = densities_layout.listElements(element_type = "MAPFRAME_ELEMENT")
densities_layout_legend_set = densities_layout.listElements(element_type = "LEGEND_ELEMENT")
densities_layout_scale_bars = densities_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)
densities_layout_north_arrows = densities_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)
densities_layout_titles = densities_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t*"
)
densities_layout_text = densities_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "cr"
)


### Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frames")

# Get the map frames of the layout elements
# List map frames
print(f"Map Frames:")
for i in densities_layout_map_frames:
    print(f"- {i.name}")
# Get map frames
densities_layout_mf1 = densities_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf1"
)[0]
densities_layout_mf2 = densities_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf2"
)[0]

# Get the map frame CIM objects for each map frame
densities_layout_mf1_cim = densities_layout_mf1.getDefinition("V3")
densities_layout_mf2_cim = densities_layout_mf2.getDefinition("V3")


### Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legends")

# Get the legends of the layout
# List legends
print(f"Legends:")
for i in densities_layout_legend_set:
    print(f"- {i.name}")
# Get legends
densities_layout_lg1 = densities_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg1"
)[0]
densities_layout_lg2 = densities_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg2"
)[0]

# Get the legend CIM objects for each legend
densities_layout_lg1_cim = densities_layout_lg1.getDefinition("V3")
densities_layout_lg2_cim = densities_layout_lg2.getDefinition("V3")


### Scale Bars and North Arrows ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Scale Bars and North Arrows")

# Get the scale bar and north arrow of the layout
# List scale bars
print(f"Scale Bars:")
for i in densities_layout_scale_bars:
    print(f"- {i.name}")
# List north arrows
print(f"North Arrows:")
for i in densities_layout_north_arrows:
    print(f"- {i.name}")
# Get scale bars and north arrows
densities_layout_sb = densities_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)[0]
densities_layout_na = densities_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)[0]

# Get the scale bar and north arrow CIM objects
densities_layout_sb_cim = densities_layout_sb.getDefinition("V3")
densities_layout_na_cim = densities_layout_na.getDefinition("V3")


### Titles and Text Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Titles and Text Elements")

# Get the titles and text elements of the layout
# List titles
print(f"Titles:")
for i in densities_layout_titles:
    print(f"- {i.name}")
# List text
print(f"Text:")
for i in densities_layout_text:
    print(f"- {i.name}")
# Get titles and text
densities_layout_t1 = densities_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t1"
)[0]
densities_layout_t2 = densities_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "t2"
)[0]
densities_layout_cr = densities_layout.listElements(
    element_type = "TEXT_ELEMENT", wildcard = "cr"
)[0]

# Get the title and text CIM objects
densities_layout_t1_cim = densities_layout_t1.getDefinition("V3")
densities_layout_t2_cim = densities_layout_t2.getDefinition("V3")
densities_layout_cr_cim = densities_layout_cr.getDefinition("V3")


### Layout CIM ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout CIM")

# Get the layout CIM object
densities_layout_cim = densities_layout.getDefinition("V3")

# Export the layout CIM to disk
export_cim("layout", densities_layout, densities_layout.name)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.7. Areas Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.7. Areas Layout Elements")


### Layout Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout Elements")

# Get all the elements of the layout by element type and functionality
# Layout element lists
areas_layout_map_frames = areas_layout.listElements(element_type = "MAPFRAME_ELEMENT")
areas_layout_legend_set = areas_layout.listElements(element_type = "LEGEND_ELEMENT")
areas_layout_scale_bars = areas_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)
areas_layout_north_arrows = areas_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)
areas_layout_titles = areas_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t*")
areas_layout_text = areas_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "cr")


### Map Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Map Frames")

# Get the map frames of the layout elements
# List map frames
print(f"Map Frames:")
for i in areas_layout_map_frames:
    print(f"- {i.name}")
# Get map frames
areas_layout_mf1 = areas_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf1"
)[0]
areas_layout_mf2 = areas_layout.listElements(
    element_type = "MAPFRAME_ELEMENT", wildcard = "mf2"
)[0]

# Get the map frame CIM objects for each map frame
areas_layout_mf1_cim = areas_layout_mf1.getDefinition("V3")
areas_layout_mf2_cim = areas_layout_mf2.getDefinition("V3")


### Legends ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Legends")

# Get the legends of the layout
# List legends
print(f"Legends:")
for i in areas_layout_legend_set:
    print(f"- {i.name}")
# Get legends
areas_layout_lg1 = areas_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg1"
)[0]
areas_layout_lg2 = areas_layout.listElements(
    element_type = "LEGEND_ELEMENT", wildcard = "lg2"
)[0]

# Get the legend CIM objects for each legend
areas_layout_lg1_cim = areas_layout_lg1.getDefinition("V3")
areas_layout_lg2_cim = areas_layout_lg2.getDefinition("V3")


### Scale Bars and North Arrows ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Scale Bars and North Arrows")

# Get the scale bar and north arrow of the layout
# List scale bars
print(f"Scale Bars:")
for i in areas_layout_scale_bars:
    print(f"- {i.name}")
# List north arrows
print(f"North Arrows:")
for i in areas_layout_north_arrows:
    print(f"- {i.name}")
# Get scale bars and north arrows
areas_layout_sb = areas_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "sb"
)[0]
areas_layout_na = areas_layout.listElements(
    element_type = "MAPSURROUND_ELEMENT", wildcard = "na"
)[0]

# Get the scale bar and north arrow CIM objects
areas_layout_sb_cim = areas_layout_sb.getDefinition("V3")
areas_layout_na_cim = areas_layout_na.getDefinition("V3")


### Titles and Text Elements ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Titles and Text Elements")

# Get the titles and text elements of the layout
# List titles
print(f"Titles:")
for i in areas_layout_titles:
    print(f"- {i.name}")
# List text
print(f"Text:")
for i in areas_layout_text:
    print(f"- {i.name}")
# Get titles and text
areas_layout_t1 = areas_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t1")[0]
areas_layout_t2 = areas_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "t2")[0]
areas_layout_cr = areas_layout.listElements(element_type = "TEXT_ELEMENT", wildcard = "cr")[0]

# Get the title and text CIM objects
areas_layout_t1_cim = areas_layout_t1.getDefinition("V3")
areas_layout_t2_cim = areas_layout_t2.getDefinition("V3")
areas_layout_cr_cim = areas_layout_cr.getDefinition("V3")


### Layout CIM ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layout CIM")

# Get the layout CIM object
areas_layout_cim = areas_layout.getDefinition("V3")

# Export the layout CIM to disk
export_cim("layout", areas_layout, areas_layout.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 11. Export Graphics List to Disk ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n11. Export Graphics List to Disk")

# Save the graphics list to disk
print("- Saving the Graphics data to disk")
with open(os.path.join(prj_dirs["data_python"], "graphics_list.pkl"), "wb") as f:
    pickle.dump(graphics_list, f)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 12. Export Feature Class Data to Shapefiles ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n12. Export Data")

# Setup the output folder for the archived data
archive_gdb = os.path.join(prj_dirs["data_gis"], "ocswitrs.gdb")
archive_gdb_supporting = os.path.join(archive_gdb, "supporting")
archive_gdb_raw = os.path.join(archive_gdb, "raw")

# Export the feature classes to the archive geodatabase
arcpy.management.CopyFeatures(collisions, os.path.join(archive_gdb_raw, "collisions"))
arcpy.management.CopyFeatures(crashes, os.path.join(archive_gdb_raw, "crashes"))
arcpy.management.CopyFeatures(parties, os.path.join(archive_gdb_raw, "parties"))
arcpy.management.CopyFeatures(victims, os.path.join(archive_gdb_raw, "victims"))
arcpy.management.CopyFeatures(roads, os.path.join(archive_gdb_supporting, "roads"))
arcpy.management.CopyFeatures(cities, os.path.join(archive_gdb_supporting, "cities"))
arcpy.management.CopyFeatures(blocks, os.path.join(archive_gdb_supporting, "blocks"))


# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of Script ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\nLast Executed:", dt.datetime.now().strftime("%Y-%m-%d"))
print("\nEnd of script")
# Last Executed: 2025-10-21
