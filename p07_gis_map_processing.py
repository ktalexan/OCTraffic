# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Part 7 - GIS Map Processing ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.3, Date: December 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOCTraffic GIS Data Processing - Part 7 - GIS Map Processing\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1. Preliminaries")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Referencing Libraries and Initialization ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.1. Referencing Libraries and Initialization")

# Import Python libraries
import os
import datetime as dt
import json
from dateutil.parser import parse
import pandas as pd
import pytz
from dotenv import load_dotenv
import arcpy
from arcpy import metadata as md

from octraffic import OCTraffic

# Initialize the OCTraffic object
ocs = OCTraffic(part = 7, version = 2025.3)

# Load environment variables from .env file
load_dotenv()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2. Project and Workspace Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.2. Project and Workspace Variables")


### Project and Geodatabase Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- ArcGIS Pro Paths")

# Get the paths for the ArcGIS Pro project and geodatabase
aprx_path = prj_dirs.get("agp_aprx", "")
gdb_path = prj_dirs.get("agp_gdb", "")
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
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.3. ArcGIS Pro Workspace ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.3. ArcGIS Pro Workspace")

# Set the workspace and environment to the root of the project geodatabase
arcpy.env.workspace = gdb_path
workspace = arcpy.env.workspace

# Enable overwriting existing outputs
arcpy.env.overwriteOutput = True

# Disable adding outputs to map
arcpy.env.addOutputsToMap = False


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.4. Map and Layout Lists ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.4. Map and Layout Lists")

### Project Maps ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Project Layouts")

# List or layouts to be created for the project
layout_list = ["maps", "injuries", "hotspots", "roads", "points", "densities", "areas"]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.5. Feature Class Definitions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.5. Feature Class Definitions")


### Raw Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Raw Data Feature Classes")

# Paths to raw data feature classes
# Get the raw data directory path, handle potential None value
raw_gdb_path = prj_dirs.get("agp_gdb_raw", "")
if raw_gdb_path is None:
    raise ValueError("Error: 'agp_gdb_raw' key not found in project directories dictionary.")
victims = os.path.join(raw_gdb_path, "victims")
parties = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "parties")
crashes = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "crashes")
collisions = os.path.join(prj_dirs.get("agp_gdb_raw", ""), "collisions")


### Supporting Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Supporting Data Feature Classes")

# Define paths for the feature classes in the supporting data feature dateset of the geodatabase
boundaries: str = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "boundaries")
cities = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "cities")
blocks = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "blocks")
roads = os.path.join(prj_dirs.get("agp_gdb_supporting", ""), "roads")


### Analysis Data Feature Classes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hot Spot Data Feature Classes")

# Define paths for the feature classes in the hot spot data feature dateset of the geodatabase
crashes_hotspots = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_hotspots")
crashes_optimized_hotspots = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_optimized_hotspots")
crashes_find_hotspots_100m1km = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_find_hotspots_100m1km")
crashes_find_hotspots_150m2km = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_find_hotspots_150m2km")
crashes_find_hotspots_100m5km = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_find_hotspots_100m5km")
crashes_hotspots_500ft_from_major_roads = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_hotspots_500ft_from_major_roads")
crashes_find_hotspots_500ft_major_roads_500ft1mi = os.path.join(prj_dirs.get("agp_gdb_hotspots", ""), "crashes_find_hotspots_500ft_major_roads_500ft1mi")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Project Maps ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2. Project Maps")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1. Setup Maps ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.1. Setup Maps")


### Remove Old Maps ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Remove Old Maps")

# First delete all raw data maps from ArcGIS pro current project
if aprx.listMaps():
    for m in aprx.listMaps():
        if m.name in map_list:
            print(f"Removing {m.name} map from the project...")
            aprx.deleteItem(m)
        else:
            print(f"Map {m.name} is not in the list of maps to be created.")
else:
    print("No maps are currently in the project.")


### Create New Maps ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Create New Maps")

# Create new raw data maps in current ArcGIS Pro project
# for each of the maps in the list, if it exists, delete it
c = 1
for m in map_list:
    for i in aprx.listMaps():
        if i.name == m:
            print(f"Deleting map: {m}")
            aprx.deleteItem(i)
    # Create new maps
    print(f"Creating map {c}: {m}")
    aprx.createMap(m)
    c += 1

# Store the map objects in variables
# OCTraffic Data Maps
map_collisions = aprx.listMaps("collisions")[0]
map_crashes = aprx.listMaps("crashes")[0]
map_parties = aprx.listMaps("parties")[0]
map_victims = aprx.listMaps("victims")[0]
map_injuries = aprx.listMaps("injuries")[0]
map_fatalities = aprx.listMaps("fatalities")[0]
# OCTraffic Hotspot Maps
map_fhs_100m1km = aprx.listMaps("fhs_100m1km")[0]
map_fhs_150m2km = aprx.listMaps("fhs_150m2km")[0]
map_fhs_100m5km = aprx.listMaps("fhs_100m5km")[0]
map_fhs_roads_500ft = aprx.listMaps("fhs_roads_500ft")[0]
map_ohs_roads_500ft = aprx.listMaps("ohs_roads_500ft")[0]
map_point_fhs = aprx.listMaps("point_fhs")[0]
map_point_ohs = aprx.listMaps("point_ohs")[0]
# OCTraffic Supporting Data Maps
map_roads = aprx.listMaps("roads")[0]
map_road_crashes = aprx.listMaps("road_crashes")[0]
map_road_hotspots = aprx.listMaps("road_hotspots")[0]
map_road_buffers = aprx.listMaps("road_buffers")[0]
map_road_segments = aprx.listMaps("road_segments")[0]
map_pop_dens = aprx.listMaps("pop_dens")[0]
map_hou_dens = aprx.listMaps("hou_dens")[0]
map_area_cities = aprx.listMaps("area_cities")[0]
map_area_blocks = aprx.listMaps("area_blocks")[0]
# OCTraffic Analysis and Processing Maps
map_summaries = aprx.listMaps("summaries")[0]
map_analysis = aprx.listMaps("analysis")[0]
map_regression = aprx.listMaps("regression")[0]


### Change Map Basemaps ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Change Map Basemaps")

# For each map, replace the existing basemap ("Topographic") with the "Dark Gray Canvas" basemap
for m in aprx.listMaps():
    print(f"Map: {m.name}")
    for l in m.listLayers():
        if l.isBasemapLayer:
            print(f"  - Removing Basemap: {l.name}")
            m.removeLayer(l)
    print(f"  - Adding Basemap: Light Gray Canvas")
    m.addBasemap("Light Gray Canvas")
# Turn off the basemap reference layer for all maps
for m in aprx.listMaps():
    print(f"Map: {m.name}")
    for l in m.listLayers():
        if l.name == "Light Gray Reference":
            l.visible = False


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2. Map Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.2. Map Metadata")


### Collisions Map 1 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Collisions Map 1 Metadata")

# Create a new metadata object for the collisions map and assign it to the map
mdo_map_collisions = md.Metadata()
mdo_map_collisions.title = "OCTraffic Collisions Map"
mdo_map_collisions.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transporation"
mdo_map_collisions.summary = f"Statewide Integrated Traffic Records System (SWITRS) Collisions Map for Orange County, California ({md_years})"
mdo_map_collisions.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">combined reports on collision crashes, parties, and victims</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_map_collisions.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_collisions.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_collisions.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_collisions.metadata = mdo_map_collisions


### Crashes Map 2 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Crashes Map 2 Metadata")

# Create a new metadata object for the crashes map and assign it to the map
mdo_map_crashes = md.Metadata()
mdo_map_crashes.title = "OCTraffic Crashes Map"
mdo_map_crashes.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_crashes.summary = f"Statewide Integrated Traffic Records System (SWITRS) Crashes Map for Orange County, California ({md_years})"
mdo_map_crashes.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">reports on crashes</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_map_crashes.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_crashes.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_crashes.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_crashes.metadata = mdo_map_crashes


### Parties Map 3 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Parties Map 3 Metadata")

# Create a new metadata object for the parties map and assign it to the map
mdo_map_parties = md.Metadata()
mdo_map_parties.title = "OCTraffic Parties Map"
mdo_map_parties.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Parties, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_parties.summary = f"Statewide Integrated Traffic Records System (SWITRS) Incident-Involved Parties Data for Orange County, California ({md_years})"
mdo_map_parties.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">reports on parties involved in crash incidents</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_map_parties_credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_parties.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_parties.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/1e07bb1002f9457fa6fd3540fdb08e29/data"

# Apply the metadata to the map
map_parties.metadata = mdo_map_parties


### Victims Map 4 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Victims Map 4 Metadata")

# Create a new metadata object for the victims map and assign it to the map
mdo_map_victims = md.Metadata()
mdo_map_victims.title = "OCTraffic Victims Map"
mdo_map_victims.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Victims, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_victims.summary = f"Statewide Integrated Traffic Records System (SWITRS) Incident-Involved Victims Data for Orange County, California ({md_years})"
mdo_map_victims.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">reports on victims/persons involved in crash incidents</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_map_victims.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_victims.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_victims.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/78682395df4744009c58625f1db0c25b/data"

# Apply the metadata to the map
map_victims.metadata = mdo_map_victims


### Injuries Map 5 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Injuries Map 5 Metadata")

# Create a new metadata object for the injuries map and assign it to the map
mdo_map_injuries = md.Metadata()
mdo_map_injuries.title = "OCTraffic Injuries Map"
mdo_map_injuries.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Injuries, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_injuries.summary = f"Statewide Integrated Traffic Records System (SWITRS) Incident-Involved Injuries Data for Orange County, California ({md_years})"
mdo_map_injuries.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">reports on injuries sustained by victims in crash incidents</span><span> in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_map_injuries.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_injuries.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_injuries.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/78682395df4744009c58625f1db0c25b/data"

# Apply the metadata to the map
map_injuries.metadata = mdo_map_injuries


### Fatalities Map 6 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Fatalities Map 6 Metadata")

# Create a new metadata object for the fatalities map and assign it to the map
mdo_map_fatalities = md.Metadata()
mdo_map_fatalities.title = "OCTraffic Fatalities Map"
mdo_map_fatalities.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transporation"
mdo_map_fatalities.summary = f"Statewide Integrated Traffic Records System (SWITRS) Fatalities Map for Orange County, California ({md_years})"
mdo_map_fatalities.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">victim fatality counts per accident in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_map_fatalities.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_fatalities.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_fatalities.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_fatalities.metadata = mdo_map_fatalities


### Hotspots (100m, 1km) Map 7 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hotspots (100m, 1km) Map 7 Metadata")

# Create a new metadata object for the hotspots (100m, 1km) map and assign it to the map
mdo_map_fhs_100m1km = md.Metadata()
mdo_map_fhs_100m1km.title = "OCTraffic Hot Spot Analysis 100m 1km Map"
mdo_map_fhs_100m1km.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_fhs_100m1km.summary = f"Statewide Integrated Traffic Records System (SWITRS) Hot Spot Analysis 100m 1km Map for Orange County, California ({md_years})"
mdo_map_fhs_100m1km.description = """<div style = "text-align:Left;"><div><div><p><span>Hot Spot Analysis for the OCTraffic Project Data using 100m bins and 1km neighborhood radius grid</span></p></div></div></div>"""
mdo_map_fhs_100m1km.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_fhs_100m1km.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_fhs_100m1km.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_fhs_100m1km.metadata = mdo_map_fhs_100m1km


### Hotspots (150m, 2km) Map 8 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hotspots (150m, 2km) Map 8 Metadata")

# Create a new metadata object for the hotspots (150m, 2km) map and assign it to the map
mdo_map_fhs_150m2km = md.Metadata()
mdo_map_fhs_150m2km.title = "OCTraffic Hot Spot Analysis 150m 2km Map"
mdo_map_fhs_150m2km.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_fhs_150m2km.summary = f"Statewide Integrated Traffic Records System (SWITRS) Hot Spot Analysis 150m 2km Map for Orange County, California ({md_years})"
mdo_map_fhs_150m2km.description = """<div style = "text-align:Left;"><div><div><p><span>Hot Spot Analysis for the OCTraffic Project Data using 150m bins and 2km neighborhood radius grid</span></p></div></div></div>"""
mdo_map_fhs_150m2km.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_fhs_150m2km.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_fhs_150m2km.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_fhs_150m2km.metadata = mdo_map_fhs_150m2km


### Hotspots (100m, 5km) Map 9 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hotspots (100m, 5km) Map 9 Metadata")

# Create a new metadata object for the hotspots (100m, 5km) map and assign it to the map
mdo_map_fhs_100m5km = md.Metadata()
mdo_map_fhs_100m5km.title = "OCTraffic Hot Spot Analysis 100m 5km Map"
mdo_map_fhs_100m5km.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_fhs_100m5km.summary = f"Statewide Integrated Traffic Records System (SWITRS) Hot Spot Analysis 100m 5km Map for Orange County, California ({md_years})"
mdo_map_fhs_100m5km.description = """<div style = "text-align:Left;"><div><div><p><span>Hot Spot Analysis for the OCTraffic Project Data using 100m bins and 5km neighborhood radius grid</span></p></div></div></div>"""
mdo_map_fhs_100m5km.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_fhs_100m5km.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_fhs_100m5km.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_fhs_100m5km.metadata = mdo_map_fhs_100m5km


### Hotspots 500ft from Major Roads Map 10 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hotspots 500ft from Major Roads Map 10 Metadata")

# Create a new metadata object for the hotspots 500ft from major roads map and assign it to the map
mdo_map_fhs_roads_500ft = md.Metadata()
mdo_map_fhs_roads_500ft.title = "OCTraffic Hot Spots within 500ft from Major Roads Map"
mdo_map_fhs_roads_500ft.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_fhs_roads_500ft.summary = f"Statewide Integrated Traffic Records System (SWITRS) Hot Spots within 500ft from Major Roads Map for Orange County, California ({md_years})"
mdo_map_fhs_roads_500ft.description = """<div style = "text-align:Left;"><div><div><p><span>Hot Spot Analysis for the OCTraffic Project Data within 500ft from Major Roads</span></p></div></div></div>"""
mdo_map_fhs_roads_500ft.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_fhs_roads_500ft.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_fhs_roads_500ft.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_fhs_roads_500ft.metadata = mdo_map_fhs_roads_500ft


### Optimized Hotspots 500ft from Major Roads Map 11 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Optimized Hotspots 500ft from Major Roads Map 11 Metadata")

# Create a new metadata object for the optimized hotspots 500ft from major roads map and assign it to the map
mdo_map_ohs_roads_500ft = md.Metadata()
mdo_map_ohs_roads_500ft.title = "OCTraffic Optimized Hot Spots within 500ft from Major Roads Map"
mdo_map_ohs_roads_500ft.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_ohs_roads_500ft.summary = f"Statewide Integrated Traffic Records System (SWITRS) Optimized Hot Spots within 500ft from Major Roads Map for Orange County, California ({md_years})"
mdo_map_ohs_roads_500ft.description = """<div style = "text-align:Left;"><div><div><p><span>Optimized Hot Spot Analysis for the OCTraffic Project Data within 500ft from Major Roads</span></p></div></div></div>"""
mdo_map_ohs_roads_500ft.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_ohs_roads_500ft.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_ohs_roads_500ft.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_ohs_roads_500ft.metadata = mdo_map_ohs_roads_500ft


### Major Road Crashes Map 12 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Major Road Crashes Map 12 Metadata")

# Create a new metadata object for the major road crashes map and assign it to the map
mdo_map_road_crashes = md.Metadata()
mdo_map_road_crashes.title = "OCTraffic Major Road Crashes Map"
mdo_map_road_crashes.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_road_crashes.summary = f"Statewide Integrated Traffic Records System (SWITRS) Road Crashes Map for Orange County, California ({md_years})"
mdo_map_road_crashes.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">reports on road crashes in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_map_road_crashes.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_road_crashes.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_road_crashes.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_road_crashes.metadata = mdo_map_road_crashes


### Major Road Hotspots Map 13 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Major Road Hotspots Map 13 Metadata")

# Create a new metadata object for the major road hotspots map and assign it to the map
mdo_map_road_hotspots = md.Metadata()
mdo_map_road_hotspots.title = "OCTraffic Major Road Hotspots Map"
mdo_map_road_hotspots.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_road_hotspots.summary = f"Statewide Integrated Traffic Records System (SWITRS) Road Hotspots Map for Orange County, California ({md_years})"
mdo_map_road_hotspots.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">reports on road hotspots in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_map_road_hotspots.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_road_hotspots.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_road_hotspots.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_road_hotspots.metadata = mdo_map_road_hotspots


### Major Road Buffers Map 14 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Major Road Buffers Map 14 Metadata")

# Create a new metadata object for the major road buffers map and assign it to the map
mdo_map_road_buffers = md.Metadata()
mdo_map_road_buffers.title = "OCTraffic Major Road Buffers Map"
mdo_map_road_buffers.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_road_buffers.summary = f"Statewide Integrated Traffic Records System (SWITRS) Road Buffers Map for Orange County, California ({md_years})"
mdo_map_road_buffers.description = f"""<div style = "text-align:Left;"><div><div><p><span style = "font-weight:bold;">Statewide Integrated Traffic Records System (SWITRS)</span><span> location point data, containing </span><span style = "font-weight:bold;">reports on road buffers in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the </span><a href = "https://www.chp.ca.gov:443/" style = "text-decoration:underline;"><span>California Highway Patrol (CHP)</span></a><span>, from incidents reported by local and government agencies. Original tabular datasets are provided by the </span><a href = "https://tims.berkeley.edu:443/" style = "text-decoration:underline;"><span>Transportation Injury Mapping System (TIMS)</span></a><span>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on <b>{date_updated}</b></span></p></div></div></div>"""
mdo_map_road_buffers.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_road_buffers.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_road_buffers.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_road_buffers.metadata = mdo_map_road_buffers


### Major Road Segments Map 16 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Major Road Segments Map 16 Metadata")

# Create a new metadata object for the roads map and assign it to the map
mdo_map_roads = md.Metadata()
mdo_map_roads.title = "OCTraffic Roads Processing Map"
mdo_map_roads.tags = "Orange County, California, Roads, Traffic, Road Safety, Transportation, Collisions, Crashes, SWITRS, OCTraffic"
mdo_map_roads.summary = "Roads Processing Map for the OCTraffic Project"
mdo_map_roads.description = """<div style = "text-align:Left;"><div><div><p><span>The Orange County Roads Network is a comprehensive representation of all roads in the area, including primary roads and highways, secondary roads, and local roads. The data are sourced from the Orange County Department of Public Works and are updated regularly to reflect the most current road network configuration.</span></p></div></div></div>"""
mdo_map_roads.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_roads.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_roads.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/76f6fbe9acbb482c9684307854d6352b/data"

# Apply the metadata to the map
map_roads.metadata = mdo_map_roads


### Hotspot Points Map 17 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Hotspot Points Map 17 Metadata")

# Create a new metadata object for the hotspot points map and assign it to the map
mdo_map_point_fhs = md.Metadata()
mdo_map_point_fhs.title = "OCTraffic Point Features Hot Spots Map"
mdo_map_point_fhs.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_point_fhs.summary = f"Statewide Integrated Traffic Records System (SWITRS) Point Features Hot Spots Map for Orange County, California ({md_years})"
mdo_map_point_fhs.description = """<div style = "text-align:Left;"><div><div><p><span>Hot Spot Analysis for the OCTraffic Project Data using point features</span></p></div></div></div>"""
mdo_map_point_fhs.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_point_fhs.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_point_fhs.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_point_fhs.metadata = mdo_map_point_fhs


### Optimized Hotspot Points Map 18 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Optimized Hotspot Points Map 18 Metadata")

# Create a new metadata object for the optimized hotspot points map and assign it to the map
mdo_map_point_ohs = md.Metadata()
mdo_map_point_ohs.title = "OCTraffic Point Features Optimized Hot Spots Map"
mdo_map_point_ohs.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_point_ohs.summary = f"Statewide Integrated Traffic Records System (SWITRS) Point Features Optimized Hot Spots Map for Orange County, California ({md_years})"
mdo_map_point_ohs.description = """<div style = "text-align:Left;"><div><div><p><span>Optimized Hot Spot Analysis for the OCTraffic Project Data using point features</span></p></div></div></div>"""
mdo_map_point_ohs.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_point_ohs.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_point_ohs.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_point_ohs.metadata = mdo_map_point_ohs


### Population Density Map 19 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Population Density Map 19 Metadata")

# Create a new metadata object for the population density map and assign it to the map
mdo_map_pop_dens = md.Metadata()
mdo_map_pop_dens.title = "OCTraffic Population Density Map"
mdo_map_pop_dens.tags = "Orange County, California, Roads, Traffic, Road Safety, Transportation, Collisions, Crashes, SWITRS, OCTraffic"
mdo_map_pop_dens.summary = "Population Density Map for the OCTraffic Project"
mdo_map_pop_dens.description = """<div style = "text-align:Left;"><div><div><p><span>The map displays the population density (per square mile) in Orange County by Census Blocks (US Census 2000). The data source is the US Centennial Census of 2000.</span></p></div></div></div>"""
mdo_map_pop_dens.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_pop_dens.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_pop_dens.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/76f6fbe9acbb482c9684307854d6352b/data"

# Apply the metadata to the map
map_pop_dens.metadata = mdo_map_pop_dens


### Housing Density Map 20 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Housing Density Map 20 Metadata")

# Create a new metadata object for the housing density map and assign it to the map
mdo_map_hou_dens = md.Metadata()
mdo_map_hou_dens.title = "OCTraffic Housing Density Map"
mdo_map_hou_dens.tags = "Orange County, California, Roads, Traffic, Road Safety, Transportation, Collisions, Crashes, SWITRS, OCTraffic"
mdo_map_hou_dens.summary = "Housing Density Map for the OCTraffic Project"
mdo_map_hou_dens.description = """<div style = "text-align:Left;"><div><div><p><span>The map displays the housing density (per square mile) in Orange County by Census Blocks (US Census 2000). The data source is the US Centennial Census of 2000.</span></p></div></div></div>"""
mdo_map_hou_dens.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_hou_dens.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_hou_dens.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/76f6fbe9acbb482c9684307854d6352b/data"

# Apply the metadata to the map
map_hou_dens.metadata = mdo_map_hou_dens


### City Areas Victims Map 21 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- City Areas Victims Map 21 Metadata")

# Create a new metadata object for the victims by city areas map and assign it to the map
mdo_map_area_cities = md.Metadata()
mdo_map_area_cities.title = "OCTraffic City Areas Victims Count Map"
mdo_map_area_cities.tags = "Orange County, California, Roads, Traffic, Road Safety, Transportation, Collisions, Crashes, SWITRS, OCTraffic"
mdo_map_area_cities.summary = "City Areas Victims Count Map for the OCTraffic Project"
mdo_map_area_cities.description = """<div style = "text-align:Left;"><div><div><p><span>The map displays the number of victims involved in traffic incidents in Orange County by City Areas. The data source is the SWITRS database maintained by the California Highway Patrol (CHP).</span></p></div></div></div>"""
mdo_map_area_cities.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_area_cities.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_area_cities.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/76f6fbe9acbb482c9684307854d6352b/data"

# Apply the metadata to the map
map_area_cities.metadata = mdo_map_area_cities


### Census Blocks Victims Map 22 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Census Blocks Victims Map 22 Metadata")

# Create a new metadata object for the victims by census blocks map and assign it to the map
mdo_map_area_blocks = md.Metadata()
mdo_map_area_blocks.title = "OCTraffic Census Blocks Victims Count Map"
mdo_map_area_blocks.tags = "Orange County, California, Roads, Traffic, Road Safety, Transportation, Collisions, Crashes, SWITRS, OCTraffic"
mdo_map_area_blocks.summary = "Census Blocks Victims Count Map for the OCTraffic Project"
mdo_map_area_blocks.description = """<div style = "text-align:Left;"><div><div><p><span>The map displays the number of victims involved in traffic incidents in Orange County by Census Blocks. The data source is the SWITRS database maintained by the California Highway Patrol (CHP).</span></p></div></div></div>"""
mdo_map_area_blocks.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_area_blocks.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_area_blocks.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/76f6fbe9acbb482c9684307854d6352b/data"

# Apply the metadata to the map
map_area_blocks.metadata = mdo_map_area_blocks


### Summaries Map 23 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Summaries Map 23 Metadata")

# Create a new metadata object for the summaries map and assign it to the map
mdo_map_summaries = md.Metadata()
mdo_map_summaries.title = "OCTraffic Summaries Map"
mdo_map_summaries.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_summaries.summary = f"Statewide Integrated Traffic Records System (SWITRS) Summaries Map for Orange County, California ({md_years})"
mdo_map_summaries.description = """<div style = "text-align:Left;"><div><div><p><span>Summarized data representation and visualization for the Orange County OCTraffic traffic incident data.</span></p></div></div></div>"""
mdo_map_summaries.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_summaries.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_summaries.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_summaries.metadata = mdo_map_summaries


### Analysis Map 24 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Analysis Map 24 Metadata")

# Create a new metadata object for the analysis map and assign it to the map
mdo_map_analysis = md.Metadata()
mdo_map_analysis.title = "OCTraffic Analysis Map"
mdo_map_analysis.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_analysis.summary = f"Statewide Integrated Traffic Records System (SWITRS) Analysis Map for Orange County, California ({md_years})"
mdo_map_analysis.description = """<div style = "text-align:Left;"><div><div><p><span>Analysis and visualization for the OCTraffic Project Data</span></p></div></div></div>"""
mdo_map_analysis.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_analysis.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_analysis.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_analysis.metadata = mdo_map_analysis


### Regression Map 25 Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Regression Map 25 Metadata")

# Create a new metadata object for the regression map and assign it to the map
mdo_map_regression = md.Metadata()
mdo_map_regression.title = "OCTraffic Regression Analysis Map"
mdo_map_regression.tags = "Orange County, California, Traffic, Traffic Conditions, Crashes, Collisions, Road Safety, Accidents, SWITRS, OCTraffic, Transportation"
mdo_map_regression.summary = f"Statewide Integrated Traffic Records System (SWITRS) Regression Analysis Map for Orange County, California ({md_years})"
mdo_map_regression.description = """<div style = "text-align:Left;"><div><div><p><span>Regression Analysis for the OCTraffic Project Data</span></p></div></div></div>"""
mdo_map_regression.credits = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"
mdo_map_regression.accessConstraints = """<div style = "text-align:Left;"><p><span>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.</span></p><p>The displayed mapped data can be used under a <a href = "https://creativecommons.org/licenses/by-sa/3.0/" target = "_blank">Creative Commons CC-SA-BY</a> License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services. </p><div>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard <a href = "https://www.ocgov.com/contact-county/disclaimer" target = "_blank">Disclaimer</a> applies.<br /></div><div><br /></div><div>For any inquiries, suggestions or questions, please contact:</div><div><br /></div><div style = "text-align:center;"><a href = "https://www.linkedin.com/in/ktalexan/" target = "_blank"><b>Dr. Kostas Alexandridis, GISP</b></a><br /></div><div style = "text-align:center;">GIS Analyst | Spatial Complex Systems Scientist</div><div style = "text-align:center;">OC Public Works/OC Survey Geospatial Applications</div><div style = "text-align:center;"><div>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701</div><div>Email: <a href = "mailto:kostas.alexandridis@ocpw.ocgov.com" target = "_blank">kostas.alexandridis@ocpw.ocgov.com</a> | Phone: (714) 967-0826</div><div><br /></div></div></div>"""
mdo_map_regression.thumbnailUri = "https://ocpw.maps.arcgis.com/sharing/rest/content/items/6b96b7d6d5394cbb95aa2fae390503a9/data"

# Apply the metadata to the map
map_regression.metadata = mdo_map_regression


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Map Layers Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\nMap Layers Processing")

# In this section we will be creating map layers for the feature classes in the geodatabase. The layers will be added to the maps and layouts.


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.1 Time Settings Configuration ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.1 Time Settings Configuration")

dt_start = dt.datetime.combine(prj_meta["date_start"], dt.time(0, 0, 0))
dt_end = dt.datetime.combine(prj_meta["date_end"], dt.time(23, 59, 59))
dt_diff = dt_end - dt_start

# Define time settings configuration for the map layers
# Define the key time parameters for the layers using a dictionary
time_settings = {
    "st": dt_start,
    "et": dt_end,
    "td": dt_diff,
    "stf": "date_datetime",
    "tsi": 1.0,
    "tsiu": "months",
    "tz": arcpy.mp.ListTimeZones("*Pacific*")[0],
}
# where, st: start time, et: end time, td: time extent, stf: time field,
# tsi: time interval, tsiu: time units, tz: time zone


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.2 Collisions Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.2 Collisions Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all previous map views
aprx.closeViews()
# Open the roads map view
map_collisions.openView()
# set the main map as active map
map = aprx.activeMap
# Remove all layers from the active map
for lyr in map_collisions.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_collisions.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_collisions_lyr_boundaries = map_collisions.addDataFromPath(boundaries)
map_collisions_lyr_cities = map_collisions.addDataFromPath(cities)
map_collisions_lyr_blocks = map_collisions.addDataFromPath(blocks)
map_collisions_lyr_roads = map_collisions.addDataFromPath(roads)
map_collisions_lyr_collisions = map_collisions.addDataFromPath(collisions)
# Set layer visibility on the map
map_collisions_lyr_boundaries.visible = False
map_collisions_lyr_cities.visible = False
map_collisions_lyr_blocks.visible = False
map_collisions_lyr_roads.visible = False
map_collisions_lyr_collisions.visible = False


### Enable Time Settings ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Enable Time Settings")

# Setup and enable time settings for the collisions map layers
ocs.set_layer_time(time_settings = time_settings, layer = map_collisions_lyr_collisions)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Collisions layer
# Apply the symbology for the Collisions data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_collisions_lyr_collisions,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Collisions.lyrx"),
    symbology_fields = [["VALUE_FIELD", "coll_severity", "coll_severity"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Roads layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_collisions_lyr_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_collisions_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities layer
# Apply the symbology for the Cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_collisions_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_collisions_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the collisions map layers
map_collisions_cim_boundaries = map_collisions_lyr_boundaries.getDefinition("V3")
map_collisions_cim_cities = map_collisions_lyr_cities.getDefinition("V3")
map_collisions_cim_blocks = map_collisions_lyr_blocks.getDefinition("V3")
map_collisions_cim_roads = map_collisions_lyr_roads.getDefinition("V3")
map_collisions_cim_collisions = map_collisions_lyr_collisions.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_collisions_cim_boundaries.renderer.heading = "Boundaries"
map_collisions_cim_cities.renderer.heading = "City Population Density"
map_collisions_cim_blocks.renderer.heading = "Population Density"
map_collisions_cim_roads.renderer.heading = "Road Categories"
map_collisions_cim_collisions.renderer.heading = "Severity Level"
# Update the map layer definitions
map_collisions_lyr_boundaries.setDefinition(map_collisions_cim_boundaries)
map_collisions_lyr_cities.setDefinition(map_collisions_cim_cities)
map_collisions_lyr_blocks.setDefinition(map_collisions_cim_blocks)
map_collisions_lyr_roads.setDefinition(map_collisions_cim_roads)
map_collisions_lyr_collisions.setDefinition(map_collisions_cim_collisions)
# Update the CIM definition for the collisions map
cim_collisions = map_collisions.getDefinition("V3")  # Collisions map CIM


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the collisions map mapx file
export_cim("map", map_collisions, "collisions")
# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_collisions.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.3 Crashes Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.3 Crashes Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all previous map views
aprx.closeViews()
# Open the crashes map view
map_crashes.openView()
# set the main map as active map
map = aprx.activeMap
# Remove all layers from the active map
for lyr in map_crashes.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_crashes.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_crashes_lyr_boundaries = map_crashes.addDataFromPath(boundaries)
map_crashes_lyr_cities = map_crashes.addDataFromPath(cities)
map_crashes_lyr_blocks = map_crashes.addDataFromPath(blocks)
map_crashes_lyr_roads = map_crashes.addDataFromPath(roads)
map_crashes_lyr_crashes = map_crashes.addDataFromPath(crashes)
# Set layer visibility on the map
map_crashes_lyr_boundaries.visible = False
map_crashes_lyr_cities.visible = False
map_crashes_lyr_blocks.visible = False
map_crashes_lyr_roads.visible = False
map_crashes_lyr_crashes.visible = False


### Enable Time Settings ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Enable Time Settings")

# Setup and enable time settings for the crashes map layers
ocs.set_layer_time(time_settings = time_settings, layer = map_crashes_lyr_crashes)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Crashes layer
# Apply the symbology for the Collisions data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_crashes_lyr_crashes,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Crashes.lyrx"),
    symbology_fields = [["VALUE_FIELD", "coll_severity", "coll_severity"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Roads layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_crashes_lyr_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_crashes_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities layer
# Apply the symbology for the Cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_crashes_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_crashes_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the crashes map layers
map_crashes_cim_boundaries = map_crashes_lyr_boundaries.getDefinition("V3")
map_crashes_cim_cities = map_crashes_lyr_cities.getDefinition("V3")
map_crashes_cim_blocks = map_crashes_lyr_blocks.getDefinition("V3")
map_crashes_cim_roads = map_crashes_lyr_roads.getDefinition("V3")
map_crashes_cim_crashes = map_crashes_lyr_crashes.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_crashes_cim_boundaries.renderer.heading = "Boundaries"
map_crashes_cim_cities.renderer.heading = "City Population Density"
map_crashes_cim_blocks.renderer.heading = "Population Density"
map_crashes_cim_roads.renderer.heading = "Road Categories"
map_crashes_cim_crashes.renderer.heading = "Severity Level"
# Update the map layer definitions
map_crashes_lyr_boundaries.setDefinition(map_crashes_cim_boundaries)
map_crashes_lyr_cities.setDefinition(map_crashes_cim_cities)
map_crashes_lyr_blocks.setDefinition(map_crashes_cim_blocks)
map_crashes_lyr_roads.setDefinition(map_crashes_cim_roads)
map_crashes_lyr_crashes.setDefinition(map_crashes_cim_crashes)
# Update the CIM definition for the crashes map
cim_crashes = map_crashes.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the crashes map mapx file
export_cim("map", map_crashes, "crashes")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_crashes.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.4 Parties Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.4 Parties Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all previous map views
aprx.closeViews()
# Open the parties map view
map_parties.openView()
# set the main map as active map
map = aprx.activeMap
# Remove all layers from the active map
for lyr in map_parties.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_parties.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_parties_lyr_boundaries = map_parties.addDataFromPath(boundaries)
map_parties_lyr_cities = map_parties.addDataFromPath(cities)
map_parties_lyr_blocks = map_parties.addDataFromPath(blocks)
map_parties_lyr_roads = map_parties.addDataFromPath(roads)
map_parties_lyr_parties = map_parties.addDataFromPath(parties)
# Set layer visibility on the map
map_parties_lyr_boundaries.visible = False
map_parties_lyr_cities.visible = False
map_parties_lyr_blocks.visible = False
map_parties_lyr_roads.visible = False
map_parties_lyr_parties.visible = False


### Enable Time Settings ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Enable Time Settings")

# Setup and enable time settings for the parties map layers
ocs.set_layer_time(time_settings = time_settings, layer = map_parties_lyr_parties)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Parties layer
# Apply the symbology for the Parties data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_parties_lyr_parties,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Parties.lyrx"),
    symbology_fields = [["VALUE_FIELD", "coll_severity", "coll_severity"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Roads layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_parties_lyr_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_parties_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities layer
# Apply the symbology for the Cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_parties_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_parties_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the parties map layers
map_parties_cim_boundaries = map_parties_lyr_boundaries.getDefinition("V3")
map_parties_cim_cities = map_parties_lyr_cities.getDefinition("V3")
map_parties_cim_blocks = map_parties_lyr_blocks.getDefinition("V3")
map_parties_cim_roads = map_parties_lyr_roads.getDefinition("V3")
map_parties_cim_parties = map_parties_lyr_parties.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_parties_cim_boundaries.renderer.heading = "Boundaries"
map_parties_cim_cities.renderer.heading = "City Population Density"
map_parties_cim_blocks.renderer.heading = "Population Density"
map_parties_cim_roads.renderer.heading = "Road Categories"
map_parties_cim_parties.renderer.heading = "Severity Level"
# Update the map layer definitions
map_parties_lyr_boundaries.setDefinition(map_parties_cim_boundaries)
map_parties_lyr_cities.setDefinition(map_parties_cim_cities)
map_parties_lyr_blocks.setDefinition(map_parties_cim_blocks)
map_parties_lyr_roads.setDefinition(map_parties_cim_roads)
map_parties_lyr_parties.setDefinition(map_parties_cim_parties)
# Update the CIM definition for the parties map
cim_parties = map_parties.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the parties map mapx file
export_cim("map", map_parties, "parties")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_parties.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.5 Victims Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.5 Victims Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all previous map views
aprx.closeViews()
# Open the victims map view
map_victims.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_victims.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_victims.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_victims_lyr_boundaries = map_victims.addDataFromPath(boundaries)
map_victims_lyr_cities = map_victims.addDataFromPath(cities)
map_victims_lyr_blocks = map_victims.addDataFromPath(blocks)
map_victims_lyr_roads = map_victims.addDataFromPath(roads)
map_victims_lyr_victims = map_victims.addDataFromPath(victims)
# Set layer visibility on the map
map_victims_lyr_boundaries.visible = False
map_victims_lyr_cities.visible = False
map_victims_lyr_blocks.visible = False
map_victims_lyr_roads.visible = False
map_victims_lyr_victims.visible = False


### Enable Time Settings ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Enable Time Settings")

# Setup and enable time settings for the victims map layers
ocs.set_layer_time(time_settings = time_settings, layer = map_victims_lyr_victims)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Victims layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_victims_lyr_victims,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Victims.lyrx"),
    symbology_fields = [["VALUE_FIELD", "coll_severity", "coll_severity"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_victims_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities layer
# Apply the symbology for the Cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_victims_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_victims_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the victims map layers
map_victims_cim_boundaries = map_victims_lyr_boundaries.getDefinition("V3")
map_victims_cim_cities = map_victims_lyr_cities.getDefinition("V3")
map_victims_cim_blocks = map_victims_lyr_blocks.getDefinition("V3")
map_victims_cim_roads = map_victims_lyr_roads.getDefinition("V3")
map_victims_cim_victims = map_victims_lyr_victims.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_victims_cim_boundaries.renderer.heading = "Boundaries"
map_victims_cim_cities.renderer.heading = "City Population Density"
map_victims_cim_blocks.renderer.heading = "Population Density"
map_victims_cim_roads.renderer.heading = "Road Categories"
map_victims_cim_victims.renderer.heading = "Severity Level"
# Update the map layer definitions
map_victims_lyr_boundaries.setDefinition(map_victims_cim_boundaries)
map_victims_lyr_cities.setDefinition(map_victims_cim_cities)
map_victims_lyr_blocks.setDefinition(map_victims_cim_blocks)
map_victims_lyr_roads.setDefinition(map_victims_cim_roads)
map_victims_lyr_victims.setDefinition(map_victims_cim_victims)
# Update the CIM definition for the victims map
cim_victims = map_victims.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_victims, "victims")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_victims.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.6 Injuries Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.6 Injuries Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all previous map views
aprx.closeViews()
# Open the injuries map view
map_injuries.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_injuries.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_injuries.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_injuries_lyr_boundaries = map_injuries.addDataFromPath(boundaries)
map_injuries_lyr_cities = map_injuries.addDataFromPath(cities)
map_injuries_lyr_blocks = map_injuries.addDataFromPath(blocks)
map_injuries_lyr_victims = map_injuries.addDataFromPath(victims)
# Set layer visibility on the map
map_injuries_lyr_boundaries.visible = False
map_injuries_lyr_cities.visible = False
map_injuries_lyr_blocks.visible = False
map_injuries_lyr_victims.visible = False


### Enable Time Settings ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Victims layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_injuries_lyr_victims,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Victims.lyrx"),
    symbology_fields = [["VALUE_FIELD", "coll_severity", "coll_severity"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_injuries_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities layer
# Apply the symbology for the Cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_injuries_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_injuries_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the regression map layers
map_injuries_cim_boundaries = map_injuries_lyr_boundaries.getDefinition("V3")
map_injuries_cim_cities = map_injuries_lyr_cities.getDefinition("V3")
map_injuries_cim_blocks = map_injuries_lyr_blocks.getDefinition("V3")
map_injuries_cim_victims = map_injuries_lyr_victims.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_injuries_cim_boundaries.renderer.heading = "Boundaries"
map_injuries_cim_cities.renderer.heading = "City Population Density"
map_injuries_cim_blocks.renderer.heading = "Population Density"
map_injuries_cim_victims.renderer.heading = "Severity Level"
# Update the map layer definitions
map_injuries_lyr_boundaries.setDefinition(map_injuries_cim_boundaries)
map_injuries_lyr_cities.setDefinition(map_injuries_cim_cities)
map_injuries_lyr_blocks.setDefinition(map_injuries_cim_blocks)
map_injuries_lyr_victims.setDefinition(map_injuries_cim_victims)
# Update the CIM definition for the regression map
cim_injuries = map_injuries.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_injuries, "injuries")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_injuries.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.7 Fatalities Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.7 Fatalities Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all previous map views
aprx.closeViews()
# Open the fatalities map view
map_fatalities.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_fatalities.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_fatalities.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_fatalities_lyr_boundaries = map_fatalities.addDataFromPath(boundaries)
map_fatalities_lyr_roads = map_fatalities.addDataFromPath(roads)
map_fatalities_lyr_roads_major_buffers = map_fatalities.addDataFromPath(roads_major_buffers)
map_fatalities_lyr_fatalities = map_fatalities.addDataFromPath(crashes)
# Set layer visibility on the map
map_fatalities_lyr_boundaries.visible = False
map_fatalities_lyr_roads.visible = False
map_fatalities_lyr_roads_major_buffers.visible = False
map_fatalities_lyr_fatalities.visible = False
# Move layers
map_fatalities.moveLayer(
    reference_layer = map_fatalities_lyr_roads,
    move_layer = map_fatalities_lyr_roads_major_buffers,
    insert_position = "BEFORE"
)
map_fatalities.moveLayer(
    reference_layer = map_fatalities_lyr_roads_major_buffers,
    move_layer = map_fatalities_lyr_fatalities,
    insert_position = "BEFORE"
)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Fatalities layer
# Apply the symbology for the Fatalities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fatalities_lyr_fatalities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Crashes Killed Victims.lyrx"),
    symbology_fields = [["VALUE_FIELD", "number_killed", "number_killed"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Major Road Buffers Layer
# Apply the symbology for the Major Road Buffers data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fatalities_lyr_roads_major_buffers,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads Buffers.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Roads layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fatalities_lyr_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fatalities_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the fatalities map layers
map_fatalities_cim_boundaries = map_fatalities_lyr_boundaries.getDefinition("V3")
map_fatalities_cim_roads = map_fatalities_lyr_roads.getDefinition("V3")
map_fatalities_cim_roads_major_buffers = map_fatalities_lyr_roads_major_buffers.getDefinition("V3")
map_fatalities_cim_Fatalities = map_fatalities_lyr_fatalities.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_fatalities_cim_boundaries.renderer.heading = "Boundaries"
map_fatalities_cim_roads.renderer.heading = "Road Categories"
map_fatalities_cim_roads_major_buffers.renderer.heading = "Major Road Buffers"
map_fatalities_cim_Fatalities.renderer.heading = "Victims Killed"
# Update the map layer definitions
map_fatalities_lyr_boundaries.setDefinition(map_fatalities_cim_boundaries)
map_fatalities_lyr_roads.setDefinition(map_fatalities_cim_roads)
map_fatalities_lyr_roads_major_buffers.setDefinition(map_fatalities_cim_roads_major_buffers)
map_fatalities_lyr_fatalities.setDefinition(map_fatalities_cim_Fatalities)
# Update the CIM definition for the fatalities map
cim_fatalities = map_fatalities.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the fatalities map mapx file
export_cim("map", map_fatalities, "fatalities")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_fatalities.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.8 Hotspots (100m, 1km) Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.8 Hotspots (100m, 1km) Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all previous map views
aprx.closeViews()
# Open the hotspots_100m1km map view
map_fhs_100m1km.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_fhs_100m1km.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_fhs_100m1km.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_fhs_100m1km_lyr_boundaries = map_fhs_100m1km.addDataFromPath(boundaries)
map_fhs_100m1km_lyr_cities = map_fhs_100m1km.addDataFromPath(cities)
map_fhs_100m1km_lyr_blocks = map_fhs_100m1km.addDataFromPath(blocks)
map_fhs_100m1km_lyr_roads = map_fhs_100m1km.addDataFromPath(roads)
map_fhs_100m1km_lyr_fhs_100m1km = map_fhs_100m1km.addDataFromPath(crashes_find_hotspots_100m1km)
# Set layer visibility on the map
map_fhs_100m1km_lyr_boundaries.visible = False
map_fhs_100m1km_lyr_cities.visible = False
map_fhs_100m1km_lyr_blocks.visible = False
map_fhs_100m1km_lyr_roads.visible = False
map_fhs_100m1km_lyr_fhs_100m1km.visible = False
# Move layers
map_fhs_100m1km.moveLayer(
    reference_layer = map_fhs_100m1km_lyr_boundaries,
    move_layer = map_fhs_100m1km_lyr_roads,
    insert_position = "AFTER"
)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Roads layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_100m1km_lyr_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_100m1km_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities layer
# Apply the symbology for the Cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_100m1km_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_100m1km_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the hotspots_100m1km map layers
map_fhs_100m1km_cim_boundaries = map_fhs_100m1km_lyr_boundaries.getDefinition("V3")
map_fhs_100m1km_cim_cities = map_fhs_100m1km_lyr_cities.getDefinition("V3")
map_fhs_100m1km_cim_blocks = map_fhs_100m1km_lyr_blocks.getDefinition("V3")
map_fhs_100m1km_cim_roads = map_fhs_100m1km_lyr_roads.getDefinition("V3")
map_fhs_100m1km_cim_fhs_100m1km = map_fhs_100m1km_lyr_fhs_100m1km.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_fhs_100m1km_cim_boundaries.renderer.heading = "Boundaries"
map_fhs_100m1km_cim_cities.renderer.heading = "City Population Density"
map_fhs_100m1km_cim_blocks.renderer.heading = "Population Density"
map_fhs_100m1km_cim_roads.renderer.heading = "Road Categories"
map_fhs_100m1km_cim_fhs_100m1km.renderer.heading = "Getis-Ord Gi*"
# Update the map layer definitions
map_fhs_100m1km_lyr_boundaries.setDefinition(map_fhs_100m1km_cim_boundaries)
map_fhs_100m1km_lyr_cities.setDefinition(map_fhs_100m1km_cim_cities)
map_fhs_100m1km_lyr_blocks.setDefinition(map_fhs_100m1km_cim_blocks)
map_fhs_100m1km_lyr_roads.setDefinition(map_fhs_100m1km_cim_roads)
map_fhs_100m1km_lyr_fhs_100m1km.setDefinition(map_fhs_100m1km_cim_fhs_100m1km)
# Update the CIM definition for the hotspots (100m, 1km) map
cim_fhs_100m1km = map_fhs_100m1km.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_fhs_100m1km, "hotspots_100m1km")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_fhs_100m1km.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.9 Hotspots (150m, 2km) Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.9 Hotspots (150m, 2km) Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all previous map views
aprx.closeViews()
# Open the hotspots_150m2km map view
map_fhs_150m2km.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_fhs_150m2km.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_fhs_150m2km.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_fhs_150m2km_lyr_boundaries = map_fhs_150m2km.addDataFromPath(boundaries)
map_fhs_150m2km_lyr_cities = map_fhs_150m2km.addDataFromPath(cities)
map_fhs_150m2km_lyr_blocks = map_fhs_150m2km.addDataFromPath(blocks)
map_fhs_150m2km_lyr_roads = map_fhs_150m2km.addDataFromPath(roads)
map_fhs_150m2km_lyr_fhs_150m2km = map_fhs_150m2km.addDataFromPath(crashes_find_hotspots_150m2km)
# Set layer visibility on the map
map_fhs_150m2km_lyr_boundaries.visible = False
map_fhs_150m2km_lyr_cities.visible = False
map_fhs_150m2km_lyr_blocks.visible = False
map_fhs_150m2km_lyr_roads.visible = False
map_fhs_150m2km_lyr_fhs_150m2km.visible = False
# Move layers
map_fhs_150m2km.moveLayer(
    reference_layer = map_fhs_150m2km_lyr_boundaries,
    move_layer = map_fhs_150m2km_lyr_roads,
    insert_position = "AFTER"
)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Roads layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_150m2km_lyr_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_150m2km_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities layer
# Apply the symbology for the Cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_150m2km_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_150m2km_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the hotspots_150m2km map layers
map_fhs_150m2km_cim_boundaries = map_fhs_150m2km_lyr_boundaries.getDefinition("V3")
map_fhs_150m2km_cim_cities = map_fhs_150m2km_lyr_cities.getDefinition("V3")
map_fhs_150m2km_cim_blocks = map_fhs_150m2km_lyr_blocks.getDefinition("V3")
map_fhs_150m2km_cim_roads = map_fhs_150m2km_lyr_roads.getDefinition("V3")
map_fhs_150m2km_cim_fhs_150m2km = map_fhs_150m2km_lyr_fhs_150m2km.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_fhs_150m2km_cim_boundaries.renderer.heading = "Boundaries"
map_fhs_150m2km_cim_cities.renderer.heading = "City Population Density"
map_fhs_150m2km_cim_blocks.renderer.heading = "Population Density"
map_fhs_150m2km_cim_roads.renderer.heading = "Road Categories"
map_fhs_150m2km_cim_fhs_150m2km.renderer.heading = "Getis-Ord Gi*"
# Update the map layer definitions
map_fhs_150m2km_lyr_boundaries.setDefinition(map_fhs_150m2km_cim_boundaries)
map_fhs_150m2km_lyr_cities.setDefinition(map_fhs_150m2km_cim_cities)
map_fhs_150m2km_lyr_blocks.setDefinition(map_fhs_150m2km_cim_blocks)
map_fhs_150m2km_lyr_roads.setDefinition(map_fhs_150m2km_cim_roads)
map_fhs_150m2km_lyr_fhs_150m2km.setDefinition(map_fhs_150m2km_cim_fhs_150m2km)
# Update the CIM definition for the hotspots (150m, 2km) map
cim_fhs_150m2km = map_fhs_150m2km.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_fhs_150m2km, "hotspots_150m2km")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_fhs_150m2km.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.10 Hotspots (100m, 5km) Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.10 Hotspots (100m, 5km) Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all previous map views
aprx.closeViews()
# Open the hotspots_100m5km map view
map_fhs_100m5km.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_fhs_100m5km.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_fhs_100m5km.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_fhs_100m5km_lyr_boundaries = map_fhs_100m5km.addDataFromPath(boundaries)
map_fhs_100m5km_lyr_cities = map_fhs_100m5km.addDataFromPath(cities)
map_fhs_100m5km_lyr_blocks = map_fhs_100m5km.addDataFromPath(blocks)
map_fhs_100m5km_lyr_roads = map_fhs_100m5km.addDataFromPath(roads)
map_fhs_100m5km_lyr_fhs_100m5km = map_fhs_100m5km.addDataFromPath(crashes_find_hotspots_100m5km)
# Set layer visibility on the map
map_fhs_100m5km_lyr_boundaries.visible = False
map_fhs_100m5km_lyr_cities.visible = False
map_fhs_100m5km_lyr_blocks.visible = False
map_fhs_100m5km_lyr_roads.visible = False
map_fhs_100m5km_lyr_fhs_100m5km.visible = False
# Move layers
map_fhs_100m5km.moveLayer(
    reference_layer = map_fhs_100m5km_lyr_boundaries,
    move_layer = map_fhs_100m5km_lyr_roads,
    insert_position = "AFTER"
)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Roads layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_100m5km_lyr_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_100m5km_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities layer
# Apply the symbology for the Cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_100m5km_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_100m5km_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the hotspots_100m5km map layers
map_fhs_100m5km_cim_boundaries = map_fhs_100m5km_lyr_boundaries.getDefinition("V3")
map_fhs_100m5km_cim_cities = map_fhs_100m5km_lyr_cities.getDefinition("V3")
map_fhs_100m5km_cim_blocks = map_fhs_100m5km_lyr_blocks.getDefinition("V3")
map_fhs_100m5km_cim_roads = map_fhs_100m5km_lyr_roads.getDefinition("V3")
map_fhs_100m5km_cim_fhs_100m5km = map_fhs_100m5km_lyr_fhs_100m5km.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_fhs_100m5km_cim_boundaries.renderer.heading = "Boundaries"
map_fhs_100m5km_cim_cities.renderer.heading = "City Population Density"
map_fhs_100m5km_cim_blocks.renderer.heading = "Population Density"
map_fhs_100m5km_cim_roads.renderer.heading = "Road Categories"
map_fhs_100m5km_cim_fhs_100m5km.renderer.heading = "Getis-Ord Gi*"
# Update the map layer definitions
map_fhs_100m5km_lyr_boundaries.setDefinition(map_fhs_100m5km_cim_boundaries)
map_fhs_100m5km_lyr_cities.setDefinition(map_fhs_100m5km_cim_cities)
map_fhs_100m5km_lyr_blocks.setDefinition(map_fhs_100m5km_cim_blocks)
map_fhs_100m5km_lyr_roads.setDefinition(map_fhs_100m5km_cim_roads)
map_fhs_100m5km_lyr_fhs_100m5km.setDefinition(map_fhs_100m5km_cim_fhs_100m5km)
# Update the CIM definition for the hotspots (100m, 5km) map
cim_fhs_100m5km = map_fhs_100m5km.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_fhs_100m5km, "hotspots_100m5km")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_fhs_100m5km.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.11 Hotspots 500ft from Major Roads Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.11 Hotspots 500ft from Major Roads Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Open the hotspots 500ft from major roads map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the hotspotsroads500ft map view
map_fhs_roads_500ft.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_fhs_roads_500ft.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_fhs_roads_500ft.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_fhs_roads_500ft_lyr_boundaries = map_fhs_roads_500ft.addDataFromPath(boundaries)
map_fhs_roads_500ft_lyr_cities = map_fhs_roads_500ft.addDataFromPath(cities)
map_fhs_roads_500ft_lyr_blocks = map_fhs_roads_500ft.addDataFromPath(blocks)
map_fhs_roads_500ft_lyr_roads = map_fhs_roads_500ft.addDataFromPath(roads)
map_fhs_roads_500ft_lyr_fhs_roads_500ft = map_fhs_roads_500ft.addDataFromPath(
    crashes_find_hotspots_500ft_major_roads_500ft1mi
)
# Set layer visibility on the map
map_fhs_roads_500ft_lyr_boundaries.visible = False
map_fhs_roads_500ft_lyr_cities.visible = False
map_fhs_roads_500ft_lyr_blocks.visible = False
map_fhs_roads_500ft_lyr_roads.visible = False
map_fhs_roads_500ft_lyr_fhs_roads_500ft.visible = False
# Move layers
map_fhs_roads_500ft.moveLayer(
    reference_layer = map_fhs_roads_500ft_lyr_boundaries,
    move_layer = map_fhs_roads_500ft_lyr_roads,
    insert_position = "AFTER"
)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Roads layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_roads_500ft_lyr_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_roads_500ft_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities layer
# Apply the symbology for the Cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_roads_500ft_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_fhs_roads_500ft_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the hotspotsroads500ft map layers
map_fhs_roads_500ft_cim_boundaries = map_fhs_roads_500ft_lyr_boundaries.getDefinition("V3")
map_fhs_roads_500ft_cim_cities = map_fhs_roads_500ft_lyr_cities.getDefinition("V3")
map_fhs_roads_500ft_cim_blocks = map_fhs_roads_500ft_lyr_blocks.getDefinition("V3")
map_fhs_roads_500ft_cim_roads = map_fhs_roads_500ft_lyr_roads.getDefinition("V3")
map_fhs_roads_500ft_cim_fhs_roads_500ft = map_fhs_roads_500ft_lyr_fhs_roads_500ft.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_fhs_roads_500ft_cim_boundaries.renderer.heading = "Boundaries"
map_fhs_roads_500ft_cim_cities.renderer.heading = "City Population Density"
map_fhs_roads_500ft_cim_blocks.renderer.heading = "Population Density"
map_fhs_roads_500ft_cim_roads.renderer.heading = "Road Categories"
map_fhs_roads_500ft_cim_fhs_roads_500ft.renderer.heading = "Getis-Ord Gi*"
# Update the map layer definitions
map_fhs_roads_500ft_lyr_boundaries.setDefinition(map_fhs_roads_500ft_cim_boundaries)
map_fhs_roads_500ft_lyr_cities.setDefinition(map_fhs_roads_500ft_cim_cities)
map_fhs_roads_500ft_lyr_blocks.setDefinition(map_fhs_roads_500ft_cim_blocks)
map_fhs_roads_500ft_lyr_roads.setDefinition(map_fhs_roads_500ft_cim_roads)
map_fhs_roads_500ft_lyr_fhs_roads_500ft.setDefinition(map_fhs_roads_500ft_cim_fhs_roads_500ft)
# Update the CIM definition for the hotspots 500ft from major roads map
cim_fhs_roads_500ft = map_fhs_roads_500ft.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_fhs_roads_500ft, "hotspots_roads_500ft")
# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_fhs_roads_500ft.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.12 Optimized Hotspots 500ft from Major Roads Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.12 Optimized Hotspots 500ft from Major Roads Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Open the optimized hotspots 500ft from major roads map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the hotspotsroads500ft map view
map_ohs_roads_500ft.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_ohs_roads_500ft.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_ohs_roads_500ft.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_ohs_roads_500ft_lyr_boundaries = map_ohs_roads_500ft.addDataFromPath(boundaries)
map_ohs_roads_500ft_lyr_cities = map_ohs_roads_500ft.addDataFromPath(cities)
map_ohs_roads_500ft_lyr_blocks = map_ohs_roads_500ft.addDataFromPath(blocks)
map_ohs_roads_500ft_lyr_roads = map_ohs_roads_500ft.addDataFromPath(roads)
map_ohs_roads_500ft_lyr_ohs_roads_500ft = map_ohs_roads_500ft.addDataFromPath(
    crashes_find_hotspots_500ft_major_roads_500ft1mi
)
# Set layer visibility on the map
map_ohs_roads_500ft_lyr_boundaries.visible = False
map_ohs_roads_500ft_lyr_cities.visible = False
map_ohs_roads_500ft_lyr_blocks.visible = False
map_ohs_roads_500ft_lyr_roads.visible = False
map_ohs_roads_500ft_lyr_ohs_roads_500ft.visible = False
# Move layers
map_ohs_roads_500ft.moveLayer(
    reference_layer = map_ohs_roads_500ft_lyr_boundaries,
    move_layer = map_ohs_roads_500ft_lyr_roads,
    insert_position = "AFTER"
)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_ohs_roads_500ft_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)

# - Cities layer
# Apply the symbology for the cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_ohs_roads_500ft_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_ohs_roads_500ft_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)

# - Roads layer
# Apply the symbology for the roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_ohs_roads_500ft_lyr_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the optimized hot spots map layers
map_ohs_roads_500ft_cim_boundaries = map_ohs_roads_500ft_lyr_boundaries.getDefinition("V3")
map_ohs_roads_500ft_cim_cities = map_ohs_roads_500ft_lyr_cities.getDefinition("V3")
map_ohs_roads_500ft_cim_blocks = map_ohs_roads_500ft_lyr_blocks.getDefinition("V3")
map_ohs_roads_500ft_cim_roads = map_ohs_roads_500ft_lyr_roads.getDefinition("V3")
map_ohs_roads_500ft_cim_ohs_roads_500ft = map_ohs_roads_500ft_lyr_ohs_roads_500ft.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_ohs_roads_500ft_cim_boundaries.renderer.heading = "Boundaries"
map_ohs_roads_500ft_cim_cities.renderer.heading = "City Population Density"
map_ohs_roads_500ft_cim_blocks.renderer.heading = "Population Density"
map_ohs_roads_500ft_cim_roads.renderer.heading = "Road Categories"
map_ohs_roads_500ft_cim_ohs_roads_500ft.renderer.heading = "Getis-Ord Gi*"
# Update the map layer definitions
map_ohs_roads_500ft_lyr_boundaries.setDefinition(map_ohs_roads_500ft_cim_boundaries)
map_ohs_roads_500ft_lyr_cities.setDefinition(map_ohs_roads_500ft_cim_cities)
map_ohs_roads_500ft_lyr_blocks.setDefinition(map_ohs_roads_500ft_cim_blocks)
map_ohs_roads_500ft_lyr_roads.setDefinition(map_ohs_roads_500ft_cim_roads)
map_ohs_roads_500ft_lyr_ohs_roads_500ft.setDefinition(map_ohs_roads_500ft_cim_ohs_roads_500ft)
# Update the CIM definition for the hotspots 500ft from major roads map
cim_ohs_roads_500ft = map_ohs_roads_500ft.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the optimized hot spots map mapx file
export_cim("map", map_ohs_roads_500ft, "optimized_hotspots_roads_500ft")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_ohs_roads_500ft.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.13 Major Road Crashes Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.13 Major Road Crashes Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the road crashes map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the road crashes map view
map_road_crashes.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_road_crashes.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_road_crashes.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_road_crashes_lyr_boundaries = map_road_crashes.addDataFromPath(boundaries)
map_road_crashes_lyr_blocks = map_road_crashes.addDataFromPath(blocks)
map_road_crashes_lyr_roads_major = map_road_crashes.addDataFromPath(roads_major)
map_road_crashes_lyr_crashes_500ft_roads = map_road_crashes.addDataFromPath(crashes_500ft_from_major_roads)
# Set layer visibility on the map
map_road_crashes_lyr_boundaries.visible = False
map_road_crashes_lyr_blocks.visible = False
map_road_crashes_lyr_roads_major.visible = False
map_road_crashes_lyr_crashes_500ft_roads.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Crashes (500ft from Major Roads) layer
# Apply the symbology for the Crashes 500ft from Major Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_crashes_lyr_crashes_500ft_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Crashes.lyrx"),
    symbology_fields = [["VALUE_FIELD", "coll_severity", "coll_severity"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Major Roads layer
# Apply the symbology for the major roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_crashes_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_crashes_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_crashes_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the regression map layers
map_road_crashes_cim_boundaries = map_road_crashes_lyr_boundaries.getDefinition("V3")
map_road_crashes_cim_blocks = map_road_crashes_lyr_blocks.getDefinition("V3")
map_road_crashes_cim_roads_major = map_road_crashes_lyr_roads_major.getDefinition("V3")
map_road_crashes_cim_crashes_500ft_roads = map_road_crashes_lyr_crashes_500ft_roads.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_road_crashes_cim_boundaries.renderer.heading = "Boundaries"
map_road_crashes_cim_blocks.renderer.heading = "Population Density"
map_road_crashes_cim_roads_major.renderer.heading = "Major Roads"
map_road_crashes_cim_roads_major.renderer.heading = "Severity Level"
# Update the map layer definitions
map_road_crashes_lyr_boundaries.setDefinition(map_road_crashes_cim_boundaries)
map_road_crashes_lyr_blocks.setDefinition(map_road_crashes_cim_blocks)
map_road_crashes_lyr_roads_major.setDefinition(map_road_crashes_cim_roads_major)
map_road_crashes_lyr_crashes_500ft_roads.setDefinition(map_road_crashes_cim_crashes_500ft_roads)
# Update the CIM definition for the regression map
cim_road_crashes = map_road_crashes.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_road_crashes, "road_crashes")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_road_crashes.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.14 Major Road Hotspots Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.14 Major Road Hotspots Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the road hotspots map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the road hotspots map view
map_road_hotspots.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_road_hotspots.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_road_hotspots.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_road_hotspots_lyr_boundaries = map_road_hotspots.addDataFromPath(boundaries)
map_road_hotspots_lyr_blocks = map_road_hotspots.addDataFromPath(blocks)
map_road_hotspots_lyr_roads_major = map_road_hotspots.addDataFromPath(roads_major)
map_road_hotspots_lyr_crashes_hotspots = map_road_hotspots.addDataFromPath(crashes_hotspots_500ft_from_major_roads)
# Set layer visibility on the map
map_road_hotspots_lyr_boundaries.visible = False
map_road_hotspots_lyr_blocks.visible = False
map_road_hotspots_lyr_roads_major.visible = False
map_road_hotspots_lyr_crashes_hotspots.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Major Roads layer
# Apply the symbology for the major roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_hotspots_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the census blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_hotspots_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_hotspots_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the regression map layers
map_road_hotspots_cim_boundaries = map_road_hotspots_lyr_boundaries.getDefinition("V3")
map_road_hotspots_cim_blocks = map_road_hotspots_lyr_blocks.getDefinition("V3")
map_road_hotspots_cim_roads_major = map_road_hotspots_lyr_roads_major.getDefinition("V3")
map_road_hotspots_cim_crashes_hotspots = map_road_hotspots_lyr_crashes_hotspots.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_road_hotspots_cim_boundaries.renderer.heading = "Boundaries"
map_road_hotspots_cim_blocks.renderer.heading = "Population Density"
map_road_hotspots_cim_roads_major.renderer.heading = "Major Roads"
map_road_hotspots_cim_crashes_hotspots.renderer.heading = "Getis-Ord Gi*"
# Update the map layer definitions
map_road_hotspots_lyr_boundaries.setDefinition(map_road_hotspots_cim_boundaries)
map_road_hotspots_lyr_blocks.setDefinition(map_road_hotspots_cim_blocks)
map_road_hotspots_lyr_roads_major.setDefinition(map_road_hotspots_cim_roads_major)
map_road_hotspots_lyr_crashes_hotspots.setDefinition(map_road_hotspots_cim_crashes_hotspots)
# Update the CIM definition for the regression map
cim_road_hotspots = map_road_hotspots.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_road_hotspots, "road_hotspots")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_road_hotspots.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.15 Major Road Buffers Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.15 Major Road Buffers Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the regression map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the regression map view
map_road_buffers.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_road_buffers.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_road_buffers.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_road_buffers_lyr_boundaries = map_road_buffers.addDataFromPath(boundaries)
map_road_buffers_lyr_blocks = map_road_buffers.addDataFromPath(blocks)
map_road_buffers_lyr_roads_major = map_road_buffers.addDataFromPath(roads_major)
map_road_buffers_lyr_road_buffers = map_road_buffers.addDataFromPath(roads_major_buffers_sum)
# Set layer visibility on the map
map_road_buffers_lyr_boundaries.visible = False
map_road_buffers_lyr_blocks.visible = False
map_road_buffers_lyr_roads_major.visible = False
map_road_buffers_lyr_road_buffers.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Road Buffer Fatalities layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_buffers_lyr_road_buffers,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads Buffers Summary.lyrx"),
    symbology_fields = [["VALUE_FIELD", "sum_number_killed", "sum_number_killed"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Major Roads layer
# Apply the symbology for the major roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_buffers_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_buffers_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_buffers_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the regression map layers
map_road_buffers_cim_boundaries = map_road_buffers_lyr_boundaries.getDefinition("V3")
map_road_buffers_cim_blocks = map_road_buffers_lyr_blocks.getDefinition("V3")
map_road_buffers_cim_roads_major = map_road_buffers_lyr_roads_major.getDefinition("V3")
map_road_buffers_cim_road_buffers = map_road_buffers_lyr_road_buffers.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_road_buffers_cim_boundaries.renderer.heading = "Boundaries"
map_road_buffers_cim_blocks.renderer.heading = "Population Density"
map_road_buffers_cim_roads_major.renderer.heading = "Major Roads"
map_road_buffers_cim_road_buffers.renderer.heading = "Fatalities (entire segment)"
# Update the map layer definitions
map_road_buffers_lyr_boundaries.setDefinition(map_road_buffers_cim_boundaries)
map_road_buffers_lyr_blocks.setDefinition(map_road_buffers_cim_blocks)
map_road_buffers_lyr_roads_major.setDefinition(map_road_buffers_cim_roads_major)
map_road_buffers_lyr_road_buffers.setDefinition(map_road_buffers_cim_road_buffers)
# Update the CIM definition for the regression map
cim_road_buffers = map_road_buffers.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_road_buffers, "road_buffers")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_road_buffers.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.16 Major Road Segments Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.16 Major Road Segments Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the major road segments map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the major road segments map view
map_road_segments.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_road_segments.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_road_segments.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_road_segments_lyr_boundaries = map_road_segments.addDataFromPath(boundaries)
map_road_segments_lyr_blocks = map_road_segments.addDataFromPath(blocks)
map_road_segments_lyr_roads_major = map_road_segments.addDataFromPath(roads_major)
map_road_segments_lyr_roads_major_split = map_road_segments.addDataFromPath(roads_major_split_buffer_sum)
# Set layer visibility on the map
map_road_segments_lyr_boundaries.visible = False
map_road_segments_lyr_blocks.visible = False
map_road_segments_lyr_roads_major.visible = False
map_road_segments_lyr_roads_major_split.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Major Roads Split Buffer Summary layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_segments_lyr_roads_major_split,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads Split Buffer Summary.lyrx"),
    symbology_fields = [["VALUE_FIELD", "sum_victim_count", "sum_victim_count"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Major Roads layer
# Apply the symbology for the major roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_segments_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_segments_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_road_segments_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the regression map layers
map_road_segments_cim_boundaries = map_road_segments_lyr_boundaries.getDefinition("V3")
map_road_segments_cim_blocks = map_road_segments_lyr_blocks.getDefinition("V3")
map_road_segments_cim_roads_major = map_road_segments_lyr_roads_major.getDefinition("V3")
map_road_segments_cim_roads_major_split = map_road_segments_lyr_roads_major_split.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_road_segments_cim_boundaries.renderer.heading = "Boundaries"
map_road_segments_cim_blocks.renderer.heading = "Population Density"
map_road_segments_cim_roads_major.renderer.heading = "Major Roads"
map_road_segments_cim_roads_major_split.renderer.heading = "Victim Count per 1,000ft segment"
# Update the map layer definitions
map_road_segments_lyr_boundaries.setDefinition(map_road_segments_cim_boundaries)
map_road_segments_lyr_blocks.setDefinition(map_road_segments_cim_blocks)
map_road_segments_lyr_roads_major.setDefinition(map_road_segments_cim_roads_major)
map_road_segments_lyr_roads_major_split.setDefinition(map_road_segments_cim_roads_major_split)
# Update the CIM definition for the regression map
cim_road_segments = map_road_segments.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_road_segments, "road_segments")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_road_segments.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.17 Roads Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.17 Roads Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the roads map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the roads map view
map_roads.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_roads.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_roads.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_roads_lyr_roads_major = map_roads.addDataFromPath(roads_major)
map_roads_lyr_roads_major_buffers = map_roads.addDataFromPath(roads_major_buffers)
map_roads_lyr_roads_major_buffers_sum = map_roads.addDataFromPath(roads_major_buffers_sum)
map_roads_lyr_roads_major_points_along_lines = map_roads.addDataFromPath(roads_major_points_along_lines)
map_roads_lyr_roads_major_split = map_roads.addDataFromPath(roads_major_split)
map_roads_lyr_roads_major_split_buffer = map_roads.addDataFromPath(roads_major_split_buffer)
map_roads_lyr_roads_major_split_buffer_sum = map_roads.addDataFromPath(roads_major_split_buffer_sum)
# Set layer visibility on the map
map_roads_lyr_roads_major.visible = False
map_roads_lyr_roads_major_buffers.visible = False
map_roads_lyr_roads_major_buffers_sum.visible = False
map_roads_lyr_roads_major_points_along_lines.visible = False
map_roads_lyr_roads_major_split.visible = False
map_roads_lyr_roads_major_split_buffer.visible = False
map_roads_lyr_roads_major_split_buffer_sum.visible = False
# move the layers
map_roads.moveLayer(
    reference_layer = map_roads_lyr_roads_major_buffers,
    move_layer = map_roads_lyr_roads_major_buffers_sum,
    insert_position = "BEFORE",
)
map_roads.moveLayer(
    reference_layer = map_roads_lyr_roads_major_buffers_sum,
    move_layer = map_roads_lyr_roads_major_points_along_lines,
    insert_position = "BEFORE",
)
map_roads.moveLayer(
    reference_layer = map_roads_lyr_roads_major_buffers_sum,
    move_layer = map_roads_lyr_roads_major_split_buffer,
    insert_position = "BEFORE",
)
map_roads.moveLayer(
    reference_layer = map_roads_lyr_roads_major_split_buffer,
    move_layer = map_roads_lyr_roads_major_split_buffer_sum,
    insert_position = "BEFORE",
)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Major Roads layer
# Apply the symbology for the Major Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_roads_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Major Road Split layer
# Apply the symbology for the major roads layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_roads_lyr_roads_major_split,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the roads map layers
map_roads_cim_roads_major = map_roads_lyr_roads_major.getDefinition("V3")
map_roads_cim_roads_major_buffers = map_roads_lyr_roads_major_buffers.getDefinition("V3")
map_roads_cim_roads_major_buffers_sum = map_roads_lyr_roads_major_buffers_sum.getDefinition("V3")
map_roads_cim_roads_major_points_along_lines = map_roads_lyr_roads_major_points_along_lines.getDefinition("V3")
map_roads_cim_roads_major_split = map_roads_lyr_roads_major_split.getDefinition("V3")
map_roads_cim_roads_major_split_buffer = map_roads_lyr_roads_major_split_buffer.getDefinition("V3")
map_roads_cim_roads_major_split_buffer_sum = map_roads_lyr_roads_major_split_buffer_sum.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_roads_cim_roads_major.renderer.heading = "Road Category"
map_roads_cim_roads_major_buffers.renderer.heading = "Major Road Buffers"
map_roads_cim_roads_major_buffers_sum.renderer.heading = "Major Road Buffers"
map_roads_cim_roads_major_points_along_lines.renderer.heading = "Major Road Points Along Lines"
map_roads_cim_roads_major_split.renderer.heading = "Road Category"
map_roads_cim_roads_major_split_buffer.renderer.heading = "Major Road Buffers"
# Update the map layer definitions
map_roads_lyr_roads_major.setDefinition(map_roads_cim_roads_major)
map_roads_lyr_roads_major_buffers.setDefinition(map_roads_cim_roads_major_buffers)
map_roads_lyr_roads_major_buffers_sum.setDefinition(map_roads_cim_roads_major_buffers_sum)
map_roads_lyr_roads_major_points_along_lines.setDefinition(map_roads_cim_roads_major_points_along_lines)
map_roads_lyr_roads_major_split.setDefinition(map_roads_cim_roads_major_split)
map_roads_lyr_roads_major_split_buffer.setDefinition(map_roads_cim_roads_major_split_buffer)
map_roads_lyr_roads_major_split_buffer_sum.setDefinition(map_roads_cim_roads_major_split_buffer_sum)
# Update the CIM definition for the roads map
cim_roads = map_roads.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")
map_roads_cim_roads_major_points_along_lines.renderer.heading = "Major Road Points Along Lines"
map_roads_cim_roads_major_split.renderer.heading = "Road Category"
map_roads_cim_roads_major_split_buffer.renderer.heading = "Major Road Buffers"
# Update the map layer definitions
map_roads_lyr_roads_major.setDefinition(map_roads_cim_roads_major)
map_roads_lyr_roads_major_buffers.setDefinition(map_roads_cim_roads_major_buffers)
map_roads_lyr_roads_major_buffers_sum.setDefinition(map_roads_cim_roads_major_buffers_sum)
map_roads_lyr_roads_major_points_along_lines.setDefinition(map_roads_cim_roads_major_points_along_lines)
map_roads_lyr_roads_major_split.setDefinition(map_roads_cim_roads_major_split)
map_roads_lyr_roads_major_split_buffer.setDefinition(map_roads_cim_roads_major_split_buffer)
map_roads_lyr_roads_major_split_buffer_sum.setDefinition(map_roads_cim_roads_major_split_buffer_sum)
# Update the CIM definition for the roads map
cim_roads = map_roads.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_roads, "roads")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_roads.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.18 Hotspot Points Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.18 Hotspot Points Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the hotspot points map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the hotspot points map view
map_point_fhs.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_point_fhs.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_point_fhs.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_point_fhs_lyr_boundaries = map_point_fhs.addDataFromPath(boundaries)
map_point_fhs_lyr_roads_major = map_point_fhs.addDataFromPath(roads_major)
map_point_fhs_lyr_fhs = map_point_fhs.addDataFromPath(crashes_hotspots)
# Set layer visibility on the map
map_point_fhs_lyr_boundaries.visible = False
map_point_fhs_lyr_roads_major.visible = False
map_point_fhs_lyr_fhs.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Major Roads layer
# Apply the symbology for the Major Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_point_fhs_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)

# - Boundaries layer
# Apply the symbology for the boundaires data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_point_fhs_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the major roads map layers
map_point_fhs_cim_boundaries = map_point_fhs_lyr_boundaries.getDefinition("V3")
map_point_fhs_cim_roads_major = map_point_fhs_lyr_roads_major.getDefinition("V3")
map_point_fhs_cim_fhs = map_point_fhs_lyr_fhs.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_point_fhs_cim_boundaries.renderer.heading = "Boundaries"
map_point_fhs_cim_roads_major.renderer.heading = "Major Roads"
map_point_fhs_cim_fhs.renderer.heading = "Getis-Ord Gi*"
# Update the map layer definitions
map_point_fhs_lyr_boundaries.setDefinition(map_point_fhs_cim_boundaries)
map_point_fhs_lyr_roads_major.setDefinition(map_point_fhs_cim_roads_major)
map_point_fhs_lyr_fhs.setDefinition(map_point_fhs_cim_fhs)
# Update the CIM definition for the major roads map
cim_point_fhs = map_point_fhs.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the major roads map mapx file
export_cim("map", map_point_fhs, "point_fhs")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_point_fhs.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.19 Optimized Hotspot Points Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.19 Optimized Hotspot Points Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the optimized hotspot points map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the optimized hot spot points map view
map_point_ohs.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_point_ohs.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_point_ohs.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_point_ohs_lyr_boundaries = map_point_ohs.addDataFromPath(boundaries)
map_point_ohs_lyr_roads_major = map_point_ohs.addDataFromPath(roads_major)
map_point_ohs_lyr_ohs = map_point_ohs.addDataFromPath(crashes_optimized_hotspots)
# Set layer visibility on the map
map_point_ohs_lyr_boundaries.visible = False
map_point_ohs_lyr_roads_major.visible = False
map_point_ohs_lyr_ohs.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Major Roads layer
# Apply the symbology for the Major Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_point_ohs_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)

# - Boundaries layer
# Apply the symbology for the boundaires data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_point_ohs_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the road buffers sum map layers
map_point_ohs_cim_boundaries = map_point_ohs_lyr_boundaries.getDefinition("V3")
map_point_ohs_cim_roads_major = map_point_ohs_lyr_roads_major.getDefinition("V3")
map_point_ohs_cim_ohs = map_point_ohs_lyr_ohs.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_point_ohs_cim_boundaries.renderer.heading = "Boundaries"
map_point_ohs_cim_roads_major.renderer.heading = "Major Roads"
map_point_ohs_cim_ohs.renderer.heading = "Getis-Ord Gi*"
# Update the map layer definitions
map_point_ohs_lyr_boundaries.setDefinition(map_point_ohs_cim_boundaries)
map_point_ohs_lyr_roads_major.setDefinition(map_point_ohs_cim_roads_major)
map_point_ohs_lyr_ohs.setDefinition(map_point_ohs_cim_ohs)
# Update the CIM definition for the major roads map
cim_point_ohs = map_point_ohs.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the road buffers map mapx file
export_cim("map", map_point_ohs, "point_ohs")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_point_ohs.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.20 Population Density Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.20 Population Density Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the densities map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the population densities map view
map_pop_dens.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_pop_dens.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_pop_dens.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_pop_dens_lyr_boundaries = map_pop_dens.addDataFromPath(boundaries)
map_pop_dens_lyr_roads_major = map_pop_dens.addDataFromPath(roads_major)
map_pop_dens_lyr_pop_dens = map_pop_dens.addDataFromPath(blocks_sum)
# Rename the layer
map_pop_dens_lyr_pop_dens.name = "OCTraffic Population Density"
# Set layer visibility on the map
map_pop_dens_lyr_boundaries.visible = False
map_pop_dens_lyr_roads_major.visible = False
map_pop_dens_lyr_pop_dens.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Population Density layer
# Apply the symbology for the Housing Density data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_pop_dens_lyr_pop_dens,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Population Density.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Major Roads layer
# Apply the symbology for the major roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_pop_dens_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_pop_dens_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the densities map layers
map_pop_dens_cim_boundaries = map_pop_dens_lyr_boundaries.getDefinition("V3")
map_pop_dens_cim_roads_major = map_pop_dens_lyr_roads_major.getDefinition("V3")
map_pop_dens_cim_pop_dens = map_pop_dens_lyr_pop_dens.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_pop_dens_cim_boundaries.renderer.heading = "Boundaries"
map_pop_dens_cim_roads_major.renderer.heading = "Major Roads"
map_pop_dens_cim_pop_dens.renderer.heading = "Population Density"
# Update the map layer definitions
map_pop_dens_lyr_boundaries.setDefinition(map_pop_dens_cim_boundaries)
map_pop_dens_lyr_roads_major.setDefinition(map_pop_dens_cim_roads_major)
map_pop_dens_lyr_pop_dens.setDefinition(map_pop_dens_cim_pop_dens)
# Apply CIM operations to the layers in the densities map
cim_pop_dens = map_pop_dens.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the densities map mapx file
export_cim("map", map_pop_dens, "pop_dens")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_pop_dens.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.21 Housing Density Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.21 Housing Density Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the housing density map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the housing density map view
map_hou_dens.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_hou_dens.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_hou_dens.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_hou_dens_lyr_boundaries = map_hou_dens.addDataFromPath(boundaries)
map_hou_dens_lyr_roads_major = map_hou_dens.addDataFromPath(roads_major)
map_hou_dens_lyr_hou_dens = map_hou_dens.addDataFromPath(blocks_sum)
# Rename the layer
map_hou_dens_lyr_hou_dens.name = "OCTraffic Housing Density"
# Set layer visibility on the map
map_hou_dens_lyr_boundaries.visible = False
map_hou_dens_lyr_roads_major.visible = False
map_hou_dens_lyr_hou_dens.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Housing Density layer
# Apply the symbology for the Housing Density data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_hou_dens_lyr_hou_dens,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Housing Density.lyrx"),
    symbology_fields = [["VALUE_FIELD", "housing_density", "housing_density"]],
    update_symbology = "MAINTAIN",
)

# - Major Roads layer
# Apply the symbology for the major roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_hou_dens_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_hou_dens_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the regression map layers
map_hou_dens_cim_boundaries = map_hou_dens_lyr_boundaries.getDefinition("V3")
map_hou_dens_cim_roads_major = map_hou_dens_lyr_roads_major.getDefinition("V3")
map_hou_dens_cim_hou_dens = map_hou_dens_lyr_hou_dens.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_hou_dens_cim_boundaries.renderer.heading = "Boundaries"
map_hou_dens_cim_roads_major.renderer.heading = "Major Roads"
map_hou_dens_cim_hou_dens.renderer.heading = "Housing Density"
# Update the map layer definitions
map_hou_dens_lyr_boundaries.setDefinition(map_hou_dens_cim_boundaries)
map_hou_dens_lyr_roads_major.setDefinition(map_hou_dens_cim_roads_major)
map_hou_dens_lyr_hou_dens.setDefinition(map_hou_dens_cim_hou_dens)
# Update the CIM definition for the regression map
cim_hou_dens = map_hou_dens.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_hou_dens, "hou_dens")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_hou_dens.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.22 Victims by City Areas Map Layers
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.22 Victims by City Areas Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Open the city areas map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the city areas map layers
map_area_cities.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_area_cities.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_area_cities.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_area_cities_lyr_boundaries = map_area_cities.addDataFromPath(boundaries)
map_area_cities_lyr_roads_major = map_area_cities.addDataFromPath(roads_major)
map_area_cities_lyr_cities = map_area_cities.addDataFromPath(cities_sum)
# Set layer visibility on the map
map_area_cities_lyr_boundaries.visible = False
map_area_cities_lyr_roads_major.visible = False
map_area_cities_lyr_cities.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Boundaries layer
# Apply the symbology for the boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_area_cities_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)

# - Major Roads layer
# Apply the symbology for the major roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_area_cities_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)

# - City Areas Summary layer
# Apply the symbology for the cities sum data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_area_cities_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities Summary.lyrx"),
    symbology_fields = [["VALUE_FIELD", "sum_victim_count", "sum_victim_count"]],
    update_symbology = "MAINTAIN",
)


### Adjust Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

mpl = map_area_cities_lyr_cities
sym = mpl.symbology
if sym.renderer.type != "GraduatedColorsRenderer":
    sym.updateRenderer("GraduatedColorsRenderer")
sym.renderer.classificationMethod = "NaturalBreaks" # classification method
sym.renderer.classificationField = "sum_victim_count" # Field for the graduated colors
sym.renderer.breakCount = 5 # Number of classes
sym.renderer.colorRamp = aprx.listColorRamps("Yellow-Orange-Red (5 Classes)")[0] # Color ramp

for class_break in sym.renderer.classBreaks:
    # Set the label for each class break
    class_break.symbol.size = 0
    class_break.symbol.outlineColor = class_break.symbol.color

mpl.symbology = sym

# Get the CIM of the layer
mpl_cim = mpl.getDefinition("V3")
# Define the renderer from layer's CIM
rnd = mpl_cim.renderer
# Set the break type and field
rnd.classBreakType = "GraduatedColor"
rnd.classificationMethod = "NaturalBreaks"
rnd.field = "sum_victim_count"
rnd.heading = "Sum Victim Count"
rnd.polygonSymbolColorTarget = "FillOutline"
#rnd.colorRamp = color_ramp
#rnd.colorRamp.name = "Yellow-Orange-Red (5 Classes)"
rnd.numberFormat.roundingValue = 0
rnd.numberFormat.useSeparator = True
rnd.numberFormat.zeroPad = False
rnd.minimumBreak = 0
for i in range(5):
    n1 = rnd.breaks[i].upperBound
    n2 = math.ceil(n1 / 1000) * 1000
    rnd.breaks[i].upperBound = n2
mpl_cim.labelVisibility = True
# Apply the number format definition and
mpl.setDefinition(mpl_cim)

# Reload the symbology after updating the CIM
sym = mpl.symbology
# Apply the layer symbology
mpl.symbology = sym


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the cities map layers
map_area_cities_cim_boundaries = map_area_cities_lyr_boundaries.getDefinition("V3")
map_area_cities_cim_roads_major = map_area_cities_lyr_roads_major.getDefinition("V3")
map_area_cities_cim_cities = map_area_cities_lyr_cities.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_area_cities_cim_boundaries.renderer.heading = "Boundaries"
map_area_cities_cim_roads_major.renderer.heading = "Major Roads"
map_area_cities_cim_cities.renderer.heading = "Victim Count"
# Update the map layer definitions
map_area_cities_lyr_boundaries.setDefinition(map_area_cities_cim_boundaries)
map_area_cities_lyr_roads_major.setDefinition(map_area_cities_cim_roads_major)
map_area_cities_lyr_cities.setDefinition(map_area_cities_cim_cities)
# Update the CIM definition for the city area map
cim_areas_cities = map_area_cities.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the Cities map mapx file
export_cim("map", map_area_cities, "area_cities")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_area_cities.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.23 Victims by Census Blocks Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.23 Victims by Census Blocks Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Open the census blocks map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the census blocks map view
map_area_blocks.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_area_blocks.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_area_blocks.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_area_blocks_lyr_boundaries = map_area_blocks.addDataFromPath(boundaries)
map_area_blocks_lyr_roads_major = map_area_blocks.addDataFromPath(roads_major)
map_area_blocks_lyr_blocks = map_area_blocks.addDataFromPath(blocks_sum)
# Set layer visibility on the map
map_area_blocks_lyr_boundaries.visible = False
map_area_blocks_lyr_roads_major.visible = False
map_area_blocks_lyr_blocks.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Boundaries layer
# Apply the symbology for the boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_area_blocks_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)

# - Major Roads layer
# Apply the symbology for the major roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_area_blocks_lyr_roads_major,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Major Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)

# - Census Blocks Summary layer
# Apply the symbology for the census blocks sum data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_area_blocks_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks Summary.lyrx"),
    symbology_fields = [["VALUE_FIELD", "sum_victim_count", "sum_victim_count"]],
    update_symbology = "DEFAULT",
)


### Adjust Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

mpl = map_area_blocks_lyr_blocks
sym = mpl.symbology
if sym.renderer.type != "GraduatedColorsRenderer":
    sym.updateRenderer("GraduatedColorsRenderer")
sym.renderer.classificationMethod = "NaturalBreaks" # classification method
sym.renderer.classificationField = "sum_victim_count" # Field for the graduated colors
sym.renderer.breakCount = 5 # Number of classes
sym.renderer.colorRamp = aprx.listColorRamps("Yellow-Orange-Red (5 Classes)")[0] # Color ramp

for class_break in sym.renderer.classBreaks:
    # Set the label for each class break
    class_break.symbol.size = 0
    class_break.symbol.outlineColor = class_break.symbol.color

mpl.symbology = sym

# Get the CIM of the layer
mpl_cim = mpl.getDefinition("V3")
# Define the renderer from layer's CIM
rnd = mpl_cim.renderer
# Set the break type and field
rnd.classBreakType = "GraduatedColor"
rnd.classificationMethod = "NaturalBreaks"
rnd.field = "sum_victim_count"
rnd.heading = "Sum Victim Count"
rnd.polygonSymbolColorTarget = "FillOutline"
#rnd.colorRamp = color_ramp
#rnd.colorRamp.name = "Yellow-Orange-Red (5 Classes)"
rnd.numberFormat.roundingValue = 0
rnd.numberFormat.useSeparator = True
rnd.numberFormat.zeroPad = False
rnd.minimumBreak = 0
#for i in range(5):
#    n1 = rnd.breaks[i].upperBound
#    n2 = math.ceil(n1 / 1000) * 1000
#    rnd.breaks[i].upperBound = n2
mpl_cim.labelVisibility = False
# Apply the number format definition and
mpl.setDefinition(mpl_cim)

# Reload the symbology after updating the CIM
sym = mpl.symbology
# Apply the layer symbology
mpl.symbology = sym


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the blocks map layers
map_area_blocks_cim_boundaries = map_area_blocks_lyr_boundaries.getDefinition("V3")
map_area_blocks_cim_roads_major = map_area_blocks_lyr_roads_major.getDefinition("V3")
map_area_blocks_cim_blocks = map_area_blocks_lyr_blocks.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_area_blocks_cim_boundaries.renderer.heading = "Boundaries"
map_area_blocks_cim_roads_major.renderer.heading = "Major Roads"
map_area_blocks_cim_blocks.renderer.heading = "Victim Count"
# Update the map layer definitions
map_area_blocks_lyr_boundaries.setDefinition(map_area_blocks_cim_boundaries)
map_area_blocks_lyr_roads_major.setDefinition(map_area_blocks_cim_roads_major)
map_area_blocks_lyr_blocks.setDefinition(map_area_blocks_cim_blocks)
# Update the CIM definition for the major roads map
cim_area_blocks = map_area_blocks.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the Blocks map mapx file
export_cim("map", map_area_blocks, "area_blocks")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_area_blocks.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.24 Summaries Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.24 Summaries Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the summaries map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the summaries map view
map_summaries.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_summaries.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_summaries.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_summaries_lyr_blocks_sum = map_summaries.addDataFromPath(blocks_sum)
map_summaries_lyr_cities_sum = map_summaries.addDataFromPath(cities_sum)
map_summaries_lyr_crashes_500ft_from_major_roads = map_summaries.addDataFromPath(crashes_500ft_from_major_roads)
# Set layer visibility on the map
map_summaries_lyr_blocks_sum.visible = False
map_summaries_lyr_cities_sum.visible = False
map_summaries_lyr_crashes_500ft_from_major_roads.visible = False
# Move the layers
map_summaries.moveLayer(
    reference_layer = map_summaries_lyr_blocks_sum, move_layer = map_summaries_lyr_cities_sum, insert_position = "AFTER"
)


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Census Block Summary layer
# Apply the symbology for the Census 2020 Blocks summary layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_summaries_lyr_blocks_sum,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities Summary layer
# Apply the symbology for the Cities summary layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_summaries_lyr_cities_sum,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Crashes 500ft from Major Roads layer
# Apply the symbology for the Crashes 500ft from Major Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_summaries_lyr_crashes_500ft_from_major_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Collisions.lyrx"),
    symbology_fields = [["VALUE_FIELD", "coll_severity", "coll_severity"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the summaries map layers
map_summaries_cim_blocks_sum = map_summaries_lyr_blocks_sum.getDefinition("V3")
map_summaries_cim_cities_sum = map_summaries_lyr_cities_sum.getDefinition("V3")
map_summaries_cim_crashes_500ft_from_major_roads = map_summaries_lyr_crashes_500ft_from_major_roads.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_summaries_cim_blocks_sum.renderer.heading = "Population Density"
map_summaries_cim_cities_sum.renderer.heading = "City Population Density"
map_summaries_cim_crashes_500ft_from_major_roads.renderer.heading = "Collision Severity"
# Update the map layer definitions
map_summaries_lyr_blocks_sum.setDefinition(map_summaries_cim_blocks_sum)
map_summaries_lyr_cities_sum.setDefinition(map_summaries_cim_cities_sum)
map_summaries_lyr_crashes_500ft_from_major_roads.setDefinition(map_summaries_cim_crashes_500ft_from_major_roads)
# Update the CIM definition for the summaries map
cim_summaries = map_summaries.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_summaries, "summaries")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_summaries.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.25 Analysis Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.25 Analysis Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the analysis map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the analysis map view
map_analysis.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_analysis.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_analysis.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_analysis_lyr_crashes_hotspots = map_analysis.addDataFromPath(crashes_hotspots)
map_analysis_lyr_crashes_optimized_hotspots = map_analysis.addDataFromPath(crashes_optimized_hotspots)
# Set layer visibility on the map
map_analysis_lyr_crashes_hotspots.visible = False
map_analysis_lyr_crashes_optimized_hotspots.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Update the CIM definition for the analysis map
analysisCim = map_analysis.getDefinition("V3")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the analysis map layers
map_analysis_cim_crashes_hotspots = map_analysis_lyr_crashes_hotspots.getDefinition("V3")
map_analysis_cim_crashes_optimized_hotspots = map_analysis_lyr_crashes_optimized_hotspots.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_analysis, "analysis")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_analysis.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.26 Regression Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.26 Regression Map Layers")


### Open Map View ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Open Map View")

# Close all open maps, open the regression map and set it as the active map
# Close all previous map views
aprx.closeViews()
# Open the regression map view
map_regression.openView()
# set the main map as active map
map = aprx.activeMap

# Remove all the layers from the map
for lyr in map_regression.listLayers():
    if not lyr.isBasemapLayer:
        print(f"Removing layer: {lyr.name}")
        map_regression.removeLayer(lyr)


### Add Layers to Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add Layers to Map")

# Add the feature classes as layers to the map (in order, as the first layer goes to the bottom of the contents)
# Add the data layers to the map
map_regression_lyr_boundaries = map_regression.addDataFromPath(boundaries)
map_regression_lyr_cities = map_regression.addDataFromPath(cities)
map_regression_lyr_blocks = map_regression.addDataFromPath(blocks)
map_regression_lyr_roads = map_regression.addDataFromPath(roads)
# Set layer visibility on the map
map_regression_lyr_boundaries.visible = False
map_regression_lyr_cities.visible = False
map_regression_lyr_blocks.visible = False
map_regression_lyr_roads.visible = False


### Layer Symbology ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer Symbology")

# Define symbology for each of the map layers. The symbology is predefined in the project's template layer folders.

# - Roads layer
# Apply the symbology for the Roads data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_regression_lyr_roads,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Roads.lyrx"),
    symbology_fields = [["VALUE_FIELD", "road_cat", "road_cat"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Census Blocks layer
# Apply the symbology for the US Census 2020 Blocks data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_regression_lyr_blocks,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Census Blocks.lyrx"),
    symbology_fields = [["VALUE_FIELD", "population_density", "population_density"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Cities layer
# Apply the symbology for the Cities data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_regression_lyr_cities,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Cities.lyrx"),
    symbology_fields = [["VALUE_FIELD", "city_pop_dens", "city_pop_dens"]],
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())

# - Boundaries layer
# Apply the symbology for the Boundaries data layer
arcpy.management.ApplySymbologyFromLayer(
    in_layer = map_regression_lyr_boundaries,
    in_symbology_layer = os.path.join(prj_dirs.get("layers_templates", ""), "OCTraffic Boundaries.lyrx"),
    symbology_fields = None,
    update_symbology = "MAINTAIN",
)
print(arcpy.GetMessages())


### Layer CIM Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Layer CIM Operations")

# Generate CIM JSON configuration for layers
# Get CIM definitions for the regression map layers
map_regression_cim_boundaries = map_regression_lyr_boundaries.getDefinition("V3")
map_regression_cim_cities = map_regression_lyr_cities.getDefinition("V3")
map_regression_cim_blocks = map_regression_lyr_blocks.getDefinition("V3")
map_regression_cim_roads = map_regression_lyr_roads.getDefinition("V3")

# Set symbology headings and update CIM definitions for layers
# Set the layer headings
map_regression_cim_boundaries.renderer.heading = "Boundaries"
map_regression_cim_cities.renderer.heading = "Cities"
map_regression_cim_blocks.renderer.heading = "Census Blocks"
map_regression_cim_roads.renderer.heading = "Roads"
# Update the map layer definitions
map_regression_lyr_boundaries.setDefinition(map_regression_cim_boundaries)
map_regression_lyr_cities.setDefinition(map_regression_cim_cities)
map_regression_lyr_blocks.setDefinition(map_regression_cim_blocks)
map_regression_lyr_roads.setDefinition(map_regression_cim_roads)
# Update the CIM definition for the regression map
cim_regression = map_regression.getDefinition("V3")


### Export Map and Map Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Export Map and Map Layers")

# Update the victims map mapx file
export_cim("map", map_regression, "regression")

# Export map layers as CIM JSON `.lyrx` files to the layers folder directory of the project.
for l in map_regression.listLayers():
    if not l.isBasemapLayer:
        export_cim("layer", l, l.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 4 Map CIM and Exporting ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4 Map CIM and Exporting")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.1 Map CIM Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.1 Map CIM Processing")


### Get Map CIM ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Get Map CIM")

# Get the CIM definitions for the OCSWIRS maps
cim_collisions = map_collisions.getDefinition("V3")
cim_crashes = map_collisions.getDefinition("V3")
cim_parties = map_collisions.getDefinition("V3")
cim_victims = map_victims.getDefinition("V3")
cim_injuries = map_injuries.getDefinition("V3")
cim_fatalities = map_fatalities.getDefinition("V3")
cim_fhs_100m1km = map_fhs_100m1km.getDefinition("V3")
cim_fhs_150m2km = map_fhs_150m2km.getDefinition("V3")
cim_fhs_100m5km = map_fhs_100m5km.getDefinition("V3")
cim_fhs_roads_500ft = map_fhs_roads_500ft.getDefinition("V3")
cim_ohs_roads_500ft = map_ohs_roads_500ft.getDefinition("V3")
cim_road_crashes = map_road_crashes.getDefinition("V3")
cim_road_hotspots = map_road_hotspots.getDefinition("V3")
cim_road_buffers = map_road_buffers.getDefinition("V3")
cim_road_segments = map_road_segments.getDefinition("V3")
cim_roads = map_roads.getDefinition("V3")
cim_point_fhs = map_point_fhs.getDefinition("V3")
cim_point_ohs = map_point_ohs.getDefinition("V3")
cim_pop_dens = map_pop_dens.getDefinition("V3")
cim_hou_dens = map_hou_dens.getDefinition("V3")
cim_area_cities = map_area_cities.getDefinition("V3")
cim_area_blocks = map_area_blocks.getDefinition("V3")
cim_summaries = map_summaries.getDefinition("V3")
cim_analysis = map_analysis.getDefinition("V3")
cim_regression = map_regression.getDefinition("V3")


### Use Service Layer IDs ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Use Service Layer IDs")

# Change the map properties to allow assignment of unique numeric IDs for sharing web layers

# Collisions Map
cim_collisions.useServiceLayerIDs = True
map_collisions.setDefinition(cim_collisions)
# Crashes Map
cim_crashes.useServiceLayerIDs = True
map_crashes.setDefinition(cim_crashes)
# Parties Map
cim_parties.useServiceLayerIDs = True
map_parties.setDefinition(cim_parties)
# Victims Map
cim_victims.useServiceLayerIDs = True
map_victims.setDefinition(cim_victims)
# Injuries Map
cim_injuries.useServiceLayerIDs = True
map_injuries.setDefinition(cim_injuries)
# Fatalities Map
cim_fatalities.useServiceLayerIDs = True
map_fatalities.setDefinition(cim_fatalities)
# FHS 100m 1km Map
cim_fhs_100m1km.useServiceLayerIDs = True
map_fhs_100m1km.setDefinition(cim_fhs_100m1km)
# FHS 150m 2km Map
cim_fhs_150m2km.useServiceLayerIDs = True
map_fhs_150m2km.setDefinition(cim_fhs_150m2km)
# FHS 100m 5km Map
cim_fhs_100m5km.useServiceLayerIDs = True
map_fhs_100m5km.setDefinition(cim_fhs_100m5km)
# FHS Roads 500ft Map
cim_fhs_roads_500ft.useServiceLayerIDs = True
map_fhs_roads_500ft.setDefinition(cim_fhs_roads_500ft)
# OHS Roads 500ft Map
cim_ohs_roads_500ft.useServiceLayerIDs = True
map_ohs_roads_500ft.setDefinition(cim_ohs_roads_500ft)
# Road Crashes Map
cim_road_crashes.useServiceLayerIDs = True
map_road_crashes.setDefinition(cim_road_crashes)
# Road Hotspots Map
cim_road_hotspots.useServiceLayerIDs = True
map_road_hotspots.setDefinition(cim_road_hotspots)
# Road Buffers Map
cim_road_buffers.useServiceLayerIDs = True
map_road_buffers.setDefinition(cim_road_buffers)
# Road Segments Map
cim_road_segments.useServiceLayerIDs = True
map_road_segments.setDefinition(cim_road_segments)
# Roads Map
cim_roads.useServiceLayerIDs = True
map_roads.setDefinition(cim_roads)
# Point FHS Map
cim_point_fhs.useServiceLayerIDs = True
map_point_fhs.setDefinition(cim_point_fhs)
# Point OHS Map
cim_point_ohs.useServiceLayerIDs = True
map_point_ohs.setDefinition(cim_point_ohs)
# Population Density Map
cim_pop_dens.useServiceLayerIDs = True
map_pop_dens.setDefinition(cim_pop_dens)
# Housing Density Map
cim_hou_dens.useServiceLayerIDs = True
map_hou_dens.setDefinition(cim_hou_dens)
# Area Cities Map
cim_area_cities.useServiceLayerIDs = True
map_area_cities.setDefinition(cim_area_cities)
# Area Blocks Map
cim_area_blocks.useServiceLayerIDs = True
map_area_blocks.setDefinition(cim_area_blocks)
# Summaries Map
cim_summaries.useServiceLayerIDs = True
map_summaries.setDefinition(cim_summaries)
# Analysis Map
cim_analysis.useServiceLayerIDs = True
map_analysis.setDefinition(cim_analysis)
# Regression Map
cim_regression.useServiceLayerIDs = True
map_regression.setDefinition(cim_regression)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.2 Export Maps to JSON ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.2 Export Maps to JSON")

# Export maps to mapx CIM JSON files
for m in aprx.listMaps():
    print(f"Exporting {m.name} map...")
    export_cim("map", m, m.name)


### Save Project ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Save Project")

# Save the project
aprx.save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of Script ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\nLast Executed:", dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print("\nEnd of Script")
# Last Executed: 2025-10-21
