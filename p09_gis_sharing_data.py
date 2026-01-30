# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Part 9 - Sharing ArcGIS Online Data ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.3, Date: January 2026
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOCTraffic GIS Data Processing - Part 9 - Sharing ArcGIS Data Online\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1 - Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1. Preliminaries")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Referencing Libraries and Initialization ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.1. Referencing Libraries and Initialization")

# Import Python libraries
import os
import datetime
import pytz
import arcpy
import pandas as pd
from arcgis.gis import GIS
from dotenv import load_dotenv

from octraffic import OCTraffic

# Initialize the OCTraffic object
octr = OCTraffic(part = 9, version = 2025.3)

# Load environment variables from .env file
load_dotenv()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2. Project and Workspace Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.2. Project and Workspace Variables")


### Project and Geodatabase Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Project and Geodatabase Paths")

# Create a directory with the project metadata
prj_meta = octr.prj_meta

# Create a dictionary with the project directories
prj_dirs = octr.prj_dirs

# Set the current working directory to the project root
os.chdir(prj_dirs["root"])

# Define the project folder (parent directory of the current working directory)
prj_folder = os.getcwd()

# Define the ArcGIS pro project variables
# Current notebook directory
# notebookDir = os.getcwd()
# Define the project folder (parent directory of the current working directory)
# projectFolder = os.path.dirname(os.getcwd())

# Load environment variables from the .env file
load_dotenv()


### ArcGIS Pro Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- ArcGIS Pro Paths")

# Get the paths for the ArcGIS Pro project and geodatabase
aprx_path: str = prj_dirs.get("agp_aprx", "")
gdb_path: str = prj_dirs.get("agp_gdb", "")
# ArcGIS Pro Project object
aprx, workspace = octr.load_aprx(aprx_path = aprx_path, gdb_path = gdb_path, add_to_map = False)
# Close all map views
aprx.closeViews()


### Data Folder Paths ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Data Folder Paths")

# The most current raw data files cover the periods from 01/01/2013 to 09/30/2024. The data files are already processed in the R s_cripts and imported into the project's geodatabase.
# Add the start date of the raw data to a new python datetime object
date_start = prj_meta["date_start"]
# Add the end date of the raw data to a new python datetime object
date_end = prj_meta["date_end"]
# Define time and date variables
time_zone = pytz.timezone("US/Pacific")
# Add today's day
today = datetime.datetime.now(time_zone)
date_updated = today.strftime("%B %d, %Y")
time_updated = today.strftime("%I:%M %p")

# Define date strings for metadata
# String defining the years of the raw data
md_years = f"{date_start.year}-{date_end.year}"
# String defining the start and end dates of the raw data
md_dates = f"Data from {date_start.strftime('%B %d, %Y')} to {date_end.strftime('%B %d, %Y')}"

# Load the graphics_list pickle data file
print("- Loading the graphics_list pickle data file")
graphics_list = pd.read_pickle(os.path.join(prj_dirs["data_python"], "graphics_list.pkl"))


### Codebook ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Codebook")

# Load the codebook from the project codebook directory
print("- Loading the codebook from the project codebook directory")
cb = octr.cb

# create a data frame from the codebook
print("- Creating a data frame from the codebook")
df_cb = octr.df_cb


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
# 2. ArcGIS Online Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2. ArcGIS Online Operations")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1. Sign In to ArcGIS Online ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.1. Sign In to ArcGIS Online")

# Define portal connection variables
portal_url = "https://www.arcgis.com"
portal_username = os.getenv("AGO_USERNAME")
portal_password = os.getenv("AGO_PASSWORD")
portal_folder = os.getenv("AGO_FOLDER")

# Connect to ArcGIS Online portal and get the gis object
try:
    gis = GIS(portal_url, os.getenv("AGO_USERNAME"), os.getenv("AGO_PASSWORD"))
    print(f"✅ Connected to {portal_url} as {portal_username}")
except Exception as e:
    print(f"❌ Failed to connect: {e}")
    raise SystemExit from e

# Define a new AGO portal metadata dictionary
portal_meta = {}

# Sign in to portal
portal_meta["info"] = arcpy.SignInToPortal(
    "https://www.arcgis.com", os.getenv("AGO_USERNAME"), os.getenv("AGO_PASSWORD")
)
# Get the portal description and key information
portal_meta["desc"] = arcpy.GetPortalDescription()


# Print Basic Portal Information
print(f"Info: \n- Token: {portal_meta['info']['token']}\n- Referer: {portal_meta['info']['referer']}\n- Expires: {datetime.datetime.fromtimestamp(portal_meta['info']['expires']).strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Portal: {portal_meta['desc']['name']}")
print(f"User: {portal_meta['desc']['user']['fullName']} ({portal_meta['desc']['user']['username']})")

# Path to store ArcGIS Online staging feature layers
# ago_prj_path = prj_dirs["data_ago"]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2. Maps and Feature Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.2. Maps and Feature Layers")


### Collisions Map and Supporting Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Collisions Map and Supporting Layers")

# Maps and Layers Definitions for the Collisions Map
map_collisions = aprx.listMaps("collisions")[0]
lyr_collisions = map_collisions.listLayers("OCTraffic Collisions")[0]
lyr_roads = map_collisions.listLayers("OCTraffic Roads")[0]
lyr_blocks = map_collisions.listLayers("OCTraffic Census Blocks")[0]
lyr_cities = map_collisions.listLayers("OCTraffic Cities")[0]
lyr_boundaries = map_collisions.listLayers("OCTraffic Boundaries")[0]


### Crashes Map and Supporting Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Crashes Map and Supporting Layers")

# Crashes Map and layer definitions
map_crashes = aprx.listMaps("crashes")[0]
lyr_crashes = map_crashes.listLayers("OCTraffic Crashes")[0]


### Parties Map and Supporting Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Parties Map and Supporting Layers")

# Parties map and layer definitions
map_parties = aprx.listMaps("parties")[0]
lyr_parties = map_parties.listLayers("OCTraffic Parties")[0]


### Victims Map and Supporting Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Victims Map and Supporting Layers")

# Victims map and layer definitions
map_victims = aprx.listMaps("victims")[0]
lyr_victims = map_victims.listLayers("OCTraffic Victims")[0]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.3. Sharing Supporting Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.3. Sharing Supporting Layers")

# Global service definition variables:
service_folder = "OCTraffic"
service_credits = "Dr. Kostas Alexandridis, GISP"
server_type = "HOSTING_SERVER"
service_type = "FEATURE"


### Boundaries Service Layer ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Boundaries Service Layer")

# Provide the service definition variables for the Boundaries layer
boundaries_service_name = "OC Boundaries"
boundaries_sd_draft_file = boundaries_service_name + ".sddraft"
boundaries_sd_draft_output = os.path.join(prj_dirs["data_ago"], boundaries_sd_draft_file)
boundaries_sd_file = boundaries_service_name + ".sd"
boundaries_sd_output = os.path.join(prj_dirs["data_ago"], boundaries_sd_file)

# Create a feature service draft object
boundaries_sd_draft = map_collisions.getWebLayerSharingDraft(
    server_type = server_type,
    service_type = service_type,
    service_name = boundaries_service_name,
    layers_and_tables = lyr_boundaries
)

# Set the properties of the feature service draft object
boundaries_sd_draft.overwriteExistingService = True
boundaries_sd_draft.portalFolder = service_folder
boundaries_sd_draft.credits = service_credits

# Create Service Definition Draft file
boundaries_sd_draft.exportToSDDraft(boundaries_sd_draft_output)

# Stage the service definition for the boundaries layer
print("Start Staging")
arcpy.server.StageService(
    in_service_definition_draft = boundaries_sd_draft_output,
    out_service_definition = boundaries_sd_output,
    staging_version = 102
)

# Publish the service definition to ArcGIS Online portal (updating the existing service)
print("Start Uploading")
arcpy.server.UploadServiceDefinition(
    in_sd_file = boundaries_sd_output,
    in_server = server_type,
    in_service_name = boundaries_service_name,
    in_folder_type = "EXISTING",
    in_folder = service_folder,
    in_startupType = "STARTED",
    in_override = "OVERRIDE_DEFINITION",
    in_public = "PUBLIC",
    in_organization = "SHARE_ORGANIZATION",
    in_groups = ["OC Traffic Data", "Orange County Open Data"]
)
print(arcpy.GetMessages())


### Cities Service Layer ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Cities Service Layer")

# Provide the service definition variables for the Cities layer
cities_service_name = "OCTraffic Cities"
cities_sd_draft_file = cities_service_name + ".sddraft"
cities_sd_draft_output = os.path.join(prj_dirs["data_ago"], cities_sd_draft_file)
cities_sd_file = cities_service_name + ".sd"
cities_sd_output = os.path.join(prj_dirs["data_ago"], cities_sd_file)

# Create a feature service draft object
cities_sd_draft = map_collisions.getWebLayerSharingDraft(
    server_type = server_type,
    service_type = service_type,
    service_name = cities_service_name,
    layers_and_tables = lyr_cities
)

# Set the properties of the feature service draft object
cities_sd_draft.overwriteExistingService = True
cities_sd_draft.portalFolder = service_folder
cities_sd_draft.credits = service_credits

# Create Service Definition Draft file
cities_sd_draft.exportToSDDraft(cities_sd_draft_output)

# Stage the service definition for the boundaries layer
print("Start Staging")
arcpy.server.StageService(
    in_service_definition_draft = cities_sd_draft_output,
    out_service_definition = cities_sd_output,
    staging_version = 102
)

# Publish the service definition to ArcGIS Online portal (updating the existing service)
print("Start Uploading")
arcpy.server.UploadServiceDefinition(
    in_sd_file = cities_sd_output,
    in_server = server_type,
    in_service_name = cities_service_name,
    in_folder_type = "EXISTING",
    in_folder = service_folder,
    in_startupType = "STARTED",
    in_override = "OVERRIDE_DEFINITION",
    in_public = "PUBLIC",
    in_organization = "SHARE_ORGANIZATION",
    in_groups = ["OC Traffic Data", "Orange County Open Data"]
)
print(arcpy.GetMessages())


### Roads Service Layer ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Roads Service Layer")

# Provide the service definition variables for the Roads layer
roads_service_name = "OCTraffic Roads"
roads_sd_draft_file = roads_service_name + ".sddraft"
roads_sd_draft_output = os.path.join(prj_dirs["data_ago"], roads_sd_draft_file)
roads_sd_file = roads_service_name + ".sd"
roads_sd_output = os.path.join(prj_dirs["data_ago"], roads_sd_file)

# Create a feature service draft object
roads_sd_draft = map_collisions.getWebLayerSharingDraft(
    server_type = server_type,
    service_type = service_type,
    service_name = roads_service_name,
    layers_and_tables = lyr_roads
)

# Set the properties of the feature service draft object
roads_sd_draft.overwriteExistingService = True
roads_sd_draft.portalFolder = service_folder
roads_sd_draft.credits = service_credits

# Create Service Definition Draft file
roads_sd_draft.exportToSDDraft(roads_sd_draft_output)

# Stage the service definition for the roads layer
print("Start Staging")
arcpy.server.StageService(
    in_service_definition_draft = roads_sd_draft_output,
    out_service_definition = roads_sd_output,
    staging_version = 102
)

# Publish the service definition to ArcGIS Online portal (updating the existing service)
print("Start Uploading")
arcpy.server.UploadServiceDefinition(
    in_sd_file = roads_sd_output,
    in_server = server_type,
    in_service_name = roads_service_name,
    in_folder_type = "EXISTING",
    in_folder = service_folder,
    in_startupType = "STARTED",
    in_override = "OVERRIDE_DEFINITION",
    in_public = "PUBLIC",
    in_organization = "SHARE_ORGANIZATION",
    in_groups = ["OC Traffic Data", "Orange County Open Data"]
)
print(arcpy.GetMessages())


### Census Blocks Service Layer ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Census Blocks Service Layer")

# Provide the service definition variables for the Blocks layer
blocks_service_name = "OCTraffic Census Blocks"
blocks_sd_draft_file = blocks_service_name + ".sddraft"
blocks_sd_draft_output = os.path.join(prj_dirs["data_ago"], blocks_sd_draft_file)
blocks_sd_file = blocks_service_name + ".sd"
blocks_sd_output = os.path.join(prj_dirs["data_ago"], blocks_sd_file)

# Create a feature service draft object
blocks_sd_draft = map_collisions.getWebLayerSharingDraft(
    server_type = server_type,
    service_type = service_type,
    service_name = blocks_service_name,
    layers_and_tables = lyr_blocks
)

# Set the properties of the feature service draft object
blocks_sd_draft.overwriteExistingService = True
blocks_sd_draft.portalFolder = service_folder
blocks_sd_draft.credits = service_credits

# Create Service Definition Draft file
blocks_sd_draft.exportToSDDraft(blocks_sd_draft_output)

# Stage the service definition for the blocks layer
print("Start Staging")
arcpy.server.StageService(
    in_service_definition_draft = blocks_sd_draft_output,
    out_service_definition = blocks_sd_output,
    staging_version = 102
)

# Publish the service definition to ArcGIS Online portal (updating the existing service)
print("Start Uploading")
arcpy.server.UploadServiceDefinition(
    in_sd_file = blocks_sd_output,
    in_server = server_type,
    in_service_name = blocks_service_name,
    in_folder_type = "EXISTING",
    in_folder = service_folder,
    in_startupType = "STARTED",
    in_override = "OVERRIDE_DEFINITION",
    in_public = "PUBLIC",
    in_organization = "SHARE_ORGANIZATION",
    in_groups = ["OC Traffic Data", "Orange County Open Data"]
)
print(arcpy.GetMessages())


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.4. Sharing SWITRS Data Layers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.4. Sharing SWITRS Data Layers")


### Crashes Service Layer ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Crashes Service Layer")

# Provide the service definition variables for the crashes layer
crashes_service_name = "OCTraffic Crashes"
crashes_sd_draft_file = crashes_service_name + ".sddraft"
crashes_sd_draft_output = os.path.join(prj_dirs["data_ago"], crashes_sd_draft_file)
crashes_sd_file = crashes_service_name + ".sd"
crashes_sd_output = os.path.join(prj_dirs["data_ago"], crashes_sd_file)

# Create a feature service draft object
crashes_sd_draft = map_crashes.getWebLayerSharingDraft(
    server_type = server_type,
    service_type = service_type,
    service_name = crashes_service_name,
    layers_and_tables = lyr_crashes
)

# Set the properties of the feature service draft object
crashes_sd_draft.overwriteExistingService = True
crashes_sd_draft.portalFolder = service_folder
crashes_sd_draft.credits = service_credits

# Create Service Definition Draft file
crashes_sd_draft.exportToSDDraft(crashes_sd_draft_output)

# Stage the service definition for the crashes layer
print("Start Staging")
arcpy.server.StageService(
    in_service_definition_draft = crashes_sd_draft_output,
    out_service_definition = crashes_sd_output,
    staging_version = 102
)

# Publish the service definition to ArcGIS Online portal (updating the existing service)
print("Start Uploading")
arcpy.server.UploadServiceDefinition(
    in_sd_file = crashes_sd_output,
    in_server = server_type,
    in_service_name = crashes_service_name,
    in_folder_type = "EXISTING",
    in_folder = service_folder,
    in_startupType = "STARTED",
    in_override = "OVERRIDE_DEFINITION",
    in_public = "PUBLIC",
    in_organization = "SHARE_ORGANIZATION",
    in_groups = ["OC Traffic Data", "Orange County Open Data"]
)
print(arcpy.GetMessages())


### Parties Service Layer ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Parties Service Layer")

# Provide the service definition variables for the parties layer
parties_service_name = "OCTraffic Parties"
parties_sd_draft_file = parties_service_name + ".sddraft"
parties_sd_draft_output = os.path.join(prj_dirs["data_ago"], parties_sd_draft_file)
parties_sd_file = parties_service_name + ".sd"
parties_sd_output = os.path.join(prj_dirs["data_ago"], parties_sd_file)

# Create a feature service draft object
parties_sd_draft = map_parties.getWebLayerSharingDraft(
    server_type = server_type,
    service_type = service_type,
    service_name = parties_service_name,
    layers_and_tables = lyr_parties
)

# Set the properties of the feature service draft object
parties_sd_draft.overwriteExistingService = True
parties_sd_draft.portalFolder = service_folder
parties_sd_draft.credits = service_credits

# Create Service Definition Draft file
parties_sd_draft.exportToSDDraft(parties_sd_draft_output)

# Stage the service definition for the parties layer
print("Start Staging")
arcpy.server.StageService(
    in_service_definition_draft = parties_sd_draft_output,
    out_service_definition = parties_sd_output,
    staging_version = 102
)

# Publish the service definition to ArcGIS Online portal (updating the existing service)
print("Start Uploading")
arcpy.server.UploadServiceDefinition(
    in_sd_file = parties_sd_output,
    in_server = server_type,
    in_service_name = parties_service_name,
    in_folder_type = "EXISTING",
    in_folder = service_folder,
    in_startupType = "STARTED",
    in_override = "OVERRIDE_DEFINITION",
    in_public = "PUBLIC",
    in_organization = "SHARE_ORGANIZATION",
    in_groups = ["OC Traffic Data", "Orange County Open Data"]
)
print(arcpy.GetMessages())


### Victims Service Layer ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Victims Service Layer")

# Provide the service definitions variables for the victims layer
victims_service_name = "OCTraffic Victims"
victims_sd_draft_file = victims_service_name + ".sddraft"
victims_sd_draft_output = os.path.join(prj_dirs["data_ago"], victims_sd_draft_file)
victims_sd_file = victims_service_name + ".sd"
victims_sd_output = os.path.join(prj_dirs["data_ago"], victims_sd_file)

# Create a feature service draft object
victims_sd_draft = map_victims.getWebLayerSharingDraft(
    server_type = server_type,
    service_type = service_type,
    service_name = victims_service_name,
    layers_and_tables = lyr_victims
)

# Set the properties of the feature service draft object
victims_sd_draft.overwriteExistingService = True
victims_sd_draft.portalFolder = service_folder
victims_sd_draft.credits = service_credits

# Create Service Definition Draft file
victims_sd_draft.exportToSDDraft(victims_sd_draft_output)

# Stage the service definition for the victims layer
print("Start Staging")
arcpy.server.StageService(
    in_service_definition_draft = victims_sd_draft_output,
    out_service_definition = victims_sd_output
)

# Publish the service definition to ArcGIS Online portal (updating the existing service)
print("Start Uploading")
arcpy.server.UploadServiceDefinition(
    in_sd_file = victims_sd_output,
    in_server = server_type,
    in_service_name = victims_service_name,
    in_folder_type = "EXISTING",
    in_folder = service_folder,
    in_startupType = "STARTED",
    in_override = "OVERRIDE_DEFINITION",
    in_public = "PUBLIC",
    in_organization = "SHARE_ORGANIZATION",
    in_groups = ["OC Traffic Data", "Orange County Open Data"]
)
print(arcpy.GetMessages())


### Collisions Service Layer ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Collisions Service Layer")

# Provide the service definitions variables for the collisions layer
collisions_service_name = "OCTraffic Collisions"
collisions_sd_draft_file = collisions_service_name + ".sddraft"
collisions_sd_draft_output = os.path.join(prj_dirs["data_ago"], collisions_sd_draft_file)
collisions_sd_file = collisions_service_name + ".sd"
collisions_sd_output = os.path.join(prj_dirs["data_ago"], collisions_sd_file)

# Create a feature service draft object
collisions_sd_draft = map_collisions.getWebLayerSharingDraft(
    server_type = server_type,
    service_type = service_type,
    service_name = collisions_service_name,
    layers_and_tables = lyr_collisions
)

# Set the properties of the feature service draft object
collisions_sd_draft.overwriteExistingService = True
collisions_sd_draft.portalFolder = service_folder
collisions_sd_draft.credits = service_credits

# Create Service Definition Draft file
collisions_sd_draft.exportToSDDraft(collisions_sd_draft_output)

# Stage the service definition for the collisions layer
print("Start Staging")
arcpy.server.StageService(
    in_service_definition_draft = collisions_sd_draft_output,
    out_service_definition = collisions_sd_output
)

# Publish the service definition to ArcGIS Online portal (updating the existing service)
print("Start Uploading")
arcpy.server.UploadServiceDefinition(
    in_sd_file = collisions_sd_output,
    in_server = server_type,
    in_service_name = collisions_service_name,
    in_folder_type = "EXISTING",
    in_folder = service_folder,
    in_startupType = "STARTED",
    in_override = "OVERRIDE_DEFINITION",
    in_public = "PUBLIC",
    in_organization = "SHARE_ORGANIZATION",
    in_groups = ["OC Traffic Data", "Orange County Open Data"]
)
print(arcpy.GetMessages())


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of Script ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\nLast Executed:", datetime.datetime.now().strftime("%Y-%m-%d"))
print("\nEnd of Script")
# Last Executed: 2026-01-03
