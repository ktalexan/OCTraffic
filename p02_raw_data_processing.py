# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Part 2 - Raw Data Processing ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.3, Date: December 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOCTraffic Data Processing - Part 2 - Raw Data Processing\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1. Preliminaries")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Libraries and Initialization ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.1. Libraries and Initialization")

# Import necessary libraries
import os, sys, datetime
import json
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# important as it "enhances" Pandas by importing these classes (from ArcGIS API for Python)
from arcgis.features import GeoAccessor
from octraffic import OCTraffic

# Initialize the OCTraffic object
ocs = OCTraffic(part = 2, version = 2025.3)

# Load environment variables from .env file
load_dotenv()

os.getcwd()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2. Project and Workspace Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.2. Project and Workspace Variables")

# Create a dictionary with the project metadata
print("\nCreating project metadata")
prj_meta = ocs.project_metadata(silent = False)

# Create a dictionary with the project directories
print("\nCreating project directories")
prj_dirs = ocs.project_directories(silent = False)

# Set the current working directory to the project root
os.chdir(prj_dirs["root"])
# Get the new current working directory
print(f"\nCurrent working directory: {os.getcwd()}\n")

# Import the LaTeX variables dictionary from the json file on disk
latex_vars_path = os.path.join(prj_dirs["metadata"], "latex_vars.json")
with open(latex_vars_path, encoding = "utf-8") as json_file:
    latex_vars = json.load(json_file)

# Print the latex variables dictionary formatted with 4 spaces
print(json.dumps(latex_vars, indent = 4))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Raw Data Import (Initialization) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2. Raw Data Import (Initialization)")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1. Raw Data from Disk ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.1. Raw Data from Disk")

# Load the crashes pickle data file
print("- Loading the crashes pickle data file")
crashes = pd.read_pickle(os.path.join(prj_dirs["data_python"], "raw_crashes.pkl"))

# load the parties pickle data file
print("- Loading the parties pickle data file")
parties = pd.read_pickle(os.path.join(prj_dirs["data_python"], "raw_parties.pkl"))

# load the victims pickle data file
print("- Loading the victims pickle data file")
victims = pd.read_pickle(os.path.join(prj_dirs["data_python"], "raw_victims.pkl"))

# load the data dictionary pickle data file
print("- Loading the data dictionary pickle data file")
data_dict = pd.read_pickle(os.path.join(prj_dirs["data_python"], "data_dict.pkl"))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2. Supporting GIS Data from Geodatabase ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.2. Supporting GIS Data from Geodatabase")

# Import the supporting feature classes (boundaries, cities, roads, blocks) from the AGPSWITRS geodatabase as spatial data frames.

# Import the boundaries feature class from the AGPSWITRS geodatabase
print("- Importing the boundaries spatial data frame")
boundaries = pd.DataFrame.spatial.from_featureclass(os.path.join(prj_dirs["agp_gdb_supporting"], "boundaries"))
# Add attributes to the boundaries data frame
boundaries.attrs = {
    "name": "boundaries",
    "label": "OCTraffic Boundaries",
    "description": "Spatially enabled dataframe containing the Orange County boundaries for the OCTraffic dataset.",
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y")
}

# Import the cities feature class from the AGPSWITRS geodatabase
print("- Importing the cities spatial data frame")
cities = pd.DataFrame.spatial.from_featureclass(os.path.join(prj_dirs["agp_gdb_supporting"], "cities"))
# Add attributes to the cities data frame
cities.attrs = {
    "name": "cities",
    "label": "OCTraffic Cities",
    "description": "Spatially enabled dataframe containing the Orange County cities for the OCTraffic dataset.",
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y")
}

# Import the roads feature class from the AGPSWITRS geodatabase
print("- Importing the roads spatial data frame")
roads = pd.DataFrame.spatial.from_featureclass(os.path.join(prj_dirs["agp_gdb_supporting"], "roads"))
# Add attributes to the roads data frame
roads.attrs = {
    "name": "roads",
    "label": "OCTraffic Roads",
    "description": "Spatially enabled dataframe containing the Orange County roads for the OCTraffic dataset.",
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y")
}

# Import the census blocks feature class from the AGPSWITRS geodatabase
print("- Importing the blocks spatial data frame")
blocks = pd.DataFrame.spatial.from_featureclass(os.path.join(prj_dirs["agp_gdb_supporting"], "blocks"))
# Add attributes to the blocks data frame
blocks.attrs = {
    "name": "blocks",
    "label": "OCTraffic Census Blocks",
    "description": "Spatially enabled dataframe containing the Orange County census blocks for the OCTraffic dataset.",
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y")
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.3. Statistics and Basic Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.3. Statistics and Basic Processing")

# Compile a list of the data frames
print("- Compiling a list of the data frames")
df_list = ["crashes", "parties", "victims", "boundaries", "cities", "roads", "blocks"]

# Get basic statistics of the imported data frames
print(
    "- Basic Data Frame Statistics\n"
    + "  Raw Data Frames:\n"
    + f"  - crashes: {crashes.shape[0]:,} rows x {crashes.shape[1]} columns\n"
    + f"  - parties: {parties.shape[0]:,} rows x {parties.shape[1]} columns\n"
    + f"  - victims: {victims.shape[0]:,} rows x {victims.shape[1]} columns\n"
    + "  Supporting Data Frames:\n"
    + f"  - boundaries: {boundaries.shape[0]:,} rows x {boundaries.shape[1]} columns\n"
    + f"  - cities: {cities.shape[0]:,} rows x {cities.shape[1]} columns\n"
    + f"  - roads: {roads.shape[0]:,} rows x {roads.shape[1]} columns\n"
    + f"  - blocks: {blocks.shape[0]:,} rows x {blocks.shape[1]} columns"
)

# List of columns for each of the raw data frames ensuring that (a) the columns are in the correct order, and (b) the columns are not duplicated (apart from the case_id column which is common in all data frames, and the party number which is common between the parties and victims data frames).
raw_cols = {
    "crashes": list(
        [
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
            "COUNTY",
        ]
    ),
    "parties": list(
        [
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
            "SPECIAL_INFO_G",
        ]
    ),
    "victims": list(
        [
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
            "VICTIM_EJECTED",
        ]
    ),
}

# Export the raw_cols dictionary to a JSON file
raw_cols_path = os.path.join(prj_dirs["codebook"], "raw_cols.json")
with open(raw_cols_path, "w", encoding = "utf-8") as f:
    json.dump(raw_cols, f, indent = 4, ensure_ascii = False)

# Reorder the columns in the crashes data frame
print("\n- Reordering the columns in the crashes data frame")
crashes = crashes[raw_cols["crashes"]]

# Reorder the columns in the parties data frame
print("- Reordering the columns in the parties data frame")
parties = parties[raw_cols["parties"]]

# Reorder the columns in the victims data frame
print("- Reordering the columns in the victims data frame")
victims = victims[raw_cols["victims"]]

# Make sure that the columns in the data frames are matching the raw_cols dictionary
if (
    not len(crashes.columns) == len(raw_cols["crashes"])
    and len(parties.columns) == len(raw_cols["parties"])
    and len(victims.columns) == len(raw_cols["victims"])
):
    print("Error: The number of columns in the data frames does not match the raw_cols dictionary.")
    sys.exit()
else:
    print("- The number of columns in the data frames matches the raw_cols dictionary.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.4. Import Codebook ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.4. Import Codebook")

# Load the codebook from the project codebook directory

cb = ocs.load_cb()

# create a data frame from the codebook
print("- Creating a data frame from the codebook")
df_cb = pd.DataFrame(cb).transpose()
# Add attributes to the codebook data frame
df_cb.attrs["name"] = "Codebook"
print(df_cb.head())


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Raw Data Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3. Raw Data Operations")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.1. Variable Names and Columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.1. Variable Names and Columns")

# For each of the data frames below, we will process their data by:
# 1. Creating a list of names for the data frame (contains new_name as name, and old_name as value).
# 2. Renaming the columns of the data frame using the new_names from the codebook list.
# 3. Removing all the deprecated and unused columns from the data frame.

###  Crashes Data Frame ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Crashes Data Frame")

# get the row names of the df_cb where the "fc" column has a subdictionary with a "crashes" key equal to 1 and the "raw" column is 1
list_sel = df_cb[df_cb["fc"].apply(lambda x: x["crashes"] == 1) & (df_cb["raw"] == 1)].index.tolist()

old_name = None  # Initialize with a default value
new_name = None  # Initialize with a default value

# Loop through the list of selected names and rename the columns in the crashes data frame
for new_name in list_sel:
    # get the old name from the codebook
    old_name = df_cb.loc[new_name, "var_raw"]
    if old_name in crashes.columns:
        # rename the column in the crashes data frame
        crashes.rename(columns = {old_name: new_name}, inplace = True)

# Remove all the columns in the crashes data frame that are not in list_sel
for col in crashes.columns:
    if col not in list_sel:
        crashes.drop(columns = col, inplace = True)

# Remove the temporary variables
del list_sel, old_name, new_name


### Parties Data Frame ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Parties Data Frame")

# get the row names of the df_cb where the "fc" column has a subdictionary with a "parties" key equal to 1 and the "raw" column is 1
list_sel = df_cb[df_cb["fc"].apply(lambda x: x["parties"] == 1) & (df_cb["raw"] == 1)].index.tolist()

old_name = None  # Initialize with a default value
new_name = None  # Initialize with a default value

# Loop through the list of selected names and rename the columns in the crashes data frame
for new_name in list_sel:
    # get the old name from the codebook
    old_name = df_cb.loc[new_name, "var_raw"]
    if old_name in parties.columns:
        # rename the column in the crashes data frame
        parties.rename(columns = {old_name: new_name}, inplace = True)

# Remove all the columns in the crashes data frame that are not in list_sel
for col in parties.columns:
    if col not in list_sel:
        parties.drop(columns = col, inplace = True)

# Remove the temporary variables
del list_sel, old_name, new_name


### Victims Data Frame ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Victims Data Frame")

# get the row names of the df_cb where the "fc" column has a subdictionary with a "victims" key equal to 1 and the "raw" column is 1
list_sel = df_cb[df_cb["fc"].apply(lambda x: x["victims"] == 1) & (df_cb["raw"] == 1)].index.tolist()

old_name = None  # Initialize with a default value
new_name = None  # Initialize with a default value

# Loop through the list of selected names and rename the columns in the crashes data frame
for new_name in list_sel:
    # get the old name from the codebook
    old_name = df_cb.loc[new_name, "var_raw"]
    if old_name in victims.columns:
        # rename the column in the crashes data frame
        victims.rename(columns = {old_name: new_name}, inplace = True)

# Remove all the columns in the crashes data frame that are not in list_sel
for col in victims.columns:
    if col not in list_sel:
        victims.drop(columns = col, inplace = True)

# Remove the temporary variables
del list_sel, old_name, new_name


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.2. Remove Leading and Trailing Whitespace ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.2. Remove Leading and Trailing Whitespace")

# Remove leading and trailing whitespace from the columns of the datasets
df = None  # Initialize with a default value
for df in [crashes, parties, victims]:
    print(f"- Removing leading and trailing whitespace from {df.attrs['name']} data frame")
    # Loop through the columns of the data frame
    for col in df.columns:
        print(f"  - Processing {col}")
        # Remove leading and trailing whitespace from the column values
        df[col] = df[col].astype(str).str.strip()

# Remove the temporary variable
del df


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.3. Add CID, PID and VID Columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.3. Add CID, PID and VID Columns")


### Add CID Columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adding CID columns")

# Generate CID column for the data frames by converting the case_id column to a string
crashes["cid"] = crashes["case_id"].astype(str).str.strip()
parties["cid"] = parties["case_id"].astype(str).str.strip()
victims["cid"] = victims["case_id"].astype(str).str.strip()

# Relocate the cid column after the case_iD column in the data frames
ocs.relocate_column(df = crashes, col_name = "cid", ref_col_name = "case_id", position = "after")
ocs.relocate_column(df = parties, col_name = "cid", ref_col_name = "case_id", position = "after")
ocs.relocate_column(df = victims, col_name = "cid", ref_col_name = "case_id", position = "after")


### Add PID Columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adding PID columns")

# Generate PID column for the data frames by converting the case_id column to a string and adding the party_number as a string, separated by a dash
parties["pid"] = parties["case_id"].astype(str).str.strip() + "-" + parties["party_number"].astype(str).str.strip()
victims["pid"] = victims["case_id"].astype(str).str.strip() + "-" + victims["party_number"].astype(str).str.strip()

# Relocate the pid column after the cid column in the data frames
ocs.relocate_column(df = parties, col_name = "pid", ref_col_name = "cid", position = "after")
ocs.relocate_column(df = victims, col_name = "pid", ref_col_name = "cid", position = "after")


### Add VID Columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adding VID columns")

# Generate VID column for the data frames by converting the case_id column to a string and adding the party_number and victim_number as strings, separated by a dash
victims["vid"] = (
    victims["case_id"].astype(str).str.strip()
    + "-"
    + victims["party_number"].astype(str).str.strip()
    + "-"
    + victims["victim_number"].astype(str).str.strip()
)

# Relocate the vid column after the pid column in the data frames
ocs.relocate_column(df = victims, col_name = "vid", ref_col_name = "pid", position = "after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.4. Add TotalCrashes, TotalParties, and TotalVictims Columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.4. Add TotalCrashes, TotalParties, and TotalVictims Columns")

# Add TotalCrashes, TotalParties, and TotalVictims columns to the crashes, parties, and victims data frames respectively

# Add count of unique cid to the crashes data frame
crashes["crashes_cid_count"] = crashes.groupby("cid")["cid"].transform("count")
# Relocate the crashes_cid_count column after the cid column in the data frame
ocs.relocate_column(df = crashes, col_name = "crashes_cid_count", ref_col_name = "cid", position = "after")

# add count of unique cid and pid to the parties data frame
parties["parties_cid_count"] = parties.groupby("cid")["cid"].transform("count")
parties["parties_pid_count"] = parties.groupby("pid")["pid"].transform("count")
# Relocate the parties_cid_count and parties_pid_count columns after the pid column in the data frame
ocs.relocate_column(df = parties, col_name = "parties_cid_count", ref_col_name = "pid", position = "after")
ocs.relocate_column(df = parties, col_name = "parties_pid_count", ref_col_name = "parties_cid_count", position = "after")

# add count of unique cid, pid and vid to the victims data frame
victims["victims_cid_count"] = victims.groupby("cid")["cid"].transform("count")
victims["victims_pid_count"] = victims.groupby("pid")["pid"].transform("count")
victims["victims_vid_count"] = victims.groupby("vid")["vid"].transform("count")
# Relocate the victims_cid_count, victims_pid_count and victims_vid_count columns after the vid column in the data frame
ocs.relocate_column(df = victims, col_name = "victims_cid_count", ref_col_name = "vid", position = "after")
ocs.relocate_column(df = victims, col_name = "victims_pid_count", ref_col_name = "victims_cid_count", position = "after")
ocs.relocate_column(df = victims, col_name = "victims_vid_count", ref_col_name = "victims_pid_count", position = "after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.5. Additional Column Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.5. Additional Column Processing")


### City Names Title Case ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Processing city names to title case")

# Convert the city names to title case in the crashes data frame
crashes["city"] = crashes["city"].str.title()

# list all the unique city names in the crashes data frame
print("- Listing all the unique city names in the crashes data frame")
print(f"  - Unique city names: {crashes['city'].nunique():,}")
print(f"  - Unique city names: {crashes['city'].unique()}")

# If needed, convert the city names to title case in the roads data frame
roads["place_name"] = roads["place_name"].str.title()


###  Convert all Counts into Numeric ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Converting all counts into numeric")

i = None  # Initialize with a default value

# Convert each column in the list for the crashes data frame to integers
for i in [
    "case_id",
    "party_count",
    "number_killed",
    "number_inj",
    "count_severe_inj",
    "count_visible_inj",
    "count_complaint_pain",
    "count_ped_killed",
    "count_ped_inj",
    "count_bic_killed",
    "count_bic_inj",
    "count_mc_killed",
    "count_mc_inj",
]:
    if i in crashes.columns:
        # Use pandas built-in type checking function
        if not pd.api.types.is_integer_dtype(crashes[i]):
            crashes[i] = pd.to_numeric(crashes[i], errors = "coerce", downcast = "integer")
            print(f"  -Crashes: Converted {i} to integer")
        else:
            print(f"  -Crashes: {i} is already an integer")

# Convert the distance, longitude, latitude, point_x and point_y columns of the crashes data frame to floats
for i in ["distance", "longitude", "latitude", "point_x", "point_y"]:
    if i in crashes.columns:
        # Use pandas built-in type checking function
        if not pd.api.types.is_float_dtype(crashes[i]):
            crashes[i] = pd.to_numeric(crashes[i], errors = "coerce", downcast = "float")
            print(f"  -Crashes: Converted {i} to float")
        else:
            print(f"  -Crashes: {i} is already a float")

# Convert the list of columns of the parties and victims data frame (if the column exist) to integers
for i in [
    "case_id",
    "party_number",
    "party_age",
    "party_number_killed",
    "party_number_inj",
    "victim_number",
    "victim_age",
    "victim_number_killed",
    "victim_number_inj",
    "vehicle_year",
]:
    if i in parties.columns:
        # Use pandas built-in type checking function
        if not pd.api.types.is_integer_dtype(parties[i]):
            parties[i] = pd.to_numeric(parties[i], errors = "coerce", downcast = "integer")
            print(f"  -Parties: Converted {i} to integer")
        else:
            print(f"  -Parties: {i} is already an integer")
    if i in victims.columns:
        # Use pandas built-in type checking function
        if not pd.api.types.is_integer_dtype(victims[i]):
            victims[i] = pd.to_numeric(victims[i], errors = "coerce", downcast = "integer")
            print(f"  -Victims: Converted {i} to integer")
        else:
            print(f"  -Victims: {i} is already an integer")

# Convert the city_area_sq_Mi, city_pop_dens, city_hou_dens of the cities data frame to floats
for i in ["city_area_sq_mi", "city_pop_dens", "city_hou_dens"]:
    if i in cities.columns:
        # Use pandas built-in type checking function
        if not pd.api.types.is_float_dtype(cities[i]):
            cities[i] = pd.to_numeric(cities[i], errors = "coerce", downcast = "float")
            print(f"  -Cities: Converted {i} to float")
        else:
            print(f"  -Cities: {i} is already a float")

# Convert the list of columns in the cities data frame to integers
for i in [
    "city_pop_total",
    "city_hou_total",
    "city_pop_asian",
    "city_pop_black",
    "city_pop_hispanic",
    "city_pop_white",
    "city_vehicles",
    "city_travel_time",
]:
    if i in cities.columns:
        # Use pandas built-in type checking function
        if not pd.api.types.is_integer_dtype(cities[i]):
            cities[i] = pd.to_numeric(cities[i], errors = "coerce", downcast = "integer")
            print(f"  -Cities: Converted {i} to integer")
        else:
            print(f"  -Cities: {i} is already an integer")

# Convert the road_length column of the roads data frame to floats
for i in ["road_length"]:
    if i in roads.columns:
        # Use pandas built-in type checking function
        if not pd.api.types.is_float_dtype(roads[i]):
            roads[i] = pd.to_numeric(roads[i], errors = "coerce", downcast = "float")
            print(f"  -Roads: Converted {i} to float")
        else:
            print(f"  -Roads: {i} is already a float")
# Remove the temporary variable
del i


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 4. Data Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4. Data Processing")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.1. Add Dataset Identifiers ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.1. Add Dataset Identifiers")

# Add the dataset identifiers to the crashes, parties, and victims data frames


### Crashes Dataset Identifier ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adding crashes dataset identifier")

# Add a unique crash tag
crashes["crash_tag"] = 1
# Move the crash_tag column after the cid column in the data frame
ocs.relocate_column(df = crashes, col_name = "crash_tag", ref_col_name = "cid", position = "after")


### Parties Dataset Identifier ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adding parties dataset identifier")

# Add a unique party tag
parties["party_tag"] = 1
# Move the party_tag column after the pid column in the data frame
ocs.relocate_column(df = parties, col_name = "party_tag", ref_col_name = "pid", position = "after")


### Victims Dataset Identifier ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adding victims dataset identifier")

# Add a unique victim tag
victims["victim_tag"] = 1
# Move the victim_tag column after the vid column in the data frame
ocs.relocate_column(df = victims, col_name = "victim_tag", ref_col_name = "vid", position = "after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 4.2. Tagging Datasets ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4.2. Tagging Datasets")

# for each of the crashes, parties and victims dataframes, add a tag column to the data frame, to indicate if the observation belongs to this dataset.


### Crashes Tag ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adding crashes tag")

# Create a count of crashes for each cid in the crashes data frame
crashes["crashes_case_tag"] = crashes.groupby("cid")["cid"].transform("count")

# Relocate the crashes_case_tag column after the crash_tag column in the data frame
ocs.relocate_column(df = crashes, col_name = "crashes_case_tag", ref_col_name = "crash_tag", position = "after")


### Parties Tag ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adding parties tag")

# Create a count of parties for each cid in the parties data frame
parties["parties_case_tag"] = parties.groupby("cid")["cid"].transform("count")
# Relocate the parties_case_tag column after the party_tag column in the data frame
ocs.relocate_column(df = parties, col_name = "parties_case_tag", ref_col_name = "party_tag", position = "after")


### Victims Tag ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Adding victims tag")

# Create a count of victims for each cid in the victims data frame
victims["victims_case_tag"] = victims.groupby("cid")["cid"].transform("count")
# Move the victims_case_tag column after the victim_tag column in the data frame
ocs.relocate_column(df = victims, col_name = "victims_case_tag", ref_col_name = "victim_tag", position = "after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 5. Date and Time Data Frame Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5. Date and Time Data Frame Operations")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.1. Convert Data Types ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.1. Convert Data Types")

# convert the accident_year column to integer, if it is not already
if not pd.api.types.is_integer_dtype(crashes["accident_year"]):
    crashes["accident_year"] = pd.to_numeric(crashes["accident_year"], errors = "coerce", downcast = "integer")
    print("- Converted accident_year to integer")
else:
    print("- accident_year is already an integer")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.2. Collision and Process Date Conversion ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.2. Collision and Process Date Conversion")

# Convert the date_process into date by using the first 4 digits as year, the next 2 digits as month, and the last 2 digits as day
crashes["date_process"] = pd.to_datetime(crashes["process_date"], format = "%Y-%m-%d", errors = "coerce")

# Convert the coll_time (Collision Time) to integer, it it is not already
if not pd.api.types.is_integer_dtype(crashes["coll_time"]):
    crashes["coll_time"] = pd.to_numeric(crashes["coll_time"], errors = "coerce", downcast = "integer")
    print("- Converted coll_time to integer")
else:
    print("- coll_time is already an integer")

# Create a temporary column to store the formatted time string
crashes["coll_time_temp"] = crashes["coll_time"].apply(ocs.format_coll_time)

# Convert the coll_date and coll_time_temp columns to a datetime object with time zone "America/Los_Angeles"
crashes["date_datetime"] = pd.to_datetime(
    crashes["coll_date"] + " " + crashes["coll_time_temp"], format = "%Y-%m-%d %H:%M:%S", errors = "coerce"
)

# Delete the temporary column
del crashes["coll_time_temp"]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.3. Create Date and Time Individual Columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.3. Create Date and Time Individual Columns")


### Year (Date) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating year column")

# Create a year column from the date_year column
crashes["dt_year"] = crashes["date_datetime"].dt.year

# Create a year datetime column from the date_datetime column as a datetime object
crashes["date_year"] = pd.to_datetime(crashes["date_datetime"].dt.year, format = "%Y", errors = "coerce")


### Quarter (Date) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating quarter column")

# Create a quarter column from the date_quarter column
crashes["dt_quarter"] = crashes["date_datetime"].dt.quarter

# Apply the function to create date_quarter column
crashes["date_quarter"] = crashes.apply(ocs.quarter_to_date, axis = 1)

# Convert the dt_quarter column to categorical
crashes["dt_quarter"] = ocs.categorical_series(var_series=crashes["dt_quarter"], var_name="dt_quarter", cb_dict=cb)


### Month (Date) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating month column")

# Create a month column from the date_month column
crashes["dt_month"] = crashes["date_datetime"].dt.month

# Create a month datetime column from the date_datetime column as a datetime object that includes the year
crashes["date_month"] = pd.to_datetime(crashes["date_datetime"].dt.strftime("%Y-%m"), format = "%Y-%m", errors = "coerce")

# Convert the dt_month column to categorical
crashes["dt_month"] = ocs.categorical_series(var_series = crashes["dt_month"], var_name = "dt_month", cb_dict = cb)


### Week of the Year (Date) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating week of the year column")

# Create a week of the year column from the date_week column
crashes["dt_year_week"] = crashes["date_datetime"].dt.isocalendar().week

# Create a week of the year datetime column from the date_datetime column as a datetime object
crashes["date_week"] = pd.to_datetime(
    crashes["date_datetime"].dt.year.astype(str)
    + "-W"
    + crashes["date_datetime"].dt.isocalendar().week.astype(str)
    + "-1",
    format = "%Y-W%W-%w",
    errors = "coerce",
)


### Day (Date) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating day column")

# Create a day datetime column from the date_datetime column as a datetime object
crashes["date_day"] = pd.to_datetime(
    crashes["date_datetime"].dt.strftime("%Y-%m-%d"), format = "%Y-%m-%d", errors = "coerce"
)


### Week Day (Date) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating week day column")

# Create a week day column from the date_week_day column
crashes["dt_week_day"] = crashes["date_datetime"].dt.isocalendar().day

# Convert the dt_week_day column to categorical
crashes["dt_week_day"] = ocs.categorical_series(var_series = crashes["dt_week_day"], var_name = "dt_week_day", cb_dict = cb)


### Day of the Month (Date) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating day of the month column")

# Create a day of the month column from the date_dayOfMonth column
crashes["dt_month_day"] = crashes["date_datetime"].dt.day


### Day of the Year (Date) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating day of the year column")

# Create a day of the year column from the date_dayOfYear column
crashes["dt_year_day"] = crashes["date_datetime"].dt.dayofyear


# region Hour and Minute (Time)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating hour and minute columns")

# Create a hour column from the dateHour column
crashes["dt_hour"] = crashes["date_datetime"].dt.hour

# Create a minute column from the dateMinute column
crashes["dt_minute"] = crashes["date_datetime"].dt.minute


### Daylight Savings Time and Time Zone (Time) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating daylight savings time and time zone columns")

# Create a daylight savings time flag column (0 = no, 1 = yes, -1 = unknown)
crashes["dt_dst"] = ocs.is_dst(crashes["date_datetime"])

# Create a time zone column from the dateTimeZone column
crashes["dt_zone"] = crashes["dt_dst"] - 8

# Convert the dt_dst column to categorical
crashes["dt_dst"] = ocs.categorical_series(var_series=crashes["dt_dst"], var_name="dt_dst", cb_dict=cb)

# Convert the dt_zone column to categorical
crashes["dt_zone"] = ocs.categorical_series(var_series=crashes["dt_zone"], var_name="dt_zone", cb_dict=cb)


### Order the New Date and Time Columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Ordering the new date and time columns")

# Define the columns to be relocated in the desired order
columns_to_relocate = [
    "date_datetime",
    "date_year",
    "date_quarter",
    "date_month",
    "date_week",
    "date_day",
    "date_process",
    "dt_year",
    "dt_quarter",
    "dt_month",
    "dt_year_week",
    "dt_week_day",
    "dt_month_day",
    "dt_year_day",
    "dt_hour",
    "dt_minute",
    "dt_dst",
    "dt_zone",
]

# Relocate the columns in the desired order
ocs.relocate_column(df=crashes, col_name=columns_to_relocate, ref_col_name="city", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.4. Collision Time Intervals ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.4. Collision Time Intervals")

# Create a new column in the crashes data frame called coll_time_intervals that has value of 1 if the dt_hour is between 00:00 and 06:00, 2 if it is between 06:00 and 12:00, 3 if it is between 12:00 and 18:00, 4 if it is between 18:00 and 24:00
crashes["coll_time_intervals"] = pd.cut(
    crashes["dt_hour"], bins = [0, 6, 12, 18, 24], labels = [1, 2, 3, 4], right = False, include_lowest = True
)

# Convert the coll_time_intervals column to categorical
crashes["coll_time_intervals"] = ocs.categorical_series(
    var_series=crashes["coll_time_intervals"], var_name="coll_time_intervals", cb_dict=cb
)

# Relocate the coll_time_intervals column after the coll_time column in the data frame
ocs.relocate_column(df=crashes, col_name="coll_time_intervals", ref_col_name="coll_time", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 5.5. Rush Hours ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5.5. Rush Hours")


### Rush Hours Intervals ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating rush hours intervals")

# create a new column in the crashes data frame called rush_hours that has value of 1 if the dt_week_day is between 2 and 6 (Monday to Friday) and the coll_time is between 07:00 and 10:00, 2 if the dt_week_day is between 2 and 6 (Monday to Friday) and the coll_time is between 16:00 and 19:00,9 if the coll_time is greater than 2400, and 3 otherwise.

# Non rush hours
crashes["rush_hours"] = 0
# Morning rush hours
crashes.loc[
    (crashes["dt_week_day"].between("Monday", "Friday")) & (crashes["dt_hour"].between(7, 10)), "rush_hours"
] = 1
# Evening rush hours
crashes.loc[
    (crashes["dt_week_day"].between("Monday", "Friday")) & (crashes["dt_hour"].between(16, 19)), "rush_hours"
] = 2
# Unknown time
crashes.loc[crashes["dt_hour"] > 2400, "rush_hours"] = 9

# Convert the rush_hours column to categorical
crashes["rush_hours"] = ocs.categorical_series(var_series=crashes["rush_hours"], var_name="rush_hours", cb_dict=cb)

# Relocate the rush_hours column after the coll_time_intervals column in the data frame
ocs.relocate_column(df=crashes, col_name="rush_hours", ref_col_name="coll_time_intervals", position="after")


### Rush Hours Indicators ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating rush hours indicators")

# create a new column in the crashes data frame called rush_hours_bin that has value of 1 if the rush_hours is Morning or Evening rush hours, and 0 otherwise
crashes["rush_hours_bin"] = 0
crashes.loc[crashes["rush_hours"].isin(["Morning (6-9am)", "Evening (4-7pm)"]), "rush_hours_bin"] = 1

# Convert the rush_hours_bin column to categorical
crashes["rush_hours_bin"] = ocs.categorical_series(
    var_series=crashes["rush_hours_bin"], var_name="rush_hours_bin", cb_dict=cb
)

# Relocate the rush_hours_bin column after the rush_hours column in the data frame
ocs.relocate_column(df=crashes, col_name="rush_hours_bin", ref_col_name="rush_hours", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 6. Collision Severity Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6. Collision Severity Processing")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.1. Factoring Collision Severity ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.1. Factoring Collision Severity")

# Convert the collision severity column to integer
crashes["coll_severity"] = crashes["coll_severity"].astype(int)

# using the codebook's "recode" key for the coll_severity column, recode the values of the coll_severity column to the new values. Specifically, the values of the coll_severity column are represented as the keys of the cb["coll_severity"]["recode"] dictionary (if converted to integers), and the new values are represented as the values of the cb["coll_severity"]["recode"] dictionary.
crashes["coll_severity"] = crashes["coll_severity"].map(
    {int(k): v for k, v in cb["coll_severity"]["recode"].items()}
)

# Convert the coll_severity column to categorical
crashes["coll_severity"] = ocs.categorical_series(
    var_series=crashes["coll_severity"], var_name="coll_severity", cb_dict=cb
)

# Create a numeric version of the collision severity column
crashes["coll_severity_num"] = crashes["coll_severity"].cat.codes.astype(int)
# crashes["coll_severity_num"] = crashes["coll_severity"].astype(int)

# Relocate the coll_severity_num column after the coll_severity column in the data frame
ocs.relocate_column(df=crashes, col_name="coll_severity_num", ref_col_name="coll_severity", position="after")

# Reverse the order of the coll_severity_num column so that higher numbers indicate more severe collisions
max_severity = crashes["coll_severity_num"].max()
crashes["coll_severity_hs"] = ((max_severity + 1) - crashes["coll_severity_num"]).astype(int)

# Relocate the coll_severity_hs column after the coll_severity_num column in the data frame
ocs.relocate_column(df=crashes, col_name="coll_severity_hs", ref_col_name="coll_severity_num", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.2. Binary Collision Severity ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.2. Binary Collision Severity")

# Generate a new column in the crashes data frame called coll_severity_bin that has value of 1 if the coll_severity is "Serious or severe injury" or "Fatal injury", and 0 otherwise
crashes["coll_severity_bin"] = 0
crashes.loc[crashes["coll_severity"].isin(["Serious or severe injury", "Fatal injury"]), "coll_severity_bin"] = 1

# Convert the coll_severity_bin column to categorical
crashes["coll_severity_bin"] = ocs.categorical_series(
    var_series=crashes["coll_severity_bin"], var_name="coll_severity_bin", cb_dict=cb
)

# Relocate the coll_severity_bin column after the coll_severity_num column in the data frame
ocs.relocate_column(df=crashes, col_name="coll_severity_bin", ref_col_name="coll_severity_num", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.3. Ranked Collision Severity ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.3. Ranked Collision Severity")

# Generate a new column in the crashes data frame called coll_severity_rank that ranks the collision severity based on the number of killed and severe injuries

# Apply the function to the crashes data frame to create the coll_severity_rank column
crashes["coll_severity_rank"] = crashes.apply(ocs.get_coll_severity_rank, axis = 1)

# Create a numeric version of the coll_severity_rank column
crashes["coll_severity_rank_num"] = crashes["coll_severity_rank"].astype(int)

# Convert the coll_severity_rank column to categorical
crashes["coll_severity_rank"] = ocs.categorical_series(
    var_series=crashes["coll_severity_rank"], var_name="coll_severity_rank", cb_dict=cb
)

# Relocate the coll_severity_rank column after the coll_severity_bin column in the data frame
ocs.relocate_column(df=crashes, col_name="coll_severity_rank", ref_col_name="coll_severity_bin", position="after")

# Relocate the coll_severity_rank_num column after the coll_severity_rank column in the data frame
ocs.relocate_column(df=crashes, col_name="coll_severity_rank_num", ref_col_name="coll_severity_rank", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 6.4. Collision Severity Indicators ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6.4. Collision Severity Indicators")


### Severe Injury Indicator ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Severe Injury Indicator")

# Severe Injury Indicator: 1 if coll_severity_num == 3, else 0
crashes["ind_severe"] = (crashes["coll_severity_num"] == 3).astype(int)

# Convert the ind_severe column to categorical
crashes["ind_severe"] = ocs.categorical_series(var_series=crashes["ind_severe"], var_name="ind_severe", cb_dict=cb)

# Relocate the ind_severe column after the coll_severity_rank_num column in the data frame
ocs.relocate_column(df=crashes, col_name="ind_severe", ref_col_name="coll_severity_rank_num", position="after")


### Fatal Injury Indicator ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating fatal Injury Indicator")

# Fatal Injury Indicator: 1 if coll_severity_num == 4, else 0
crashes["ind_fatal"] = (crashes["coll_severity_num"] == 4).astype(int)

# Convert the ind_fatal column to categorical
crashes["ind_fatal"] = ocs.categorical_series(var_series=crashes["ind_fatal"], var_name="ind_fatal", cb_dict=cb)

# Relocate the ind_fatal column after the ind_severe column in the data frame
ocs.relocate_column(df=crashes, col_name="ind_fatal", ref_col_name="ind_severe", position="after")


### Multiple Severe or Fatal Injuries Indicator ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Multiple Severe or Fatal Injuries Indicator")

# Multiple Severe or Fatal Injuries Indicator: 1 if coll_severity_rank in [2, 5, 6, 7, 8], else 0
crashes["ind_multi"] = crashes["coll_severity_rank_num"].isin([2, 5, 6, 7, 8]).astype(int)

# Convert the ind_multi column to categorical
crashes["ind_multi"] = ocs.categorical_series(var_series=crashes["ind_multi"], var_name="ind_multi", cb_dict=cb)

# Relocate the ind_multi column after the ind_fatal column in the data frame
ocs.relocate_column(df=crashes, col_name="ind_multi", ref_col_name="ind_fatal", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 7. Generate New Counts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7. Generate New Counts")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.1. Generate Victim Count ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.1. Generate Victim Count")

# Generate a new column in the crashes data frame called victim_count that is the sum of the number_killed and number_inj columns
crashes["victim_count"] = (crashes["number_killed"] + crashes["number_inj"]).astype(int)

# Relocate the victim_count column after the party_count column in the data frame
ocs.relocate_column(df=crashes, col_name="victim_count", ref_col_name="party_count", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.2. Generate Combined Fatality and Severity Counts ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.2. Generate Combined Fatality and Severity Counts")

# Generate a new column in the crashes data frame called count_fatal_severe that is the sum of the number_killed and count_severe_inj columns
crashes["count_fatal_severe"] = (crashes["number_killed"] + crashes["count_severe_inj"]).astype(int)

# Relocate the count_fatal_severe column after the count_complaint_pain column in the data frame
ocs.relocate_column(df=crashes, col_name="count_fatal_severe", ref_col_name="count_complaint_pain", position="after")

# Generate a new column in the crashes data frame called count_minor_pain that is the sum of the count_visible_inj and count_complaint_pain columns
crashes["count_minor_pain"] = (crashes["count_visible_inj"] + crashes["count_complaint_pain"]).astype(int)

# Relocate the count_minor_pain column after the count_fatal_severe column in the data frame
ocs.relocate_column(df=crashes, col_name="count_minor_pain", ref_col_name="count_fatal_severe", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 7.3. Generate Car Passenger Killed and Injured Count ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7.3. Generate Car Passenger Killed and Injured Count")

# Generate a new column in the crashes data frame called count_car_killed that is the difference between the number_killed and the sum of count_ped_killed, count_bic_killed, and count_mc_killed columns
crashes["count_car_killed"] = (
    crashes["number_killed"] - crashes["count_ped_killed"] - crashes["count_bic_killed"] - crashes["count_mc_killed"]
).astype(int)

# Relocate the count_car_killed column after the count_complaint_pain column
ocs.relocate_column(df=crashes, col_name="count_car_killed", ref_col_name="count_minor_pain", position="after")

# Generate a new column in the crashes data frame called count_car_inj that is the difference between the number_inj and the sum of count_ped_inj, count_bic_inj, and count_mc_inj columns
crashes["count_car_inj"] = (
    crashes["number_inj"] - crashes["count_ped_inj"] - crashes["count_bic_inj"] - crashes["count_mc_inj"]
).astype(int)

# Relocate the count_car_inj column after the count_car_killed column
ocs.relocate_column(df=crashes, col_name="count_car_inj", ref_col_name="count_car_killed", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 8. Crash Characteristics ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8. Crash Characteristics")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.1. Primary Crash Factor ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.1. Primary Crash Factor")

# Map the values, set unmapped to 999
crashes["primary_coll_factor"] = (
    crashes["primary_coll_factor"].map(cb["primary_coll_factor"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
crashes.loc[crashes["primary_coll_factor"] == 999, "primary_coll_factor"] = np.nan

# Convert the primary_coll_factor column to categorical
crashes["primary_coll_factor"] = ocs.categorical_series(
    var_series=crashes["primary_coll_factor"], var_name="primary_coll_factor", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.2. Collision Type ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.2. Collision Type")

# Recode the collision type to numeric
crashes["type_of_coll"] = crashes["type_of_coll"].map(cb["type_of_coll"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["type_of_coll"] == 999, "type_of_coll"] = np.nan

# Convert the type_of_coll column to categorical
crashes["type_of_coll"] = ocs.categorical_series(
    var_series=crashes["type_of_coll"], var_name="type_of_coll", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.3. Pedestrian Crash ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.3. Pedestrian Crash")

# Recode the pedestrian crash to numeric
crashes["ped_accident"] = crashes["ped_accident"].map(cb["ped_accident"]["recode"]).fillna(0).astype(int)

# Convert the ped_accident column to categorical
crashes["ped_accident"] = ocs.categorical_series(
    var_series=crashes["ped_accident"], var_name="ped_accident", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.4. Bicycle Crash ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.4. Bicycle Crash")

# Recode the bicycle crash to numeric
crashes["bic_accident"] = crashes["bic_accident"].map(cb["bic_accident"]["recode"]).fillna(0).astype(int)

# Convert the bic_accident column to categorical
crashes["bic_accident"] = ocs.categorical_series(
    var_series=crashes["bic_accident"], var_name="bic_accident", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.5. Motorcycle Crash ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.5. Motorcycle Crash")

# Recode the motorcycle crash (mc_accident) to numeric
crashes["mc_accident"] = crashes["mc_accident"].map(cb["mc_accident"]["recode"]).fillna(0).astype(int)

# Convert the mc_accident column to categorical
crashes["mc_accident"] = ocs.categorical_series(var_series=crashes["mc_accident"], var_name="mc_accident", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.6. Truck Crash ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.6. Truck Crash")

# Recode the truck crash (truck_accident) to numeric
crashes["truck_accident"] = crashes["truck_accident"].map(cb["truck_accident"]["recode"]).fillna(0).astype(int)

# Convert the truck_accident column to categorical
crashes["truck_accident"] = ocs.categorical_series(
    var_series=crashes["truck_accident"], var_name="truck_accident", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.7. Hit and Run ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.7. Hit and Run")


### Hit and Run (type of) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Hit and Run (type of)")

# Recode the hit and run (hit_and_run) to numeric
crashes["hit_and_run"] = crashes["hit_and_run"].map(cb["hit_and_run"]["recode"]).fillna(0).astype(int)

# Convert the hit_and_run column to categorical
crashes["hit_and_run"] = ocs.categorical_series(var_series=crashes["hit_and_run"], var_name="hit_and_run", cb_dict=cb)


### Hit and Run (binary) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Hit and Run (binary)")

# Create a binary column called hit_and_run_bin that has value of 1 if the hit_and_run is "Misdemeanor" or "Felony", and 0 otherwise
crashes["hit_and_run_bin"] = 0
crashes.loc[crashes["hit_and_run"].isin(["Misdemeanor", "Felony"]), "hit_and_run_bin"] = 1

# Convert the hit_and_run_bin column to categorical
crashes["hit_and_run_bin"] = ocs.categorical_series(
    var_series=crashes["hit_and_run_bin"], var_name="hit_and_run_bin", cb_dict=cb
)

# Relocate the hit_and_run_bin column after the hit_and_run column in the data frame
ocs.relocate_column(df=crashes, col_name="hit_and_run_bin", ref_col_name="hit_and_run", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.8. Alcohol Involved ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.8. Alcohol Involved")

# Recode the alcohol involved (alcohol_involved) to numeric
crashes["alcohol_involved"] = (
    crashes["alcohol_involved"].map(cb["alcohol_involved"]["recode"]).fillna(0).astype(int)
)

# Convert the alcohol_involved column to categorical
crashes["alcohol_involved"] = ocs.categorical_series(
    var_series=crashes["alcohol_involved"], var_name="alcohol_involved", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.9. CHP Shift ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.9. CHP Shift")

# Recode the CHP shift (chp_shift) to numeric
crashes["chp_shift"] = crashes["chp_shift"].astype(int)

# Convert the chp_shift column to categorical
crashes["chp_shift"] = ocs.categorical_series(var_series=crashes["chp_shift"], var_name="chp_shift", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.10. Special Conditions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.10. Special Conditions")

# Recode the special conditions (special_cond) to numeric
crashes["special_cond"] = crashes["special_cond"].map(cb["special_cond"]["recode"]).fillna(np.nan).astype(int)

# Convert the special_cond column to categorical
crashes["special_cond"] = ocs.categorical_series(
    var_series=crashes["special_cond"], var_name="special_cond", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.11. Beat Type ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.11. Beat Type")

# Recode the beat type (beat_type) to numeric
crashes["beat_type"] = crashes["beat_type"].astype(int)

# Convert the beat_type column to categorical
crashes["beat_type"] = ocs.categorical_series(var_series=crashes["beat_type"], var_name="beat_type", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.12. CHP Beat Type ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.12. CHP Beat Type")

# Recode the CHP beat type (chp_beat_type) to numeric
crashes["chp_beat_type"] = (
    crashes["chp_beat_type"].map(cb["chp_beat_type"]["recode"]).fillna(np.nan).astype(int)
)

# Convert the chp_beat_type column to categorical
crashes["chp_beat_type"] = ocs.categorical_series(
    var_series=crashes["chp_beat_type"], var_name="chp_beat_type", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.13. CHP Beat Class ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.13. CHP Beat Class")

# Recode the CHP beat class (chp_beat_class) to numeric
crashes["chp_beat_class"] = crashes["chp_beat_class"].astype(int)

# Convert the chp_beat_class column to categorical
crashes["chp_beat_class"] = ocs.categorical_series(
    var_series=crashes["chp_beat_class"], var_name="chp_beat_class", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.14. Direction ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.14. Direction")

# Recode the direction (direction) to numeric
crashes["direction"] = crashes["direction"].map(cb["direction"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["direction"] == 999, "direction"] = np.nan

# Convert the direction column to categorical
crashes["direction"] = ocs.categorical_series(var_series=crashes["direction"], var_name="direction", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.15. Intersection ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.15. Intersection")

# Recode the intersection (intersection) to numeric
crashes["intersection"] = crashes["intersection"].map(cb["intersection"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["intersection"] == 999, "intersection"] = np.nan

# Convert the intersection column to categorical
crashes["intersection"] = ocs.categorical_series(
    var_series=crashes["intersection"], var_name="intersection", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.16. Weather Conditions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.16. Weather Conditions")

# Recode the weather_1 to numeric
crashes["weather_1"] = crashes["weather_1"].map(cb["weather_1"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["weather_1"] == 999, "weather_1"] = np.nan

# Recode the weather_2 to numeric
crashes["weather_2"] = crashes["weather_2"].map(cb["weather_2"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["weather_2"] == 999, "weather_2"] = np.nan

# Combine the weather_1 and weather_2 columns into a new column called weather_comb
crashes["weather_comb"] = (crashes["weather_1"].astype(float) * 10 + crashes["weather_2"].astype(float)).astype("Int64")

# Convert the weather_1 column to categorical
crashes["weather_1"] = ocs.categorical_series(var_series=crashes["weather_1"], var_name="weather_1", cb_dict=cb)

# Convert the weather_2 column to categorical
crashes["weather_2"] = ocs.categorical_series(var_series=crashes["weather_2"], var_name="weather_2", cb_dict=cb)

# Convert the weather_comb column to categorical
crashes["weather_comb"] = ocs.categorical_series(
    var_series=crashes["weather_comb"], var_name="weather_comb", cb_dict=cb
)

# Relocate the weather_comb column after the weather_2 column in the data frame
ocs.relocate_column(df=crashes, col_name="weather_comb", ref_col_name="weather_2", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.17. Road Surface ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.17. Road Surface")

# Recode the road surface (road_surface) to numeric
crashes["road_surface"] = crashes["road_surface"].map(cb["road_surface"]["recode"]).fillna(999).astype(int)

# Set 999 values to NaN
crashes.loc[crashes["road_surface"] == 999, "road_surface"] = np.nan

# Convert the road_surface column to categorical
crashes["road_surface"] = ocs.categorical_series(
    var_series=crashes["road_surface"], var_name="road_surface", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.18. Road Condition ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.18. Road Condition")

# Recode the road condition 1 (road_cond_1) to numeric
crashes["road_cond_1"] = crashes["road_cond_1"].map(cb["road_cond_1"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["road_cond_1"] == 999, "road_cond_1"] = np.nan

# Recode the road condition 2 (road_cond_2) to numeric
crashes["road_cond_2"] = crashes["road_cond_2"].map(cb["road_cond_2"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["road_cond_2"] == 999, "road_cond_2"] = np.nan

# Convert the road_cond_1 column to categorical
crashes["road_cond_1"] = ocs.categorical_series(var_series=crashes["road_cond_1"], var_name="road_cond_1", cb_dict=cb)

# Convert the road_cond_2 column to categorical
crashes["road_cond_2"] = ocs.categorical_series(var_series=crashes["road_cond_2"], var_name="road_cond_2", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.19. Lighting ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.19. Lighting")

# Recode the lighting (lighting) to numeric
crashes["lighting"] = crashes["lighting"].map(cb["lighting"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["lighting"] == 999, "lighting"] = np.nan

# Convert the lighting column to categorical
crashes["lighting"] = ocs.categorical_series(var_series=crashes["lighting"], var_name="lighting", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.20. Control Device ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.20. Control Device")

# Recode the control device (control_device) to numeric
crashes["control_device"] = (
    crashes["control_device"].map(cb["control_device"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
crashes.loc[crashes["control_device"] == 999, "control_device"] = np.nan

# Convert the control_device column to categorical
crashes["control_device"] = ocs.categorical_series(
    var_series=crashes["control_device"], var_name="control_device", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.21. State Highway Indicator ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.21. State Highway Indicator")

# Recode the state highway indicator (state_hwy_ind) to numeric
crashes["state_hwy_ind"] = crashes["state_hwy_ind"].map(cb["state_hwy_ind"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["state_hwy_ind"] == 999, "state_hwy_ind"] = np.nan

# Convert the state_hwy_ind column to categorical
crashes["state_hwy_ind"] = ocs.categorical_series(
    var_series=crashes["state_hwy_ind"], var_name="state_hwy_ind", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.22. Side of Highway ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.22. Side of Highway")

# Recode the side of highway (side_of_hwy) to numeric
crashes["side_of_hwy"] = crashes["side_of_hwy"].map(cb["side_of_hwy"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["side_of_hwy"] == 999, "side_of_hwy"] = np.nan

# Convert the side_of_hwy column to categorical
crashes["side_of_hwy"] = ocs.categorical_series(var_series=crashes["side_of_hwy"], var_name="side_of_hwy", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.23. Tow Away ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.23. Tow Away")

# Recode the tow away (tow_away) to numeric
crashes["tow_away"] = crashes["tow_away"].map(cb["tow_away"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["tow_away"] == 999, "tow_away"] = np.nan

# Convert the tow_away column to categorical
crashes["tow_away"] = ocs.categorical_series(var_series=crashes["tow_away"], var_name="tow_away", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.24. PCF Code of Violation ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.24. PCF Code of Violation")

# Recode the PCF code of violation (pcf_code_of_viol) to numeric
crashes["pcf_code_of_viol"] = (
    crashes["pcf_code_of_viol"].map(cb["pcf_code_of_viol"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
crashes.loc[crashes["pcf_code_of_viol"] == 999, "pcf_code_of_viol"] = np.nan

# Convert the pcf_code_of_viol column to categorical
crashes["pcf_code_of_viol"] = ocs.categorical_series(
    var_series=crashes["pcf_code_of_viol"], var_name="pcf_code_of_viol", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.25. PCF Violation Category ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.25. PCF Violation Category")

# Recode the PCF violation category (pcf_viol_category) to numeric
crashes["pcf_viol_category"] = (
    crashes["pcf_viol_category"].map(cb["pcf_viol_category"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
crashes.loc[crashes["pcf_viol_category"] == 999, "pcf_viol_category"] = np.nan

# Convert the pcf_viol_category column to categorical
crashes["pcf_viol_category"] = ocs.categorical_series(
    var_series=crashes["pcf_viol_category"], var_name="pcf_viol_category", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.26. MVIW ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.26. MVIW")

## Recode the MVIW (mviw) to numeric
crashes["mviw"] = crashes["mviw"].map(cb["mviw"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["mviw"] == 999, "mviw"] = np.nan

# Convert the mviw column to categorical
crashes["mviw"] = ocs.categorical_series(var_series=crashes["mviw"], var_name="mviw", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.27. Pedestrian Action ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.27. Pedestrian Action")

# Recode the pedestrian action (ped_action) to numeric
crashes["ped_action"] = crashes["ped_action"].map(cb["ped_action"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["ped_action"] == 999, "ped_action"] = np.nan

# Convert the ped_action column to categorical
crashes["ped_action"] = ocs.categorical_series(var_series=crashes["ped_action"], var_name="ped_action", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.28. Not Private Property ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.28. Not Private Property")

# Recode the not private property (not_private_property) to numeric
crashes["not_private_property"] = (
    crashes["not_private_property"].map(cb["not_private_property"]["recode"]).fillna(0).astype(int)
)

# Convert the not_private_property column to categorical
crashes["not_private_property"] = ocs.categorical_series(
    var_series=crashes["not_private_property"], var_name="not_private_property", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.29. State Wide Vehicle Type at Fault ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.29. State Wide Vehicle Type at Fault")

# Recode the state wide vehicle type at fault (stwd_veh_type_at_fault) to numeric
crashes["stwd_veh_type_at_fault"] = (
    crashes["stwd_veh_type_at_fault"].map(cb["stwd_veh_type_at_fault"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
crashes.loc[crashes["stwd_veh_type_at_fault"] == 999, "stwd_veh_type_at_fault"] = np.nan

# Convert the stwd_veh_type_at_fault column to categorical
crashes["stwd_veh_type_at_fault"] = ocs.categorical_series(
    var_series=crashes["stwd_veh_type_at_fault"], var_name="stwd_veh_type_at_fault", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.30. CHP Vehicle Type at Fault ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.30. CHP Vehicle Type at Fault")

# Recode the CHP vehicle type at fault (chp_veh_type_at_fault) to numeric
crashes["chp_veh_type_at_fault"] = (
    crashes["chp_veh_type_at_fault"].map(cb["chp_veh_type_at_fault"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
crashes.loc[crashes["chp_veh_type_at_fault"] == 999, "chp_veh_type_at_fault"] = np.nan

# Convert the chp_veh_type_at_fault column to categorical
crashes["chp_veh_type_at_fault"] = ocs.categorical_series(
    var_series=crashes["chp_veh_type_at_fault"], var_name="chp_veh_type_at_fault", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 8.31. Primary and Secondary Ramp ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8.31. Primary and Secondary Ramp")

# Recode the primary ramp (primary_ramp) to numeric
crashes["primary_ramp"] = crashes["primary_ramp"].map(cb["primary_ramp"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
crashes.loc[crashes["primary_ramp"] == 999, "primary_ramp"] = np.nan

# Convert the primary_ramp column to categorical
crashes["primary_ramp"] = ocs.categorical_series(
    var_series=crashes["primary_ramp"], var_name="primary_ramp", cb_dict=cb
)

# Recode the secondary ramp (secondary_ramp) to numeric
crashes["secondary_ramp"] = (
    crashes["secondary_ramp"].map(cb["secondary_ramp"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
crashes.loc[crashes["secondary_ramp"] == 999, "secondary_ramp"] = np.nan

# Convert the secondary_ramp column to categorical
crashes["secondary_ramp"] = ocs.categorical_series(
    var_series=crashes["secondary_ramp"], var_name="secondary_ramp", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 9. Party Characteristics ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9. Party Characteristics")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.1. Party Type ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.1. Party Type")

# Recode the party type to numeric
parties["party_type"] = parties["party_type"].map(cb["party_type"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["party_type"] == 999, "party_type"] = np.nan

# Convert the party_type column to categorical
parties["party_type"] = ocs.categorical_series(var_series=parties["party_type"], var_name="party_type", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.2. At Fault ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.2. At Fault")

# Recode the at fault (at_fault) to numeric
parties["at_fault"] = parties["at_fault"].map(cb["at_fault"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["at_fault"] == 999, "at_fault"] = np.nan

# Convert the at_fault column to categorical
parties["at_fault"] = ocs.categorical_series(var_series=parties["at_fault"], var_name="at_fault", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.3. Party Sex ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.3. Party Sex")

# Recode the party sex (party_sex) to numeric
parties["party_sex"] = parties["party_sex"].map(cb["party_sex"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["party_sex"] == 999, "party_sex"] = np.nan

# Convert the party_sex column to categorical
parties["party_sex"] = ocs.categorical_series(var_series=parties["party_sex"], var_name="party_sex", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.4. Party Age ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.4. Party Age")

# Recode the party age (party_age) to numeric
parties["party_age"] = parties["party_age"].astype(int)
# Set the unknown party age to NA
parties.loc[parties["party_age"] >= 998, "party_age"] = np.nan

# Create a new column for party age group (party_age_group)
bins = cb["party_age_group"]["recode"]
bins.append(np.inf)
parties["party_age_group"] = pd.cut(
    parties["party_age"],
    bins = cb["party_age_group"]["recode"],
    labels = [v for k, v in cb["party_age_group"]["labels"].items()],
    include_lowest = True,
    right = False,
)

# Relocate the party_age_group column after the party_age column in the data frame
ocs.relocate_column(df=parties, col_name="party_age_group", ref_col_name="party_age", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.5. Party Race ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.5. Party Race")

# Recode the party_race to numeric
parties["party_race"] = parties["party_race"].map(cb["party_race"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["party_race"] == 999, "party_race"] = np.nan

# Convert the party_race column to categorical
parties["party_race"] = ocs.categorical_series(var_series=parties["party_race"], var_name="party_race", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.6. Inattention ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.6. Inattention")

# Recode the parties.inattention to numeric
parties["inattention"] = parties["inattention"].map(cb["inattention"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["inattention"] == 999, "inattention"] = np.nan

# Convert the inattention column to categorical
parties["inattention"] = ocs.categorical_series(var_series=parties["inattention"], var_name="inattention", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.7. Party Sobriety ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.7. Party Sobriety")

# Recode the dfParty party sobriety (party_sobriety) to numeric
parties["party_sobriety"] = (
    parties["party_sobriety"].map(cb["party_sobriety"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
parties.loc[parties["party_sobriety"] == 999, "party_sobriety"] = np.nan

# Convert the party_sobriety column to categorical
parties["party_sobriety"] = ocs.categorical_series(
    var_series=parties["party_sobriety"], var_name="party_sobriety", cb_dict=cb
)


### Party Sobriety Indicator ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Party Sobriety Indicator")

# create a new variable dui_alcohol_ind if the party sobriety is "Had Been Drinking, Under Influence" and 0 otherwise
parties["dui_alcohol_ind"] = np.where(parties["party_sobriety"] == "Had Been Drinking, Under Influence", 1, 0)

# Convert the dui_alcohol_ind column to categorical
parties["dui_alcohol_ind"] = ocs.categorical_series(
    var_series=parties["dui_alcohol_ind"], var_name="dui_alcohol_ind", cb_dict=cb
)

# Relocate the dui_alcohol_ind column after the party_sobriety column in the data frame
ocs.relocate_column(df=parties, col_name="dui_alcohol_ind", ref_col_name="party_sobriety", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.8. Party Drug Physical ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.8. Party Drug Physical")

# Recode the parties party drug physical (party_drug_physical) to numeric
parties["party_drug_physical"] = (
    parties["party_drug_physical"].map(cb["party_drug_physical"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
parties.loc[parties["party_drug_physical"] == 999, "party_drug_physical"] = np.nan

# Convert the party_drug_physical column to categorical
parties["party_drug_physical"] = ocs.categorical_series(
    var_series=parties["party_drug_physical"], var_name="party_drug_physical", cb_dict=cb
)


### Party Drug Physical Indicator (dui drug indicator) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Party Drug Physical Indicator (dui drug indicator)")

# create a new variable dui_drug_ind to be 1 if the party drug physical is "Under Drug Influence" and 0 otherwise
parties["dui_drug_ind"] = np.where(parties["party_drug_physical"] == "Under Drug Influence", 1, 0)

# Convert the dui_drug_ind column to categorical
parties["dui_drug_ind"] = ocs.categorical_series(
    var_series=parties["dui_drug_ind"], var_name="dui_drug_ind", cb_dict=cb
)

# Relocate the dui_drug_ind column after the party_drug_physical column in the data frame
ocs.relocate_column(df=parties, col_name="dui_drug_ind", ref_col_name="party_drug_physical", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.9. Direction of Travel ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.9. Direction of Travel")

# Recode the parties direction of travel (dir_of_travel) to numeric
parties["dir_of_travel"] = parties["dir_of_travel"].map(cb["dir_of_travel"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["dir_of_travel"] == 999, "dir_of_travel"] = np.nan

# Convert the dir_of_travel column to categorical
parties["dir_of_travel"] = ocs.categorical_series(
    var_series=parties["dir_of_travel"], var_name="dir_of_travel", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.10. Party Safety Equipment ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.10. Party Safety Equipment")


### Party Safety Equipment 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Party Safety Equipment 1")

# Recode the parties party safety equipment 1 (party_safety_eq_1) to numeric
parties["party_safety_eq_1"] = (
    parties["party_safety_eq_1"].map(cb["party_safety_eq_1"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
parties.loc[parties["party_safety_eq_1"] == 999, "party_safety_eq_1"] = np.nan

# Convert the party_safety_eq_1 column to categorical
parties["party_safety_eq_1"] = ocs.categorical_series(
    var_series=parties["party_safety_eq_1"], var_name="party_safety_eq_1", cb_dict=cb
)


### Party Safety Equipment 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Party Safety Equipment 2")

# Recode the parties party safety equipment 2 (party_safety_eq_2) to numeric
parties["party_safety_eq_2"] = (
    parties["party_safety_eq_2"].map(cb["party_safety_eq_2"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
parties.loc[parties["party_safety_eq_2"] == 999, "party_safety_eq_2"] = np.nan

# Convert the party_safety_eq_2 column to categorical
parties["party_safety_eq_2"] = ocs.categorical_series(
    var_series=parties["party_safety_eq_2"], var_name="party_safety_eq_2", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.11. Financial Responsibility ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.11. Financial Responsibility")

# Recode the parties financial responsibility (finan_respons) to numeric
parties["finan_respons"] = parties["finan_respons"].map(cb["finan_respons"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["finan_respons"] == 999, "finan_respons"] = np.nan

# Convert the finan_respons column to categorical
parties["finan_respons"] = ocs.categorical_series(
    var_series=parties["finan_respons"], var_name="finan_respons", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.12. Party Special Information ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.12. Party Special Information")


### Party Special Information 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Party Special Information 1")

# Recode the parties special information 1 (sp_info_1) to numeric
parties["sp_info_1"] = parties["sp_info_1"].map(cb["sp_info_1"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["sp_info_1"] == 999, "sp_info_1"] = np.nan

# Convert the sp_info_1 column to categorical
parties["sp_info_1"] = ocs.categorical_series(var_series=parties["sp_info_1"], var_name="sp_info_1", cb_dict=cb)


### Party Special Information 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Party Special Information 2")

# Recode the parties special information 2 (sp_info_2) to numeric
parties["sp_info_2"] = parties["sp_info_2"].map(cb["sp_info_2"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["sp_info_2"] == 999, "sp_info_2"] = np.nan

# Convert the sp_info_2 column to categorical
parties["sp_info_2"] = ocs.categorical_series(var_series=parties["sp_info_2"], var_name="sp_info_2", cb_dict=cb)


### Party Special Information 3 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Party Special Information 3")

# Recode the parties special information 3 (sp_info_3) to numeric
parties["sp_info_3"] = parties["sp_info_3"].map(cb["sp_info_3"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["sp_info_3"] == 999, "sp_info_3"] = np.nan

# Convert the sp_info_3 column to categorical
parties["sp_info_3"] = ocs.categorical_series(var_series=parties["sp_info_3"], var_name="sp_info_3", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.13. OAF Violation Code ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.13. OAF Violation Code")

# Recode the parties OAF violation code (oaf_viol_code) to numeric
parties["oaf_viol_code"] = parties["oaf_viol_code"].map(cb["oaf_viol_code"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["oaf_viol_code"] == 999, "oaf_viol_code"] = np.nan

# Convert the oaf_viol_code column to categorical
parties["oaf_viol_code"] = ocs.categorical_series(
    var_series=parties["oaf_viol_code"], var_name="oaf_viol_code", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.14. OAF Violation Category ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.14. OAF Violation Category")

# Recode the parties OAF violation category (oaf_viol_cat) to numeric
parties["oaf_viol_cat"] = parties["oaf_viol_cat"].map(cb["oaf_viol_cat"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["oaf_viol_cat"] == 999, "oaf_viol_cat"] = np.nan

# Convert the oaf_viol_cat column to categorical
parties["oaf_viol_cat"] = ocs.categorical_series(
    var_series=parties["oaf_viol_cat"], var_name="oaf_viol_cat", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.15. OAF Violation Section ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.15. OAF Violation Section")


### OAF Violation Section 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating OAF Violation Section 1")

# Recode the parties OAF violation section 1 (oaf_1) to numeric
parties["oaf_1"] = parties["oaf_1"].map(cb["oaf_1"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["oaf_1"] == 999, "oaf_1"] = np.nan

# Convert the oaf_1 column to categorical
parties["oaf_1"] = ocs.categorical_series(var_series=parties["oaf_1"], var_name="oaf_1", cb_dict=cb)


### OAF Violation Section 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating OAF Violation Section 2")

# Recode the parties OAF violation section 2 (oaf_2) to numeric
parties["oaf_2"] = parties["oaf_2"].map(cb["oaf_2"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["oaf_2"] == 999, "oaf_2"] = np.nan

# Convert the oaf_2 column to categorical
parties["oaf_2"] = ocs.categorical_series(var_series=parties["oaf_2"], var_name="oaf_2", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.16. Movement Preceding Accident ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.16. Movement Preceding Accident")

# Recode the parties movement preceding accident (move_pre_acc) to numeric
parties["move_pre_acc"] = parties["move_pre_acc"].map(cb["move_pre_acc"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["move_pre_acc"] == 999, "move_pre_acc"] = np.nan

# Convert the move_pre_acc column to categorical
parties["move_pre_acc"] = ocs.categorical_series(
    var_series=parties["move_pre_acc"], var_name="move_pre_acc", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.17. Vehicle Year ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.17. Vehicle Year")

# print(parties["vehicle_year"].sort_values().unique())

# Correct values of vehicle year
parties.loc[parties["vehicle_year"] == 215, "vehicle_year"] = 2015
parties.loc[parties["vehicle_year"] == 1201, "vehicle_year"] = 2011
parties.loc[parties["vehicle_year"] == 2047, "vehicle_year"] = 2017
parties.loc[parties["vehicle_year"] == 2101, "vehicle_year"] = 2011
parties.loc[parties["vehicle_year"] == 2102, "vehicle_year"] = 2012
parties.loc[parties["vehicle_year"] == 2108, "vehicle_year"] = 2018
parties.loc[parties["vehicle_year"] == 2203, "vehicle_year"] = 2023
parties.loc[parties["vehicle_year"] == 2302, "vehicle_year"] = 2022
parties.loc[parties["vehicle_year"] == 2916, "vehicle_year"] = 2016

# Convert the vehicle year to numeric (integer 64)
parties["vehicle_year"] = parties["vehicle_year"].fillna(999).astype(int)
# Set 999 values to NaN
parties.loc[parties["vehicle_year"] == 999, "vehicle_year"] = np.nan

# Create a new column vehicle_year_group and recode the vehicle year to numeric by decades
bins = cb["vehicle_year_group"]["recode"]
bins.append(np.inf)
parties["vehicle_year_group"] = pd.cut(
    parties["vehicle_year"],
    bins = cb["vehicle_year_group"]["recode"],
    labels = [v for k, v in cb["vehicle_year_group"]["labels"].items()],
    include_lowest = True,
    right = False,
)

# Relocate the vehicle_year_group column after the vehicle_year column in the data frame
ocs.relocate_column(df=parties, col_name="vehicle_year_group", ref_col_name="vehicle_year", position="after")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.18. Vehicle Type ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.18. Vehicle Type")

# Recode the parties state wide vehicle type (stwd_vehicle_type) to numeric
parties["stwd_vehicle_type"] = (
    parties["stwd_vehicle_type"].map(cb["stwd_vehicle_type"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
parties.loc[parties["stwd_vehicle_type"] == 999, "stwd_vehicle_type"] = np.nan

# Convert the stwd_vehicle_type column to categorical
parties["stwd_vehicle_type"] = ocs.categorical_series(
    var_series=parties["stwd_vehicle_type"], var_name="stwd_vehicle_type", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.19. CHP Vehicle Towing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.19. CHP Vehicle Towing")

# Recode the parties CHP vehicle towing (chp_veh_type_towing) to numeric
parties["chp_veh_type_towing"] = (
    parties["chp_veh_type_towing"].map(cb["chp_veh_type_towing"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
parties.loc[parties["chp_veh_type_towing"] == 999, "chp_veh_type_towing"] = np.nan

# Convert the chp_veh_type_towing column to categorical
parties["chp_veh_type_towing"] = ocs.categorical_series(
    var_series=parties["chp_veh_type_towing"], var_name="chp_veh_type_towing", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.20. CHP Vehicle Type Towed ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.20. CHP Vehicle Type Towed")

# Recode the parties CHP vehicle type towed (chp_veh_type_towed) to numeric
parties["chp_veh_type_towed"] = (
    parties["chp_veh_type_towed"].map(cb["chp_veh_type_towed"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
parties.loc[parties["chp_veh_type_towed"] == 999, "chp_veh_type_towed"] = np.nan

# Convert the chp_veh_type_towed column to categorical
parties["chp_veh_type_towed"] = ocs.categorical_series(
    var_series=parties["chp_veh_type_towed"], var_name="chp_veh_type_towed", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 9.21. Special Info ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9.21. Special Info")


### Special Info F ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Special Info F")

# Recode the parties special info F (special_info_f) to numeric
parties["special_info_f"] = (
    parties["special_info_f"].map(cb["special_info_f"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
parties.loc[parties["special_info_f"] == 999, "special_info_f"] = np.nan

# Convert the special_info_f column to categorical
parties["special_info_f"] = ocs.categorical_series(
    var_series=parties["special_info_f"], var_name="special_info_f", cb_dict=cb
)


### Special Info G ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Special Info G")

# Recode the parties special info G (special_info_g) to numeric
parties["special_info_g"] = (
    parties["special_info_g"].map(cb["special_info_g"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
parties.loc[parties["special_info_g"] == 999, "special_info_g"] = np.nan

# Convert the special_info_g column to categorical
parties["special_info_g"] = ocs.categorical_series(
    var_series=parties["special_info_g"], var_name="special_info_g", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 10. Victim Characteristics ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10. Victim Characteristics")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.1. Victim Role ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.1. Victim Role")

# Recode the victims victim role (victim_role) to numeric
victims["victim_role"] = victims["victim_role"].fillna(999).astype(int)
# Set 999 values to NaN
victims.loc[victims["victim_role"] == 999, "victim_role"] = np.nan

# Convert the victim_role column to categorical
victims["victim_role"] = ocs.categorical_series(var_series=victims["victim_role"], var_name="victim_role", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.2. Victim Sex ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.2. Victim Sex")

# Recode the victims victim sex (victim_sex) to numeric
victims["victim_sex"] = victims["victim_sex"].map(cb["victim_sex"]["recode"]).fillna(999).astype(int)
# Set 999 values to NaN
victims.loc[victims["victim_sex"] == 999, "victim_sex"] = np.nan

# Convert the victim_sex column to categorical
victims["victim_sex"] = ocs.categorical_series(var_series=victims["victim_sex"], var_name="victim_sex", cb_dict=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.3. Victim Age ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.3. Victim Age")

# Recode the victim age (victim_age) to numeric
victims["victim_age"] = victims["victim_age"].astype(int)
# Set the unknown victim age to NA
victims.loc[victims["victim_age"] >= 998, "victim_age"] = np.nan

# Create a new column for victim age group (victim_age_group)
bins = cb["victim_age_group"]["recode"]
bins.append(np.inf)
victims["victim_age_group"] = pd.cut(
    victims["victim_age"],
    bins = cb["victim_age_group"]["recode"],
    labels = [v for k, v in cb["victim_age_group"]["labels"].items()],
    include_lowest = True,
    right = False,
)

# Relocate the victim_age_group column after the victim_age column in the data frame
parties.insert(victims.columns.get_loc("victim_age") + 1, "victim_age_group", victims.pop("victim_age_group"))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.4. Victim Degree of Injury ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.4. Victim Degree of Injury")

# Create a binary variable for the victim degree of injury
victims["victim_degree_of_injury_bin"] = (
    victims["victim_degree_of_injury"].map(cb["victim_degree_of_injury_bin"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
victims.loc[victims["victim_degree_of_injury_bin"] == 999, "victim_degree_of_injury_bin"] = np.nan

# Recode the victims victim degree of injury (victim_degree_of_injury) to numeric
victims["victim_degree_of_injury"] = (
    victims["victim_degree_of_injury"].map(cb["victim_degree_of_injury"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
victims.loc[victims["victim_degree_of_injury"] == 999, "victim_degree_of_injury"] = np.nan

# Convert the victim_degree_of_injury column to categorical
victims["victim_degree_of_injury"] = ocs.categorical_series(
    var_series=victims["victim_degree_of_injury"], var_name="victim_degree_of_injury", cb_dict=cb
)

# Convert the victim_degree_of_injury_bin column to categorical
victims["victim_degree_of_injury_bin"] = ocs.categorical_series(
    var_series=victims["victim_degree_of_injury_bin"], var_name="victim_degree_of_injury_bin", cb_dict=cb
)

# Relocate the victim_degree_of_injury_bin column after the victim_degree_of_injury column
ocs.relocate_column(
    df = victims, col_name = "victim_degree_of_injury_bin", ref_col_name = "victim_degree_of_injury", position = "after"
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.5. Victim Seating Position ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.5. Victim Seating Position")

# Recode the victims victim seating position (victim_seating_position) to numeric
victims["victim_seating_position"] = (
    victims["victim_seating_position"].map(cb["victim_seating_position"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
victims.loc[victims["victim_seating_position"] == 999, "victim_seating_position"] = np.nan

# Convert the victim_seating_position column to categorical
victims["victim_seating_position"] = ocs.categorical_series(
    var_series=victims["victim_seating_position"], var_name="victim_seating_position", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.6. Victim Safety Equipment ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.6. Victim Safety Equipment")


### Victim Safety Equipment 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Victim Safety Equipment 1")

# Recode the victims victim safety equipment 1 (victim_safety_eq_1) to numeric
victims["victim_safety_eq_1"] = (
    victims["victim_safety_eq_1"].map(cb["victim_safety_eq_1"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
victims.loc[victims["victim_safety_eq_1"] == 999, "victim_safety_eq_1"] = np.nan

# Convert the victim_safety_eq_1 column to categorical
victims["victim_safety_eq_1"] = ocs.categorical_series(
    var_series=victims["victim_safety_eq_1"], var_name="victim_safety_eq_1", cb_dict=cb
)


### Victim Safety Equipment 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Creating Victim Safety Equipment 2")

# Recode the victims victim safety equipment 2 (victim_safety_eq_2) to numeric
victims["victim_safety_eq_2"] = (
    victims["victim_safety_eq_2"].map(cb["victim_safety_eq_2"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
victims.loc[victims["victim_safety_eq_2"] == 999, "victim_safety_eq_2"] = np.nan

# Convert the victim_safety_eq_2 column to categorical
victims["victim_safety_eq_2"] = ocs.categorical_series(
    var_series=victims["victim_safety_eq_2"], var_name="victim_safety_eq_2", cb_dict=cb
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 10.7. Victim Ejected ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n10.7. Victim Ejected")

# Recode the victims victim ejected (victim_ejected) to numeric
victims["victim_ejected"] = (
    victims["victim_ejected"].map(cb["victim_ejected"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
victims.loc[victims["victim_ejected"] == 999, "victim_ejected"] = np.nan

# Convert the victim_ejected column to categorical
victims["victim_ejected"] = ocs.categorical_series(
    var_series=victims["victim_ejected"], var_name="victim_ejected", cb_dict=cb
)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 11. Add Column Attributes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n11. Add Column Attributes")

# Add column attributes to the crashes data frame
crashes = ocs.add_attributes(df = crashes, cb = cb)

# Add column attributes to the parties data frame
parties = ocs.add_attributes(df = parties, cb = cb)

# Add column attributes to the victims data frame
victims = ocs.add_attributes(df = victims, cb = cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 12. Merge Datasets ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n12. Merge Datasets")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 12.1. Preparing Roads Dataset for Merging ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n12.1. Preparing Roads Dataset for Merging")


# Create a table of road categories by city (contains counts for each category)
# Use pandas crosstab to replicate R's table function
road_cat_table = pd.crosstab(roads["place_name"], roads["road_cat"], dropna = False)

# Remove rows where the city (place_name) is NA
road_cat_table = road_cat_table[~road_cat_table.index.isna()]

# Convert the table to a data frame and rename the city column to "city"
road_cat_table = road_cat_table.reset_index()
road_cat_table.rename(columns = {"place_name": "city"}, inplace = True)

# Aggregate the roads dataset by city and calculate the mean road length across all road lengths in each city
mean_road_length = (
    roads[roads["place_name"] != ""]
    .groupby("place_name")["road_length"]
    .mean()
    .reset_index()
    .rename(columns = {"place_name": "city", "road_length": "road_length_mean"})
)

# Aggregate the roads dataset by city and calculate the sum of all road lengths in each city
sum_road_length = (
    roads[roads["place_name"] != ""]
    .groupby("place_name")["road_length"]
    .sum()
    .reset_index()
    .rename(columns = {"place_name": "city", "road_length": "road_length_sum"})
)

# Merge the mean and sum road length columns into the aggregated roads data frame (road_cat_table)
roads_aggr = road_cat_table.merge(mean_road_length, on = "city", how = "left")
roads_aggr = roads_aggr.merge(sum_road_length, on = "city", how = "left")

# Reorder the aggregated roads data frame
roads_aggr = roads_aggr[["city", "Primary", "Secondary", "Local", "road_length_mean", "road_length_sum"]]

# Rename the columns of the aggregated roads data frame
roads_aggr.rename(
    columns = {"city": "place_name", "Primary": "roads_primary", "Secondary": "roads_secondary", "Local": "roads_local"},
    inplace = True,
)

# Check if the columns in the roads_aggr data frame exist in the roads data frame
roads_aggr_cols = []
for col in roads_aggr.columns:
    if col not in roads.columns:
        # add the column to the list of columns to be added
        roads_aggr_cols.append(col)
# if the roads_aggr_cols is not empty, create a subset of the roads_aggr, that has the "place_name" and the columns to be added, else, break
if roads_aggr_cols:
    print(f"Adding new columns to the roads data frame: {roads_aggr_cols}")
    roads_aggr_subset = roads_aggr[["place_name"] + roads_aggr_cols]
    # Merge the aggregated roads data frame into the original roads data frame
    roads = roads.merge(
        roads_aggr_subset, on = "place_name", how = "left", suffixes = (".roads", ".aggr"), validate = "many_to_one"
    )
    del roads_aggr_subset
else:
    print("No new columns to add to the roads data frame")

# Convert the columns to the correct data types
roads["roads_primary"] = pd.to_numeric(roads["roads_primary"], errors = "coerce", downcast = "integer")
roads["roads_secondary"] = pd.to_numeric(roads["roads_secondary"], errors = "coerce", downcast = "integer")
roads["roads_local"] = pd.to_numeric(roads["roads_local"], errors = "coerce", downcast = "integer")
roads["road_length_mean"] = pd.to_numeric(roads["road_length_mean"], errors = "coerce", downcast = "float")
roads["road_length_sum"] = pd.to_numeric(roads["road_length_sum"], errors = "coerce", downcast = "float")

# Rename the "place_name" column of the roads_aggr data frame back to "city"
roads_aggr.rename(columns = {"place_name": "city"}, inplace = True)

# Delete the "accident_year" column from the parties and victims data frames
parties.drop(columns = ["accident_year"], inplace = True, errors = "ignore")
victims.drop(columns = ["accident_year"], inplace = True, errors = "ignore")

del road_cat_table, mean_road_length, sum_road_length, roads_aggr_cols


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 12.2. Merging Datasets ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n12.2. Merging Datasets")


### Checking the Data Types of the Columns ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Checking the Data Types of the Columns")

# Create a list of the columns in crashes with their dtypes
crashes_cols = crashes.dtypes.reset_index()
crashes_cols.rename(columns = {"index": "column_name", 0: "data_type"}, inplace = True)

# Create a list of the columns in parties with their dtypes
parties_cols = parties.dtypes.reset_index()
parties_cols.rename(columns = {"index": "column_name", 0: "data_type"}, inplace = True)

# Create a list of the columns in victims with their dtypes
victims_cols = victims.dtypes.reset_index()
victims_cols.rename(columns = {"index": "column_name", 0: "data_type"}, inplace = True)

# Find the columns that are the same between crashes_cols and parties_cols, and compare their data_types
common_cols_1 = pd.merge(crashes_cols, parties_cols, on = "column_name", suffixes = (".crashes", ".parties"), how = "inner")
print(common_cols_1)

# Find the columns that are the same between crashes_cols and victims_cols, and compare their data_types
common_cols_2 = pd.merge(crashes_cols, victims_cols, on = "column_name", suffixes = (".crashes", ".victims"), how = "inner")
print(common_cols_2)

# Find the columns that are the same between parties_cols and victims_cols, and compare their data_types
common_cols_3 = pd.merge(parties_cols, victims_cols, on = "column_name", suffixes = (".parties", ".victims"), how = "inner")
print(common_cols_3)

del (crashes_cols, parties_cols, victims_cols, common_cols_1, common_cols_2, common_cols_3)


### Merge the Three Datasets ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Merging the Three Datasets")

# Ensure merge keys have the same dtype in both DataFrames to avoid buffer dtype mismatch errors
crashes["case_id"] = crashes["case_id"].astype(str)
parties["case_id"] = parties["case_id"].astype(str)
victims["case_id"] = victims["case_id"].astype(str)
crashes["cid"] = crashes["cid"].astype(str)
parties["cid"] = parties["cid"].astype(str)
victims["cid"] = victims["cid"].astype(str)
parties["pid"] = parties["pid"].astype(str)
victims["pid"] = victims["pid"].astype(str)


# First, merge the crashes and parties datasets based on the Case ID and CID columns (outer join)
crashes_parties = crashes.merge(
    parties,
    left_on = ["case_id", "cid"],
    right_on = ["case_id", "cid"],
    how = "outer",
    suffixes = (".crashes", ".parties"),
    validate = "one_to_many",
)

# Secondly, merge the crashes_parties dataset with the victims dataset based on the Case ID, CID, PID and Party Number columns (outer join)
crashes_parties_victims = crashes_parties.merge(
    victims,
    left_on = ["case_id", "cid", "pid", "party_number"],
    right_on = ["case_id", "cid", "pid", "party_number"],
    how = "outer",
    suffixes = (".crashes_parties", ".victims"),
    validate = "one_to_many",
)

# Thirdly, merge the crashes_parties_victims dataset with the cities dataset based on the City column (inner left join)
crashes_parties_victims_cities = crashes_parties_victims.merge(
    cities,
    left_on = "city",
    right_on = "city",
    how = "left",
    suffixes = (".crashes_parties_victims", ".cities"),
    validate = "many_to_one",
)

# The final merge, contains the combined collisions data frame
# Merge the crashes_parties_victims_cities dataset with the aggregated roads data (roads_aggr) on the "city" column (left join)
collisions = crashes_parties_victims_cities.merge(
    roads_aggr, left_on = "city", right_on = "city", how = "left", suffixes = (".join3", ".roads"), validate = "many_to_one"
)

# Convert the collisions to a pandas DataFrame
collisions = pd.DataFrame(collisions)

# Delete the "OBJECTID" and "SHAPE" columns from the collisions data frame
# These columns are not needed for the analysis
collisions.drop(columns = ["OBJECTID", "SHAPE"], inplace = True, errors = "ignore")

# Reset the index of the collisions data frame
collisions.reset_index(drop = True, inplace = True)

# Get the codebook keys that are in the collisions data frame
cb_keys = [key for key in cb.keys() if key in collisions.columns]

# Order the columns of the collisions data frame based on the cb_keys list
collisions = collisions[cb_keys]

# Add the column attributes to the collisions data frame
collisions = ocs.add_attributes(df = collisions, cb = cb)

# We can now remove all the temporary datasets (except of the aggregated roads data frame)
del (crashes_parties, crashes_parties_victims, crashes_parties_victims_cities, roads_aggr, cb_keys)


### Add date and time variables to the parties and victims datasets ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Add date and time variables to the parties and victims datasets")

# Add date and time variables from the crashes data frame to the parties data frame on the CID column
datetime_cols = [
    "cid",
    "date_datetime",
    "date_year",
    "date_quarter",
    "date_month",
    "date_week",
    "date_day",
    "date_process",
    "dt_year",
    "dt_quarter",
    "dt_month",
    "dt_year_week",
    "dt_week_day",
    "dt_month_day",
    "dt_year_day",
    "dt_hour",
    "dt_minute",
    "dt_dst",
    "dt_zone",
    "coll_date",
    "coll_time",
    "accident_year",
    "process_date",
    "coll_severity",
    "coll_severity_num",
    "coll_severity_rank",
    "coll_severity_rank_num",
    "coll_severity_hs"
]
parties = parties.merge(crashes[datetime_cols], on = "cid", how = "left", suffixes = (".parties", ".crashes"))

# Get the codebook keys that are in the parties data frame
cb_keys = [key for key in cb.keys() if key in parties.columns]

# Order the columns of the parties data frame based on the cb_keys list
parties = parties[cb_keys]

# Add the column attributes to the parties data frame
parties = ocs.add_attributes(df = parties, cb = cb)

# Add date and time variables from the crashes data frame to the victims data frame on the CID column
victims = victims.merge(crashes[datetime_cols], on = "cid", how = "left", suffixes = (".victims", ".crashes"))

# Get the codebook keys that are in the victims data frame
cb_keys = [key for key in cb.keys() if key in victims.columns]

# Order the columns of the victims data frame based on the cb_keys list
victims = victims[cb_keys]

# Add the column attributes to the victims data frame
victims = ocs.add_attributes(df = victims, cb = cb)

del datetime_cols, cb_keys


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 12.3. Update the tag variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n12.3. Update the tag variables")

# First, sort the collisions data frame by cid, pid, and vid
collisions.sort_values(by = ["cid", "pid", "vid"], inplace = True)
collisions.reset_index(drop = True, inplace = True)

# Replace the crash_tag column with 1 if it is the first occurrence of a CID, otherwise 0
collisions["crash_tag"] = (~collisions["cid"].duplicated()).astype(int)

# Replace the party_tag column with 1 if it is the first occurrence of a PID, otherwise 0
collisions["party_tag"] = (~collisions["pid"].duplicated()).astype(int)

# Replace the victim_tag column with 1 if it is the first occurrence of a VID, otherwise 0
collisions["victim_tag"] = (~collisions["vid"].duplicated()).astype(int)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 12.4. Create Combined Indicator Column ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n12.4. Create Combined Indicator Column")

# Create a combined indicator column that combines the crash_tag, party_tag and victim_tag columns
collisions["combined_ind"] = (
    collisions["crash_tag"].astype(int) * 100
    + collisions["party_tag"].astype(int) * 10
    + collisions["victim_tag"].astype(int)
)

# Recode the collisions combined_ind column to numeric
collisions["combined_ind"] = (
    collisions["combined_ind"].astype(str).map(cb["combined_ind"]["recode"]).fillna(999).astype(int)
)
# Set 999 values to NaN
collisions.loc[collisions["combined_ind"] == 999, "combined_ind"] = np.nan

# Convert the combined_ind column to categorical
collisions["combined_ind"] = ocs.categorical_series(
    var_series = collisions["combined_ind"], var_name = "combined_ind", cb_dict = cb
)

# Relocate the collisions combined_ind column after the victim_tag column
ocs.relocate_column(df = collisions, col_name = "combined_ind", ref_col_name = "victim_tag", position = "after")

# Add the column attributes to the collisions data frame
collisions = ocs.add_attributes(df = collisions, cb = cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 13. Spatial Operations ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n13. Spatial Operations")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 13.1. Sort the Data Frames by Datetime ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n13.1. Sort the Data Frames by Datetime")

# Sort the rows of the crashes data frame by the cid column
crashes.sort_values(by=["cid"], inplace=True)
crashes.reset_index(drop=True, inplace=True)

# Sort the rows of the parties data frame by the cid column
parties.sort_values(by=["cid"], inplace=True)
parties.reset_index(drop=True, inplace=True)

# Sort the rows of the victims data frame by the cid column
victims.sort_values(by=["cid"], inplace=True)
victims.reset_index(drop=True, inplace=True)

# Sort the rows of the collisions data frame by the cid column
collisions.sort_values(by=["cid"], inplace=True)
collisions.reset_index(drop=True, inplace=True)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 13.2. Add X and Y Coordinates ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n13.2. Add X and Y Coordinates")

# Add point_x and point_y from crashes to parties by matching cid
parties = parties.merge(crashes[["cid", "point_x", "point_y"]], on="cid", how="left", suffixes=("", ".crashes"))

# add point_x and point_y from crashes to the victims database by matching cid
victims = victims.merge(crashes[["cid", "point_x", "point_y"]], on="cid", how="left", suffixes=("", ".crashes"))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 13.3. Dataset and Column Attributes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n13.3. Dataset and Column Attributes")


### Define the Dataset Attributes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Define the Dataset Attributes")

# Crashes Dataframe Attributes
crashes.attrs = {
    "name": "crashes",
    "label": "OCTraffic Crashes Data",
    "description": f"Dataframe containing the OCTraffic crashes data ({prj_meta['date_start'].year} - {prj_meta['date_end'].year})",
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y")
}

# Parties Dataframe Attributes
parties.attrs = {
    "name": "parties",
    "label": "OCTraffic Parties Data",
    "description": f"Dataframe containing the OCTraffic parties data ({prj_meta['date_start'].year} - {prj_meta['date_end'].year})",
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y")
}

# Victims Dataframe Attributes
victims.attrs = {
    "name": "victims",
    "label": "OCTraffic Victims Data",
    "description": f"Dataframe containing the OCTraffic victims data ({prj_meta['date_start'].year} - {prj_meta['date_end'].year})",
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y")
}

# Collisions Dataframe Attributes
collisions.attrs = {
    "name": "collisions",
    "label": "OCTraffic Collisions Data",
    "description": f"Dataframe containing the OCTraffic collisions data ({prj_meta['date_start'].year} - {prj_meta['date_end'].year})",
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y")
}


### Define the Column Attributes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Define the Column Attributes")

# Add column attributes to the crashes data frame
crashes = ocs.add_attributes(df=crashes, cb=cb)
# Add column attributes to the parties data frame
parties = ocs.add_attributes(df=parties, cb=cb)
# Add column attributes to the victims data frame
victims = ocs.add_attributes(df=victims, cb=cb)
# Add column attributes to the collisions data frame
collisions = ocs.add_attributes(df=collisions, cb=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 13.4. Convert to Spatial Data Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n13.4. Convert to Spatial Data Frames")


### Select cases within OC boundaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Select cases within OC boundaries")

# Get the old number of cases
len_crashes_old = len(crashes)
len_parties_old = len(parties)
len_victims_old = len(victims)
len_collisions_old = len(collisions)

# Add the original counts to the latex_vars dictionary
latex_vars["crashesOriginal"] = len_crashes_old
latex_vars["partiesOriginal"] = len_parties_old
latex_vars["victimsOriginal"] = len_victims_old
latex_vars["collisionsOriginal"] = len_collisions_old

# Minimum confined coordinates for Orange County
oc_bounding_coor = {"xmin": -118.11978472, "xmax": -117.41283672, "ymin": 33.38712529, "ymax": 33.94763946}

# Select crashes within the bounding coordinates of Orange County
crashes = crashes[
    (crashes["point_x"] >= oc_bounding_coor["xmin"])
    & (crashes["point_x"] <= oc_bounding_coor["xmax"])
    & (crashes["point_y"] >= oc_bounding_coor["ymin"])
    & (crashes["point_y"] <= oc_bounding_coor["ymax"])
]

# Select parties within the bounding coordinates of Orange County
parties = parties[
    (parties["point_x"] >= oc_bounding_coor["xmin"])
    & (parties["point_x"] <= oc_bounding_coor["xmax"])
    & (parties["point_y"] >= oc_bounding_coor["ymin"])
    & (parties["point_y"] <= oc_bounding_coor["ymax"])
]

# Select victims within the bounding coordinates of Orange County
victims = victims[
    (victims["point_x"] >= oc_bounding_coor["xmin"])
    & (victims["point_x"] <= oc_bounding_coor["xmax"])
    & (victims["point_y"] >= oc_bounding_coor["ymin"])
    & (victims["point_y"] <= oc_bounding_coor["ymax"])
]

# Select collisions within the bounding coordinates of Orange County
collisions = collisions[
    (collisions["point_x"] >= oc_bounding_coor["xmin"])
    & (collisions["point_x"] <= oc_bounding_coor["xmax"])
    & (collisions["point_y"] >= oc_bounding_coor["ymin"])
    & (collisions["point_y"] <= oc_bounding_coor["ymax"])
]

# Get the new number of cases
len_crashes_new = len(crashes)
len_parties_new = len(parties)
len_victims_new = len(victims)
len_collisions_new = len(collisions)

print(
    f"  - Orange County Cases:\n    - OC Crashes: {len_crashes_new:,} of {len_crashes_old:,} (difference: {len_crashes_old - len_crashes_new:,})\n    - OC Parties: {len_parties_new:,} of {len_parties_old:,} (difference: {len_parties_old - len_parties_new:,})\n    - OC Victims: {len_victims_new:,} of {len_victims_old:,} (difference: {len_victims_old - len_victims_new:,})\n    - OC Collisions: {len_collisions_new:,} of {len_collisions_old:,} (difference: {len_collisions_old - len_collisions_new:,})"
)

# Add the OC counts to the latex_vars dictionary
latex_vars["crashesCount"] = len_crashes_new
latex_vars["partiesCount"] = len_parties_new
latex_vars["victimsCount"] = len_victims_new
latex_vars["collisionsCount"] = len_collisions_new

# Get the counts of crashes, parties, victims, and collisions by year
date_field = datetime.datetime.now().strftime("%Y-%m-%d")

# Loop through the years in the project metadata and calculate counts for each year
for year in prj_meta.get("years", []):
    #print(f"Processing year: {year}")
    crashes_year = ocs.counts_by_year(crashes, year)
    parties_year = ocs.counts_by_year(parties, year)
    victims_year = ocs.counts_by_year(victims, year)
    # Update the project metadata with the counts for the year as the geocoded data
    prj_meta["tims"][str(year)]["geocoded"]["crashes"] = crashes_year
    prj_meta["tims"][str(year)]["geocoded"]["parties"] = parties_year
    prj_meta["tims"][str(year)]["geocoded"]["victims"] = victims_year
    # Update the project metadata with the counts for the year as the difference between reported and geocoded data
    prj_meta["tims"][str(year)]["excluded"]["crashes"] = prj_meta["tims"][str(year)]["reported"]["crashes"] - crashes_year
    prj_meta["tims"][str(year)]["excluded"]["parties"] = prj_meta["tims"][str(year)]["reported"]["parties"] - parties_year
    prj_meta["tims"][str(year)]["excluded"]["victims"] = prj_meta["tims"][str(year)]["reported"]["victims"] - victims_year
    # Update the "date_gp" field in the project metadata
    prj_meta["tims"][str(year)]["date_gp"] = date_field

# Update the tims metadata json file
metadata_path = os.path.join(prj_dirs["metadata"], "tims_metadata.json")

# Replace the existing tims_metadata.json file with the updated metadata
with open(metadata_path, "w", encoding = "utf-8") as f:
    json.dump(prj_meta["tims"], f, indent = 4)

# Update the metadata info
prj_meta = ocs.project_metadata(silent=False)


# Delete the temporary variables
del (
    len_crashes_old,
    len_parties_old,
    len_victims_old,
    len_collisions_old,
    len_crashes_new,
    len_parties_new,
    len_victims_new,
    len_collisions_new,
)

### Convert to Spatial Data Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Convert to Spatial Data Frames")

print("  - Converting Crashes data to ArcGIS Spatial Data Frames")
# Convert the crashes_agp data frame to ArcGIS spatial DataFrame
crashes_agp = GeoAccessor.from_xy(crashes, x_column = "point_x", y_column = "point_y", sr = 4326)

print("  - Converting Parties data to ArcGIS Spatial Data Frames")
# Convert the parties_agp data frame to ArcGIS spatial DataFrame
parties_agp = GeoAccessor.from_xy(parties, x_column = "point_x", y_column = "point_y", sr = 4326)

print("  - Converting Victims data to ArcGIS Spatial Data Frames")
# Convert the victims_agp data frame to ArcGIS spatial DataFrame
victims_agp = GeoAccessor.from_xy(victims, x_column = "point_x", y_column = "point_y", sr = 4326)

print("  - Converting Collisions data to ArcGIS Spatial Data Frames")
# Convert the collisions_agp data frame to ArcGIS spatial DataFrame
collisions_agp = GeoAccessor.from_xy(collisions, x_column = "point_x", y_column = "point_y", sr = 4326)

print("- Changing the spatial reference to Web Mercator")
# Change the spatial reference of the crashes, parties, victims and collisions data frames to Web Mercator
crashes_agp.spatial.project(3857, transformation_name = None)
parties_agp.spatial.project(3857, transformation_name = None)
victims_agp.spatial.project(3857, transformation_name = None)
collisions_agp.spatial.project(3857, transformation_name = None)

# Define the path to the raw geodatabase
gdb_raw = prj_dirs["agp_gdb_raw"]
gdbmain_raw = prj_dirs["gdbmain_raw"]


# Set the location for each spatial data frame
crashes_agp_location = os.path.join(gdb_raw, "crashes")
parties_agp_location = os.path.join(gdb_raw, "parties")
victims_agp_location = os.path.join(gdb_raw, "victims")
collisions_agp_location = os.path.join(gdb_raw, "collisions")

# Set the location for each spatial data frame in the main geodatabase
crashes_main_location = os.path.join(gdbmain_raw, "crashes")
parties_main_location = os.path.join(gdbmain_raw, "parties")
victims_main_location = os.path.join(gdbmain_raw, "victims")
collisions_main_location = os.path.join(gdbmain_raw, "collisions")

print("- Exporting the spatial data frames to the geodatabase")

print("  - Exporting Crashes data to the geodatabase")
# Export the crashes spatial data frame to the geodatabase
crashes_agp.spatial.to_featureclass(
    location = crashes_agp_location, overwrite = True, has_z = None, has_m = None, sanitize_columns = False
)
# Export the crashes spatial data frame to the main geodatabase
crashes_agp.spatial.to_featureclass(
    location = crashes_main_location, overwrite = True, has_z = None, has_m = None, sanitize_columns = False
)

print("  - Exporting Parties data to the geodatabase")
# Export the parties spatial data frame to the geodatabase
parties_agp.spatial.to_featureclass(
    location = parties_agp_location, overwrite = True, has_z = None, has_m = None, sanitize_columns = False
)
# Export the parties spatial data frame to the main geodatabase
parties_agp.spatial.to_featureclass(
    location = parties_main_location, overwrite = True, has_z = None, has_m = None, sanitize_columns = False
)

print("  - Exporting Victims data to the geodatabase")
# Export the victims spatial data frame to the geodatabase
victims_agp.spatial.to_featureclass(
    location = victims_agp_location, overwrite = True, has_z = None, has_m = None, sanitize_columns = False
)
# Export the victims spatial data frame to the main geodatabase
victims_agp.spatial.to_featureclass(
    location = victims_main_location, overwrite = True, has_z = None, has_m = None, sanitize_columns = False
)

print("  - Exporting Collisions data to the geodatabase")
# Export the collisions spatial data frame to the geodatabase
collisions_agp.spatial.to_featureclass(
    location = collisions_agp_location, overwrite = True, has_z = None, has_m = None, sanitize_columns = False
)
# Export the collisions spatial data frame to the main geodatabase
collisions_agp.spatial.to_featureclass(
    location = collisions_main_location, overwrite = True, has_z = None, has_m = None, sanitize_columns = False
)


### Update Geoprocessing Counts in Tims Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("- Update Geoprocessing Counts in Tims Metadata")

# Get the counts of crashes, parties, and victims per year
crashes_agp_counts = crashes_agp["dt_year"].value_counts().sort_index().to_dict()
parties_agp_counts = parties_agp["dt_year"].value_counts().sort_index().to_dict()
victims_agp_counts = victims_agp["dt_year"].value_counts().sort_index().to_dict()

# get the min and max years from the crashes_agp_counts
min_year = min(crashes_agp_counts.keys())
max_year = max(crashes_agp_counts.keys())

# Create a dictionary to store the geoprocessing counts
for year in range(min_year, max_year + 1):
    # Update the TIMS metadata for the geocoded data counts
    ocs.update_tims_metadata(
        year = year,
        data_type = "geocoded",
        data_counts = [crashes_agp_counts.get(year, 0), parties_agp_counts.get(year, 0), victims_agp_counts.get(year, 0)]
    )

del crashes_agp_counts, parties_agp_counts, victims_agp_counts, min_year, max_year


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 14. Final Column Processing ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n14. Final Column Processing")

# Create a dictionary with the processed columns for each dataset (as per the codebook)
proc_cols = {
    "crashes": [col for col in crashes.columns if cb[col]["fc"]["crashes"] == 1],
    "parties": [col for col in parties.columns if cb[col]["fc"]["parties"] == 1],
    "victims": [col for col in victims.columns if cb[col]["fc"]["victims"] == 1],
    "collisions": [col for col in collisions.columns if cb[col]["fc"]["collisions"] == 1],
}

# Export the processed columns to a JSON file
proc_cols_path = os.path.join(os.path.join(prj_dirs["codebook"], "proc_cols.json"))
with open(proc_cols_path, "w", encoding = "utf-8") as f:
    json.dump(proc_cols, f, indent = 4)
del f


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 15. Save to Disk ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n15. Save to Disk")

# Save the data to disk
ocs.save_to_disk(dir_list = prj_dirs, local_vars = locals(), global_vars = globals())

# Save the latex variables to a JSON file
latex_vars_path = os.path.join(prj_dirs["metadata"], "latex_vars.json")
with open(latex_vars_path, "w", encoding = "utf-8") as f:
    json.dump(latex_vars, f, indent = 4)
del f


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of Script ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\nLast Execution:", datetime.date.today().strftime("%Y-%m-%d"))
print("\nEnd of Script")
# Last Execution: 2025-12-30
