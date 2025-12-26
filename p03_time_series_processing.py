# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Part 3 - Time Series Processing ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.3, Date: December 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOCTraffic Data Processing - Part 3 - Time Series Processing\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1. Preliminaries")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Libraries and Initialization ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.1. Libraries and Initialization")

# Import necessary libraries
import os, datetime
import json
import pandas as pd
from dotenv import load_dotenv

# important as it "enhances" Pandas by importing these classes (from ArcGIS API for Python)
from octraffic import octraffic

# Initialize the OCTraffic object
ocs = octraffic()

# Load environment variables from .env file
load_dotenv()

os.getcwd()

# Part and Version
part = 3
version = 2025.3


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2. Project and Workspace Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.2. Project and Workspace Variables")

# Create a dictionary with the project metadata
print("\nCreating project metadata")
prj_meta = ocs.project_metadata(part = part, version = version, silent = False)

# Create a dictionary with the project directories
print("\nCreating project directories")
prj_dirs = ocs.project_directories(base_path = os.getcwd(), silent = False)

# Set the current working directory to the project root
os.chdir(prj_dirs["root"])
# Get the new current working directory
print(f"\nCurrent working directory: {os.getcwd()}\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.3. Load Collision Data from Disk ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.3. Loading Collisions Data from Disk")

# Load the crashes pickle data file
print("- Loading the crashes pickle data file")
crashes = pd.read_pickle(os.path.join(prj_dirs["data_python"], "crashes.pkl"))

# load the parties pickle data file
print("- Loading the parties pickle data file")
parties = pd.read_pickle(os.path.join(prj_dirs["data_python"], "parties.pkl"))

# load the victims pickle data file
print("- Loading the victims pickle data file")
victims = pd.read_pickle(os.path.join(prj_dirs["data_python"], "victims.pkl"))

# load the collisions pickle data file
print("- Loading the collisions pickle data file")
collisions = pd.read_pickle(os.path.join(prj_dirs["data_python"], "collisions.pkl"))

# load the cities pickle data file
print("- Loading the cities pickle data file")
cities = pd.read_pickle(os.path.join(prj_dirs["data_python"], "cities.pkl"))

# load the roads pickle data file
print("- Loading the roads pickle data file")
roads = pd.read_pickle(os.path.join(prj_dirs["data_python"], "roads.pkl"))

# load the blocks pickle data file
print("- Loading the blocks pickle data file")
blocks = pd.read_pickle(os.path.join(prj_dirs["data_python"], "blocks.pkl"))

# load the boundaries pickle data file
print("- Loading the boundaries pickle data file")
boundaries = pd.read_pickle(os.path.join(prj_dirs["data_python"], "boundaries.pkl"))

# load the data dictionary pickle data file
print("- Loading the data dictionary pickle data file")
data_dict = pd.read_pickle(os.path.join(prj_dirs["data_python"], "data_dict.pkl"))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.4. Load Codebook from Disk ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.4. Loading Codebook from Disk")

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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.5. Add Attributes to Data Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.5. Adding Attributes to Data Frames")

# Set column attributes for each data frame
crashes = ocs.add_attributes(df=crashes, cb=cb)
parties = ocs.add_attributes(df=parties, cb=cb)
victims = ocs.add_attributes(df=victims, cb=cb)
collisions = ocs.add_attributes(df=collisions, cb=cb)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Aggregation Functions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2. Aggregation Functions")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1. Aggregation by Year ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.1. Aggregation by Year")

# Aggregate the data frames by year
ts_year_crashes = ocs.ts_aggregate(dt = "date_year", df = crashes, cb = cb)
ts_year_parties = ocs.ts_aggregate(dt = "date_year", df = parties, cb = cb)
ts_year_victims = ocs.ts_aggregate(dt = "date_year", df = victims, cb = cb)
ts_year_collisions = ocs.ts_aggregate(dt = "date_year", df = collisions, cb = cb)

# Combine the year aggregated data frames into a dictionary
ts_year = {
    "crashes": ts_year_crashes,
    "parties": ts_year_parties,
    "victims": ts_year_victims,
    "collisions": ts_year_collisions,
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2. Aggregation by Quarter ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.2. Aggregation by Quarter")

# Aggregate the data frames by quarter
ts_quarter_crashes = ocs.ts_aggregate(dt = "date_quarter", df = crashes, cb = cb)
ts_quarter_parties = ocs.ts_aggregate(dt = "date_quarter", df = parties, cb = cb)
ts_quarter_victims = ocs.ts_aggregate(dt = "date_quarter", df = victims, cb = cb)
ts_quarter_collisions = ocs.ts_aggregate(dt = "date_quarter", df = collisions, cb = cb)

# Combine the quarter aggregated data frames into a dictionary
ts_quarter = {
    "crashes": ts_quarter_crashes,
    "parties": ts_quarter_parties,
    "victims": ts_quarter_victims,
    "collisions": ts_quarter_collisions,
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.3. Aggregation by Month ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.3. Aggregation by Month")

# Aggregate the data frames by month
ts_month_crashes = ocs.ts_aggregate(dt = "date_month", df = crashes, cb = cb)
ts_month_parties = ocs.ts_aggregate(dt = "date_month", df = parties, cb = cb)
ts_month_victims = ocs.ts_aggregate(dt = "date_month", df = victims, cb = cb)
ts_month_collisions = ocs.ts_aggregate(dt = "date_month", df = collisions, cb = cb)

# Combine the month aggregated data frames into a dictionary
ts_month = {
    "crashes": ts_month_crashes,
    "parties": ts_month_parties,
    "victims": ts_month_victims,
    "collisions": ts_month_collisions,
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.4. Aggregation by Week ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.4. Aggregation by Week")

# Aggregate the data frames by week
ts_week_crashes = ocs.ts_aggregate(dt = "date_week", df = crashes, cb = cb)
ts_week_parties = ocs.ts_aggregate(dt = "date_week", df = parties, cb = cb)
ts_week_victims = ocs.ts_aggregate(dt = "date_week", df = victims, cb = cb)
ts_week_collisions = ocs.ts_aggregate(dt = "date_week", df = collisions, cb = cb)

# Combine the week aggregated data frames into a dictionary
ts_week = {
    "crashes": ts_week_crashes,
    "parties": ts_week_parties,
    "victims": ts_week_victims,
    "collisions": ts_week_collisions,
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.5. Aggregation by Day ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.5. Aggregation by Day")

# Aggregate the data frames by day
ts_day_crashes = ocs.ts_aggregate(dt = "date_day", df = crashes, cb = cb)
ts_day_parties = ocs.ts_aggregate(dt = "date_day", df = parties, cb = cb)
ts_day_victims = ocs.ts_aggregate(dt = "date_day", df = victims, cb = cb)
ts_day_collisions = ocs.ts_aggregate(dt = "date_day", df = collisions, cb = cb)

# Combine the day aggregated data frames into a dictionary
ts_day = {
    "crashes": ts_day_crashes,
    "parties": ts_day_parties,
    "victims": ts_day_victims,
    "collisions": ts_day_collisions,
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.6. Add Date Index to Time Series Data ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.6. Adding Date Index to Time Series Data")

# Add date index to the crashes time series data
ts_year["crashes"].index = pd.to_datetime(ts_year["crashes"]["date_year"])
ts_quarter["crashes"].index = pd.to_datetime(ts_quarter["crashes"]["date_quarter"])
ts_month["crashes"].index = pd.to_datetime(ts_month["crashes"]["date_month"])
ts_week["crashes"].index = pd.to_datetime(ts_week["crashes"]["date_week"])
ts_day["crashes"].index = pd.to_datetime(ts_day["crashes"]["date_day"])

# Add date index to the parties time series data
ts_year["parties"].index = pd.to_datetime(ts_year["parties"]["date_year"])
ts_quarter["parties"].index = pd.to_datetime(ts_quarter["parties"]["date_quarter"])
ts_month["parties"].index = pd.to_datetime(ts_month["parties"]["date_month"])
ts_week["parties"].index = pd.to_datetime(ts_week["parties"]["date_week"])
ts_day["parties"].index = pd.to_datetime(ts_day["parties"]["date_day"])

# Add date index to the victims time series data
ts_year["victims"].index = pd.to_datetime(ts_year["victims"]["date_year"])
ts_quarter["victims"].index = pd.to_datetime(ts_quarter["victims"]["date_quarter"])
ts_month["victims"].index = pd.to_datetime(ts_month["victims"]["date_month"])
ts_week["victims"].index = pd.to_datetime(ts_week["victims"]["date_week"])
ts_day["victims"].index = pd.to_datetime(ts_day["victims"]["date_day"])

# Add date index to the collisions time series data
ts_year["collisions"].index = pd.to_datetime(ts_year["collisions"]["date_year"])
ts_quarter["collisions"].index = pd.to_datetime(ts_quarter["collisions"]["date_quarter"])
ts_month["collisions"].index = pd.to_datetime(ts_month["collisions"]["date_month"])
ts_week["collisions"].index = pd.to_datetime(ts_week["collisions"]["date_week"])
ts_day["collisions"].index = pd.to_datetime(ts_day["collisions"]["date_day"])


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Save the Time Series Data ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3. Save the Time Series Data")

# Save the data to disk
ocs.save_to_disk(
    dir_list = prj_dirs,
    local_vars = locals(),
    global_vars = globals()
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of Script ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\nLast Execution:", datetime.datetime.now().strftime("%Y-%m-%d"))
print("\nEnd of Script")
# Last Execution: 2025-12-26
