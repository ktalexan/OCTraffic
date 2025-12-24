# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCSWITRS Data Processing
# Title: Part 3 - Time Series Processing
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.2, Date: September 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOC SWITRS GIS Data Processing - Part 3 - Time Series Processing\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1. Preliminaries")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Libraries and Initialization ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.1. Libraries and Initialization")

# Import necessary libraries
import os, sys, datetime, time
import json, pickle
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
import pytz
from dotenv import load_dotenv
import arcpy, arcgis
from arcpy import metadata as md

# important as it "enhances" Pandas by importing these classes (from ArcGIS API for Python)
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import ocswitrs as ocs
import codebook.cbl as cbl

# Load environment variables from .env file
load_dotenv()

os.getcwd()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2. Project and Workspace Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.2. Project and Workspace Variables")

# Create a dictionary with the project metadata
print("\nCreating project metadata")
prj_meta = ocs.project_metadata(part=3, version=2025.2, silent=False,)

# Create a dictionary with the project directories
print("\nCreating project directories")
prj_dirs = ocs.project_directories(base_path=os.getcwd(), silent=False)

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
with open(cb_path, encoding="utf-8") as json_file:
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
## 2.1. Aggregating Individual Data Frames ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.1. Aggregating Individual Data Frames")

# Define the individual aggregation function
def ts_aggregate(dt: str, df: pd.DataFrame, cb: dict = cb) -> pd.DataFrame:
    """Aggregate a dataframe by a specified date column and return a new dataframe with aggregated statistics.
    This function takes a date column and a dataframe, and aggregates the dataframe by the date column.
    It computes the sum, mean, median, min, max, and standard deviation for specified columns in the dataframe.
    Args:
        dt (str): The name of the date column to aggregate by.
        df (pd.DataFrame): The dataframe to aggregate.
        cb (dict): A configuration dictionary containing metadata about the columns.
    Returns:
        pd.DataFrame: A new dataframe with the aggregated statistics.
    Raises:
        KeyError: If the specified date column does not exist in the dataframe.
        ValueError: If the dataframe is empty or does not contain any columns to aggregate.
    Examples:
        ts_year_crashes = ts_aggregate(dt="date_year", df=crashes, cb=cb)
    """

    def _aggregate_helper(cols, agg_type, suffix):
        agg_df = ts[[dt] + cols].copy()
        for col in cols:
            if col in agg_df.columns:
                agg_df.rename(columns={col: f"{col}_{suffix}"}, inplace=True)
        for col in agg_df.columns:
            if agg_df[col].dtype.name == "category":
                agg_df[col] = agg_df[col].cat.codes
        agg_df = agg_df.groupby(dt).agg(agg_type).reset_index()
        return agg_df

    df_name = df.attrs["name"]
    ts = df.copy()
    df_list_sum = [col for col in df.columns if cb[col]["ts_include"][df_name] == 1 and cb[col]["ts_stats"]["sum"] == 1]
    df_list_mean = [
        col for col in df.columns if cb[col]["ts_include"][df_name] == 1 and cb[col]["ts_stats"]["mean"] == 1
    ]
    df_list_median = [
        col for col in df.columns if cb[col]["ts_include"][df_name] == 1 and cb[col]["ts_stats"]["median"] == 1
    ]
    df_list = list(set(df_list_sum + df_list_mean + df_list_median))
    ta = _aggregate_helper(df_list_sum, "sum", "sum")
    tb = _aggregate_helper(df_list_mean, "mean", "mean")
    tc = _aggregate_helper(df_list_median, "median", "median")
    td = _aggregate_helper(df_list, "min", "min")
    te = _aggregate_helper(df_list, "max", "max")
    tf = _aggregate_helper(df_list, "std", "sd")
    tg = _aggregate_helper(df_list, "sem", "se")
    ts_aggregated = ta.merge(tb, on=dt, how="outer")
    ts_aggregated = ts_aggregated.merge(tc, on=dt, how="outer")
    ts_aggregated = ts_aggregated.merge(td, on=dt, how="outer")
    ts_aggregated = ts_aggregated.merge(te, on=dt, how="outer")
    ts_aggregated = ts_aggregated.merge(tf, on=dt, how="outer")
    ts_aggregated = ts_aggregated.merge(tg, on=dt, how="outer")
    
    # Sort the aggregated dataframe by the date column
    ts_aggregated.sort_values(by=dt, inplace=True)
    
    # Return the aggregated dataframe
    return ts_aggregated


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2. Aggregation by Year ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n4. Aggregation by Year")

# Aggregate the data frames by year
ts_year_crashes = ts_aggregate(dt = "date_year", df = crashes, cb = cb)
ts_year_parties = ts_aggregate(dt = "date_year", df = parties, cb = cb)
ts_year_victims = ts_aggregate(dt = "date_year", df = victims, cb = cb)
ts_year_collisions = ts_aggregate(dt = "date_year", df = collisions, cb = cb)

# Combine the year aggregated data frames into a dictionary
ts_year = {
    "crashes": ts_year_crashes,
    "parties": ts_year_parties,
    "victims": ts_year_victims,
    "collisions": ts_year_collisions,
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.3. Aggregation by Quarter ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n5. Aggregation by Quarter")

# Aggregate the data frames by quarter
ts_quarter_crashes = ts_aggregate(dt = "date_quarter", df = crashes, cb = cb)
ts_quarter_parties = ts_aggregate(dt = "date_quarter", df = parties, cb = cb)
ts_quarter_victims = ts_aggregate(dt = "date_quarter", df = victims, cb = cb)
ts_quarter_collisions = ts_aggregate(dt = "date_quarter", df = collisions, cb = cb)

# Combine the quarter aggregated data frames into a dictionary
ts_quarter = {
    "crashes": ts_quarter_crashes,
    "parties": ts_quarter_parties,
    "victims": ts_quarter_victims,
    "collisions": ts_quarter_collisions,
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.4. Aggregation by Month ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n6. Aggregation by Month")

# Aggregate the data frames by month
ts_month_crashes = ts_aggregate(dt = "date_month", df = crashes, cb = cb)
ts_month_parties = ts_aggregate(dt = "date_month", df = parties, cb = cb)
ts_month_victims = ts_aggregate(dt = "date_month", df = victims, cb = cb)
ts_month_collisions = ts_aggregate(dt = "date_month", df = collisions, cb = cb)

# Combine the month aggregated data frames into a dictionary
ts_month = {
    "crashes": ts_month_crashes,
    "parties": ts_month_parties,
    "victims": ts_month_victims,
    "collisions": ts_month_collisions,
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.5. Aggregation by Week ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n7. Aggregation by Week")

# Aggregate the data frames by week
ts_week_crashes = ts_aggregate(dt = "date_week", df = crashes, cb = cb)
ts_week_parties = ts_aggregate(dt = "date_week", df = parties, cb = cb)
ts_week_victims = ts_aggregate(dt = "date_week", df = victims, cb = cb)
ts_week_collisions = ts_aggregate(dt = "date_week", df = collisions, cb = cb)

# Combine the week aggregated data frames into a dictionary
ts_week = {
    "crashes": ts_week_crashes,
    "parties": ts_week_parties,
    "victims": ts_week_victims,
    "collisions": ts_week_collisions,
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.6. Aggregation by Day ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n8. Aggregation by Day")

# Aggregate the data frames by day
ts_day_crashes = ts_aggregate(dt = "date_day", df = crashes, cb = cb)
ts_day_parties = ts_aggregate(dt = "date_day", df = parties, cb = cb)
ts_day_victims = ts_aggregate(dt = "date_day", df = victims, cb = cb)
ts_day_collisions = ts_aggregate(dt = "date_day", df = collisions, cb = cb)

# Combine the day aggregated data frames into a dictionary
ts_day = {
    "crashes": ts_day_crashes,
    "parties": ts_day_parties,
    "victims": ts_day_victims,
    "collisions": ts_day_collisions,
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.7. Add Date Index to Time Series Data ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n9. Adding Date Index to Time Series Data")

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
# Last Execution: 2025-10-21
