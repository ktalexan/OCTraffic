# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Part 0 - Update Metadata for OCTraffic Datasets ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.3, Date: January 2026
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOCTraffic Data Processing - Part 0: Update Metadata for OCTraffic Datasets\n")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1. Preliminaries")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Referencing Libraries and Initialization ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.1. Referencing Libraries and Initialization")

# Importing necessary libraries
import os
import datetime
import json
from dotenv import load_dotenv
from octraffic import OCTraffic

# Initialize the OCTraffic class
octr = OCTraffic(part = 0, version = 2026.1)

# Loading environment variables from .env file
load_dotenv()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2. Data Definition Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.2. Data Definition Variables")

# Define the last year of data and the cutoff between final and provisional data
FIRST_YEAR = 2012
TIMS_START_YEAR = 2013
TIMS_FINAL_YEAR = 2023
TIMS_END_YEAR = 2025
# Create a string with the current month and year. The month is the month's name
current_date = datetime.datetime.now().strftime("%B %Y")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.3. Project and Workspace Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.3. Project and Workspace Variables")

# Create a dictionary with the project metadata
print("\nCreating project metadata")
prj_meta = octr.prj_meta

# Create a dictionary with the project directories
print("\nCreating project directories")
prj_dirs = octr.prj_dirs



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Update TIMS Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2. Update TIMS Metadata")

# get a range of years for the project
project_years = list(range(FIRST_YEAR, TIMS_END_YEAR + 1))
excluded_years = list(range(FIRST_YEAR, TIMS_START_YEAR))
final_years = list(range(TIMS_START_YEAR, TIMS_FINAL_YEAR + 1))
provisional_years = list(range(TIMS_FINAL_YEAR + 1, TIMS_END_YEAR + 1))
missing_years = []
json_template = {
    "year": "",
    "date_start": "",
    "date_end": "",
    "date_updated": "",
    "date_gp": None,
    "status": "",
    "files": {
        "crashes": "",
        "parties": "",
        "victims": ""
    },
    "reported": {
        "crashes": 0,
        "parties": 0,
        "victims": 0
    },
    "geocoded": {
        "crashes": 0,
        "parties": 0,
        "victims": 0
    },
    "excluded": {
        "crashes": 0,
        "parties": 0,
        "victims": 0
    },
    "notes": ""
}

# Create a list the years in project_years that are not in prj_meta["years"]
for year in project_years:
    if year not in prj_meta["years"]:
        # Append the year to missing_years
        missing_years.append(year)
        # Append the prj_meta["tims"] with a new json_template for that year
        new_entry = json_template.copy()
        new_entry["year"] = str(year)
        new_entry["date_start"] = f"{year}-01-01"
        new_entry["date_end"] = f"{year}-03-31"
        new_entry["date_updated"] = current_date
        new_entry["date_gp"] = None
        new_entry["status"] = "provisional"
        new_entry["files"]["crashes"] = f"Crashes_{year}.csv"
        new_entry["files"]["parties"] = f"Parties_{year}.csv"
        new_entry["files"]["victims"] = f"Victims_{year}.csv"               
        prj_meta["tims"][str(year)] = new_entry

# For each year key in prj_meta["tims"], update the metadata
for year_key in prj_meta["tims"].keys():
    if int(year_key) in excluded_years:
        prj_meta["tims"][year_key]["status"] = "final"
        prj_meta["tims"][year_key]["notes"] = f"Data Removed from TIMS (updated on {current_date})"
    elif int(year_key) in final_years:
        prj_meta["tims"][year_key]["status"] = "final"
        prj_meta["tims"][year_key]["notes"] = f"Final Data in TIMS (updated on {current_date})"
    elif int(year_key) in provisional_years:
        prj_meta["tims"][year_key]["status"] = "provisional"
        prj_meta["tims"][year_key]["notes"] = f"Provisional Data in TIMS (updated on {current_date})"
    else:
        prj_meta["tims"][year_key]["status"] = "unknown"
        prj_meta["tims"][year_key]["notes"] = f"Unknown Data in TIMS (updated on {current_date})"

# Update the counts from the data files
for year_key in prj_meta["tims"].keys():
    print(f"\nUpdating counts for year {year_key}...")
    crashes_path = os.path.join(prj_dirs["data_raw"], prj_meta["tims"][year_key]["files"]["crashes"])
    parties_path = os.path.join(prj_dirs["data_raw"], prj_meta["tims"][year_key]["files"]["parties"])
    victims_path = os.path.join(prj_dirs["data_raw"], prj_meta["tims"][year_key]["files"]["victims"])
    
    # Count rows in crashes file
    try:
        with open(crashes_path, "r", encoding = "utf-8") as f:
            row_count = sum(1 for line in f) - 1  # Subtract 1 for header
        prj_meta["tims"][year_key]["reported"]["crashes"] = row_count
        print(f"Crashes rows: {row_count:,}")
    except FileNotFoundError:
        prj_meta["tims"][year_key]["reported"]["crashes"] = 0
        print(f"Crashes file not found: {crashes_path}")

    # Count rows in parties file
    try:
        with open(parties_path, "r", encoding = "utf-8") as f:
            row_count = sum(1 for line in f) - 1  # Subtract 1 for header
        prj_meta["tims"][year_key]["reported"]["parties"] = row_count
        print(f"Parties rows: {row_count:,}")
    except FileNotFoundError:
        prj_meta["tims"][year_key]["reported"]["parties"] = 0
        print(f"Parties file not found: {parties_path}")

    # Count rows in victims file
    try:
        with open(victims_path, "r", encoding = "utf-8") as f:
            row_count = sum(1 for line in f) - 1  # Subtract 1 for header
        prj_meta["tims"][year_key]["reported"]["victims"] = row_count
        print(f"Victims rows: {row_count:,}")
    except FileNotFoundError:
        prj_meta["tims"][year_key]["reported"]["victims"] = 0
        print(f"Victims file not found: {victims_path}")


# print the prj_meta["tims"] as a json string with indentation of 4 spaces
print("\nUpdated TIMS Metadata:")
print(json.dumps(prj_meta["tims"], indent = 4))

# Update the tims metadata json file
metadata_path = os.path.join(prj_dirs["metadata"], "tims_metadata.json")

# Replace the existing tims_metadata.json file with the updated metadata
with open(metadata_path, "w", encoding = "utf-8") as f:
    json.dump(prj_meta["tims"], f, indent = 4)

# Update the metadata info
octr = OCTraffic(part = 0, version = 2025.3)
prj_meta = octr.prj_meta


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of Script ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\nLast Execution:", datetime.datetime.now().strftime("%Y-%m-%d"))
print("\nEnd of Script")
# Last Execution: 2026-01-01
