# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Part 1 - Merging Raw Data ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.3, Date: January 2026
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOCTraffic Data Processing - Part 1 - Merging Raw Data\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1. Preliminaries")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Referencing Libraries and Initialization ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.1. Referencing Libraries and Initialization")

# Import necessary libraries
import os, datetime
import json
import pandas as pd
from dotenv import load_dotenv
from octraffic import OCT

# Initialize the OCT class
octr = OCT(part = 1, version = 2025.3)

# Load environment variables from .env file
load_dotenv()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.2. Project and Workspace Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.2. Project and Workspace Variables")

# Create a dictionary with the project metadata
print("\nCreating project metadata")
prj_meta = octr.prj_meta

# Create a dictionary with the project directories
print("\nCreating project directories")
prj_dirs = octr.prj_dirs
# Import the codeboom
print("\nImporting codeboom")
cb = octr.cb
df_cb = octr.df_cb
# Set the current working directory to the project root
os.chdir(prj_dirs["root"])
# Get the new current working directory
print(f"\nCurrent working directory: {os.getcwd()}\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Import Raw Data (Initialization) ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2. Import Raw Data (Initialization)")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1. Importing Raw Data from Disk ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.1. Importing Raw Data from Disk")

print("- Creating a new Data Dictionary")
# Create a new pandas data frame to store the data dictionary
data_dict = pd.DataFrame(columns = ["year", "date_start", "date_end", "count_crashes", "count_parties", "count_victims"])

# Set a mew index for the dataframe
i = 1
# Initialize variables
data = None
date_start = prj_meta["date_start"]
date_end = prj_meta["date_end"]
count_crashes = 0
count_parties = 0
count_victims = 0

# Loop through the years in the project metadata
for year in list(prj_meta["years"]):
    # Set the index for the dataframe year
    data_dict.loc[i, "year"] = year
    # Loop through the levels of the data
    for level in ["Crashes", "Parties", "Victims"]:
        # Read the data from the CSV file
        data = pd.read_csv(
            os.path.join(prj_dirs["data_raw"], f"{level}_{year}.csv"),
            encoding = "utf-8",  # specify the encoding
            sep = ",",  # default delimiter
            na_values = ["", "NA", "N/A"],  # values to treat as NaN
            low_memory = False,  # helpful for large files
        )
        # If the level is "Crashes":
        if level == "Crashes":
            # Safely parse COLLISION_DATE to datetime and handle missing/invalid values
            if "COLLISION_DATE" in data.columns:
                collision_dates = pd.to_datetime(data["COLLISION_DATE"], errors = "coerce")
                if collision_dates.notna().any():
                    date_start = collision_dates.min()
                    date_end = collision_dates.max()
                else:
                    # Fallback to project metadata if all dates are invalid
                    date_start = prj_meta["start_date"]
                    date_end = prj_meta["end_date"]
            else:
                # Fallback to project metadata if column is missing
                date_start = prj_meta["start_date"]
                date_end = prj_meta["end_date"]
            count_crashes = len(data)
            # Set the values in the dataframe
            data_dict.loc[i, "date_start"] = date_start
            data_dict.loc[i, "date_end"] = date_end
            data_dict.loc[i, "count_crashes"] = count_crashes
        # If the level is "Parties":
        elif level == "Parties":
            count_parties = len(data)
            # Set the values in the dataframe
            data_dict.loc[i, "count_parties"] = count_parties
        # If the level is "Victims":
        elif level == "Victims":
            count_victims = len(data)
            # Set the values in the dataframe
            data_dict.loc[i, "count_victims"] = count_victims

    # Update the tims metadata
    octr.update_tims_metadata(year, "reported", data_counts = [count_crashes, count_parties, count_victims])

    # Increment the index
    i += 1
# Remove the temporary variables
del data, date_start, date_end, count_crashes, count_parties, count_victims

# Add data dictionary attributes
data_dict.attrs = {
    "name": "data_dict",
    "label": "Data Dictionary",
    "description": "A data dictionary containing metadata for the raw data files.",
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y"),
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2. Merging Temporal Raw Data Files ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.2. Merging Temporal Raw Data Files")

print("- Merging the raw Crashes data files")
# Create a new pandas dataframe to store the crashes data
crashes = pd.DataFrame()
# Loop through the years in the project metadata
for year in list(prj_meta["years"]):
    # Read the data from the CSV file
    data = pd.read_csv(
        os.path.join(prj_dirs["data_raw"], f"Crashes_{year}.csv"),
        encoding = "utf-8",  # specify the encoding
        sep = ",",  # default delimiter
        na_values = ["", "NA", "N/A"],  # values to treat as NaN
        low_memory = False,  # helpful for large files
    )
    # Add the data to the crashes dataframe
    crashes = pd.concat([crashes, data], ignore_index = True)
    # Remove the temporary variable
    del data

# Add crashes attributes
crashes.attrs = {
    "name": "crashes",
    "label": "OCTraffic Crashes Data",
    "description": (
        f"A dataframe containing raw crashes data ({prj_meta['date_start'].year}-{prj_meta['date_end'].year})."
    ),
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y"),
}

print("- Merging the raw Parties data files")
# Create a new pandas dataframe to store the parties data
parties = pd.DataFrame()
# Loop through the years in the project metadata
for year in list(prj_meta["years"]):
    # Read the data from the CSV file
    data = pd.read_csv(
        os.path.join(prj_dirs["data_raw"], f"Parties_{year}.csv"),
        encoding = "utf-8",  # specify the encoding
        sep = ",",  # default delimiter
        na_values = ["", "NA", "N/A"],  # values to treat as NaN
        low_memory = False,  # helpful for large files
    )
    # Add the data to the parties dataframe
    parties = pd.concat([parties, data], ignore_index = True)
    # Remove the temporary variable
    del data

# Name the parties dataframe
parties.attrs = {
    "name": "parties",
    "label": "OCTraffic Parties Data",
    "description": (
        f"A dataframe containing raw parties data ({prj_meta['date_start'].year}-{prj_meta['date_end'].year})."
    ),
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y"),
}


print("- Merging the raw Victims data files")
# Create a new pandas dataframe to store the victims data
victims = pd.DataFrame()
# Loop through the years in the project metadata
for year in list(prj_meta["years"]):
    # Read the data from the CSV file
    data = pd.read_csv(
        os.path.join(prj_dirs["data_raw"], f"Victims_{year}.csv"),
        encoding = "utf-8",  # specify the encoding
        sep = ",",  # default delimiter
        na_values = ["", "NA", "N/A"],  # values to treat as NaN
        low_memory = False,  # helpful for large files
    )
    # Add the data to the victims dataframe
    victims = pd.concat([victims, data], ignore_index = True)
    # Remove the temporary variable
    del data

# Name the victims dataframe
victims.attrs = {
    "name": "victims",
    "label": "OCTraffic Victims Data",
    "description": (
        f"A dataframe containing raw victims data ({prj_meta['date_start'].year}-{prj_meta['date_end'].year})."
    ),
    "version": prj_meta["version"],
    "updated": datetime.date.today().strftime("%m/%d/%Y"),
}

print("- Counting the rows in each of the dataframes")
# Count the rows in each of the crashes, parties, and victims dataframes
crashes_count = len(crashes)
parties_count = len(parties)
victims_count = len(victims)

# Print the number of rows in each of the dataframes showing the counts in a comma thousand format
print(
    f"  Number of rows:\n  - Crashes: {crashes_count:,}\n  - Parties: {parties_count:,}\n  - Victims: {victims_count:,}"
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.3. Save the Merged Data to Disk ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.3. Save the Merged Data to Disk")

print("- Saving Crashes, Parties, and Victims dataframes to disk")
# Save the crashes, parties, and victims dataframes to pickle files
crashes.to_pickle(os.path.join(prj_dirs["data_python"], "raw_crashes.pkl"))
parties.to_pickle(os.path.join(prj_dirs["data_python"], "raw_parties.pkl"))
victims.to_pickle(os.path.join(prj_dirs["data_python"], "raw_victims.pkl"))

print("- Saving the data dictionary to disk")
# Save the data dictionary to python pickle file
data_dict.to_pickle(os.path.join(prj_dirs["data_python"], "data_dict.pkl"))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.4. Create LaTex Variables Dictionary ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.4. Create LaTex Variables Dictionary")

print("- Creating LaTex Variables Dictionary")

# Define number of months between start and end date
months = (
    (prj_meta["date_end"].year - prj_meta["date_start"].year) * 12
    + prj_meta["date_end"].month
    - prj_meta["date_start"].month
)
if prj_meta["date_end"].day >= prj_meta["date_start"].day:
    months += 1

# Define LaTeX variables
latex_vars = {
    "prjVersion": prj_meta['version'],
    "prjUpdated": datetime.date.today().strftime("%B %d, %Y"),
    "prjDateWhole": prj_meta['date_end'].strftime('%B %d, %Y'),
    "prjDateUs": prj_meta['date_end'].strftime('%m/%d/%Y'),
    "prjMonths": months,
    "crashesOriginal": crashes_count,
    "partiesOriginal": parties_count,
    "victimsOriginal": victims_count,
    "collisionsOriginal": 0,
    "crashesCount": 0,
    "partiesCount": 0,
    "victimsCount": 0,
    "collisionsCount": 0,
    "crashesFatalities": 0,
    "partiesFatalities": 0,
    "victimsFatalities": 0
}

# Define the path to save the LaTeX variables dictionary
latex_vars_path = os.path.join(prj_dirs["metadata"], "latex_vars.json")

# Export the dictionary to a JSON file
try:
    with open(latex_vars_path, "w", encoding = "utf-8") as json_file:
        json.dump(latex_vars, json_file, indent = 4)
    print(f"- LaTeX variables dictionary saved to {latex_vars_path}")
except OSError as e:
    print(f"- Error saving LaTeX variables dictionary: {e}")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of Script ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\nLast Execution:", datetime.datetime.now().strftime("%Y-%m-%d"))
print("\nEnd of Script")
# Last Execution: 2026-01-01
