# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Part 10 - Updating ArcGIS Online Metadata ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.3, Date: January 2026
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOCTraffic GIS Data Processing - Part 10 - Updating ArcGIS Online Metadata\n")


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
import datetime as dt
import pytz, arcpy
from arcgis.gis import GIS
from dotenv import load_dotenv

from octraffic import OCTraffic

# Initialize the OCTraffic object
ocs = OCTraffic(part = 10, version = 2025.3)

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
prj_meta = ocs.prj_meta

# Create a dictionary with the project directories
prj_dirs = ocs.prj_dirs

# Set the current working directory to the project root
os.chdir(prj_dirs["root"])

# Define the project folder (parent directory of the current working directory)
prj_folder = os.getcwd()


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
cb = ocs.cb

# create a data frame from the codebook
print("- Creating a data frame from the codebook")
df_cb = ocs.df_cb



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
print(
    f"Info: \n- Token: {portal_meta['info']['token']}\n- Referer: {portal_meta['info']['referer']}\n- Expires: {dt.datetime.fromtimestamp(portal_meta['info']['expires']).strftime('%Y-%m-%d %H:%M:%S')}"
)
print(f"Portal: {portal_meta['desc']['name']}")
print(f"User: {portal_meta['desc']['user']['fullName']} ({portal_meta['desc']['user']['username']})")

# Path to store ArcGIS Online staging feature layers
# ago_prj_path = prj_dirs["data_ago"]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Update ArcGIS Online Web Maps and Apps Item Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3. Update ArcGIS Online Web Maps and Apps Item Metadata")

## 3.1. Define Metadata Variables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.1. Define Metadata Variables")

# Define a list with the web map names to be updated

agol_list = {
    "Web maps": [
        "OCTraffic Victims Map",
        "OCTraffic Parties Map",
        "OCTraffic Crashes Map",
        "OCTraffic Collisions App Map",
        "OCTraffic Historical Collisions Map"
    ],
    "Dashboards": [
        "OCTraffic Crashes Dashboard",
        "OCTraffic Collisions Dashboard"
    ],
    "Instant Apps": [
        "OCTraffic Collisions Slider"
    ]
}

# Get group ids for "OC Traffic Data" and "Orange County Open Data"
group_traffic_data = gis.groups.search("OC Traffic Data", max_groups = 1)[0]
group_oc_data = gis.groups.search("Orange County Open Data", max_groups = 1)[0]

# Define logos to be used as thumbnails
logos_dict = {
    "crashes": "https://ocpw.maps.arcgis.com/sharing/rest/content/items/745f7297cfd84acba5d265666904eac3/data",
    "parties": "https://ocpw.maps.arcgis.com/sharing/rest/content/items/803d02a3a75a49bfa3ba32e106e2f180/data",
    "victims": "https://ocpw.maps.arcgis.com/sharing/rest/content/items/c7507771df294d939f766b999dfdd607/data",
    "collisions": "https://ocpw.maps.arcgis.com/sharing/rest/content/items/214e7a4900dd4b998180890cfa555bb1/data",
    "roads_secondary": "https://ocpw.maps.arcgis.com/sharing/rest/content/items/59dfc446288d4fbaa0d38d7bd0f49474/data",
    "roads_local": "https://ocpw.maps.arcgis.com/sharing/rest/content/items/f54f039885c74aa49f3d1cdf7021f9d9/data",
    "traffic_light": "https://ocpw.maps.arcgis.com/sharing/rest/content/items/b6319f0916d54c6bb8020a3e9920741c/data"
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.2. Update Metadata for Victims Web Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.2. Update Metadata for Victims Web Map")

# Define the query to search for the Victims Web Map
query_victims_map = f"""title:"{agol_list['Web maps'][0]}" AND owner: {portal_username}"""

# Search for the Victims Web Map item in ArcGIS Online and return the first item as result
item_victims_map = gis.content.search(
    query = query_victims_map,
    item_type = "Web Map",
    max_items = 1
)[0]

# Define a dictionary to hold the metadata updates for the Victims Web Map
md_victims_map = {}

# Define the summary metadata for the Victims Web Map
md_victims_map["snippet"] = f"Statewide Integrated Traffic Records System (SWITRS) Incident-Involved Victims Data for Orange County, California ({md_years})"

# Define the description metadata for the Victims Web Map
md_victims_map["description"] = f"""<p><strong>Web Map of the Statewide Integrated Traffic Records System (SWITRS)</strong>&nbsp;location point data, containing&nbsp;<strong>reports on victims/persons involved in crash incidents</strong>&nbsp;in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.chp.ca.gov:443/">California Highway Patrol (CHP)</a>, from incidents reported by local and government agencies. Original tabular datasets are provided by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://tims.berkeley.edu:443/">Transportation Injury Mapping System (TIMS)</a>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on&nbsp;<strong>{date_updated}</strong></p>"""

# Define the license information metadata for the Victims Web Map
md_victims_map["licenseInfo"] = """<p>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.&nbsp;<br>The displayed mapped data can be used under a&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://creativecommons.org/licenses/by-sa/3.0/">Creative Commons CC-SA-BY</a>&nbsp;License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services.<br>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.ocgov.com/contact-county/disclaimer">Disclaimer</a>&nbsp;applies.</p><p>&nbsp;For any inquiries, suggestions or questions, please contact:</p><p style="text-align:center;"><a target="_blank" rel="noopener noreferrer" href="https://www.linkedin.com/in/ktalexan/"><strong>Dr. Kostas Alexandridis, GISP</strong></a><br>GIS Analyst | Spatial Complex Systems Scientist<br>OC Public Works/OC Survey Geospatial Applications<br>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701<br>Email:&nbsp;<a href="mailto:kostas.alexandridis@ocpw.ocgov.com">kostas.alexandridis@ocpw.ocgov.com</a>&nbsp;| Phone: (714) 967-0826</p>"""

# Define the tags metadata for the Victims Web Map
md_victims_map["tags"] = ["Orange County", "California", "OCTraffic", "SWITRS", "Traffic", "Traffic Conditions", "Crashes", "Victims", "Collisions", "Road Safety", "Accidents", "Transportation"]

# Define the access information metadata for the Victims Web Map
md_victims_map["accessInformation"] = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"

# Define the thumbnail url for the Victims Web Map
md_victims_map["thumbnailurl"] = logos_dict["victims"]

# Make sure the item was found
if item_victims_map:
    print(f"Found item: {item_victims_map.title} (ID: {item_victims_map.id})")

    # Update the item metadata
    item_victims_map.update(
        {
            "overwrite": True,
            "snippet": md_victims_map["snippet"],
            "description": md_victims_map["description"],
            "licenseInfo": md_victims_map["licenseInfo"],
            "tags": md_victims_map["tags"],
            "accessInformation": md_victims_map["accessInformation"],
            "thumbnailurl": md_victims_map["thumbnailurl"],
            "categories": "Traffic, Transportation",
        }
    )
    print("Metadata updated successfully.")
else:
    print("Item not found.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.3. Update Metadata for Parties Web Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.3. Update Metadata for Parties Web Map")

# Search for the Parties Web Map item in ArcGIS Online and return the first item as result
query_parties_map = f"""title:"{agol_list['Web maps'][1]}" AND owner: {portal_username}"""

# Search for the Parties Web Map item in ArcGIS Online and return the first item as result
item_parties_map = gis.content.search(
    query = query_parties_map,
    item_type = "Web Map",
    max_items = 1
)[0]

# Define a dictionary to hold the metadata updates for the Parties Web Map
md_parties_map = {}

# Define the summary metadata for the Parties Web Map
md_parties_map["snippet"] = f"Statewide Integrated Traffic Records System (SWITRS) Incident-Involved Parties Data for Orange County, California ({md_years})"

# Define the description metadata for the Parties Web Map
md_parties_map["description"] = f"""<p><strong>Web Map of the Statewide Integrated Traffic Records System (SWITRS)</strong>&nbsp;location point data, containing&nbsp;<strong>reports on parties/vehicles involved in crash incidents</strong>&nbsp;in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.chp.ca.gov:443/">California Highway Patrol (CHP)</a>, from incidents reported by local and government agencies. Original tabular datasets are provided by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://tims.berkeley.edu:443/">Transportation Injury Mapping System (TIMS)</a>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on&nbsp;<strong>{date_updated}</strong></p>"""

# Define the license information metadata for the Parties Web Map
md_parties_map["licenseInfo"] = """<p>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.&nbsp;<br>The displayed mapped data can be used under a&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://creativecommons.org/licenses/by-sa/3.0/">Creative Commons CC-SA-BY</a>&nbsp;License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services.<br>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.ocgov.com/contact-county/disclaimer">Disclaimer</a>&nbsp;applies.</p><p>&nbsp;For any inquiries, suggestions or questions, please contact:</p><p style="text-align:center;"><a target="_blank" rel="noopener noreferrer" href="https://www.linkedin.com/in/ktalexan/"><strong>Dr. Kostas Alexandridis, GISP</strong></a><br>GIS Analyst | Spatial Complex Systems Scientist<br>OC Public Works/OC Survey Geospatial Applications<br>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701<br>Email:&nbsp;<a href="mailto:kostas.alexandridis@ocpw.ocgov.com">kostas.alexandridis@ocpw.ocgov.com</a>&nbsp;| Phone: (714) 967-0826</p>"""

# Define the tags metadata for the Parties Web Map
md_parties_map["tags"] = ["Orange County", "California", "OCTraffic", "SWITRS", "Traffic", "Traffic Conditions", "Crashes", "Parties", "Vehicles", "Collisions", "Road Safety", "Accidents", "Transportation"]

# Define the access information metadata for the Parties Web Map
md_parties_map["accessInformation"] = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"

# Define the thumbnail url for the Parties Web Map
md_parties_map["thumbnailurl"] = logos_dict["parties"]

# Make sure the item was found
if item_parties_map:
    print(f"Found item: {item_parties_map.title} (ID: {item_parties_map.id})")
    # Update the item metadata
    item_parties_map.update(
        {
            "overwrite": True,
            "snippet": md_parties_map["snippet"],
            "description": md_parties_map["description"],
            "licenseInfo": md_parties_map["licenseInfo"],
            "tags": md_parties_map["tags"],
            "accessInformation": md_parties_map["accessInformation"],
            "thumbnailurl": md_parties_map["thumbnailurl"],
            "categories": "Traffic, Transportation",
        }
    )
    print("Metadata updated successfully.")
else:
    print("Item not found.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.4. Update Metadata for Crashes Web Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.4. Update Metadata for Crashes Web Map")

# Search for the Crashes Web Map item in ArcGIS Online and return the first item as result
query_crashes_map = f"""title:"{agol_list['Web maps'][2]}" AND owner: {portal_username}"""

# Search for the Crashes Web Map item in ArcGIS Online and return the first item as result
item_crashes_map = gis.content.search(
    query = query_crashes_map,
    item_type = "Web Map",
    max_items = 1
)[0]


# Define a dictionary to hold the metadata updates for the Crashes Web Map
md_crashes_map = {}

# Define the summary metadata for the Crashes Web Map
md_crashes_map["snippet"] = f"Statewide Integrated Traffic Records System (SWITRS) Crash Incidents Data for Orange County, California ({md_years})"

# Define the description metadata for the Crashes Web Map
md_crashes_map["description"] = f"""<p><strong>Web Map of the Statewide Integrated Traffic Records System (SWITRS)</strong>&nbsp;location point data, containing&nbsp;<strong>reports on crash incidents</strong>&nbsp;in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.chp.ca.gov:443/">California Highway Patrol (CHP)</a>, from incidents reported by local and government agencies. Original tabular datasets are provided by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://tims.berkeley.edu:443/">Transportation Injury Mapping System (TIMS)</a>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on&nbsp;<strong>{date_updated}</strong></p>"""

# Define the license information metadata for the Crashes Web Map
md_crashes_map["licenseInfo"] = """<p>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.&nbsp;<br>The displayed mapped data can be used under a&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://creativecommons.org/licenses/by-sa/3.0/">Creative Commons CC-SA-BY</a>&nbsp;License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services.<br>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.ocgov.com/contact-county/disclaimer">Disclaimer</a>&nbsp;applies.</p><p>&nbsp;For any inquiries, suggestions or questions, please contact:</p><p style="text-align:center;"><a target="_blank" rel="noopener noreferrer" href="https://www.linkedin.com/in/ktalexan/"><strong>Dr. Kostas Alexandridis, GISP</strong></a><br>GIS Analyst | Spatial Complex Systems Scientist<br>OC Public Works/OC Survey Geospatial Applications<br>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701<br>Email:&nbsp;<a href="mailto:kostas.alexandridis@ocpw.ocgov.com">kostas.alexandridis@ocpw.ocgov.com</a>&nbsp;| Phone: (714) 967-0826</p>"""

# Define the tags metadata for the Crashes Web Map
md_crashes_map["tags"] = ["Orange County", "California", "OCTraffic", "SWITRS", "Traffic", "Traffic Conditions", "Crashes", "Collisions", "Road Safety", "Accidents", "Transportation"]

# Define the access information metadata for the Crashes Web Map
md_crashes_map["accessInformation"] = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"

# Define the thumbnail url for the Crashes Web Map
md_crashes_map["thumbnailurl"] = logos_dict["crashes"]

# Make sure the item was found
if item_crashes_map:
    print(f"Found item: {item_crashes_map.title} (ID: {item_crashes_map.id})")
    # Update the item metadata
    item_crashes_map.update(
        {
            "overwrite": True,
            "snippet": md_crashes_map["snippet"],
            "description": md_crashes_map["description"],
            "licenseInfo": md_crashes_map["licenseInfo"],
            "tags": md_crashes_map["tags"],
            "accessInformation": md_crashes_map["accessInformation"],
            "thumbnailurl": md_crashes_map["thumbnailurl"],
            "categories": "Traffic, Transportation",
        }
    )
    print("Metadata updated successfully.")
else:
    print("Item not found.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.5. Update Metadata for Collisions App Web Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.5. Update Metadata for Collisions App Web Map")

# Search for the Collisions App Web Map item in ArcGIS Online and return the first item as result
query_collisions_app_map = f"""title:"{agol_list['Web maps'][3]}" AND owner: {portal_username}"""

# Search for the Collisions App Web Map item in ArcGIS Online and return the first item as result
item_collisions_app_map = gis.content.search(
    query = query_collisions_app_map,
    item_type = "Web Map",
    max_items = 1
)[0]

# Define a dictionary to hold the metadata updates for the Collisions App Web Map
md_collisions_app_map = {}

# Define the summary metadata for the Collisions App Web Map
md_collisions_app_map["snippet"] = f"Statewide Integrated Traffic Records System (SWITRS) Collision Incidents Data for Orange County, California ({md_years})"

# Define the description metadata for the Collisions App Web Map
md_collisions_app_map["description"] = f"""<p><strong>Web Map of the Statewide Integrated Traffic Records System (SWITRS)</strong>&nbsp;location point data, containing&nbsp;<strong>reports on crashes, parties, and victims/persons involved in crash incidents</strong>&nbsp;in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.chp.ca.gov:443/">California Highway Patrol (CHP)</a>, from incidents reported by local and government agencies. Original tabular datasets are provided by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://tims.berkeley.edu:443/">Transportation Injury Mapping System (TIMS)</a>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on&nbsp;<strong>{date_updated}</strong></p>"""

# Define the license information metadata for the Collisions App Web Map
md_collisions_app_map["licenseInfo"] = """<p>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.&nbsp;<br>The displayed mapped data can be used under a&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://creativecommons.org/licenses/by-sa/3.0/">Creative Commons CC-SA-BY</a>&nbsp;License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services.<br>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.ocgov.com/contact-county/disclaimer">Disclaimer</a>&nbsp;applies.</p><p>&nbsp;For any inquiries, suggestions or questions, please contact:</p><p style="text-align:center;"><a target="_blank" rel="noopener noreferrer" href="https://www.linkedin.com/in/ktalexan/"><strong>Dr. Kostas Alexandridis, GISP</strong></a><br>GIS Analyst | Spatial Complex Systems Scientist<br>OC Public Works/OC Survey Geospatial Applications<br>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701<br>Email:&nbsp;<a href="mailto:kostas.alexandridis@ocpw.ocgov.com">kostas.alexandridis@ocpw.ocgov.com</a>&nbsp;| Phone: (714) 967-0826</p>"""

# Define the tags metadata for the Collisions App Web Map
md_collisions_app_map["tags"] = ["Orange County", "California", "OCTraffic", "SWITRS", "Traffic", "Traffic Conditions", "Crashes", "Collisions", "Road Safety", "Accidents", "Transportation"]

# Define the access information metadata for the Collisions App Web Map
md_collisions_app_map["accessInformation"] = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"

# Define the thumbnail url for the Collisions App Web Map
md_collisions_app_map["thumbnailurl"] = logos_dict["collisions"]

# Make sure the item was found
if item_collisions_app_map:
    print(f"Found item: {item_collisions_app_map.title} (ID: {item_collisions_app_map.id})")
    # Update the item metadata
    item_collisions_app_map.update(
        {
            "overwrite": True,
            "snippet": md_collisions_app_map["snippet"],
            "description": md_collisions_app_map["description"],
            "licenseInfo": md_collisions_app_map["licenseInfo"],
            "tags": md_collisions_app_map["tags"],
            "accessInformation": md_collisions_app_map["accessInformation"],
            "thumbnailurl": md_collisions_app_map["thumbnailurl"],
            "categories": "Traffic, Transportation",
        }
    )
    print("Metadata updated successfully.")
else:
    print("Item not found.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.6. Update Metadata for Historical Collisions Web Map ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.6. Update Metadata for Historical Collisions Web Map")

# Search for the Historical Collisions Web Map item in ArcGIS Online and return the first item as result
query_historical_collisions_map = f"""title:"{agol_list['Web maps'][4]}" AND owner: {portal_username}"""

# Search for the Historical Collisions Web Map item in ArcGIS Online and return the first item as result
item_historical_collisions_map = gis.content.search(
    query = query_historical_collisions_map,
    item_type = "Web Map",
    max_items = 1
)[0]


# Define a dictionary to hold the metadata updates for the Historical Collisions Web Map
md_historical_collisions_map = {}

# Define the summary metadata for the Historical Collisions Web Map
md_historical_collisions_map["snippet"] = f"Statewide Integrated Traffic Records System (SWITRS) Historical Collision Incidents Data for Orange County, California ({md_years})"

# Define the description metadata for the Historical Collisions Web Map
md_historical_collisions_map["description"] = f"""<p><strong>Web Map of the Statewide Integrated Traffic Records System (SWITRS)</strong>&nbsp;location point data, containing&nbsp;<strong>reports on collision crashes, parties and victims in crash incidents</strong>&nbsp;in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.chp.ca.gov:443/">California Highway Patrol (CHP)</a>, from incidents reported by local and government agencies. Original tabular datasets are provided by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://tims.berkeley.edu:443/">Transportation Injury Mapping System (TIMS)</a>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on&nbsp;<strong>{date_updated}</strong></p>"""

# Define the license information metadata for the Historical Collisions Web Map
md_historical_collisions_map["licenseInfo"] = """<p>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.&nbsp;<br>The displayed mapped data can be used under a&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://creativecommons.org/licenses/by-sa/3.0/">Creative Commons CC-SA-BY</a>&nbsp;License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services.<br>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.ocgov.com/contact-county/disclaimer">Disclaimer</a>&nbsp;applies.</p><p>&nbsp;For any inquiries, suggestions or questions, please contact:</p><p style="text-align:center;"><a target="_blank" rel="noopener noreferrer" href="https://www.linkedin.com/in/ktalexan/"><strong>Dr. Kostas Alexandridis, GISP</strong></a><br>GIS Analyst | Spatial Complex Systems Scientist<br>OC Public Works/OC Survey Geospatial Applications<br>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701<br>Email:&nbsp;<a href="mailto:kostas.alexandridis@ocpw.ocgov.com">kostas.alexandridis@ocpw.ocgov.com</a>&nbsp;| Phone: (714) 967-0826</p>"""

# Define the tags metadata for the Historical Collisions Web Map
md_historical_collisions_map["tags"] = ["Orange County", "California", "OCTraffic", "SWITRS", "Traffic", "Traffic Conditions", "Crashes", "Collisions", "Road Safety", "Accidents", "Transportation"]

# Define the access information metadata for the Historical Collisions Web Map
md_historical_collisions_map["accessInformation"] = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"

# Define the thumbnail url for the Historical Collisions Web Map
md_historical_collisions_map["thumbnailurl"] = logos_dict["collisions"]

# Make sure the item was found
if item_historical_collisions_map:
    print(f"Found item: {item_historical_collisions_map.title} (ID: {item_historical_collisions_map.id})")
    # Update the item metadata
    item_historical_collisions_map.update(
        {
            "overwrite": True,
            "snippet": md_historical_collisions_map["snippet"],
            "description": md_historical_collisions_map["description"],
            "licenseInfo": md_historical_collisions_map["licenseInfo"],
            "tags": md_historical_collisions_map["tags"],
            "accessInformation": md_historical_collisions_map["accessInformation"],
            "thumbnailurl": md_historical_collisions_map["thumbnailurl"],
            "categories": "Traffic, Transportation",
        }
    )
    print("Metadata updated successfully.")
else:
    print("Item not found.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.7. Update Metadata for Crashes Dashboard ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.7. Update Metadata for Crashes Dashboard")

# Search for the Crashes Dashboard item in ArcGIS Online and return the first item as result
query_crashes_dashboard = f"""title:"{agol_list['Dashboards'][0]}" AND owner: {portal_username}"""

# Search for the Crashes Dashboard item in ArcGIS Online and return the first item as result
item_crashes_dashboard = gis.content.search(
    query = query_crashes_dashboard,
    item_type = "Dashboard",
    max_items = 1
)[0]

# Define a dictionary to hold the metadata updates for the Crashes Dashboard
md_crashes_dashboard = {}

# Define the summary metadata for the Crashes Dashboard
md_crashes_dashboard["snippet"] = f"Dashboard for Statewide Integrated Traffic Records System (SWITRS) Crash Data for Orange County, California ({md_years})"

# Define the description metadata for the Crashes Dashboard
md_crashes_dashboard["description"] = f"""<p><strong>Dashboard for the Statewide Integrated Traffic Records System (SWITRS)</strong>&nbsp;location point data, containing&nbsp;<strong>reports on crash incidents</strong>&nbsp;in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.chp.ca.gov:443/">California Highway Patrol (CHP)</a>, from incidents reported by local and government agencies. Original tabular datasets are provided by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://tims.berkeley.edu:443/">Transportation Injury Mapping System (TIMS)</a>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on&nbsp;<strong>{date_updated}</strong></p>"""

# Define the license information metadata for the Crashes Dashboard
md_crashes_dashboard["licenseInfo"] = """<p>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.&nbsp;<br>The displayed mapped data can be used under a&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://creativecommons.org/licenses/by-sa/3.0/">Creative Commons CC-SA-BY</a>&nbsp;License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services.<br>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.ocgov.com/contact-county/disclaimer">Disclaimer</a>&nbsp;applies.</p><p>&nbsp;For any inquiries, suggestions or questions, please contact:</p><p style="text-align:center;"><a target="_blank" rel="noopener noreferrer" href="https://www.linkedin.com/in/ktalexan/"><strong>Dr. Kostas Alexandridis, GISP</strong></a><br>GIS Analyst | Spatial Complex Systems Scientist<br>OC Public Works/OC Survey Geospatial Applications<br>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701<br>Email:&nbsp;<a href="mailto:kostas.alexandridis@ocpw.ocgov.com">kostas.alexandridis@ocpw.ocgov.com</a>&nbsp;| Phone: (714) 967-0826</p>"""

# Define the tags metadata for the Crashes Dashboard
md_crashes_dashboard["tags"] = ["Orange County", "California", "OCTraffic", "SWITRS", "Traffic", "Traffic Conditions", "Crashes", "Collisions", "Road Safety", "Accidents", "Transportation", "Dashboard"]

# Define the access information metadata for the Crashes Dashboard
md_crashes_dashboard["accessInformation"] = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"

# Define the thumbnail url for the Crashes Dashboard
md_crashes_dashboard["thumbnailurl"] = logos_dict["crashes"]

# Make sure the item was found
if item_crashes_dashboard:
    print(f"Found item: {item_crashes_dashboard.title} (ID: {item_crashes_dashboard.id})")
    # Update the item metadata
    item_crashes_dashboard.update(
        {
            "overwrite": True,
            "snippet": md_crashes_dashboard["snippet"],
            "description": md_crashes_dashboard["description"],
            "licenseInfo": md_crashes_dashboard["licenseInfo"],
            "tags": md_crashes_dashboard["tags"],
            "accessInformation": md_crashes_dashboard["accessInformation"],
            "thumbnailurl": md_crashes_dashboard["thumbnailurl"],
            "categories": "Traffic, Transportation"
        }
    )
    print("Metadata updated successfully.")
else:
    print("Item not found.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.8. Update Metadata for Collisions Dashboard ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.8. Update Metadata for Collisions Dashboard")

# Search for the Collisions Dashboard item in ArcGIS Online and return the first item as result
query_collisions_dashboard = f"""title:"{agol_list['Dashboards'][1]}" AND owner: {portal_username}"""

# Search for the Collisions Dashboard item in ArcGIS Online and return the first item as result
item_collisions_dashboard = gis.content.search(
    query = query_collisions_dashboard,
    item_type = "Dashboard",
    max_items = 1
)[0]

# Define a dictionary to hold the metadata updates for the Collisions Dashboard
md_collisions_dashboard = {}

# Define the summary metadata for the Collisions Dashboard
md_collisions_dashboard["snippet"] = f"Dashboard for Statewide Integrated Traffic Records System (SWITRS) Collision Data for Orange County, California ({md_years})"

# Define the description metadata for the Collisions Dashboard
md_collisions_dashboard["description"] = f"""<p><strong>Dashboard for the Statewide Integrated Traffic Records System (SWITRS)</strong>&nbsp;location point data, containing&nbsp;<strong>reports on collision crashes, parties, and victims involved in crash incidents</strong>&nbsp;in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.chp.ca.gov:443/">California Highway Patrol (CHP)</a>, from incidents reported by local and government agencies. Original tabular datasets are provided by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://tims.berkeley.edu:443/">Transportation Injury Mapping System (TIMS)</a>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on&nbsp;<strong>{date_updated}</strong></p>"""

# Define the license information metadata for the Collisions Dashboard
md_collisions_dashboard["licenseInfo"] = """<p>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.&nbsp;<br>The displayed mapped data can be used under a&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://creativecommons.org/licenses/by-sa/3.0/">Creative Commons CC-SA-BY</a>&nbsp;License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services.<br>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.ocgov.com/contact-county/disclaimer">Disclaimer</a>&nbsp;applies.</p><p>&nbsp;For any inquiries, suggestions or questions, please contact:</p><p style="text-align:center;"><a target="_blank" rel="noopener noreferrer" href="https://www.linkedin.com/in/ktalexan/"><strong>Dr. Kostas Alexandridis, GISP</strong></a><br>GIS Analyst | Spatial Complex Systems Scientist<br>OC Public Works/OC Survey Geospatial Applications<br>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701<br>Email:&nbsp;<a href="mailto:kostas.alexandridis@ocpw.ocgov.com">kostas.alexandridis@ocpw.ocgov.com</a>&nbsp;| Phone: (714) 967-0826</p>"""

# Define the tags metadata for the Collisions Dashboard
md_collisions_dashboard["tags"] = ["Orange County", "California", "OCTraffic", "SWITRS", "Traffic", "Traffic Conditions", "Crashes", "Collisions", "Road Safety", "Accidents", "Transportation", "Dashboard"]

# Define the access information metadata for the Collisions Dashboard
md_collisions_dashboard["accessInformation"] = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"

# Define the thumbnail url for the Collisions Dashboard
md_collisions_dashboard["thumbnailurl"] = logos_dict["collisions"]

# Make sure the item was found
if item_collisions_dashboard:
    print(f"Found item: {item_collisions_dashboard.title} (ID: {item_collisions_dashboard.id})")
    # Update the item metadata
    item_collisions_dashboard.update(
        {
            "overwrite": True,
            "snippet": md_collisions_dashboard["snippet"],
            "description": md_collisions_dashboard["description"],
            "licenseInfo": md_collisions_dashboard["licenseInfo"],
            "tags": md_collisions_dashboard["tags"],
            "accessInformation": md_collisions_dashboard["accessInformation"],
            "thumbnailurl": md_collisions_dashboard["thumbnailurl"],
            "categories": "Traffic, Transportation"
        }
    )
    print("Metadata updated successfully.")
else:
    print("Item not found.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 3.9. Update Metadata for OC Traffic Collision Slider Instant App ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n3.9. Update Metadata for OC Traffic Collision Slider Instant App")

# Search for the OC Traffic Collision Slider Instant App item in ArcGIS Online and return the first item as result
query_oc_traffic_collision_slider_app = f"""title:"{agol_list['Instant Apps'][0]}" AND owner: {portal_username}"""

# Search for the OC Traffic Collision Slider Instant App item in ArcGIS Online and return the first item as result
item_oc_traffic_collision_slider_app = gis.content.search(
    query = query_oc_traffic_collision_slider_app,
    item_type = "Web Mapping Application",
    max_items = 1
)[0]

# Define a dictionary to hold the metadata updates for the OC Traffic Collision Slider Instant App
md_oc_traffic_collision_slider_app = {}

# Define the summary metadata for the OC Traffic Collision Slider Instant App
md_oc_traffic_collision_slider_app["snippet"] = f"OC Traffic Collision Slider Instant App for Statewide Integrated Traffic Records System (SWITRS) Data in Orange County, California ({md_years})"

# Define the description metadata for the OC Traffic Collision Slider Instant App
md_oc_traffic_collision_slider_app["description"] = f"""<p><strong>OC Traffic Collision Slider Instant App for the Statewide Integrated Traffic Records System (SWITRS)</strong>&nbsp;location point data, containing&nbsp;<strong>reports on collision crashes, parties, and victims involved in crash incidents</strong>&nbsp;in Orange County, California for {md_years} ({md_dates}). The data are collected and maintained by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.chp.ca.gov:443/">California Highway Patrol (CHP)</a>, from incidents reported by local and government agencies. Original tabular datasets are provided by the&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://tims.berkeley.edu:443/">Transportation Injury Mapping System (TIMS)</a>. Only records with reported locational GPS attributes in Orange County are included in the spatial database (either from X and Y geocoded coordinates, or the longitude and latitude coordinates generated by the CHP officer on site). Incidents without valid coordinates are omitted from this spatial dataset representation. Last Updated on&nbsp;<strong>{date_updated}</strong></p>"""

# Define the license information metadata for the OC Traffic Collision Slider Instant App
md_oc_traffic_collision_slider_app["licenseInfo"] = """<p>The SWITRS data displayed are provided by the California Highway Patrol (CHP) reports through the Transportation Injury Mapping System (TIMS) of the University of California, Berkeley. Issues of report accuracy should be addressed to CHP.&nbsp;<br>The displayed mapped data can be used under a&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://creativecommons.org/licenses/by-sa/3.0/">Creative Commons CC-SA-BY</a>&nbsp;License, providing attribution to TIMS, CHP, and OC Public Works, OC Survey Geospatial Services.<br>We make every effort to provide the most accurate and up-to-date data and information. Nevertheless, the data feed is provided, 'as is' and OC Public Work's standard&nbsp;<a target="_blank" rel="noopener noreferrer" href="https://www.ocgov.com/contact-county/disclaimer">Disclaimer</a>&nbsp;applies.</p><p>&nbsp;For any inquiries, suggestions or questions, please contact:</p><p style="text-align:center;"><a target="_blank" rel="noopener noreferrer" href="https://www.linkedin.com/in/ktalexan/"><strong>Dr. Kostas Alexandridis, GISP</strong></a><br>GIS Analyst | Spatial Complex Systems Scientist<br>OC Public Works/OC Survey Geospatial Applications<br>601 N. Ross Street, P.O. Box 4048, Santa Ana, CA 92701<br>Email:&nbsp;<a href="mailto:kostas.alexandridis@ocpw.ocgov.com">kostas.alexandridis@ocpw.ocgov.com</a>&nbsp;| Phone: (714) 967-0826</p>"""

# Define the tags metadata for the OC Traffic Collision Slider Instant App
md_oc_traffic_collision_slider_app["tags"] = ["Orange County", "California", "OCTraffic", "SWITRS", "Traffic", "Traffic Conditions", "Crashes", "Collisions", "Road Safety", "Accidents", "Transportation", "Web Mapping Application", "Instant App"]

# Define the access information metadata for the OC Traffic Collision Slider Instant App
md_oc_traffic_collision_slider_app["accessInformation"] = "Dr. Kostas Alexandridis, GISP, Data Scientist, OC Public Works, OC Survey Geospatial Services"

# Define the thumbnail url for the OC Traffic Collision Slider Instant App
md_oc_traffic_collision_slider_app["thumbnailurl"] = logos_dict["collisions"]

# Make sure the item was found
if item_oc_traffic_collision_slider_app:
    print(f"Found item: {item_oc_traffic_collision_slider_app.title} (ID: {item_oc_traffic_collision_slider_app.id})")
    # Update the item metadata
    item_oc_traffic_collision_slider_app.update(
        {
            "overwrite": True,
            "snippet": md_oc_traffic_collision_slider_app["snippet"],
            "description": md_oc_traffic_collision_slider_app["description"],
            "licenseInfo": md_oc_traffic_collision_slider_app["licenseInfo"],
            "tags": md_oc_traffic_collision_slider_app["tags"],
            "accessInformation": md_oc_traffic_collision_slider_app["accessInformation"],
            "thumbnailurl": md_oc_traffic_collision_slider_app["thumbnailurl"],
            "categories": "Traffic, Transportation"
        }
    )
    print("Metadata updated successfully.")
else:
    print("Item not found.")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of Script ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\nLast Executed:", dt.datetime.now().strftime("%Y-%m-%d"))
print("\nEnd of Script")\
# Last Executed: 2026-01-04
