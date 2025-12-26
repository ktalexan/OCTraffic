# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Part 5 - Time Series Analysis ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.3, Date: December 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOCTraffic Data Processing - Part 5 - Time Series Analysis\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Preliminaries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1. Preliminaries")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.1. Libraries and Initialization ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.1. Libraries and Initialization")

# Import necessary libraries
import os
import datetime as dt
import json
from dateutil.parser import parse
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.nonparametric.smoothers_lowess import lowess
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle, Patch
import seaborn as sns
from dotenv import load_dotenv

from octraffic import octraffic

# Initialize the OCTraffic object
ocs = octraffic()

# Set default fonts for matplotlib and seaborn
plt.rcParams["font.family"] = "serif"
plt.rcParams["font.serif"] = ["Times New Roman"]

# Load environment variables from .env file
load_dotenv()

os.getcwd()

# Part and Version
part = 5
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

# load the ts_year pickle data file
print("- Loading the ts_year pickle data file")
ts_year = pd.read_pickle(os.path.join(prj_dirs["data_python"], "ts_year.pkl"))

# load the ts_quarter pickle data file
print("- Loading the ts_quarter pickle data file")
ts_quarter = pd.read_pickle(os.path.join(prj_dirs["data_python"], "ts_quarter.pkl"))

# load the ts_month pickle data file
print("- Loading the ts_month pickle data file")
ts_month = pd.read_pickle(os.path.join(prj_dirs["data_python"], "ts_month.pkl"))

# load the ts_week pickle data file
print("- Loading the ts_week pickle data file")
ts_week = pd.read_pickle(os.path.join(prj_dirs["data_python"], "ts_week.pkl"))

# load the ts_day pickle data file
print("- Loading the ts_day pickle data file")
ts_day = pd.read_pickle(os.path.join(prj_dirs["data_python"], "ts_day.pkl"))

# Load the graphics_list pickle data file
print("- Loading the graphics_list pickle data file")
graphics_list = pd.read_pickle(os.path.join(prj_dirs["data_python"], "graphics_list.pkl"))


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
# 2. Seasonal Time Series Analysis ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2. Seasonal Time Series Analysis")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1. Create Time Series Objects ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.1. Create Time Series Objects")

# Number of crashes (crash_tag_sum) per time period
ts_list_crashes = {
    "quarter": ts_quarter["crashes"]["crash_tag_sum"],
    "month": ts_month["crashes"]["crash_tag_sum"],
    "week": ts_week["crashes"]["crash_tag_sum"],
    "day": ts_day["crashes"]["crash_tag_sum"],
}

# Number of victims (victim_count_sum) per time period
ts_list_victims = {
    "quarter": ts_quarter["crashes"]["victim_count_sum"],
    "month": ts_month["crashes"]["victim_count_sum"],
    "week": ts_week["crashes"]["victim_count_sum"],
    "day": ts_day["crashes"]["victim_count_sum"],
}

# Number of fatal accidents (number_killed_sum) per time period
ts_list_fatal = {
    "quarter": ts_quarter["crashes"]["number_killed_sum"],
    "month": ts_month["crashes"]["number_killed_sum"],
    "week": ts_week["crashes"]["number_killed_sum"],
    "day": ts_day["crashes"]["number_killed_sum"],
}

# Fatal or severe accidents (count_fatal_severe_sum) per time period
ts_list_fatal_severe = {
    "quarter": ts_quarter["crashes"]["count_fatal_severe_sum"],
    "month": ts_month["crashes"]["count_fatal_severe_sum"],
    "week": ts_week["crashes"]["count_fatal_severe_sum"],
    "day": ts_day["crashes"]["count_fatal_severe_sum"],
}

# Number of injuries (number_inj_sum) per time period
ts_list_injuries = {
    "quarter": ts_quarter["crashes"]["number_inj_sum"],
    "month": ts_month["crashes"]["number_inj_sum"],
    "week": ts_week["crashes"]["number_inj_sum"],
    "day": ts_day["crashes"]["number_inj_sum"],
}

# Mean collision severity (coll_severity_num_mean) per time period
ts_list_severity = {
    "quarter": ts_quarter["crashes"]["coll_severity_num_mean"],
    "month": ts_month["crashes"]["coll_severity_num_mean"],
    "week": ts_week["crashes"]["coll_severity_num_mean"],
    "day": ts_day["crashes"]["coll_severity_num_mean"],
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2. Decompose Time Series ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.2. Decompose Time Series")


### Number of Crashes ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Number of Crashes")

# Create the STL decomposition for the monthly crashes time series
stl_m_crashes, fig_m_crashes = ocs.create_stl_plot(
    ts_list_crashes["month"], season = "monthly", model = "additive", label = "Number of Crashes", covid = False, robust = True
)

# Save the figure to disk
fig_m_crashes.savefig(os.path.join(prj_dirs["analysis_graphics"], "fig_m_crashes.png"), dpi = 300, bbox_inches = "tight")

# Show the figure
# fig_m_crashes.show()
# plt.close(fig_m_crashes)

# Create the STL decomposition for the weekly crashes time series
stl_w_crashes, fig_w_crashes = ocs.create_stl_plot(
    ts_list_crashes["week"], season = "weekly", model = "additive", label = "Number of Crashes", covid = False, robust = True
)

# Save the figure to disk
fig_w_crashes.savefig(os.path.join(prj_dirs["analysis_graphics"], "fig_w_crashes.png"), dpi = 300, bbox_inches = "tight")

# Show the figure
# fig_w_crashes.show()
# plt.close(fig_w_crashes)


### Number of Victims ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Number of Victims")

# Create the STL decomposition for the monthly victims time series
stl_m_victims, fig_m_victims = ocs.create_stl_plot(
    ts_list_victims["month"], season = "monthly", model = "additive", label = "Number of Victims", covid = False, robust = True
)

# Save the figure to disk
fig_m_victims.savefig(os.path.join(prj_dirs["analysis_graphics"], "fig_m_victims.png"), dpi = 300, bbox_inches = "tight")

# Show the figure
# fig_m_victims.show()
# plt.close(fig_m_victims)

# Create the STL decomposition for the weekly victims time series
stl_w_victims, fig_w_victims = ocs.create_stl_plot(
    ts_list_victims["week"], season = "weekly", model = "additive", label = "Number of Victims", covid = False, robust = True
)

# Save the figure to disk
fig_w_victims.savefig(os.path.join(prj_dirs["analysis_graphics"], "fig_w_victims.png"), dpi = 300, bbox_inches = "tight")

# Show the figure
# fig_w_victims.show()
# plt.close(fig_w_victims)


### Fatal Accidents ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Fatal Accidents")

# Create the STL decomposition for the monthly fatal accidents time series
stl_m_fatal, fig_m_fatal = ocs.create_stl_plot(
    ts_list_fatal["month"], season = "monthly", model = "additive", label = "Fatal Accidents", covid = False, robust = True
)

# Save the figure to disk
fig_m_fatal.savefig(os.path.join(prj_dirs["analysis_graphics"], "fig_m_fatal.png"), dpi = 300, bbox_inches = "tight")

# Show the figure
# fig_m_fatal.show()
# plt.close(fig_m_fatal)

# Create the STL decomposition for the weekly fatal accidents time series
stl_w_fatal, fig_w_fatal = ocs.create_stl_plot(
    ts_list_fatal["week"], season = "weekly", model = "additive", label = "Fatal Accidents", covid = False, robust = True
)

# Save the figure to disk
fig_w_fatal.savefig(os.path.join(prj_dirs["analysis_graphics"], "fig_w_fatal.png"), dpi = 300, bbox_inches = "tight")

# Show the figure
# fig_w_fatal.show()
# plt.close(fig_w_fatal)


### Fatal or Severe Accidents ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Fatal or Severe Accidents")

# Create the STL decomposition for the monthly fatal or severe accidents time series
stl_m_fatal_severe, fig_m_fatal_severe = ocs.create_stl_plot(
    ts_list_fatal_severe["month"],
    season = "monthly",
    model = "additive",
    label = "Fatal or Severe Accidents",
    covid = False,
    robust = True,
)

# Save the figure to disk
fig_m_fatal_severe.savefig(
    os.path.join(prj_dirs["analysis_graphics"], "fig_m_fatal_severe.png"), dpi = 300, bbox_inches = "tight"
)

# Show the figure
# fig_m_fatal_severe.show()
# plt.close(fig_m_fatal_severe)

# Create the STL decomposition for the weekly fatal or severe accidents time series
stl_w_fatal_severe, fig_w_fatal_severe = ocs.create_stl_plot(
    ts_list_fatal_severe["week"],
    season = "weekly",
    model = "additive",
    label = "Fatal or Severe Accidents",
    covid = False,
    robust = True,
)

# Save the figure to disk
fig_w_fatal_severe.savefig(
    os.path.join(prj_dirs["analysis_graphics"], "fig_w_fatal_severe.png"), dpi = 300, bbox_inches = "tight"
)

# Show the figure
# fig_w_fatal_severe.show()
# plt.close(fig_w_fatal_severe)


### Number of Injuries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Number of Injuries")

# Create the STL decomposition for the monthly injuries time series
stl_m_injuries, fig_m_injuries = ocs.create_stl_plot(
    ts_list_injuries["month"], season = "monthly", model = "additive", label = "Number of Injuries", covid = False, robust = True
)

# Save the figure to disk
fig_m_injuries.savefig(os.path.join(prj_dirs["analysis_graphics"], "fig_m_injuries.png"), dpi = 300, bbox_inches = "tight")

# Show the figure
# fig_m_injuries.show()
# plt.close(fig_m_injuries)

# Create the STL decomposition for the weekly injuries time series
stl_w_injuries, fig_w_injuries = ocs.create_stl_plot(
    ts_list_injuries["week"], season = "weekly", model = "additive", label = "Number of Injuries", covid = False, robust = True
)

# Save the figure to disk
fig_w_injuries.savefig(os.path.join(prj_dirs["analysis_graphics"], "fig_w_injuries.png"), dpi = 300, bbox_inches = "tight")

# Show the figure
# fig_w_injuries.show()
# plt.close(fig_w_injuries)


### Mean Collision Severity ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Mean Collision Severity")

# Create the STL decomposition for the monthly mean collision severity time series
stl_m_severity, fig_m_severity = ocs.create_stl_plot(
    ts_list_severity["month"],
    season = "monthly",
    model = "additive",
    label = "Mean Collision Severity",
    covid = False,
    robust = True,
)

# Save the figure to disk
fig_m_severity.savefig(os.path.join(prj_dirs["analysis_graphics"], "fig_m_severity.png"), dpi = 300, bbox_inches = "tight")

# Show the figure
# fig_m_severity.show()
# plt.close(fig_m_severity)

# Create the STL decomposition for the weekly mean collision severity time series
stl_w_severity, fig_w_severity = ocs.create_stl_plot(
    ts_list_severity["week"],
    season = "weekly",
    model = "additive",
    label = "Mean Collision Severity",
    covid = False,
    robust = True,
)

# Save the figure to disk
fig_w_severity.savefig(os.path.join(prj_dirs["analysis_graphics"], "fig_w_severity.png"), dpi = 300, bbox_inches = "tight")

# Show the figure
# fig_w_severity.show()
# plt.close(fig_w_severity)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.3. Figure 4 - monthly Fatalities Time Series ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.3. Figure 4 - Monthly Fatalities Time Series")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Figure 3
graphics_list = ocs.graphics_entry(
    gr_type = 2,
    gr_id = 4,
    gr_attr = {
        "category": "time series",
        "name": "Monthly Fatalities Time Series",
        "description": "Time series plot of the number of fatal accidents in monthly data",
        "caption": (
            f"Display of the monthly time series data for the number of killed victims, along with a local LOESS regression trend fit with its 95% confidence intervals. The data are reported over {len(ts_month['crashes'])} months time period, between {prj_meta['date_start'].strftime('%B %d, %Y')} and {prj_meta['date_end'].strftime('%B %d, %Y')}"
        ),
        "type": "time series",
        "method": "ggplot2",
        "file_format": ".png",
        "file": "Time Series Monthly Fatalities",
        "status": "final",
    },
    gr_list = graphics_list,
    gr_dirs = prj_dirs,
)


### Create the Overlay Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create the Overlay Figure")

# Create the monthly fatalities time series figure
fig4, ax = ocs.create_monthly_fatalities_figure(ts_month)
# Show the figure
fig4.show()
plt.close(fig4)


### Save the Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Figure")

# Save the figure to a file
fig4, ax = ocs.create_monthly_fatalities_figure(ts_month)
fig4.savefig(
    fname = graphics_list["graphics"]["fig4"]["path"],
    transparent = True,
    dpi = graphics_list["graphics"]["fig4"]["resolution"],
    format = "png",
    metadata = None,
    bbox_inches = "tight",
    pad_inches = 0.1,
    facecolor = "auto",
    edgecolor = "auto",
    backend = None,
)
plt.close(fig4)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.4. Weekly Crashes Decomposition Plots ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.4. Weekly Crashes Decomposition Plots")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Figure 5
graphics_list = ocs.graphics_entry(
    gr_type = 2,
    gr_id = 5,
    gr_attr = {
        "category": "stl decomposition",
        "name": "STL decomposition of the number of crashes weekly data",
        "description": "STL decomposition plot of the number of crash incidents in weekly time series data",
        "caption": (
            "STL decomposition of the number of collision incidents in the weekly time series data for Orange County, California"
        ),
        "type": "decomposition",
        "method": "ggplot2",
        "file_format": ".png",
        "file": "STL Plot Weekly Crashes",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)


### Create the STL Decomposition Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create the STL Decomposition Figure")

# Create the STL decomposition for the weekly crashes time series
stl_w_crashes, fig5 = ocs.create_stl_plot(
    ts_list_crashes["week"], season = "weekly", model = "additive", label = "Number of Crashes", covid = True, robust = True
)

# Show the figure
# fig5.show()
# plt.close(fig5)


### Save the Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Figure")

# Save the figure to a file
fig5.savefig(
    fname = graphics_list["graphics"]["fig5"]["path"],
    transparent = True,
    dpi = graphics_list["graphics"]["fig5"]["resolution"],
    format = "png",
    metadata = None,
    bbox_inches = "tight",
    pad_inches = 0.1,
    facecolor = "auto",
    edgecolor = "auto",
    backend = None,
)
plt.close(fig5)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.5. Figure 6 - Weekly Fatal Accidents Decomposition Plots ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.5. Figure 6 - Weekly Fatal Accidents Decomposition Plots")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Figure 6
graphics_list = ocs.graphics_entry(
    gr_type = 2,
    gr_id = 6,
    gr_attr = {
        "category": "stl decomposition",
        "name": "STL decomposition of the number of fatal accidents weekly data",
        "description": "STL decomposition plot of the number of fatal accidents in weekly time series data",
        "caption": (
            "STL decomposition of the number of fatal accidents in the weekly time series data for Orange County, California"
        ),
        "type": "decomposition",
        "method": "ggplot2",
        "file_format": ".png",
        "file": "STL Plot Weekly Fatalities",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)


### Create the STL Decomposition Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create the STL Decomposition Figure")

# Create the STL decomposition for the weekly fatal accidents time series
stl_w_fatal, fig6 = ocs.create_stl_plot(
    ts_list_fatal["week"], season = "weekly", model = "additive", label = "Fatal Accidents", covid = True, robust = True
)

# Show the figure
# fig6.show()
# plt.close(fig6)


### Save the Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Figure")

# Save the figure to a file
fig6.savefig(
    fname = graphics_list["graphics"]["fig6"]["path"],
    transparent = True,
    dpi = graphics_list["graphics"]["fig6"]["resolution"],
    format = "png",
    metadata = None,
    bbox_inches = "tight",
    pad_inches = 0.1,
    facecolor = "auto",
    edgecolor = "auto",
    backend = None,
)
plt.close(fig6)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.6. Figure 7 - Mean Weekly Collision Severity Decomposition Plots ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.6. Figure 7 - Mean Weekly Collision Severity Decomposition Plots")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Figure 7
graphics_list = ocs.graphics_entry(
    gr_type = 2,
    gr_id = 7,
    gr_attr = {
        "category": "stl decomposition",
        "name": "STL decomposition of the mean collision severity weekly data",
        "description": "STL decomposition plot of the mean collision severity in weekly time series data",
        "caption": (
            "STL decomposition of the mean collision severity in the weekly time series data for Orange County, California"
        ),
        "type": "decomposition",
        "method": "ggplot2",
        "file_format": ".png",
        "file": "STL Plot Weekly Severity",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)


### Create the STL Decomposition Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create the STL Decomposition Figure")

# Create the STL decomposition for the weekly mean collision severity time series
stl_w_severity, fig7 = ocs.create_stl_plot(
    ts_list_severity["week"],
    season = "weekly",
    model = "additive",
    label = "Mean Collision Severity",
    covid = True,
    robust = True,
)

# Show the figure
# fig7.show()
# plt.close(fig7)


### Save the Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Figure")

# Save the figure to a file
fig7.savefig(
    fname = graphics_list["graphics"]["fig7"]["path"],
    transparent = True,
    dpi = graphics_list["graphics"]["fig7"]["resolution"],
    format = "png",
    metadata = None,
    bbox_inches = "tight",
    pad_inches = 0.1,
    facecolor = "auto",
    edgecolor = "auto",
    backend = None,
)
plt.close(fig7)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.7. Figure 8 - Number of Victims vs. Mean Severity ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.7. Figure 8 - Number of Victims vs. Mean Severity")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Figure 8
graphics_list = ocs.graphics_entry(
    gr_type = 2,
    gr_id = 8,
    gr_attr = {
        "category": "time series overlap",
        "name": "Overlap plot of the number of victims vs. mean collision severity",
        "description": "Overlap plot of the number of victims vs. the mean severity rank in weekly time series data",
        "caption": (
            f"Overlapping display of the weekly time series data for (a) the number of victims along with a Loess local regression trend line fit, and (b) the mean collision severity ordinal rank along with its Loess local regression trend line fit. Data was reported over {len(ts_month['crashes'])} months of weekly intervals, from {prj_meta['date_start'].strftime('%m/%d/%Y')} to {prj_meta['date_end'].strftime('%m/%d/%Y')}"
        ),
        "type": "time series",
        "method": "ggplot2",
        "file_format": ".png",
        "file": "Overlap Victims vs Severity",
        "status": "final",
    },
    gr_list = graphics_list,
    gr_dirs = prj_dirs,
)


### Create the Overlap Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create the Overlap Figure")

# Define the time series data for Figure 8 (weekly number of victims vs. mean collision severity)
fig8_data = ts_week["crashes"][["date_week", "victim_count_sum", "coll_severity_num_mean"]].copy()

# Calculate z-scores for standardization (equivalent to R's scale function)
fig8_data["z_victims"] = (fig8_data["victim_count_sum"] - fig8_data["victim_count_sum"].mean()) / fig8_data["victim_count_sum"].std()
fig8_data["z_severity"] = (
fig8_data["coll_severity_num_mean"] - fig8_data["coll_severity_num_mean"].mean()
) / fig8_data["coll_severity_num_mean"].std()

# Rename columns for clarity
fig8_data.columns = ["time", "victims", "severity", "z_victims", "z_severity"]

# Note 1: The z-scores are calculated for the raw and trend values of the number of victims and mean collision severity. The reason is for enabling standardized scale comparison between the two variables in the plot.

# Note 2: We will use the z-score transformations both on the scaled (z = (x - mean(x)) / sd(x)) and inverse scaled (x = z * sd(x) + mean(x)) values of the raw and trend data for the number of victims and mean collision severity, in order to adjust the axes scales of the plot.

# Example usage of the function with the data we prepared above
# create_victims_severity_plot(fig8_data, save_path="figure8_victims_vs_severity.png")
fig8, ax1, ax2 = ocs.create_victims_severity_plot(
    fig8_data,
    show_plot = False
)

# Show the figure
#fig8.show()
#plt.close(fig8)


### Save the Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Figure")

# Save the figure to a file
fig8.savefig(
    fname = graphics_list["graphics"]["fig8"]["path"],
    transparent = True,
    dpi = graphics_list["graphics"]["fig8"]["resolution"],
    format = "png",
    metadata = None,
    bbox_inches = "tight",
    pad_inches = 0.1,
    facecolor = "auto",
    edgecolor = "auto",
    backend = None,
)
plt.close(fig8)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.8. Figure 9 - Median Age for Parties and Victims ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.8. Figure 9 - Median Age for Parties and Victims")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Figure 9
graphics_list = ocs.graphics_entry(
    gr_type = 2,
    gr_id = 9,
    gr_attr = {
        "category": "Median Age Pyramid",
        "name": "Median Age Pyramid for Parties and Victims",
        "description": "Pyramid plot for median age for parties and victims in collision incidents",
        "caption": "Visual representation of median age pyramid plot distribution for parties and victims of collision incidents.",
        "type": "time series",
        "method": "ggplot2",
        "file_format": ".png",
        "file": "Median Age Distribution",
        "status": "final",
    },
    gr_list = graphics_list,
    gr_dirs = prj_dirs,
)


### Create the Median Age Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create the Median Age Figure")

# Create the age pyramid plot for parties and victims
fig = ocs.create_age_pyramid_plot(collisions)
# Show the figure
fig.show()
# close the figure to free memory
#plt.close(fig)  # Close the figure to free memory


### Save the Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Figure")

# Save the figure to a file
fig.savefig(
    fname = graphics_list["graphics"]["fig9"]["path"],
    transparent = True,
    dpi = graphics_list["graphics"]["fig9"]["resolution"],
    format = "png",
    metadata = None,
    bbox_inches = "tight",
    pad_inches = 0.1,
    facecolor = "auto",
    edgecolor = "auto",
    backend = None,
)

plt.close(fig)  # Close the figure to free memory


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
print("\nLast Execution:", dt.datetime.now().strftime("%Y-%m-%d"))
print("\nEnd of Script")
# Last Executed: 2025-10-21
