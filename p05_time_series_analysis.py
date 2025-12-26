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


# Function to create the monthly fatalities time series figure
def create_monthly_fatalities_figure(ts_month):
    """
    Creates a time series plot of monthly fatal crashes with LOESS smoothing and CI.
    Args:
        ts_month: DataFrame containing monthly time series data with crashes
    Returns:
        fig, ax: The figure and axes objects for further customization if needed
    Raises:
        ValueError: If the input data is not in the expected format
    Example:
        >>> fig, ax = create_monthly_fatalities_figure(ts_month)
        >>> plt.show()
    """
    # Define the time series data for Figure 4 (monthly number of killed victims)
    fig4_data = ts_month["crashes"][["date_month", "number_killed_sum"]]
    fig4_data.columns = ["time", "fatalities"]

    # Create the time series overlay plot for the monthly number of killed victims
    fig4, ax = plt.subplots(figsize = (12, 8))

    # Set initial y-limits based on data to avoid the Rectangle error
    y_max_value = fig4_data["fatalities"].max()
    ax.set_ylim(0, y_max_value)

    # First, add the Covid-19 restrictions area of interest annotation layer (behind everything else)
    covid_start = pd.to_datetime("2020-03-01")
    covid_end = pd.to_datetime("2022-03-01")
    covid_start_num = float(mdates.date2num(covid_start))
    covid_end_num = float(mdates.date2num(covid_end))
    rect = Rectangle(
        (covid_start_num, 0), covid_end_num - covid_start_num, float(ax.get_ylim()[1]), facecolor = "green", alpha = 0.2
    )
    ax.add_patch(rect)

    # Add the covid-19 reference lines (left and right)
    ax.axvline(x = covid_start_num, linewidth = 0.5, linestyle = "dashed", color = "darkgreen")
    ax.axvline(x = covid_end_num, linewidth = 0.5, linestyle = "dashed", color = "darkgreen")

    # Add the covid-19 reference text annotation
    covid_mid_dt = covid_start + (covid_end - covid_start) / 2
    covid_mid_num = float(mdates.date2num(covid_mid_dt))
    ax.annotate(
        "COVID-19\nRestrictions",
        xy = (covid_mid_num, 2),
        xycoords = "data",
        ha = "center",
        fontsize = 10,
        fontweight = "bold",
        fontstyle = "italic",
        color = "darkgreen",
    )

    # Add the time series line for the number of killed victims
    ax.plot(fig4_data["time"], fig4_data["fatalities"], color = "navy", linewidth = 1.5, alpha = 0.6)

    # Add the smoothed LOESS trend line for the number of killed victims
    # Compute LOWESS
    lowess_result = lowess(fig4_data["fatalities"], mdates.date2num(fig4_data["time"]), frac = 0.2, return_sorted = True)

    # Calculate 95% confidence intervals for LOESS
    lowess_x = lowess_result[:, 0]
    lowess_y = lowess_result[:, 1]
    residuals = []

    # Calculate residuals for each point
    for i, date_num in enumerate(mdates.date2num(fig4_data["time"])):
        # Find closest point in lowess_result
        idx = (np.abs(lowess_x - date_num)).argmin()
        residuals.append(fig4_data["fatalities"].iloc[i] - lowess_y[idx])

    # Calculate standard error and confidence interval
    std_error = np.std(residuals)

    # Calculate the standard error of the mean (SEM)
    n_points = len(residuals)
    sem = std_error / np.sqrt(n_points)
    mean_ci_width = 1.96 * sem  # 1.96 for 95% confidence

    # Create upper and lower bounds for mean CI
    mean_ci_upper = lowess_y + mean_ci_width
    mean_ci_lower = lowess_y - mean_ci_width

    # Plot the 95% CI for the mean as a filled area
    ax.fill_between(
        mdates.num2date(lowess_x), mean_ci_lower, mean_ci_upper, color = "orange", alpha = 0.4, label = "95% CI for Mean"
    )

    # Plot the smoothed trend line
    ax.plot(mdates.num2date(lowess_result[:, 0]), lowess_result[:, 1], color = "darkred", linewidth = 3)

    # Set the graph labels
    ax.set_xlabel("Date", fontsize = 16)
    ax.set_ylabel("Number of Killed Victims", fontsize = 16)

    # Format the date axis
    # ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_major_locator(mdates.YearLocator())

    # General graph theme (approximating HighChart theme from R)
    sns.set_style("whitegrid")
    ax.tick_params(axis = "y", which = "major", labelsize = 14)
    ax.tick_params(axis = "x", which = "major", labelsize = 14)
    ax.spines["bottom"].set_color("black")
    ax.spines["bottom"].set_linewidth(0.5)

    # Show all ticks by 5 on the axis, but keep gridlines only at 5, 15, 25
    ax.xaxis.grid(False)  # No vertical gridlines
    ax.yaxis.grid(False)  # First disable all gridlines

    # Calculate y-axis range and create tick marks every 5 units
    y_max = int(ax.get_ylim()[1])
    all_ticks = np.arange(0, y_max, 5)
    gridline_positions = [5, 15, 25]

    # Set all tick positions with marks every 5 units
    ax.set_yticks(all_ticks)

    # Draw gridlines only at specific positions (5, 15, 25) - thin and dashed
    for pos in gridline_positions:
        ax.axhline(y = pos, color = 'gray', linestyle = '--', linewidth = 0.7, alpha = 0.7, zorder = 0)

    # Set up the legend
    legend_elements = [
        plt.Line2D([0], [0], color = "navy", lw = 1, alpha = 0.6, label = "Number of Killed Victims"),
        plt.Line2D([0], [0], color = "darkred", lw = 2, label = "Fatalities Trend (Lowess)"),
        Patch(facecolor = "orange", alpha = 0.1, label = "95% CI for Mean"),
    ]
    ax.legend(
        handles = legend_elements,
        loc = "upper left",
        bbox_to_anchor = (0.02, 0.98),
        frameon = True,
        facecolor = "whitesmoke",
        edgecolor = "gray",
        fontsize = 12,
    )

    # Display the time series plot for the monthly number of victims killed
    plt.tight_layout()

    # Return the figure and axes for further customization if needed
    return fig4, ax


# fig4, ax = create_monthly_fatalities_figure(ts_month)
# Show the figure
# fig4.show()
# plt.close(fig4)


### Save the Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Figure")

# Save the figure to a file
fig4, ax = create_monthly_fatalities_figure(ts_month)
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

# Create the victims vs severity overlay plot function
def create_victims_severity_plot(data, save_path = None, show_plot = True):
    """Creates a time series overlay plot of victims vs collision severity.

    This function creates a dual-axis plot showing the number of victims and
    mean collision severity over time, including LOESS trend lines and
    COVID-19 period highlighting.

    Args:
        data: A pandas DataFrame containing the time series data with columns:
            time: datetime values for the x-axis
            victims: number of victims values
            severity: collision severity values
            z_victims: standardized victims count
            z_severity: standardized severity values
        save_path: Optional path to save the figure. If None, the figure is not saved.
        show_plot: Boolean indicating whether to display the plot. Default is True.

    Returns:
        tuple: (fig, ax1, ax2) containing the figure and axis objects

    Raises:
        ValueError: If the required columns are not in the data
    """
    # Verify the required columns exist
    required_cols = ["time", "victims", "severity", "z_victims", "z_severity"]
    for col in required_cols:
        if col not in data.columns:
            raise ValueError(f"Required column '{col}' not found in input data")  # Create the time series overlay plot
    fig, ax1 = plt.subplots(figsize = (12, 8))

    # Remove all gridlines for cleaner look
    ax1.grid(False)

    # Convert date columns to matplotlib date format if they aren't already
    if not pd.api.types.is_datetime64_any_dtype(data["time"]):
        data["time"] = pd.to_datetime(data["time"])

    # Add the Covid-19 restrictions area of interest annotation layer
    covid_start = pd.to_datetime("2020-03-01")
    covid_end = pd.to_datetime("2022-03-01")

    # Convert to numerical format that matplotlib can use - ensure float types
    covid_start_num = float(mdates.date2num(covid_start))
    covid_end_num = float(mdates.date2num(covid_end))

    # Create the shaded background for COVID period
    ax1.axvspan(covid_start_num, covid_end_num, alpha = 0.2, color = "green")

    # Add the covid-19 reference lines (left and right)
    ax1.axvline(x = covid_start_num, linewidth = 0.5, linestyle = "dashed", color = "darkgreen")
    ax1.axvline(
        x = covid_end_num, linewidth = 0.5, linestyle = "dashed", color = "darkgreen"
    )  # Add the covid-19 reference text annotation
    covid_mid = covid_start + (covid_end - covid_start) / 2
    covid_mid_num = float(mdates.date2num(covid_mid))

    # Calculate proper position for the text annotation at bottom of plot
    # First, get current data view limits rather than axis limits
    # which might not be set until after plotting
    ymin = min(data["z_victims"].min(), data["z_severity"].min()) - 0.5

    ax1.text(
        x = covid_mid_num,
        y = ymin + 0.25,
        s = "COVID-19\nRestrictions",
        fontweight = "bold",
        fontstyle = "italic",
        color = "darkgreen",
        size = 11,
        horizontalalignment = "center",
        verticalalignment = "bottom",
    )

    # Add the time series line for the number of victims (primary axis)
    ax1.plot(data["time"], data["z_victims"], color = "royalblue", linewidth = 0.85, alpha = 0.4, label = "Number of Victims")

    # Create a second y-axis for severity
    ax2 = ax1.twinx()
    ax2.plot(
        data["time"], data["z_severity"], color = "darkorange", linewidth = 0.85, alpha = 0.4, label = "Mean Severity Rank"
    )  # Add LOESS trend lines (equivalent to R's geom_smooth)
    # For victims trend (using statsmodels lowess)
    # Convert datetime to float for lowess calculation
    time_numeric = mdates.date2num(data["time"].values)  # Calculate LOESS trend for victims
    lowess_victims = sm.nonparametric.lowess(data["z_victims"].values, time_numeric, frac = 0.2)

    # Calculate 95% confidence intervals for the LOESS mean estimates
    # Use standard error of the mean (SEM) rather than standard deviation of residuals
    residuals_victims = data["z_victims"].values - np.interp(time_numeric, lowess_victims[:, 0], lowess_victims[:, 1])
    sem_victims = np.std(residuals_victims) / np.sqrt(len(residuals_victims))
    ci_width_victims = 1.96 * sem_victims  # 95% CI is 1.96 * standard error of mean

    # Create upper and lower bounds for victims confidence interval around the trend line
    upper_victims = lowess_victims[:, 1] + ci_width_victims
    lower_victims = lowess_victims[:, 1] - ci_width_victims

    # Add shaded confidence interval for victims trend
    ax1.fill_between(mdates.num2date(lowess_victims[:, 0]), lower_victims, upper_victims, color = "navy", alpha = 0.2)

    # Plot the victims trend line on top of confidence interval
    ax1.plot(
        mdates.num2date(lowess_victims[:, 0]),
        lowess_victims[:, 1],
        color = "navy",
        linewidth = 2.5,
        label = "Victims Loess\nRegression Trend (95% CI)",
    )

    # For severity trend
    lowess_severity = sm.nonparametric.lowess(
        data["z_severity"].values, time_numeric, frac = 0.2
    )  # Calculate 95% confidence intervals for severity LOESS
    # Use standard error of the mean (SEM) rather than standard deviation of residuals
    residuals_severity = data["z_severity"].values - np.interp(
        time_numeric, lowess_severity[:, 0], lowess_severity[:, 1]
    )
    sem_severity = np.std(residuals_severity) / np.sqrt(len(residuals_severity))
    ci_width_severity = 1.96 * sem_severity  # 95% CI is 1.96 * standard error of mean

    # Create upper and lower bounds for severity confidence interval around the trend line
    upper_severity = lowess_severity[:, 1] + ci_width_severity
    lower_severity = lowess_severity[:, 1] - ci_width_severity

    # Add shaded confidence interval for severity trend
    ax2.fill_between(mdates.num2date(lowess_severity[:, 0]), lower_severity, upper_severity, color = "maroon", alpha = 0.2)

    # Plot the severity trend line on top of confidence interval
    ax2.plot(
        mdates.num2date(lowess_severity[:, 0]),
        lowess_severity[:, 1],
        color = "maroon",
        linewidth = 2.5,
        label = "Severity Loess\nRegression Trend (95% CI)",
    )  # Configure date formatting on x-axis
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_minor_locator(mdates.MonthLocator([1, 4, 7, 10]))  # Show ticks at Jan, Apr, Jul, Oct

    # Style the x-axis ticks
    ax1.tick_params(axis = "x", which = "major", length = 8, width = 1.2, color = "black", bottom = True)
    ax1.tick_params(axis = "x", which = "minor", length = 4, width = 0.8, color = "gray", bottom = True)

    # Add light grid lines for major (year) ticks only on the x-axis
    ax1.grid(axis = "x", which = "major", linestyle = "--", linewidth = 0.5, color = "gray", alpha = 0.7)

    # Define function to convert z-scores back to original scale
    def z_to_original(z, original_mean, original_std):
        return z * original_std + original_mean

    # Set up formatters for the y-axes to convert z-scores back to original values
    victims_mean = data["victims"].mean()
    victims_std = data["victims"].std()
    severity_mean = data["severity"].mean()
    severity_std = data["severity"].std()

    # Function to format y-axis ticks for victims
    def victims_formatter(x, pos):
        return f"{z_to_original(x, victims_mean, victims_std):.0f}"

    # Function to format y-axis ticks for severity
    def severity_formatter(x, pos):
        return f"{z_to_original(x, severity_mean, severity_std):.2f}"

    # Apply formatters to axes
    ax1.yaxis.set_major_formatter(FuncFormatter(victims_formatter))
    ax2.yaxis.set_major_formatter(FuncFormatter(severity_formatter))

    # Set axis labels
    ax1.set_xlabel("Date", fontsize = 15, color = "black")
    ax1.set_ylabel("Number of Victims", fontsize = 15, color = "navy", fontweight = "bold")
    ax2.set_ylabel("Mean Severity Rank", fontsize = 15, color = "maroon", fontweight = "bold")

    # Style the axes and ticks
    ax1.tick_params(axis = "x", colors = "black", labelsize = 13)
    ax1.tick_params(axis = "y", colors = "navy", labelsize = 13)
    ax2.tick_params(axis = "y", colors = "maroon", labelsize = 13)  # Create a combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    lines = lines1 + lines2
    labels = labels1 + labels2  # Add legend with custom styling
    legend = ax1.legend(
        lines,
        labels,
        loc = "upper left",
        frameon = True,
        fontsize = 11,
        title = "Time Series Legend",
        title_fontsize = 12,
        bbox_to_anchor = (0.01, 0.99),
        framealpha = 1,
        edgecolor = "gray",
        facecolor = "whitesmoke",
        ncol = 2,
    )
    legend.get_title().set_fontweight("bold")

    # Set a clean background style similar to theme_hc in R
    ax1.set_facecolor("white")
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.spines["bottom"].set_color("black")
    ax1.spines["left"].set_color("black")
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_color("maroon")
    ax2.spines["left"].set_visible(False)

    # Remove gridlines for the second y-axis (severity axis)
    ax2.grid(False)

    # Adjust the layout
    plt.tight_layout()

    # Save the figure if path is provided
    if save_path is not None:
        plt.savefig(save_path, dpi = 300)

    # Show the plot if requested
    if show_plot:
        plt.show()

    return fig, ax1, ax2


# Example usage of the function with the data we prepared above
# create_victims_severity_plot(fig8_data, save_path="figure8_victims_vs_severity.png")
fig8, ax1, ax2 = create_victims_severity_plot(
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

# Function to create the median age pyramid plot for parties and victims
def create_age_pyramid_plot(collisions):
    """
    Creates an age pyramid plot for parties and victims of collisions.
    Args:
        collisions (pd.DataFrame): DataFrame containing collision data with party_age and victim_age columns
    Returns:
        matplotlib.figure.Figure: The age pyramid plot
    Raises:
        ValueError: If required columns are missing from the dataframe
    Examples:
        >>> fig = create_age_pyramid_plot(collisions_df)
        >>> plt.show()
    """
    # Select the party and victim age from the collision data
    fig9a_data = collisions[["party_age", "victim_age"]]

    # Create a table of the party and victim age
    party_counts = fig9a_data["party_age"].value_counts().reset_index()
    party_counts.columns = ["Age", "Freq"]
    party_counts["Type"] = "Party"

    victim_counts = fig9a_data["victim_age"].value_counts().reset_index()
    victim_counts.columns = ["Age", "Freq"]
    victim_counts["Type"] = "Victim"

    # Combine the data
    fig9a_data = pd.concat([party_counts, victim_counts])

    # Convert the age column to integers
    fig9a_data["Age"] = fig9a_data["Age"].astype(int)

    # Remove all rows that are NA
    fig9a_data = fig9a_data.dropna(subset = ["Age", "Freq"])

    # Remove all rows with age > 100
    fig9a_data = fig9a_data[fig9a_data["Age"] <= 100]

    # Create figure for the plot
    fig, ax = plt.subplots(figsize = (12, 10))

    # Plot party data (negative values)
    party_data = fig9a_data[fig9a_data["Type"] == "Party"]
    victim_data = fig9a_data[fig9a_data["Type"] == "Victim"]

    # Create bar plots
    ax.barh(party_data["Age"], -party_data["Freq"], color = "royalblue", label = "Party Age")
    ax.barh(victim_data["Age"], victim_data["Freq"], color = "darkorange", label = "Victim Age")

    # Format x-axis labels with commas and no negative signs
    def abs_comma(x, pos):
        return '{:,}'.format(int(abs(x)))

    ax.xaxis.set_major_formatter(FuncFormatter(abs_comma))  # Set axis limits
    max_freq = max(fig9a_data["Freq"].max(), fig9a_data["Freq"].max())
    ax.set_xlim(-max_freq, max_freq)
    ax.set_ylim(0, 100)
    ax.set_yticks(range(0, 101, 20))

    # Labels and titles
    ax.set_xlabel("Collisions", fontsize = 14)
    ax.set_ylabel("Age", fontsize = 14)
    ax.set_title("Median Age Pyramid for Parties and Victims of Collisions", fontsize = 14)
    
    # Style adjustments
    ax.tick_params(axis = "both", which = "major", labelsize = 12, colors = "black")
    ax.spines["left"].set_visible(True)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.legend(loc = "upper right", fontsize = 12)

    # Remove vertical gridlines
    ax.grid(axis = "x", visible = False)
    ax.grid(axis = "y", linestyle = "--", alpha = 0.7, linewidth = 0.7, color = "gray")

    return fig


# Create the age pyramid plot for parties and victims
fig = create_age_pyramid_plot(collisions)
# Show the figure
#fig.show()
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
