# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Part 4 - Collision Data Analysis ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.3, Date: December 2025
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print("\nOCTraffic Data Processing - Part 4 - Collision Data Analysis\n")


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
import json, pickle
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

from octraffic import OCTraffic

# Initialize the OCTraffic object
ocs = OCTraffic(part = 4, version = 2025.3)

# Set default fonts for matplotlib and seaborn
plt.rcParams["font.family"] = "serif"
plt.rcParams["font.serif"] = ["Times New Roman"]

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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 1.4. Load Codebook from Disk ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n1.4. Loading Codebook from Disk")

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
# 2. Data Analysis ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2. Data Analysis")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.1. Table 1 - Collision Severity Stats ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.1. Table 1 - Collision Severity Stats")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Table 1
graphics_list = ocs.graphics_entry(
    gr_type = 1,
    gr_id = 1,
    gr_attr = {
        "name": "Collision severity ordinal statistics",
        "description": "Key statistics of the collision severity variable",
        "caption": "Collision severity ordinal classification and OCTraffic dataset counts",
        "method": "gtsummary",
        "file_format": ".tex",
        "file": "Collision Severity Stats",
        "status": "final",
    },
    gr_list = None,
    gr_dirs = prj_dirs,
)


### Construct Summary Tables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Construct Summary Tables")

# Create a summary table of the collision severity variable by parties and victims per crash

# create a data frame for the crashes data by severity
df1 = pd.DataFrame({"severity": crashes["coll_severity"], "crashes": crashes["crash_tag"]})

# Create a summary table of the crashes by severity
# Group by severity and calculate descriptive statistics for crashes
x1 = pd.DataFrame(
    df1.groupby(by = "severity", observed = True)["crashes"]
    .agg(
        count = "count",
        sum = "sum",
        min = "min",
        max = "max",
        range = lambda x: x.max() - x.min(),
        mean = "mean",
        var = "var",
        std = "std",
        median = "median",
        iqr = lambda x: x.quantile(0.75) - x.quantile(0.25),
        sem = "sem",
        ci = lambda x: 1.96 * (x.std() / (len(x) ** 0.5)),
        ci_lower = lambda x: x.mean() - 1.96 * (x.std() / (len(x) ** 0.5)),
        ci_upper = lambda x: x.mean() + 1.96 * (x.std() / (len(x) ** 0.5)),
        skew = "skew",
    )
    .reset_index()
)

# Add an overall row with the same aggregation for all values
overall_stats = {
    "severity": "Overall",
    "count": df1["crashes"].count(),
    "sum": df1["crashes"].sum(),
    "min": df1["crashes"].min(),
    "max": df1["crashes"].max(),
    "range": df1["crashes"].max() - df1["crashes"].min(),
    "mean": df1["crashes"].mean(),
    "var": df1["crashes"].var(),
    "std": df1["crashes"].std(),
    "median": df1["crashes"].median(),
    "iqr": df1["crashes"].quantile(0.75) - df1["crashes"].quantile(0.25),
    "sem": df1["crashes"].std() / (len(df1["crashes"]) ** 0.5),
    "ci": 1.96 * (df1["crashes"].std() / (len(df1["crashes"]) ** 0.5)),
    "ci_lower": df1["crashes"].mean() - 1.96 * (df1["crashes"].std() / (len(df1["crashes"]) ** 0.5)),
    "ci_upper": df1["crashes"].mean() + 1.96 * (df1["crashes"].std() / (len(df1["crashes"]) ** 0.5)),
    "skew": df1["crashes"].skew(),
}
x1 = pd.concat([x1, pd.DataFrame([overall_stats])], ignore_index = True)

# Create a data frame for the parties data by severity
df2 = pd.DataFrame(
    {
        "severity": collisions[collisions["party_tag"] == 1]["coll_severity"],
        "parties": collisions[collisions["party_tag"] == 1]["party_tag"],
        "party_count": collisions[collisions["party_tag"] == 1]["party_count"],
    }
)

# Create a summary table of the parties by severity
# Group by severity and calculate descriptive statistics for party_count
x2 = pd.DataFrame(
    df2.groupby(by = "severity", observed = True)["party_count"]
    .agg(
        count = "count",
        sum = "sum",
        min = "min",
        max = "max",
        range = lambda x: x.max() - x.min(),
        mean = "mean",
        var = "var",
        std = "std",
        median = "median",
        iqr = lambda x: x.quantile(0.75) - x.quantile(0.25),
        sem = "sem",
        ci = lambda x: 1.96 * (x.std() / (len(x) ** 0.5)),
        ci_lower = lambda x: x.mean() - 1.96 * (x.std() / (len(x) ** 0.5)),
        ci_upper = lambda x: x.mean() + 1.96 * (x.std() / (len(x) ** 0.5)),
        skew = "skew",
    )
    .reset_index()
)

# Add an overall row with the same aggregation for all values
overall_stats = {
    "severity": "Overall",
    "count": df2["party_count"].count(),
    "sum": df2["party_count"].sum(),
    "min": df2["party_count"].min(),
    "max": df2["party_count"].max(),
    "range": df2["party_count"].max() - df2["party_count"].min(),
    "mean": df2["party_count"].mean(),
    "var": df2["party_count"].var(),
    "std": df2["party_count"].std(),
    "median": df2["party_count"].median(),
    "iqr": df2["party_count"].quantile(0.75) - df2["party_count"].quantile(0.25),
    "sem": df2["party_count"].std() / (len(df2["party_count"]) ** 0.5),
    "ci": 1.96 * (df2["party_count"].std() / (len(df2["party_count"]) ** 0.5)),
    "ci_lower": df2["party_count"].mean() - 1.96 * (df2["party_count"].std() / (len(df2["party_count"]) ** 0.5)),
    "ci_upper": df2["party_count"].mean() + 1.96 * (df2["party_count"].std() / (len(df2["party_count"]) ** 0.5)),
    "skew": df2["party_count"].skew(),
}
x2 = pd.concat([x2, pd.DataFrame([overall_stats])], ignore_index = True)


# Create a data frame for the victims data by severity
df3 = pd.DataFrame(
    {
        "severity": collisions[collisions["victim_tag"] == 1]["coll_severity"],
        "victims": collisions[collisions["victim_tag"] == 1]["party_tag"],
        "victim_count": collisions[collisions["victim_tag"] == 1]["victim_count"],
    }
)

# Create a summary table of the victims by severity
# Group by severity and calculate descriptive statistics for victim_count
x3 = pd.DataFrame(
    df3.groupby(by = "severity", observed = True)["victim_count"]
    .agg(
        count = "count",
        sum = "sum",
        min = "min",
        max = "max",
        range = lambda x: x.max() - x.min(),
        mean = "mean",
        var = "var",
        std = "std",
        median = "median",
        iqr = lambda x: x.quantile(0.75) - x.quantile(0.25),
        sem = "sem",
        ci = lambda x: 1.96 * (x.std() / (len(x) ** 0.5)),
        ci_lower = lambda x: x.mean() - 1.96 * (x.std() / (len(x) ** 0.5)),
        ci_upper = lambda x: x.mean() + 1.96 * (x.std() / (len(x) ** 0.5)),
        skew = "skew",
    )
    .reset_index()
)

# Add an overall row with the same aggregation for all values
overall_stats = {
    "severity": "Overall",
    "count": df3["victim_count"].count(),
    "sum": df3["victim_count"].sum(),
    "min": df3["victim_count"].min(),
    "max": df3["victim_count"].max(),
    "range": df3["victim_count"].max() - df3["victim_count"].min(),
    "mean": df3["victim_count"].mean(),
    "var": df3["victim_count"].var(),
    "std": df3["victim_count"].std(),
    "median": df3["victim_count"].median(),
    "iqr": df3["victim_count"].quantile(0.75) - df3["victim_count"].quantile(0.25),
    "sem": df3["victim_count"].std() / (len(df3["victim_count"]) ** 0.5),
    "ci": 196 * (df3["victim_count"].std() / (len(df3["victim_count"]) ** 0.5)),
    "ci_lower": df3["victim_count"].mean() - 1.96 * (df3["victim_count"].std() / (len(df3["victim_count"]) ** 0.5)),
    "ci_upper": df3["victim_count"].mean() + 1.96 * (df3["victim_count"].std() / (len(df3["victim_count"]) ** 0.5)),
    "skew": df3["victim_count"].skew(),
}
x3 = pd.concat([x3, pd.DataFrame([overall_stats])], ignore_index = True)

# Remove the overall stats dictionary
del overall_stats

# Add the fatality counts for crashes, parties, and victims to the latex_vars dictionary
latex_vars["crashesFatalities"] = int(x1.loc[x1["severity"] == "Fatal injury", "count"].values[0])
latex_vars["partiesFatalities"] = int(x2.loc[x2["severity"] == "Fatal injury", "count"].values[0])
latex_vars["victimsFatalities"] = int(x3.loc[x3["severity"] == "Fatal injury", "count"].values[0])


### Conduct Non-Parametric Rank Tests ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Conduct Non-Parametric Rank Tests")

# Perform the chi-squared and Kruskal-Wallis tests for the crashes, parties, and victims by severity variable. Specifically, the chi-squared goodness of fit test for the crashes variable counts, the chi-squared test for the parties and victims variable counts (by party_count and victim_count respectively), and the Kruskal-Wallis test for the parties and victims variable counts (by party_count and victim_count respectively).
# Perform the chi-squared test for the crashes, parties and victims variables
crashes_chi2 = ocs.chi2_gof_test(df1, "severity")
parties_chi2 = ocs.chi2_test(df2, "party_count", "severity")
victims_chi2 = ocs.chi2_test(df3, "victim_count", "severity")
# Perform the Kruskal-Wallis test for the parties and victims variable counts
parties_kw = ocs.kruskal_test(df2, "party_count", "severity")
victims_kw = ocs.kruskal_test(df3, "victim_count", "severity")

tbl1_tests = pd.DataFrame(
    {
        "dataset": ["crashes", "parties", "victims", "parties", "victims"],
        "observations": [
            "{:,}".format(crashes_chi2["observations"]),
            "{:,}".format(parties_chi2["observations"]),
            "{:,}".format(victims_chi2["observations"]),
            "{:,}".format(parties_kw["observations"]),
            "{:,}".format(victims_kw["observations"]),
        ],
        "test": [
            crashes_chi2["test"],
            parties_chi2["test"],
            victims_chi2["test"],
            parties_kw["test"],
            victims_kw["test"],
        ],
        "statistic": [
            crashes_chi2["statistic"],
            parties_chi2["statistic"],
            victims_chi2["statistic"],
            parties_kw["statistic"],
            victims_kw["statistic"],
        ],
        "p-value": [
            crashes_chi2["p-value"],
            parties_chi2["p-value"],
            victims_chi2["p-value"],
            parties_kw["p-value"],
            victims_kw["p-value"],
        ],
        "p-value_display": [
            crashes_chi2["p-value_display"],
            parties_chi2["p-value_display"],
            victims_chi2["p-value_display"],
            parties_kw["p-value_display"],
            victims_kw["p-value_display"],
        ],
    }
)
print(tbl1_tests)


### Create Table 1 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create Table 1")

# Create a summary table of the crashes, parties, and victims by severity
tbl1_data = pd.DataFrame(
    {
        "severity": pd.concat([x1["severity"], pd.Series(["p-value"])], ignore_index = True),
        "crashes": pd.concat(
            [x1["count"].apply(lambda x: "{:,}".format(x)), pd.Series([crashes_chi2["p-value_display"]])],
            ignore_index = True,
        ),
        "parties": pd.concat(
            [x2["count"].apply(lambda x: "{:,}".format(x)), pd.Series([crashes_chi2["p-value_display"]])],
            ignore_index = True,
        ),
        "victims": pd.concat(
            [x3["count"].apply(lambda x: "{:,}".format(x)), pd.Series([crashes_chi2["p-value_display"]])],
            ignore_index = True,
        ),
        "party_mean": pd.concat(
            [x2["mean"].apply(lambda x: "{:,.3f}".format(x)), pd.Series([parties_kw["p-value_display"]])],
            ignore_index = True,
        ),
        "party_std": pd.concat([x2["std"].apply(lambda x: "{:,.3f}".format(x)), pd.Series([""])], ignore_index = True),
        "party_range": pd.concat(
            [x2.apply(lambda row: f"{row['min']}-{row['max']}", axis = 1), pd.Series([""])], ignore_index = True
        ),
        "victim_mean": pd.concat(
            [x3["mean"].apply(lambda x: "{:,.3f}".format(x)), pd.Series([victims_kw["p-value_display"]])],
            ignore_index = True,
        ),
        "victim_std": pd.concat([x3["std"].apply(lambda x: "{:,.3f}".format(x)), pd.Series([""])], ignore_index = True),
        "victim_range": pd.concat(
            [x3.apply(lambda row: f"{row['min']}-{row['max']}", axis = 1), pd.Series([""])], ignore_index = True
        ),
    }
)

# Define the multi-index columns for the table
multi_columns = [
    ("", "Severity Level"),
    ("", "Crashes"),
    ("", "Parties"),
    ("", "Victims"),
    ("Party Count", "mean"),
    ("Party Count", "std"),
    ("Party Count", "range"),
    ("Victim Count", "mean"),
    ("Victim Count", "std"),
    ("Victim Count", "range"),
]
# Assign the multi-index columns to the data frame
tbl1_data.columns = pd.MultiIndex.from_tuples(multi_columns)

# get the last row of tbl1_data
last_row = tbl1_data.iloc[-1].copy()
# Change the last row values
last_row.iloc[1] = f"{last_row.iloc[1]}\\footnotemark[2]"
last_row.iloc[2] = f"{last_row.iloc[2]}\\footnotemark[2]"
last_row.iloc[3] = f"{last_row.iloc[3]}\\footnotemark[2]"
last_row.iloc[4] = f"{last_row.iloc[4]}\\footnotemark[3]"
last_row.iloc[7] = f"{last_row.iloc[7]}\\footnotemark[3]"

# Create a modified table with the last row changed
tbl1_mod = tbl1_data.copy()
tbl1_mod.iloc[-1] = last_row

# Create the LaTeX table from the data frame
tbl1_latex = tbl1_mod.to_latex(
    buf = None,
    columns = None,
    header = True,
    index = False,
    na_rep = "NaN",
    formatters = None,
    float_format = None,
    sparsify = None,
    index_names = True,
    bold_rows = False,
    column_format = "lrrrrrcrrc",
    longtable = False,
    escape = False,
    encoding = None,
    decimal = ".",
    multicolumn = True,
    multicolumn_format = "c",
    multirow = True,
    caption = graphics_list["tables"]["tbl1"]["caption"],
    label = graphics_list["tables"]["tbl1"]["id"].lower(),
    position = None,
)

# Add option [h!] to the table
tbl1_latex = tbl1_latex.replace("\\begin{table}", "\\begin{table}[h!]")
# Escape special characters
tbl1_latex = tbl1_latex.replace("<", "\\textless{}")
# Make subheading text italic
tbl1_latex = tbl1_latex.replace("mean", "\\textit{mean}")
tbl1_latex = tbl1_latex.replace("std", "\\textit{std}")
tbl1_latex = tbl1_latex.replace("range", "\\textit{range}")
tbl1_latex = tbl1_latex.replace(f"Party Count", "Party Count\\footnotemark[1]")
tbl1_latex = tbl1_latex.replace(f"Victim Count", "Victim Count\\footnotemark[1]")

# Split the LaTeX table into lines for easier manipulation
tbl1_latex_lines = tbl1_latex.splitlines()

# Find the line to insert the midrule
midrule_idx = None
for i, line in enumerate(tbl1_latex_lines):
    # If line begins with "9":
    if line.startswith("Overall"):
        midrule_idx = i
        break
# Add the midrule
if midrule_idx is not None:
    tbl1_latex_lines.insert(midrule_idx, r"\midrule")
    del midrule_idx

# Join the lines back together
tbl1_latex = "\n".join(tbl1_latex_lines)
# Delete the temporary latex lines
del tbl1_latex_lines

# Add footnotes after the table
footnotes = (
    "\\footnotetext[1]{Per each collision: \\textit{mean, sd, range(min, max)}}\n"
    "\\footnotetext[2]{Pearson's Chi-Squared test}\n"
    "\\footnotetext[3]{Kruskal-Wallis rank sum test}"
)
if "\\end{tabular}" in tbl1_latex:
    tbl1_latex = tbl1_latex.replace("\\end{tabular}", "\\end{tabular}\n" + footnotes)
elif "\\end{longtable}" in tbl1_latex:
    tbl1_latex = tbl1_latex.replace("\\end{longtable}", "\\end{longtable}\n" + footnotes)
else:
    tbl1_latex += "\n" + footnotes

print(tbl1_latex)

# Define the path to save the LaTeX table
tbl1_path = graphics_list["tables"]["tbl1"]["path"]
# Save the LaTeX table to a file
with open(tbl1_path, "w", encoding="utf-8") as f:
    f.write(tbl1_latex)
    print(f"\n- Table 1 saved to {tbl1_path}")


### Save the Graphics Dataset ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Graphics Dataset")

# Compile the graphics dataset
tbl1_dataset = {"raw": {"x1": x1, "x2": x2, "x3": x3}, "data": tbl1_data, "tests": tbl1_tests, "latex": tbl1_latex}

# Save the graphics dataset to disk
tbl1_dataset_path = os.path.join(prj_dirs["data_python"], graphics_list["tables"]["tbl1"]["file"] + ".pkl")
with open(tbl1_dataset_path, "wb") as f:
    pickle.dump(tbl1_dataset, f)
    print(f"\n- Graphics dataset saved to {tbl1_dataset_path}")

# Save the updated latex_vars dictionary to the json file on disk replacing the old one
latex_vars_path = os.path.join(prj_dirs["metadata"], "latex_vars.json")
with open(latex_vars_path, "w", encoding = "utf-8") as json_file:
    json.dump(latex_vars, json_file, indent=4)
    print(f"\n- Updated LaTeX variables saved to {latex_vars_path}")

# Save the LaTeX variables dictionary to a .tex file
latex_vars_tex_path = os.path.join(prj_dirs["graphics"], "latex_vars.tex")
with open(latex_vars_tex_path, "w") as f:
    for key, value in latex_vars.items():
        f.write(f"\\newcommand{{\\{key}}}{{{value}}}\n")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.2. Table 2 - Ranked Collision Severity Stats ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.2. Table 2 - Ranked Collision Severity Stats")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Table 2
graphics_list = ocs.graphics_entry(
    gr_type=1,
    gr_id=2,
    gr_attr={
        "name": "Collision severity rank classification statistics",
        "description": "Key classification and statistics of the collision severity rank variable",
        "caption": "Ranked collision severity ordinal classification, related parameters, and OCTraffic dataset counts",
        "method": "gtsummary",
        "file_format": ".tex",
        "file": "Collision Severity Rank Stats",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)


### Construct Summary Tables ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Construct Summary Tables")

# Select the variables for the table
tbl2_raw = collisions[["crash_tag", "party_tag", "victim_tag", "coll_severity_rank"]].copy()
tbl2_raw.columns = ["Crashes", "Parties", "Victims", "Severity"]

tbl2_data = pd.DataFrame()
for i, level in enumerate(tbl2_raw["Severity"].cat.categories):
    crashes_sum = round(tbl2_raw[tbl2_raw["Severity"] == level]["Crashes"].sum(), 0)
    parties_sum = round(tbl2_raw[tbl2_raw["Severity"] == level]["Parties"].sum(), 0)
    victims_sum = round(tbl2_raw[tbl2_raw["Severity"] == level]["Victims"].sum(), 0)
    row = {
        "Rank": int(i),
        "Level": str(level),
        "Crashes": f"{crashes_sum:,}",
        "Parties": f"{parties_sum:,}",
        "Victims": f"{victims_sum:,}",
    }
    tbl2_data = pd.concat([tbl2_data, pd.DataFrame([row])], ignore_index = True)

# Create a temp data frame for the total counts of crashes, parties, and victims
total_raw = {
    "Rank": 9,
    "Level": "Total",
    "Crashes": f"{round(tbl2_raw['Crashes'].sum(), 0):,}",
    "Parties": f"{round(tbl2_raw['Parties'].sum(), 0):,}",
    "Victims": f"{round(tbl2_raw['Victims'].sum(), 0):,}",
}

tbl2_data = pd.concat([tbl2_data, pd.DataFrame([total_raw])], ignore_index = True)

# Add new columns to the data frame
tbl2_data["Fatalities"] = None
tbl2_data["Injuries"] = None
tbl2_data["Type"] = None

# Loop through each row in the tbl2_data data frame
for i in range(len(tbl2_data)):
    # Get the rank for the current row
    rank = tbl2_data.loc[i, "Rank"]
    match rank:
        case 0:
            tbl2_data.loc[i, "Fatalities"] = "None"
            tbl2_data.loc[i, "Injuries"] = "None"
            tbl2_data.loc[i, "Type"] = "Minor"
        case 1:
            tbl2_data.loc[i, "Fatalities"] = "None"
            tbl2_data.loc[i, "Injuries"] = "Single"
            tbl2_data.loc[i, "Type"] = "Severe"
        case 2:
            tbl2_data.loc[i, "Fatalities"] = "None"
            tbl2_data.loc[i, "Injuries"] = "Multiple"
            tbl2_data.loc[i, "Type"] = "Severe"
        case 3:
            tbl2_data.loc[i, "Fatalities"] = "Single"
            tbl2_data.loc[i, "Injuries"] = "None"
            tbl2_data.loc[i, "Type"] = "Fatal"
        case 4:
            tbl2_data.loc[i, "Fatalities"] = "Single"
            tbl2_data.loc[i, "Injuries"] = "Single"
            tbl2_data.loc[i, "Type"] = "Fatal"
        case 5:
            tbl2_data.loc[i, "Fatalities"] = "Single"
            tbl2_data.loc[i, "Injuries"] = "Multiple"
            tbl2_data.loc[i, "Type"] = "Fatal"
        case 6:
            tbl2_data.loc[i, "Fatalities"] = "Multiple"
            tbl2_data.loc[i, "Injuries"] = "None"
            tbl2_data.loc[i, "Type"] = "Fatal"
        case 7:
            tbl2_data.loc[i, "Fatalities"] = "Multiple"
            tbl2_data.loc[i, "Injuries"] = "Single"
            tbl2_data.loc[i, "Type"] = "Fatal"
        case 8:
            tbl2_data.loc[i, "Fatalities"] = "Multiple"
            tbl2_data.loc[i, "Injuries"] = "Multiple"
            tbl2_data.loc[i, "Type"] = "Fatal"
        case 9:
            tbl2_data.loc[i, "Fatalities"] = ""
            tbl2_data.loc[i, "Injuries"] = ""
            tbl2_data.loc[i, "Type"] = ""

# Relocate the Fatalities, Injuries, and Type columns after the Level column
ocs.relocate_column(df = tbl2_data, col_name = ["Fatalities", "Injuries", "Type"], ref_col_name = "Level", position = "after")

print(tbl2_data)


### Conduct Non-Parametric Rank Tests ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Conduct Non-Parametric Rank Tests")

# Perform the chi-square tests for the crashes, parties, and victims by severity
crashes_chi2 = ocs.chi2_gof_test(tbl2_raw[tbl2_raw["Crashes"] == 1], "Severity")
parties_chi2 = ocs.chi2_gof_test(tbl2_raw[tbl2_raw["Parties"] == 1], "Severity")
victims_chi2 = ocs.chi2_gof_test(tbl2_raw[tbl2_raw["Victims"] == 1], "Severity")

# Create a DataFrame for the chi square test results
tbl2_tests = pd.DataFrame(
    {
        "dataset": ["crashes", "parties", "victims"],
        "observations": [
            "{:,}".format(crashes_chi2["observations"]),
            "{:,}".format(parties_chi2["observations"]),
            "{:,}".format(victims_chi2["observations"]),
        ],
        "test": [crashes_chi2["test"], parties_chi2["test"], victims_chi2["test"]],
        "statistic": [crashes_chi2["statistic"], parties_chi2["statistic"], victims_chi2["statistic"]],
        "p-value": [crashes_chi2["p-value"], parties_chi2["p-value"], victims_chi2["p-value"]],
        "p-value_display": [
            crashes_chi2["p-value_display"],
            parties_chi2["p-value_display"],
            victims_chi2["p-value_display"],
        ],
    }
)

print(tbl2_tests)


### Create Table 2 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create Table 2")

# last row of tbl2_data containing the p-values
last_row = [
    9,
    "p-value\\footnotemark[1]",
    "",
    "",
    "",
    tbl2_tests.loc[0]["p-value_display"].replace("<", "\\textless{}"),
    tbl2_tests.loc[1]["p-value_display"].replace("<", "\\textless{}"),
    tbl2_tests.loc[2]["p-value_display"].replace("<", "\\textless{}"),
]

# Create a copy of tbl2_data to modify
tbl2_mod = tbl2_data.copy()

# Append the last row to tbl2_mod
tbl2_mod.loc[len(tbl2_mod)] = last_row

# In the rank column replace the value 10 with empty string
tbl2_mod["Rank"] = tbl2_mod["Rank"].replace(9, "")

# Create the LaTeX table from the data frame
tbl2_latex = tbl2_mod.to_latex(
    buf = None,
    columns = None,
    header = True,
    index = False,
    na_rep = "NaN",
    formatters = None,
    float_format = None,
    sparsify = None,
    index_names = True,
    bold_rows = False,
    column_format = "crrrrrrr",
    longtable = False,
    escape = False,
    encoding = None,
    decimal = ".",
    multicolumn = True,
    multicolumn_format = "c",
    multirow = True,
    caption = graphics_list["tables"]["tbl2"]["caption"],
    label = graphics_list["tables"]["tbl2"]["id"].lower(),
    position = None,
)

# Add option [h!] to the table
tbl2_latex = tbl2_latex.replace("\\begin{table}", "\\begin{table}[h!]")

# Split the LaTeX table into lines for easier manipulation
tbl2_latex_lines = tbl2_latex.splitlines()

# Merge columns 1 and 2 under the heading 'Rank and Level'
# Find the header row (usually after \toprule)
header_idx = None
midrule_idx = None
for i, line in enumerate(tbl2_latex_lines):
    if "\\toprule" in line:
        header_idx = i + 1
        break
# Extract the header line and break it into parts
if header_idx is not None:
    header_line = tbl2_latex_lines[header_idx]
    header_parts = header_line.split(" & ")
    # Replace columns 1 and 2 with a merged heading
    header_parts[0] = "\\multicolumn{2}{c}{Rank and Level}"
    del header_parts[1]  # Remove the next column header
    # Reconstruct the header line
    tbl2_latex_lines[header_idx] = " & ".join(header_parts)

# Find the line to insert the midrule
for i, line in enumerate(tbl2_latex_lines):
    # If line begins with "8":
    if line.startswith("8"):
        midrule_idx = i + 1
        break
# Add the midrule
if midrule_idx is not None:
    tbl2_latex_lines.insert(midrule_idx, r"\midrule")
    # Delete the midrule index variable
del midrule_idx

# Join the lines back together
tbl2_latex = "\n".join(tbl2_latex_lines)
# Delete the temporary latex lines
del tbl2_latex_lines

# Add footnotes after the table
footnotes = "\\footnotetext[1]{Pearson's Chi-Squared test}"
if "\\end{tabular}" in tbl2_latex:
    tbl2_latex = tbl2_latex.replace("\\end{tabular}", "\\end{tabular}\n" + footnotes)
elif "\\end{longtable}" in tbl2_latex:
    tbl2_latex = tbl2_latex.replace("\\end{longtable}", "\\end{longtable}\n" + footnotes)
else:
    tbl2_latex += "\n" + footnotes

print(tbl2_latex)

# Define the path to save the LaTeX table
tbl2_path = graphics_list["tables"]["tbl2"]["path"]
# Save the LaTeX table to a file
with open(tbl2_path, "w", encoding="utf-8") as f:
    f.write(tbl2_latex)
    print(f"\n- Table 2 saved to {tbl2_path}")


### Save the Graphics Dataset ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Graphics Dataset")


# Compile the graphics dataset
tbl2_dataset = {"raw": tbl2_raw, "data": tbl2_data, "tests": tbl2_tests, "latex": tbl2_latex}

# Save the graphics dataset to disk
tbl2_dataset_path = os.path.join(prj_dirs["data_python"], graphics_list["tables"]["tbl2"]["file"] + ".pkl")
with open(tbl2_dataset_path, "wb") as f:
    pickle.dump(tbl2_dataset, f)
    print(f"\n- Graphics dataset saved to {tbl2_dataset_path}")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.3. Figure 1 - Histogram - Victim Count ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.3. Figure 1 - Histogram - Victim Count")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Figure 1
graphics_list = ocs.graphics_entry(
    gr_type=2,
    gr_id=1,
    gr_attr={
        "category": "histogram",
        "name": "Histogram of victim count in crashes",
        "description": "Histogram plot of the number of victims in crash incidents",
        "caption": "Top-10 victim frequency counts of the number of victims in collision accidents",
        "type": "frequency",
        "method": "ggplot2",
        "file_format": ".png",
        "file": "Histogram Victim Count",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)


### Create the Histogram Plot ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create the Histogram Plot")

# Run the function and get the figure and axes objects
fig1, ax1 = plt.subplots(figsize=(12, 8))
fig1, ax1 = ocs.plot_victim_count_histogram(crashes, fig=fig1, ax=ax1)
fig1.show()
plt.close(fig1)


### Save the Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Figure")

# Save the figure to a file
fig1, ax1 = plt.subplots(figsize=(12, 8))
fig1, ax1 = ocs.plot_victim_count_histogram(crashes, fig=fig1, ax=ax1)
fig1.savefig(
    fname=graphics_list["graphics"]["fig1"]["path"],
    transparent=True,
    dpi=graphics_list["graphics"]["fig1"]["resolution"],
    format="png",
    metadata=None,
    bbox_inches="tight",
    pad_inches=0.1,
    facecolor="auto",
    edgecolor="auto",
    backend=None,
)
plt.close(fig1)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.4. Figure 2 - Bar Chart-Type of Collision ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.4. Figure 2 - Bar Chart-Type of Collision")

### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Figure 2
graphics_list = ocs.graphics_entry(
    gr_type=2,
    gr_id=2,
    gr_attr={
        "category": "bar",
        "name": "Bar graph of collision types",
        "description": "Bar graph of the number of collisions by type of collision",
        "caption": "Number of collisions by collision type",
        "type": "distribution",
        "method": "ggplot2",
        "file_format": ".png",
        "file": "Bar Type of Collision",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)


### Create the Bar Chart Plot ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create the Bar Chart Plot")

# Run the function to create the bar chart
fig2, ax2 = plt.subplots(figsize=(12, 8))
fig2, ax2 = ocs.plot_collision_type_bar(crashes, fig=fig2, ax=ax2)
fig2.show()
plt.close(fig2)


### Save the Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Figure")

# Save the figure to a file
fig2, ax2 = plt.subplots(figsize=(12, 8))
fig2, ax2 = ocs.plot_collision_type_bar(crashes, fig=fig2, ax=ax2)
fig2.savefig(
    fname=graphics_list["graphics"]["fig2"]["path"],
    transparent=True,
    dpi=graphics_list["graphics"]["fig2"]["resolution"],
    format="png",
    metadata=None,
    bbox_inches="tight",
    pad_inches=0.1,
    facecolor="auto",
    edgecolor="auto",
    backend=None,
)
plt.close(fig2)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.5. Figure 3 - Bar Chart-Fatal Accidents ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.5. Figure 3 - Bar Chart-Fatal Accidents")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Figure 3
graphics_list = ocs.graphics_entry(
    gr_type=2,
    gr_id=3,
    gr_attr={
        "category": "bar",
        "name": "Cumulative Bars of Number of Fatalities by Type",
        "description": "Cumulative Bar graph of the number of fatal collisions by type",
        "caption": "Number of fatal collisions by type of fatality",
        "type": "distribution",
        "method": "ggplot2",
        "file_format": ".png",
        "file": "Cumulative Number of Fatalities",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)


### Create the Cumulative Bar Chart Plot ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create the Cumulative Bar Chart Plot")

# Call the function to plot
fig3, ax3 = plt.subplots(figsize=(12, 8))
fig3, ax3 = ocs.plot_fatalities_by_type_and_year(ts_year["crashes"], fig=fig3, ax=ax3)
fig3.show()
plt.close(fig3)


### Save the Figure ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Figure")

# Save the figure to a file
fig3, ax3 = plt.subplots(figsize=(12, 8))
fig3, ax3 = ocs.plot_fatalities_by_type_and_year(ts_year["crashes"], fig=fig3, ax=ax3)
fig3.savefig(
    fname=graphics_list["graphics"]["fig3"]["path"],
    transparent=True,
    dpi=graphics_list["graphics"]["fig3"]["resolution"],
    format="png",
    metadata=None,
    bbox_inches="tight",
    pad_inches=0.1,
    facecolor="auto",
    edgecolor="auto",
    backend=None,
)
plt.close(fig3)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.6. Table 3 - Summary Monthly Collision Stats ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.6. Table 4 - Summary Monthly Collision Stats")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Table 3
graphics_list = ocs.graphics_entry(
    gr_type=1,
    gr_id=3,
    gr_attr={
        "name": "Monthly Accident Summary Statistics",
        "description": "Key accident statistics for the summary variables in the OCTraffic datasets",
        "caption": (
            f"Summary total values for monthly time series of traffic accidents in Orange County ({prj_meta['date_start'].year}-{prj_meta['date_end'].year})"
        ),
        "method": "stat.desc",
        "file_format": ".tex",
        "file": "Monthly Summary Stats",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)


### Construct Summary Table ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Construct Summary Table")

# Applying the function to the collisions ts_month DataFrame (will be used in the next section)
stats_month = ocs.compute_monthly_stats(ts_month["collisions"])

# Dictionary for column labels and orders
tbl3_labels = {
    "crash_tag": {"label": "Crashes", "var_order": 1},
    "party_tag": {"label": "Parties", "var_order": 2},
    "victim_tag": {"label": "Victims", "var_order": 3},
    "ind_fatal": {"label": "Fatal Injuries", "var_order": 4},
    "ind_severe": {"label": "Severe Injuries", "var_order": 5},
    "ind_multi": {"label": "Multiple Victim Injuries", "var_order": 6},
    "ped_accident": {"label": "Pedestrian Accidents", "var_order": 7},
    "bic_accident": {"label": "Bicycle Accidents", "var_order": 8},
    "mc_accident": {"label": "Motorcycle Accidents", "var_order": 9},
    "truck_accident": {"label": "Truck Accidents", "var_order": 10},
    "number_killed": {"label": "Killed Victims", "var_order": 11},
    "number_inj": {"label": "Injured Victims", "var_order": 12},
    "count_fatal_severe": {"label": "Fatal or Severe Injuries", "var_order": 13},
    "count_minor_pain": {"label": "Minor or Pain Injuries", "var_order": 14},
    "count_severe_inj": {"label": "Severe Injuries", "var_order": 15},
    "count_visible_inj": {"label": "Visible Injuries", "var_order": 16},
    "count_complaint_pain": {"label": "Complaint of Pain Injuries", "var_order": 17},
    "count_car_killed": {"label": "Killed Car Victims", "var_order": 18},
    "count_ped_killed": {"label": "Killed Pedestrians", "var_order": 19},
    "count_bic_killed": {"label": "Killed Bicycles", "var_order": 20},
    "count_mc_killed": {"label": "Killed Motorcyclists", "var_order": 21},
    "count_car_inj": {"label": "Injured Car Victims", "var_order": 22},
    "count_ped_inj": {"label": "Injured Pedestrians", "var_order": 23},
    "count_bic_inj": {"label": "Injured Bicycles", "var_order": 24},
    "count_mc_inj": {"label": "Injured Motorcyclists", "var_order": 25},
    "at_fault": {"label": "At Fault", "var_order": 26},
    "hit_and_run_bin": {"label": "Hit and Run", "var_order": 27},
    "alcohol_involved": {"label": "Alcohol Involved", "var_order": 28},
    "dui_alcohol_ind": {"label": "DUI Alcohol", "var_order": 29},
    "dui_drug_ind": {"label": "DUI Drugs", "var_order": 30},
    "rush_hours_bin": {"label": "Rush Hours", "var_order": 31},
    "intersection": {"label": "Intersection", "var_order": 32},
    "state_hwy_ind": {"label": "State Highway", "var_order": 33},
    "tow_away": {"label": "Tow Away", "var_order": 34},
}

# Filter rows where var.name ends with "sum"
tbl3_data = stats_month[stats_month["var.name"].str.endswith("sum")].copy()
# Remove the postfix "_sum" from var.name
tbl3_data.loc[:, "var.name"] = tbl3_data["var.name"].str.replace("_sum$", "", regex=True)
# Only keep rows where var.name is in tbl3_labels
tbl3_data = tbl3_data[tbl3_data["var.name"].isin(tbl3_labels.keys())].copy()

# Add label and order columns based on tbl3_labels
tbl3_data.loc[:, "label"] = tbl3_data["var.name"].map(lambda x: tbl3_labels[x]["label"])
tbl3_data.loc[:, "order"] = tbl3_data["var.name"].map(lambda x: tbl3_labels[x]["var_order"])

# Relocate the label and order columns
ocs.relocate_column(df=tbl3_data, col_name="order", ref_col_name="stat.type", position="after")
ocs.relocate_column(df=tbl3_data, col_name="label", ref_col_name="order", position="after")

# Remove the "var.name" column
tbl3_data = tbl3_data.drop(columns=["var.name"])

# Reorder the columns by "order"
tbl3_data = tbl3_data.sort_values("order")

# Keep only the specified columns and reorder
cols = ["stat.type", "order", "label", "sum", "min", "max", "mean", "std.dev", "SE.mean", "CI.mean.0.95"]
tbl3_data = tbl3_data[[col for col in cols if col in tbl3_data.columns]]

# Round integer columns (no decimal places)
for col in ["sum", "min", "max"]:
    if col in tbl3_data.columns:
        tbl3_data.loc[:, col] = tbl3_data[col].astype("Int64").round(0)
# Round float columns (2 decimal places)
for col in ["mean", "std.dev", "SE.mean", "CI.mean.0.95"]:
    if col in tbl3_data.columns:
        tbl3_data.loc[:, col] = tbl3_data[col].astype(float).round(3)

# Sort by "order" and filter rows with order < 100
tbl3_data = tbl3_data.sort_values("order")
tbl3_data = tbl3_data[tbl3_data["order"] < 100]

# Remove "stat.type" and "order" columns
tbl3_data = tbl3_data.drop(columns=["stat.type", "order"])

# Rename columns
col_rename = {
    "label": "Factor (Count or Bin)",
    "sum": "Total",
    "min": "Min",
    "max": "Max",
    "mean": "Mean",
    "std.dev": "SD",
    "SE.mean": "SE",
    "CI.mean.0.95": "CI",
}
tbl3_data = tbl3_data.rename(columns=col_rename)

# Ensuring that Total, Min, and Max are always integers
for col in ["Total", "Min", "Max"]:
    if col in tbl3_data.columns:
        tbl3_data[col] = tbl3_data[col].astype("Int64")

# Ensure that Mean, SD, SE, and CI are floats with 3 decimal places
for col in ["Mean", "SD", "SE", "CI"]:
    if col in tbl3_data.columns:
        tbl3_data[col] = tbl3_data[col].astype(float).round(3)

# Format all numeric columns with a thousands comma separator if they have at least 4 digits
for col in tbl3_data.columns:
    if pd.api.types.is_numeric_dtype(tbl3_data[col]):
        tbl3_data[col] = tbl3_data[col].apply(lambda x: f"{x:,}" if pd.notnull(x) and abs(x) >= 1000 else x)

print(tbl3_data)


### Create Table 3 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create Table 3")

tbl3_mod = tbl3_data.copy()
# Convert all columns to string type
tbl3_mod = tbl3_mod.astype(str)

# Create the LaTeX table from the data frame
tbl3_latex = tbl3_mod.to_latex(
    buf=None,
    columns=None,
    header=True,
    index=False,
    na_rep="NaN",
    formatters=None,
    float_format=None,
    sparsify=None,
    index_names=True,
    bold_rows=False,
    column_format="lrrrrrrr",
    longtable=False,
    escape=False,
    encoding=None,
    decimal=".",
    multicolumn=True,
    multicolumn_format="c",
    multirow=True,
    caption=graphics_list["tables"]["tbl3"]["caption"],
    label=graphics_list["tables"]["tbl3"]["id"].lower(),
    position=None,
)

# Add option [h!] to the table
tbl3_latex = tbl3_latex.replace("\\begin{table}", "\\begin{table}[h!]")

# Split the LaTeX table into lines for easier manipulation
tbl3_latex_lines = tbl3_latex.splitlines()

# Replace line 5 with a custom header
header_line = r"Factor (Count or Bin) & Total\footnotemark[2] & Min\footnotemark[1] & Max\footnotemark[1] & Mean\footnotemark[1] & SD\footnotemark[1] & SE\footnotemark[1] & CI\footnotemark[1] \\"
tbl3_latex_lines[5] = header_line

midrule_idx = [13, 18, 26, 35, 41]
for idx in midrule_idx:
    # Insert a midrule after each specified index
    tbl3_latex_lines.insert(idx, r"\midrule")

# Join the lines back together
tbl3_latex = "\n".join(tbl3_latex_lines)
# Delete the temporary latex lines
del tbl3_latex_lines


# Add footnotes after the table
footnotes = (
    "\\footnotetext[1]{"
    + f"Monthly generated cumulative summary time series from {prj_meta['date_start'].strftime('%m/%d/%Y')} through {prj_meta['date_end'].strftime('%m/%d/%Y')} (n = {len(ts_month['collisions'])})"
    + "}\n"
    "\\footnotetext[2]{"
    + f"Over the entire time period ({prj_meta['date_start'].year} - {prj_meta['date_end'].year})"
    + "}"
)
if "\\end{tabular}" in tbl3_latex:
    tbl3_latex = tbl3_latex.replace("\\end{tabular}", "\\end{tabular}\n" + footnotes)
elif "\\end{longtable}" in tbl3_latex:
    tbl3_latex = tbl3_latex.replace("\\end{longtable}", "\\end{longtable}\n" + footnotes)
else:
    tbl3_latex += "\n" + footnotes

print(tbl3_latex)

# Define the path to save the LaTeX table
tbl3_path = graphics_list["tables"]["tbl3"]["path"]
# Save the LaTeX table to a file
with open(tbl3_path, "w", encoding="utf-8") as f:
    f.write(tbl3_latex)
    print(f"\n- Table 3 saved to {tbl3_path}")


### Save the Graphics Dataset ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Graphics Dataset")


# Compile the graphics dataset
tbl3_dataset = {"data": tbl3_data, "latex": tbl3_latex}

# Save the graphics dataset to disk
tbl3_dataset_path = os.path.join(prj_dirs["data_python"], graphics_list["tables"]["tbl3"]["file"] + ".pkl")
with open(tbl3_dataset_path, "wb") as f:
    pickle.dump(tbl3_dataset, f)
    print(f"\n- Graphics dataset saved to {tbl3_dataset_path}")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.7. Table 4 - Average and Median Monthly Collision Stats ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.7. Table 4 - Average and Median Monthly Collision Stats")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Table 4
graphics_list = ocs.graphics_entry(
    gr_type=1,
    gr_id=4,
    gr_attr={
        "name": "Monthly Accident Average and Median Statistics",
        "description": "Key statistics for the average and median variable values in the OCTraffic datasets",
        "caption": (
            f"Average and median values for monthly time series of traffic accidents in Orange County ({prj_meta['date_start'].year}-{prj_meta['date_end'].year})"
        ),
        "method": "stat.desc",
        "file_format": ".tex",
        "file": "Monthly Average Stats",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)


### Construct Summary Table ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Construct Summary Table")

# Filter stats_month to get rows where stat.type is 'mean' or 'median'
tbl4_data = stats_month[stats_month["stat.type"].isin(["mean", "median"])].copy()

# Remove the rows for "coll_severity_rank_num" and "city_travel_time" as they are not needed
tbl4_data = tbl4_data[~tbl4_data["var.name"].isin(["coll_severity_rank_num_mean", "city_travel_time_mean"])].copy()

# Dictionary for column labels and orders
tbl4_labels = {
    "party_number": {"label": "Parties", "var_order": 1},
    "victim_number": {"label": "Victims", "var_order": 2},
    "party_number_killed": {"label": "Killed Parties", "var_order": 3},
    "party_number_inj": {"label": "Injured Parties", "var_order": 4},
    "victim_degree_of_injury": {"label": "Victim Degree of Injury", "var_order": 5},
    "coll_severity_num": {"label": "Severity", "var_order": 6},
    "party_age": {"label": "Median Party Age", "var_order": 7},
    "victim_age": {"label": "Median Victim Age", "var_order": 8},
    "hit_and_run": {"label": "Hit and Run", "var_order": 9},
    "distance": {"label": "Distance", "var_order": 10},
    "lighting": {"label": "Lighting", "var_order": 11},
    "vehicle_year_group": {"label": "Vehicle Year Group", "var_order": 12},
    "city_area_sq_mi": {"label": "City Area (sq. mi)", "var_order": 13},
    "city_pop_total": {"label": "City Population", "var_order": 14},
    "city_hou_total": {"label": "City Housing Units", "var_order": 15},
    "city_pop_dens": {"label": "City Population Density", "var_order": 16},
    "city_hou_dens": {"label": "City Housing Density", "var_order": 17},
    "city_pop_asian": {"label": "City Asian Population", "var_order": 18},
    "city_pop_black": {"label": "City Black Population", "var_order": 19},
    "city_pop_hispanic": {"label": "City Hispanic Population", "var_order": 20},
    "city_pop_white": {"label": "City White Population", "var_order": 21},
    "city_vehicles": {"label": "City Vehicles", "var_order": 22},
    "city_mean_travel_time": {"label": "City Mean Travel Time", "var_order": 23},
    "roads_primary": {"label": "City Primary Roads", "var_order": 24},
    "roads_secondary": {"label": "City Secondary Roads", "var_order": 25},
    "roads_local": {"label": "City Local Roads", "var_order": 26},
    "road_length_mean": {"label": "City Mean Road Length", "var_order": 27},
    "road_length_sum": {"label": "City Total Road Length", "var_order": 38},
}


# Remove the "_mean" or "_median" suffix from var.name
tbl4_data["var.name"] = tbl4_data["var.name"].str.replace(r"_(mean|median)$", "", regex=True)

# Add label and order columns based on tbl3_labels
tbl4_data.loc[:, "label"] = tbl4_data["var.name"].map(lambda x: tbl4_labels[x]["label"])
tbl4_data.loc[:, "order"] = tbl4_data["var.name"].map(lambda x: tbl4_labels[x]["var_order"])

# Relocate the label and order columns
ocs.relocate_column(df=tbl4_data, col_name="order", ref_col_name="stat.type", position="after")
ocs.relocate_column(df=tbl4_data, col_name="label", ref_col_name="order", position="after")

# Remove the "var.name" column
tbl4_data = tbl4_data.drop(columns=["var.name"])
# Reorder the columns by "order"
tbl4_data = tbl4_data.sort_values("order")

# Round float columns (2 decimal places) for mean, std.dev, min, max, and median
for col in ["mean", "std.dev", "min", "max", "median"]:
    if col in tbl4_data.columns:
        tbl4_data.loc[:, col] = tbl4_data[col].astype(float).round(2)

# Keep only the specificied columns and reorder them
cols = ["label", "mean", "min", "max", "std.dev", "median"]
tbl4_data = tbl4_data[[col for col in cols if col in tbl4_data.columns]]

# Rename columns
col_rename = {
    "label": "Factor (Count or Bin)",
    "mean": "Mean",
    "min": "Min",
    "max": "Max",
    "std.dev": "SD",
    "median": "Median",
}
tbl4_data = tbl4_data.rename(columns=col_rename)


# Ensure that variables are floats with 3 decimal places
for col in ["Mean", "Min", "Max", "SD", "Median"]:
    if col in tbl4_data.columns:
        tbl4_data[col] = tbl4_data[col].astype(float).round(3)

# Format all numeric columns with a thousands comma separator if they have at least 4 digits
for col in tbl4_data.columns:
    if pd.api.types.is_numeric_dtype(tbl4_data[col]):
        tbl4_data[col] = tbl4_data[col].apply(lambda x: f"{x:,}" if pd.notnull(x) and abs(x) >= 1000 else x)

print(tbl4_data)


### Create Table 4 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create Table 4")

tbl4_mod = tbl4_data.copy()
# Convert all columns to string type
tbl4_mod = tbl4_mod.astype(str)

# Create the LaTeX table from the data frame
tbl4_latex = tbl4_mod.to_latex(
    buf=None,
    columns=None,
    header=True,
    index=False,
    na_rep="NaN",
    formatters=None,
    float_format=None,
    sparsify=None,
    index_names=True,
    bold_rows=False,
    column_format="lrrrrrrr",
    longtable=False,
    escape=False,
    encoding=None,
    decimal=".",
    multicolumn=True,
    multicolumn_format="c",
    multirow=True,
    caption=graphics_list["tables"]["tbl4"]["caption"],
    label=graphics_list["tables"]["tbl4"]["id"].lower(),
    position=None,
)

# Add option [h!] to the table
tbl4_latex = tbl4_latex.replace("\\begin{table}", "\\begin{table}[h!]")

# Split the LaTeX table into lines for easier manipulation
tbl4_latex_lines = tbl4_latex.splitlines()

# Replace line 5 with a custom header
header_line = r"Factor (Mean or Median) & Mean\footnotemark[1] & Min\footnotemark[1] & Max\footnotemark[1] & SD\footnotemark[1] & Median\footnotemark[1] \\"
tbl4_latex_lines[5] = header_line


midrule_idx = [12, 20, 32]
for idx in midrule_idx:
    # Insert a midrule after each specified index
    tbl4_latex_lines.insert(idx, r"\midrule")

# Join the lines back together
tbl4_latex = "\n".join(tbl4_latex_lines)
# Delete the temporary latex lines
del tbl4_latex_lines

tbl4_latex = tbl4_latex.replace(f"Severity", "Severity\\footnotemark[2]")
tbl4_latex = tbl4_latex.replace(f"Hit and Run", "Hit and Run\\footnotemark[3]")
tbl4_latex = tbl4_latex.replace(f"Lighting", "Lighting\\footnotemark[4]")

# Add footnotes after the table
footnotes = (
    "\\footnotetext[1]{"
    + f"Monthly generated cumulative summary time series from {prj_meta['date_start'].strftime('%m/%d/%Y')} through {prj_meta['date_end'].strftime('%m/%d/%Y')} (n = {len(ts_month['collisions'])})"
    + "}\n"
    "\\footnotetext[2]{Increasing ordinal severity rank from 0 (no injury) to 4 (fatal)}\n"
    "\\footnotetext[3]{Ordinal hit-and-run classification: 0 (No), 1 (Misdemeanor), 2 (Felony)}\n"
    "\\footnotetext[4]{Decreasing lighting intensity (higher value is darker conditions), from 1 to 4}"
)
if "\\end{tabular}" in tbl4_latex:
    tbl4_latex = tbl4_latex.replace("\\end{tabular}", "\\end{tabular}\n" + footnotes)
elif "\\end{longtable}" in tbl4_latex:
    tbl4_latex = tbl4_latex.replace("\\end{longtable}", "\\end{longtable}\n" + footnotes)
else:
    tbl4_latex += "\n" + footnotes

print(tbl4_latex)


# Define the path to save the LaTeX table
tbl4_path = graphics_list["tables"]["tbl4"]["path"]
# Save the LaTeX table to a file
with open(tbl4_path, "w", encoding="utf-8") as f:
    f.write(tbl4_latex)
    print(f"\n- Table 4 saved to {tbl4_path}")


### Save the Graphics Dataset ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Graphics Dataset")


# Compile the graphics dataset
tbl4_dataset = {"data": tbl4_data, "latex": tbl4_latex}

# Save the graphics dataset to disk
tbl4_dataset_path = os.path.join(prj_dirs["data_python"], graphics_list["tables"]["tbl4"]["file"] + ".pkl")
with open(tbl4_dataset_path, "wb") as f:
    pickle.dump(tbl4_dataset, f)
    print(f"\n- Graphics dataset saved to {tbl4_dataset_path}")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## 2.8. Table 5 - Collision Incidents by Year ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n2.8. Table 5 - Collision Incidents by Year")


### Metadata ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Metadata")

# Add graphics metadata for Table 5
graphics_list = ocs.graphics_entry(
    gr_type=1,
    gr_id=5,
    gr_attr={
        "name": "Collision Incidents by Year",
        "description": "Key statistics for the collision incidents by year in the OCTraffic datasets",
        "caption": "Collision Incidents Categorization by Year in the OCTraffic datasets",
        "method": "collap",
        "file_format": ".tex",
        "file": "Collision Incidents by Year",
        "status": "final",
    },
    gr_list=graphics_list,
    gr_dirs=prj_dirs,
)


### Construct Summary Table ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Construct Summary Table")

# Create a dataframe with selected columns from tsYear
tbl5_data = ts_year["collisions"][
    [
        "crash_tag_sum",
        "party_tag_sum",
        "victim_tag_sum",
        "number_killed_sum",
        "number_inj_sum",
        "count_severe_inj_sum",
        "count_visible_inj_sum",
        "count_complaint_pain_sum",
        "count_car_killed_sum",
        "count_car_inj_sum",
        "count_ped_killed_sum",
        "count_ped_inj_sum",
        "count_bic_killed_sum",
        "count_bic_inj_sum",
        "count_mc_killed_sum",
        "count_mc_inj_sum",
    ]
].copy()

# Extract years from dateYear column and set them as the DataFrame index
tbl5_data.index = ts_year["collisions"]["date_year"].dt.year

# Rename the columns of the tbl5_data data frame
tbl5_data.columns = [
    "Crashes",
    "Parties",
    "Victims",
    "Fatal",
    "Injuries Total",
    "Injuries Severe",
    "Injuries Visible",
    "Injuries Pain",
    "Cars Killed",
    "Cars Injured",
    "Pedestrians Killed",
    "Pedestrians Injured",
    "Bicyclists Killed",
    "Bicyclists Injured",
    "Motorcyclists Killed",
    "Motorcyclists Injured",
]


# Add summary statistics rows (total, mean, sd) to the bottom of the DataFrame
total_row = tbl5_data.sum(axis=0).round(0)
mean_row = tbl5_data.mean(axis=0).round(0)
std_row = tbl5_data.std(axis=0).round(0)

# Create a new DataFrame with the statistics rows and concatenate with the original
stats_df = pd.DataFrame([total_row, mean_row, std_row], index=["Total", "Mean", "SD"])

# Concatenate the original DataFrame with the statistics DataFrame
tbl5_data = pd.concat([tbl5_data, stats_df])

# Add thousand separators to the numeric columns
for col in tbl5_data.columns:
    tbl5_data[col] = tbl5_data[col].map(lambda x: f"{x:,.0f}" if isinstance(x, (int, float)) else x)

# Add a new column with the year extracted from the index
tbl5_data["Year"] = tbl5_data.index
# Reorder the columns to have 'Year' as the first column
tbl5_data = tbl5_data[["Year"] + [col for col in tbl5_data.columns if col != "Year"]]

print(tbl5_data)


### Create Table 5 ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Create Table 5")


# Convert all columns to string type
tbl5_mod = tbl5_data.copy().astype(str)

# Create the LaTeX table from the data frame
tbl5_latex = tbl5_mod.to_latex(
    buf=None,
    columns=None,
    header=True,
    index=False,
    na_rep="NaN",
    formatters=None,
    float_format=None,
    sparsify=None,
    index_names=True,
    bold_rows=False,
    column_format="crrrrrrrrrrrrrrrr",
    longtable=False,
    escape=False,
    encoding=None,
    decimal=".",
    multicolumn=True,
    multicolumn_format="c",
    multirow=True,
    caption=graphics_list["tables"]["tbl5"]["caption"],
    label=graphics_list["tables"]["tbl5"]["id"].lower(),
    position=None,
)

# Change the LaTeX table to use the 'sidewaystable' style
tbl5_latex = tbl5_latex.replace("\\begin{table}", "\\begin{sidewaystable}[ht!]")
tbl5_latex = tbl5_latex.replace("\\end{table}", "\\end{sidewaystable}")

# Replace the header rows with custom formatting
tbl5_latex = tbl5_latex.replace("Year & Crashes & Parties & Victims & Fatal & Injuries Total & Injuries Severe & Injuries Visible & Injuries Pain & Cars Killed & Cars Injured & Pedestrians Killed & Pedestrians Injured & Bicyclists Killed & Bicyclists Injured & Motorcyclists Killed & Motorcyclists Injured", "\\multirow[c]{2}{*}{Year} & \\multicolumn{1}{c}{\\multirow{2}{*}{Crashes}} & \\multicolumn{1}{c}{\\multirow{2}{*}{Parties}} & \\multicolumn{1}{c}{\\multirow{2}{*}{Victims}} & \\multicolumn{1}{c}{\\multirow{2}{*}{Fatal}} & \\multicolumn{4}{c}{Injuries} & \\multicolumn{2}{c}{Cars} & \\multicolumn{2}{c}{Pedestrians} & \\multicolumn{2}{c}{Bicyclists} & \\multicolumn{2}{c}{Motorcyclists} \\\\\n\\cmidrule{6-9}\\cmidrule{10-11}\\cmidrule{12-13}\\cmidrule{14-15}\\cmidrule{16-17}\n&  &  &  &  & {Total} & {Severe} & {Visible} & {Pain} & {Killed} & {Injured} & {Killed} & {Injured} & {Killed} & {Injured} & {Killed} & {Injured}"
)

# Add midrule for better readability
tbl5_latex = tbl5_latex.replace("Total &", "\\midrule\nTotal &")


# Add footnote for the table
footnote = (
    "\\footnotesize{"
    + f"Note: Data cover annual time series from {prj_meta['date_start'].strftime('%m/%d/%Y')} through {prj_meta['date_end'].strftime('%m/%d/%Y')} (n = {len(ts_year['collisions'])})"
    + "}"
)

tbl5_latex = tbl5_latex.replace("\\end{tabular}", "\\end{tabular}\n" + footnote)

print(tbl5_latex)

# Define the path to save the LaTeX table
tbl5_path = graphics_list["tables"]["tbl5"]["path"]
# Save the LaTeX table to a file
with open(tbl5_path, "w", encoding="utf-8") as f:
    f.write(tbl5_latex)
    print(f"\n- Table 5 saved to {tbl5_path}")


### Save the Graphics Dataset ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print("\n- Save the Graphics Dataset")

# Compile the graphics dataset
tbl5_dataset = {"data": tbl5_data, "latex": tbl5_latex}

# Save the graphics dataset to disk
tbl5_dataset_path = os.path.join(prj_dirs["data_python"], graphics_list["tables"]["tbl5"]["file"] + ".pkl")
with open(tbl5_dataset_path, "wb") as f:
    pickle.dump(tbl5_dataset, f)


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
# Last Execution: 2025-12-30
