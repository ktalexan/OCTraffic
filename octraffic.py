# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Project: OCTraffic Data Processing
# Title: Create Project Functions ----
# Author: Dr. Kostas Alexandridis, GISP
# Version: 2025.2, Date: 2025-12-24
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Import necessary libraries ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import os, sys, datetime, pickle
from typing import Union, List, Optional
import json, pytz
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
import scipy.stats as stats
import statsmodels.api as sm
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MultipleLocator
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import codebook.cbl as cbl

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Class containing OCTraffic data processing functions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
class octraffic:
    """
    Class containing OCTraffic data processing functions.
    Attributes:
        None
    Methods:
        1. project_metadata(self, part: int, version: float, silent: bool = False) -> dict
        2. export_tims_metadata(self, metadata: dict) -> None
        3. update_tims_metadata(self, year: int, type: str = "reported", data_counts = None) -> None
        4. project_directories(self, base_path: str, silent: bool = False) -> dict
        5. relocate_column(self, df: pd.DataFrame, col_name: Union[str, List[str]], ref_col_name: str, position: str = "after") -> pd.DataFrame
        6. categorical_series(self, var_series: pd.Series, var_name: str, cb_dict: dict) -> pd.Series
        7. is_dst(self, dt_series: pd.Series, tz_name: str = "America/Los_Angeles") -> pd.Series
        8. add_attributes(self, df: pd.DataFrame, cb: dict) -> pd.DataFrame
        9. save_to_disk(self, dir_list: dict, local_vars: dict = locals(), global_vars: dict = globals()) -> None
        10. graphics_entry(self, gr_type: int, gr_id: int, gr_attr: dict, gr_list: Optional[dict] = None, gr_dirs: Optional[dict] = None) -> None
        11. chi2_test(self, df: pd.DataFrame, col1: str, col2: str) -> dict
        12. chi2_gof_test(self, df: pd.DataFrame, col: str) -> dict
        13. kruskal_test(self, df: pd.DataFrame, col1: str, col2: str) -> dict
        14. p_value_display(self, p_value: float) -> str
        15. create_stl_plot(self, time_series, season, model="additive", label=None, covid=False, robust=True) -> tuple
        16. format_coll_time(self, x: int) -> str
        17. quarter_to_date(self, row: pd.Series, ts: bool = True) -> pd.Timestamp
        18. get_coll_severity_rank(self, row: pd.Series) -> int
        19. counts_by_year(self, df: pd.DataFrame, year: int) -> int
        20. ts_aggregate(self, dt: str, df: pd.DataFrame, cb: dict = cb) -> pd.DataFrame
        21. plot_victim_count_histogram(self, df: pd.DataFrame, fig: matplotlib.figure.Figure=None, ax: matplotlib.axes.Axes=None) -> tuple
        22. plot_collision_type_bar(self, df: pd.DataFrame, fig: matplotlib.figure.Figure=None, ax: matplotlib.axes.Axes=None) -> tuple
        23. plot_fatalities_by_type_and_year(self, df: pd.DataFrame, fig: matplotlib.figure.Figure=None, ax: matplotlib.axes.Axes=None) -> tuple
        24. compute_monthly_stats(self, ts_month: pd.DataFrame) -> pd.DataFrame
    Examples:
        >>> from octraffic import octraffic
        >>> ocs = octraffic()
        >>> ocs.create_stl_plot(time_series, season, model="additive", label=None, covid=False, robust=True)
    """

    def __init__(self):
        pass

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 1. Project metadata function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def project_metadata(self, part: int, version: float, silent: bool = False) -> dict:
        """
        Function to generate project metadata for the OCTraffic data processing project.
        Args:
            part (int): The part number of the project.
            version (float): The version number of the project.
            silent (bool): If True, suppresses the print output. Default is False.
        Returns:
            metadata (dict): A dictionary containing the project metadata. The dictionary includes: name, title, description, version, author, years, date_start, date_end, date_updated, and TIMS metadata.
        Raises:
            ValueError: If part is not an integer, or if version is not numeric.
        Example:
            >>> metadata = project_metadata(1, 1.0)
        Notes:
            This function reads the TIMS metadata from a JSON file located in the "metadata" directory.
            It generates a dictionary with project metadata based on the provided part and version.
            The function also checks if the TIMS metadata file exists and raises an error if it does not.
        """
        
        # Check if the TIMS metadata file exists
        metadata_file = os.path.join(os.getcwd(), "metadata", "tims_metadata.json")
        if not os.path.exists(metadata_file):
            raise FileNotFoundError(f"Metadata file {metadata_file} does not exist.")
        
        # Load the TIMS metadata
        with open(metadata_file, 'r') as f:
            tims_metadata = json.load(f)
        
        # Get the first and last dates from the TIMS metadata
        start_date = datetime.date.fromisoformat(tims_metadata[list(tims_metadata.keys())[0]]["date_start"])
        end_date = datetime.date.fromisoformat(tims_metadata[list(tims_metadata.keys())[-1]]["date_end"])

        # Check if the part is integer
        if not isinstance(part, int):
            raise ValueError("Part must be an integer.")

        # Check if the version is numeric
        if not isinstance(version, (int, float)):
            raise ValueError("Version must be a number.")
        
        # Set dateUpdated to the current date
        date_updated = datetime.date.today()
        
        # Match the part to a specific step and description (with default case)
        match part:
            case 0:
                step = "Part 0: TIMS Metadata Update"
                desc = "Updating the TIMS metadata for the OCTraffic data processing project."
            case 1:
                step = "Part 1: Raw Data Merging"
                desc = "Merging and verifying the raw SWITRS annual data files into single files."
            case 2:
                step = "Part 2: Raw Data Processing"
                desc = "Processing the raw data files into data frames and generating additional variables."
            case 3:
                step = "Part 3: Time Series Processing"
                desc = "Generating and processing time series data from OCTraffic collision data."
            case 4:
                step = "Part 4: Collision Data Analysis"
                desc = "Analyzing the collision data and generating graphs and statistics."
            case 5:
                step = "Part 5: Time Series Analysis"
                desc = "Analyzing the OCTraffic time series data and generating graphs and statistics."
            case 6:
                step = "Part 6: GIS Feature Class Processing"
                desc = "Processing data frame tabular data into GIS feature classes, and performing geoprocessing operations and analysis."
            case 7:
                step = "Part 7: GIS Map Processing"
                desc = "Processing GIS feature classes into GIS maps."
            case 8:
                step = "Part 8: GIS Layout Processing"
                desc = "Processing GIS maps and layers into GIS layouts."
            case 9:
                step = "Part 9: ArcGIS Online Feature Sharing"
                desc = "Sharing GIS feature classes to ArcGIS Online."
            case 10:
                step = "Part 10: ArcGIS Online Metadata Update"
                desc = "Updating the metadata of the shared ArcGIS Online feature classes."
            case _:
                step = "Part 0: General Data Processing"
                desc = "General data processing and analysis (default)."
        
        # Create a dictionary to hold the metadata
        metadata = {
            "name": "OCTraffic Data Processing",
            "title": step,
            "description": desc,
            "version": version,
            "author": "Dr. Kostas Alexandridis, GISP",
            "years": [tims_metadata[key]["year"] for key in tims_metadata.keys()],
            "date_start": start_date,
            "date_end": end_date,
            "date_updated": date_updated,
            "tims": tims_metadata
        }

        # If it is not silent, print the metadata
        if not silent:
            print(
                f"Project Metadata:\n- Name: {metadata['name']}\n- Title: {metadata['title']}\n- Description: {metadata['description']}\n- Version: {metadata['version']}\n- Author: {metadata['author']}\n- Start Date: {metadata['date_start'].strftime('%B %d, %Y')}\n- End Date: {metadata['date_end'].strftime('%B %d, %Y')}\n- Years: {list(metadata['years'])}\n- Last Updated: {metadata['date_updated'].strftime('%B %d, %Y')}"
            )

        # Return the metadata
        return metadata


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 2. Export TIMS Metadata function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def export_tims_metadata(self, metadata: dict) -> None:
        """
        Exports the TIMS metadata to a JSON file.
        Args:
            metadata (dict): The metadata dictionary to export.
        Returns:
            None
        Raises:
            FileNotFoundError: If the metadata directory does not exist.
        Example:
            >>> export_tims_metadata(tims_metadata)
        Notes:
            This function exports the TIMS metadata to a JSON file located in the "metadata" directory.
        """
        tims_metadata = {}
        # Check if the metadata is in locals() and is a dictionary
        # NOTE: logic adjusted slightly as metadata is passed as arg, locals() check in original might have been for robustness if called without args or slightly confused logic
        # keeping original logic structure but 'metadata' is argument. 
        
        if isinstance(metadata, dict):
             if "tims" in metadata:
                tims_metadata = metadata["tims"]
             else:
                raise ValueError("- Metadata does not contain 'tims' key.")
        else:
            raise ValueError("- Metadata must be a dictionary.")
        
        # Define the path to the metadata directory
        tims_path = os.path.join(os.getcwd(), "metadata", "tims_metadata.json")
        # Write the TIMS metadata to a JSON file and overwrite if it exists
        with open(tims_path, 'w') as f:
            json.dump(tims_metadata, f, indent = 4)
        
        # if successful, print a message
        print(f"- TIMS metadata exported to disk successfully.")
        
        return None


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 3. Update TIMS Metadata Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def update_tims_metadata(self, year: int, type: str = "reported", data_counts = None) -> None:
        """
        Update the TIMS metadata file with the counts of crashes, parties, and victims for a given year and type.
        Args:
            year (int): The year for which the metadata is being updated.
            type (str): The type of data being updated. Valid values are "reported", "geocoded", or "excluded".
            data_counts (list, optional): A list containing the counts of crashes, parties, and victims.
        Returns:
            None
        Raises:
            ValueError: If the type is not one of the valid values.
            FileNotFoundError: If the metadata file does not exist.
        Example:
            >>> update_tims_metadata(2024, "reported", [100, 200, 300])
        Notes:
            This function updates the TIMS metadata file with the counts of crashes, parties, and victims for a given year and type.
        """    
        # Types definition
        types = ["reported", "geocoded", "excluded"]
        if type not in types:
            raise ValueError(f"Invalid type '{type}'. Valid types are: {', '.join(types)}")
        
        if data_counts is None:
            data_counts = [0, 0, 0]  # Default to zero counts for crashes, parties, victims
        
        # Check if data_counts is a list of three integers
        if not isinstance(data_counts, list) or len(data_counts) != 3 or not all(isinstance(x, int) for x in data_counts):
            raise ValueError("data_counts must be a list of three integers: [crashes, parties, victims]")
        
        # Get the counts from the data_counts list
        count_crashes = data_counts[0]
        count_parties = data_counts[1]
        count_victims = data_counts[2]


        # Check if the TIMS metadata file exists
        metadata_file = os.path.join(os.getcwd(), "metadata", "tims_metadata.json")
        if not os.path.exists(metadata_file):
            raise FileNotFoundError(f"Metadata file {metadata_file} does not exist.")
        
        # Load the TIMS metadata
        with open(metadata_file, 'r') as f:
            tims_metadata = json.load(f)
        
        # Update the metadata for the specified type
        if type == "reported":
            tims_metadata[str(year)]["reported"]["crashes"] = count_crashes
        elif type == "geocoded":
            tims_metadata[str(year)]["geocoded"]["parties"] = count_parties
        elif type == "excluded":
            tims_metadata[str(year)]["excluded"]["victims"] = count_victims
        
        # Save the updated metadata back to the file
        with open(metadata_file, 'w') as f:
            json.dump(tims_metadata, f, indent=4)
        
        print(f"TIMS metadata for {year} ({type}) updated successfully:\nCrashes: {count_crashes:,}, Parties: {count_parties:,}, Victims: {count_victims:,}")


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 4. Project Directories function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def project_directories(self, base_path: str, silent: bool = False) -> dict:
        """
        Function to generate project directories for the OCTraffic data processing project.
        Args:
            base_path (str): The base path of the project.
            silent (bool): If True, suppresses the print output. Default is False.
        Returns:
            prj_dirs (dict): A dictionary containing the project directories.
        Raises:
            ValueError: If base_path is not a string.
        Example:
            >>> prj_dirs = project_directories("/path/to/project")
        Notes:
            This function creates a dictionary of project directories based on the base path.
            The function also checks if the base path exists and raises an error if it does not.
        """
        prj_dirs = {
            "root": base_path,
            "admin": os.path.join(base_path, "admin"),
            "agp": os.path.join(base_path, "octagp"),
            "agp_aprx": os.path.join(base_path, "octagp", "octagp.aprx"),
            "agp_gdb": os.path.join(base_path, "octagp", "octagp.gdb"),
            "agp_gdb_raw": os.path.join(base_path, "octagp", "octagp.gdb", "raw"),
            "agp_gdb_supporting": os.path.join(base_path, "octagp", "octagp.gdb", "supporting"),
            "agp_gdb_analysis": os.path.join(base_path, "octagp", "octagp.gdb", "analysis"),
            "agp_gdb_hotspots": os.path.join(base_path, "octagp", "octagp.gdb", "hotspots"),
            "gdbmain": os.path.join(base_path, "gis", "octmain.gdb"),
            "gdbmain_raw": os.path.join(base_path, "gis", "octmain.gdb", "raw"),
            "gdbmain_supporting": os.path.join(base_path, "gis", "octmain.gdb", "supporting"),
            "analysis": os.path.join(base_path, "analysis"),
            "codebook": os.path.join(base_path, "codebook"),
            "data": os.path.join(base_path, "data"),
            "data_ago": os.path.join(base_path, "data", "ago"),
            "data_archived": os.path.join(base_path, "data", "archived"),
            "data_processed": os.path.join(base_path, "data", "processed"),
            "data_python": os.path.join(base_path, "data", "python"),
            "data_raw": os.path.join(base_path, "data", "raw"),
            "gis": os.path.join(base_path, "gis"),
            "gis_layers": os.path.join(base_path, "gis", "layers"),
            "gis_layers_templates": os.path.join(base_path, "gis", "layers", "templates"),
            "gis_layouts": os.path.join(base_path, "gis", "layouts"),
            "gis_maps": os.path.join(base_path, "gis", "maps"),
            "gis_styles": os.path.join(base_path, "gis", "styles"),
            "graphics": os.path.join(base_path, "graphics"),
            "graphics_gis": os.path.join(base_path, "graphics", "gis"),
            "metadata": os.path.join(base_path, "metadata"),
            "notebooks": os.path.join(base_path, "notebooks"),
            "notebooks_archived": os.path.join(base_path, "notebooks", "archived"),
            "scripts": os.path.join(base_path, "scripts"),
            "scripts_archived": os.path.join(base_path, "scripts", "archived")
        }
        # Print the project directories
        if not silent:
            print("Project Directories:")
            for key, value in prj_dirs.items():
                print(f"- {key}: {value}")
        # Return the project directories
        return prj_dirs


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 5. Relocate Dataframe Column Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def relocate_column(
        self, df: pd.DataFrame, col_name: Union[str, List[str]], ref_col_name: str, position: str = "after"
    ) -> None:
        """
        Relocates a column in a DataFrame to a new position relative to another column.
        Args:
            df (pd.DataFrame): The DataFrame to modify.
            col_name (Union[str, List[str]]): The name of the column to relocate.
            ref_col_name (str): The name of the reference column.
            position (str): "before" or "after" the reference column. Default is "after".
        Returns:
            pd.DataFrame: The modified DataFrame with the relocated column.
        Raises:
            ValueError: If the reference column does not exist in the DataFrame.
            ValueError: If the column names are not strings.
        Example:
            >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})
            >>> relocate_column(df, "C", "B", "after")
            >>> print(df)
        Notes:
            This function relocates a column in a DataFrame to a new position relative to another column.
        """

        # Make sure the ref_col_name exists in the DataFrame
        if ref_col_name not in df.columns:
            raise ValueError(f"Reference column '{ref_col_name}' does not exist in the DataFrame.")

        # Check if the column names are strings
        if isinstance(col_name, str):
            ref_type = 1
        elif isinstance(col_name, list):
            ref_type = 2
        else:
            raise ValueError("test must be a string or a list.")

        if position == "before":
            new_position = df.columns.get_loc(ref_col_name)
        elif position == "after":
            new_position = df.columns.get_loc(ref_col_name)
            # check if the new_position is integer
            if isinstance(new_position, int):
                new_position += 1
            else:
                raise ValueError("Reference column position is not an integer.")
        else:
            raise ValueError("Position must be 'before' or 'after'.")

        # If the column name is a string, move it to the new position
        if ref_type == 1:
            # Move the column to the new position
            col = df.pop(col_name)
            df.insert(new_position, col_name, col)
        # If the column name is a list, move each column to the new position one after the other
        elif ref_type == 2:
            for cname in col_name:
                # Move the column to the new position
                col = df.pop(cname)
                df.insert(new_position, cname, col)
                new_position += 1


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 6. Categorical Pandas Series function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def categorical_series(self, var_series: pd.Series, var_name: str, cb_dict: dict) -> pd.Series:
        """
        Function that converts a pandas series to categorical or ordered categorical type.
        Requires importing the 'cbl' module for the variable labels, and the 'CategoricalDtype' from pandas.
        Args:
            var_series (pandas series): The data to be converted.
            var_name (str): The name of the series.
            codebook (dict): The codebook containing the variable labels and types.
        Returns:
            categoricalSeries (pandas series): The converted series.
        Raises:
            TypeError: If var_series is not a pandas series.
            ValueError: If var_name is not a string.
            ValueError: If codebook is not a dictionary.
            ValueError: If var_name is not in the codebook.
            ValueError: If var_type is not 'binary', 'categorical' or 'ordered'.
        Example:
            >>> categoricalSeries(dfCrashes["dtWeekDay"], "dtWeekDay", "categorical")
        Notes:
            This function converts a pandas series to categorical or ordered categorical type.
        """

        # First, check if the series is labeled:
        if cb_dict[var_name]["labeled"] == 1:
            # Get the label type
            var_type = cb_dict[var_name]["label_type"]
            # Check if the type is correct
            if var_type not in ["binary", "nominal", "ordinal"]:
                raise ValueError(f"Variable {var_name} Categorical type is not valid.")
        else:
            # Check if the series is labeled
            raise ValueError(f"Variable {var_name} is not labeled.")

        # Get the codes for the series
        var_labels = getattr(cbl, var_name)
        cats = [v for k, v in var_labels.items()]

        # Set the codes to None
        cat_series = None
        # Labeled codes
        labeled_series = var_series.map(var_labels)

        # Obtain the codes if the type is binary, nominal or ordinal
        if var_type == "binary":
            # Binary codes
            cat_series = labeled_series.astype(CategoricalDtype(categories = cats, ordered = False))
        elif var_type == "nominal":
            # Categorical codes
            cat_series = labeled_series.astype(CategoricalDtype(categories = cats, ordered = False))
        elif var_type == "ordinal":
            # Get the ordered categories
            # Ordinal codes
            cat_series = labeled_series.astype(CategoricalDtype(categories = cats, ordered = True))
        else:
            # Raise an error if the type is not valid
            raise ValueError(f"Variable {var_name} Categorical type is not valid.")

        # Return the codes
        return cat_series


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 7. Determine Daylight Saving Time function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def is_dst(self, dt_series: pd.Series, tz_name: str = "America/Los_Angeles") -> pd.Series:
        """
        Function to determine if a datetime series is in Daylight Saving Time (DST).
        Args:
            dt_series (pd.Series): The datetime series to check.
            tz_name (str): The timezone name. Default is "America/Los_Angeles".
        Returns:
            dst_result (pd.Series): The result of the check.
            0 if not in DST, 1 if in DST, -1 if unknown.
        Raises:
            ValueError: If dt_series is not a pandas series.
            ValueError: If tz_name is not a string.
            ValueError: If tz_name is not a valid timezone.
        Example:
            >>> isDst(dfCrashes["dtCrashDateTime"], "America/Los_Angeles")
        Notes:
            This function determines if a datetime series is in Daylight Saving Time (DST).
        """
        try:
            # Convert pandas Series to Python datetime objects with timezone info
            def check_dst(dt):
                if pd.isna(dt):
                    return False
                # Convert to datetime object if it's not already
                if not isinstance(dt, datetime.datetime):
                    dt = pd.Timestamp(dt).to_pydatetime()
                # Get the timezone
                tz = pytz.timezone(tz_name)
                # Localize the datetime (assume it's in UTC if not timezone-aware)
                if dt.tzinfo is None:
                    dt = pytz.utc.localize(dt).astimezone(tz)
                else:
                    dt = dt.astimezone(tz)
                # Check if it's in DST
                dst_result = None
                if bool(dt.dst()) is False:
                    dst_result = 0
                elif bool(dt.dst()) is True:
                    dst_result = 1
                else:
                    dst_result = -1
                # Return the result
                return dst_result

            # Apply the function to each datetime in the series
            return dt_series.apply(check_dst)
        except (pytz.UnknownTimeZoneError, ValueError, TypeError) as e:
            print(f"Error determining DST: {str(e)}")
            return pd.Series([False] * len(dt_series), index = dt_series.index)


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 8. Add Codebook Attributes Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def add_attributes(self, df: pd.DataFrame, cb: dict) -> pd.DataFrame:
        """
        Adds column attributes to a DataFrame based on a codebook dictionary.
        Args:
            df (pd.DataFrame): The DataFrame to modify.
            cb (dict): Codebook dictionary where keys are column names and values are dicts of attributes.
        Returns:
            pd.DataFrame: The DataFrame with updated column attributes.
        Raises:
            ValueError: If df is not a pandas DataFrame.
            ValueError: If cb is not a dictionary.
        Example:
            >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})
            >>> cb = {"A": {"var_alias": "Column A", "var_desc": "Description of Column A"}, "B": {"var_alias": "Column B", "var_desc": "Description of Column B"}, "C": {"var_alias": "Column C", "var_desc": "Description of Column C"}}
            >>> add_attributes(df, cb)
            >>> print(df)
        Notes:
            This function adds column attributes to a DataFrame based on a codebook dictionary.
        """
        for cname in df.columns:
            if cname in cb:
                attrs = cb[cname]
                df[cname].attrs["alias"] = attrs.get("var_alias")
                df[cname].attrs["description"] = attrs.get("var_desc")
                df[cname].attrs["order"] = attrs.get("order")
                df[cname].attrs["class"] = attrs.get("var_class")
                df[cname].attrs["category"] = attrs.get("var_cat")
                df[cname].attrs["type"] = attrs.get("var_type")
                df[cname].attrs["source"] = attrs.get("source")
                is_labeled = attrs.get("labeled")
                df[cname].attrs["labeled"] = "Yes" if is_labeled == 1 else "No"
                df[cname].attrs["fc"] = attrs.get("fc")
                df[cname].attrs["ts"] = attrs.get("ts")
                df[cname].attrs["stats"] = attrs.get("stats")
                df[cname].attrs["notes"] = attrs.get("notes")
        return df


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 9. Save to Disk Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def save_to_disk(self, dir_list: dict, local_vars: dict = locals(), global_vars: dict = globals()) -> None:
        """
        Save the data frames, codebook, and graphics list to disk.
        Args:
            dir_list (dict): A dictionary containing project directories.
            local_vars (dict): A dictionary containing local variables. Default is locals().
            global_vars (dict): A dictionary containing global variables. Default is globals().
        Returns:
            None
        Raises:
            FileNotFoundError: If the specified directories do not exist.
        Example:
            >>> save_to_disk(prj_dirs)
        Notes:
            This function saves the data frames, codebook, and graphics list to disk.
        """

        print("1. Saving the data frames to disk")

        # Save the raw data frames to disk
        df_names = [
            "crashes",
            "crashes_agp",
            "parties",
            "parties_agp",
            "victims",
            "victims_agp",
            "collisions",
            "collisions_agp",
            "cities",
            "roads",
            "boundaries",
            "blocks",
        ]

        # Check if the data frames exist in the local or global scope
        for name in df_names:
            if name in local_vars:
                print("  - Saving the", name, "data frame:", f"{name}.pkl")
                with open(os.path.join(dir_list["data_python"], f"{name}.pkl"), "wb") as f:
                    pickle.dump(local_vars[name], f)
            elif name in global_vars:
                print("  - Saving the", name, "data frame:", f"{name}.pkl")
                with open(os.path.join(dir_list["data_python"], f"{name}.pkl"), "wb") as f:
                    pickle.dump(global_vars[name], f)

        print("2. Saving the Codebook and Reference Tables to disk")

        # Save the codebook to disk
        if "cb" in local_vars:
            print("  - Saving the codebook to disk")
            with open(os.path.join(dir_list["codebook"], "cb.pkl"), "wb") as f:
                pickle.dump(local_vars["cb"], f)
        elif "cb" in global_vars:
            print("  - Saving the codebook to disk")
            with open(os.path.join(dir_list["codebook"], "cb.pkl"), "wb") as f:
                pickle.dump(global_vars["cb"], f)

        # Save the codebook reference table to disk
        if "df_cb" in local_vars:
            print("  - Saving the codebook reference table to disk")
            with open(os.path.join(dir_list["codebook"], "df_cb.pkl"), "wb") as f:
                pickle.dump(local_vars["df_cb"], f)
        elif "df_cb" in global_vars:
            print("  - Saving the codebook reference table to disk")
            with open(os.path.join(dir_list["codebook"], "df_cb.pkl"), "wb") as f:
                pickle.dump(global_vars["df_cb"], f)
        
        print("3. Export the TIMS Metadata to disk")
        
        # Save the TIMS metadata to disk
        if "prj_meta" in local_vars:
            print("  - Exporting the TIMS metadata to disk")
            self.export_tims_metadata(local_vars["prj_meta"])

        print("4. Saving the Time Series Data Frames to disk")

        # Save the time series data frames to disk
        ts_names = ["ts_year", "ts_quarter", "ts_month", "ts_week", "ts_day"]
        for name in ts_names:
            if name in local_vars:
                print("  - Saving the", name, "data frame:", f"{name}.pkl")
                with open(os.path.join(dir_list["data_python"], f"{name}.pkl"), "wb") as f:
                    pickle.dump(local_vars[name], f)
            elif name in global_vars:
                print("  - Saving the", name, "data frame:", f"{name}.pkl")
                with open(os.path.join(dir_list["data_python"], f"{name}.pkl"), "wb") as f:
                    pickle.dump(global_vars[name], f)

        # Save the graphics list to disk
        if "graphics_list" in local_vars:
            print("4. Saving the Graphics data to disk")
            with open(os.path.join(dir_list["data_python"], "graphics_list.pkl"), "wb") as f:
                pickle.dump(local_vars["graphics_list"], f)
        elif "graphics_list" in global_vars:
            print("4. Saving the Graphics data to disk")
            with open(os.path.join(dir_list["data_python"], "graphics_list.pkl"), "wb") as f:
                pickle.dump(global_vars["graphics_list"], f)


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 10. Graphics Entry Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def graphics_entry(
        self, gr_type: int, gr_id: int, gr_attr: dict, gr_list: Optional[dict] = None, gr_dirs: Optional[dict] = None
    ) -> dict:
        """
        Adds a table or graphic entry to the specified list.
        Args:
            gr_type (int): The type of entry (1 = Table, 2 = Graphic).
            gr_id (int): The entry id.
            gr_attr (dict): A dictionary of attributes for the entry.
            gr_list (dict): The dictionary to add the entry to. Should have 'tables' and 'graphics' keys.
            gr_dirs (dict, optional): Project directories, required for graphics.
        Returns:
            dict: The updated list_name dictionary.
        Raises:
            ValueError: If entry_type is not 1 or 2.
        Examples:
            >>> tables_graphics = {"tables": [], "graphics": []}
            >>> attr = {"name": "Summary", "description": "desc", "caption": "cap", "method": "describe", "fileFormat": "csv", "file": "summary.csv", "status": "draft"}
            >>> graphics_entry(tables_graphics, 1, 1, attr)
        Notes:
            This function adds a table or graphic entry to the specified list.
        """
        # Check if the gr_list is empty or not
        if not gr_list or not isinstance(gr_list, dict):
            # Create a new graphics list if it is empty
            print("Creating a new graphics list.")
            gr_list = {"tables": {}, "graphics": {}}
        elif "tables" not in gr_list or "graphics" not in gr_list:
            raise ValueError("gr_list must contain 'tables' and 'graphics' keys.")
        else:
            # continue with the existing graphics list
            print("Using the provided graphics list.")

        # Get the number of tables and graphics in the list
        nt = len(gr_list["tables"])
        ng = len(gr_list["graphics"])
        print(f"Number of tables: {nt}, Number of graphics: {ng}")

        # Get the entry type and id
        gr_type = int(gr_type)
        gr_id = int(gr_id)

        # Check if the entry type is valid
        if gr_type == 1:
            # check if eid is smaller or equal to nt
            if gr_id <= nt:
                print(f"Table {gr_id} already exists. Exiting function...")
                return gr_list
            else:
                # If it is a table, create a new table entry
                entry_id = f"tbl{nt + 1}"
                print(f"Creating new table entry with id: {entry_id}")
                gr_path = gr_dirs["graphics"] if gr_dirs is not None and "graphics" in gr_dirs else ""
                # Add the table fields from the attributes list provided
                # for the table entry the attributes must have the following entries provided: name, description, caption, method, fileFormat, file, status
                entry_table = {
                    "id": f"tbl{nt + 1}",
                    "category": "Table",
                    "category_no": nt + 1,
                    "name": str(gr_attr.get("name", "")),
                    "description": str(gr_attr.get("description", "")),
                    "caption": str(gr_attr.get("caption", "")),
                    "type": "Table",
                    "method": str(gr_attr.get("method", "")),
                    "path": os.path.join(
                        gr_path,
                        f"tbl{nt + 1}_{gr_attr.get('file', '').lower().replace(' ', '_')}"
                        + str(gr_attr.get("file_format", "")),
                    ),
                    "file_format": str(gr_attr.get("file_format", "")),
                    "file": f"tbl{nt + 1}_{gr_attr.get('file', '').lower().replace(' ', '_')}",
                    "status": str(gr_attr.get("status", "")),
                    "date": str(datetime.date.today()),
                }
                gr_list["tables"][entry_id] = entry_table
        elif gr_type == 2:
            # Check if the entry is a graphic
            # For the graphic entry the attributes must have the following entries provided: category, name, description, caption, type, method, path, fileFormat, file, resolution, width, height, status
            # check if eid is smaller or equal to ng
            if gr_id <= ng:
                print(f"Figure {gr_id} already exists. Exiting function...")
                return gr_list
            else:
                # If it is a graphic, create a new graphic entry
                entry_id = f"fig{ng + 1}"
                gr_path = gr_dirs["graphics"] if gr_dirs is not None and "graphics" in gr_dirs else ""
                entry_graphic = {
                    "id": f"fig{ng + 1}",
                    "category": str(gr_attr.get("category", "")),
                    "category_no": ng + 1,
                    "name": str(gr_attr.get("name", "")),
                    "description": str(gr_attr.get("description", "")),
                    "caption": str(gr_attr.get("caption", "")),
                    "type": str(gr_attr.get("type", "")),
                    "method": str(gr_attr.get("method", "")),
                    "path": os.path.join(
                        gr_path,
                        f"fig{ng + 1}_{gr_attr.get('file', '').lower().replace(' ', '_')}"
                        + str(gr_attr.get("file_format", "")),
                    ),
                    "file_format": str(gr_attr.get("file_format", "")),
                    "file": f"fig{ng + 1}_{gr_attr.get('file', '').lower().replace(' ', '_')}",
                    "resolution": 300,
                    "width": 12,
                    "height": 8,
                    "status": str(gr_attr.get("status", "")),
                    "date": str(datetime.date.today()),
                }
                gr_list["graphics"][entry_id] = entry_graphic
        else:
            raise ValueError("entry_type must be 1 (Table) or 2 (Graphic).")

        # Print the entry name
        print(f"Graphics List:\n {gr_list}")
        # Return the updated list_name
        return gr_list


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 11. Chi-squared Independence Test Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def chi2_test(self, df: pd.DataFrame, col1: str, col2: str) -> dict:
        """
        Perform a Chi-squared test of independence on two categorical variables.
        Args:
            df (pd.DataFrame): The DataFrame containing the data.
            col1 (str): The name of the first categorical column.
            col2 (str): The name of the second categorical column.
        Returns:
            dict: A dictionary containing the test name, statistic, p-value, p-value display, and number of observations.
        Raises:
            ValueError: If df is not a pandas DataFrame.
            ValueError: If col1 or col2 is not a string.
        Example:
            >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})
            >>> chi2_test(df, "A", "B")
        Notes:
            This function performs a Chi-squared test of independence on two categorical variables.
        """
        contingency_table = pd.crosstab(df[col1], df[col2])
        test = stats.chi2_contingency(contingency_table)
        t = "Chi-squared test of independence"
        s = test.statistic
        p = test.pvalue
        d = self.p_value_display(test.pvalue)
        n = len(df)
        return {"test": t, "statistic": s, "p-value": p, "p-value_display": d, "observations": n}


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 12. Chi-squared Goodness-of-fit Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def chi2_gof_test(self, df: pd.DataFrame, col: str) -> dict:
        """
        Perform a Chi-squared Goodness-of-Fit test on a categorical variable.
        Args:
            df (pd.DataFrame): The DataFrame containing the data.
            col (str): The name of the categorical column.
        Returns:
            dict: A dictionary containing the test name, statistic, p-value, p-value display, and number of observations.
        Raises:
            ValueError: If df is not a pandas DataFrame.
            ValueError: If col is not a string.
        Example:
            >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})
            >>> chi2_gof_test(df, "A")
        Notes:
            This function performs a Chi-squared Goodness-of-Fit test on a categorical variable.
        """
        # Create observed and expected counts for the df[col] column
        observed = df[col].value_counts().values
        expected = np.full_like(observed, dtype = np.float64, fill_value = np.mean(observed))
        stat = stats.chisquare(f_obs = observed, f_exp = expected)
        t = "Chi-squared Goodness-of-Fit test"
        s = stat.statistic
        p = stat.pvalue
        d = self.p_value_display(stat.pvalue)
        n = len(df)
        return {"test": t, "statistic": s, "p-value": p, "p-value_display": d, "observations": n}


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 13. Kruskal-Wallis H-test Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def kruskal_test(self, df: pd.DataFrame, col1: str, col2: str) -> dict:
        """
        Perform a Kruskal-Wallis H-test for independent samples on VictimCount grouped by Severity.
        Args:
            df (pd.DataFrame): The DataFrame containing 'victim_count' and 'severity' columns.
            col1 (str): The name of the column containing the values to compare (e.g., 'victim_count').
            col2 (str): The name of the column containing the grouping variable (e.g., 'severity').
        Returns:
            dict: A dictionary containing the test name, statistic, p-value, p-value display, and number of observations.
        Raises:
            ValueError: If df is not a pandas DataFrame.
            ValueError: If col1 or col2 is not a string.
            KeyError: If required columns are missing.
        Examples:
            >>> kruskal_test(df, "victim_count", "severity")
        Notes:
            This function performs a Kruskal-Wallis H-test for independent samples on VictimCount grouped by Severity.
        """
        groups = [group[col1].values for name, group in df.groupby(col2, observed = True)]
        test = stats.kruskal(*groups)
        t = "Kruskal-Wallis H-test"
        s = test.statistic
        p = test.pvalue
        d = self.p_value_display(test.pvalue)
        n = len(df)
        return {"test": t, "statistic": s, "p-value": p, "p-value_display": d, "observations": n}


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 14. P-Value Display Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def p_value_display(self, p_value: float) -> str:
        """
        Displays the p-value in a readable format.
        Args:
            p_value (float): The p-value to display.
        Returns:
            str: Formatted p-value string.
        Raises:
            ValueError: If p_value is not a float.
        Examples:
            >>> p_value_display_v2(0.0005)
            '<0.001'
            >>> p_value_display_v2(0.005)
            '<0.01'
            >>> p_value_display_v2(0.02)
            '<0.05'
            >>> p_value_display_v2(0.08)
            '0.08'
        Notes:
            This function displays the p-value in a readable format.
        """
        if p_value < 0.001:
            return "<0.001"
        elif 0.001 <= p_value < 0.01:
            return "<0.01"
        elif 0.01 <= p_value < 0.05:
            return "<0.05"
        else:
            return f"{p_value:.2f}"


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 15. Create STL Plot Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def create_stl_plot(self, time_series, season, model = "additive", label = None, covid = False, robust = True) -> tuple:
        """
        Create a Seasonal-Trend decomposition using LOESS (STL).
        Args:
            time_series (pandas.Series): Time series data with DatetimeIndex
            season (str): The periodicity of the data (quarterly, monthly, weekly, daily)
            model (str): Type of model to use ('additive' or 'multiplicative')
            label (str): Label for the time series (optional)
            covid (bool): Whether to show COVID-19 period annotation (March 2020 - March 2022)
            robust (bool): Whether to use the robust estimation (helps with outliers)
        Returns:
            tuple: (decomposition_result, figure)
        Raises:
            ValueError: If season is not one of the specified options.
        Example:
            >>> decomposition, fig = create_stl_plot(ts, 'monthly', robust=True)
        Notes:
            This function creates a Seasonal-Trend decomposition using LOESS (STL).
        """
        # Check the seasonality and set the period and model accordingly
        period = None
        footnote = None
        match season:
            case "quarterly":
                period = 4
                footnote = "Note: Data reflect quarterly time series (seasonality set to 4 periods per year)."
            case "monthly":
                period = 12
                footnote = "Note: Data reflect monthly time series (seasonality set to 12 periods per year)."
            case "weekly":
                period = 52
                footnote = "Note: Data reflect weekly time series (seasonality set to 52 periods per year)."
            case "daily":
                period = 365
                footnote = "Note: Data reflect daily time series (seasonality set to 365 periods per year)."
            case "-":
                raise ValueError("Seasonality must be one of: quarterly, monthly, weekly, daily.")

        # set the title for the original time series
        original_title = f"Original Time Series: {label}" if label else "Original Time Series"  # Perform STL decomposition
        if robust:
            # Use STL for robust decomposition
            stl = sm.tsa.STL(time_series, period = period)
            decomposition = stl.fit()
        else:
            # Use seasonal_decompose for non-robust decomposition
            decomposition = sm.tsa.seasonal_decompose(
                time_series, period = period, model = model
            )  # Create figure with subplots for original, trend, seasonal, and residual
        fig = plt.figure(figsize = (12, 10))  # Plot original time series
        ax1 = plt.subplot(411)
        ax1.plot(time_series, color = "royalblue")
        ax1.set_title(original_title, fontweight = "bold")
        # Format y-axis values based on their magnitude
        ax1.yaxis.set_major_formatter(
            plt.FuncFormatter(
                lambda x, loc: (
                    "{:,.0f}".format(x) if abs(x) == 0 else "{:,.2f}".format(x) if abs(x) < 1 else "{:,.0f}".format(x)
                )
            )
        )
        # Remove top and right spines
        ax1.spines["top"].set_visible(False)
        ax1.spines["right"].set_visible(False)  # Plot trend component
        ax2 = plt.subplot(412)
        ax2.plot(
            decomposition.trend, color = "brown", linewidth = 3
        )  # Increased line thickness
        ax2.set_title("Trend Component", fontweight = "bold")
        # Format y-axis values based on their magnitude
        ax2.yaxis.set_major_formatter(
            plt.FuncFormatter(
                lambda x, loc: (
                    "{:,.0f}".format(x) if abs(x) == 0 else "{:,.2f}".format(x) if abs(x) < 1 else "{:,.0f}".format(x)
                )
            )
        )
        # Remove top and right spines
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_visible(False)  # Plot seasonal component
        ax3 = plt.subplot(413)
        ax3.plot(decomposition.seasonal, color = "darkgreen")
        ax3.set_title("Seasonal Component", fontweight = "bold")
        # Format y-axis values based on their magnitude
        ax3.yaxis.set_major_formatter(
            plt.FuncFormatter(
                lambda x, loc: (
                    "{:,.0f}".format(x) if abs(x) == 0 else "{:,.2f}".format(x) if abs(x) < 1 else "{:,.0f}".format(x)
                )
            )
        )
        # Remove top and right spines
        ax3.spines["top"].set_visible(False)
        ax3.spines["right"].set_visible(False)

        # Plot residual component
        ax4 = plt.subplot(414)
        ax4.plot(decomposition.resid, color = "purple")
        ax4.set_title("Residual Component", fontweight = "bold")
        # Format y-axis values based on their magnitude
        ax4.yaxis.set_major_formatter(
            plt.FuncFormatter(
                lambda x, loc: (
                    "{:,.0f}".format(x) if abs(x) == 0 else "{:,.2f}".format(x) if abs(x) < 1 else "{:,.0f}".format(x)
                )
            )
        )
        # Remove top and right spines
        ax4.spines["top"].set_visible(False)
        ax4.spines["right"].set_visible(False)
        # Ensure time axes are aligned across all subplots
        fig.subplots_adjust(hspace = 0.3)
        for ax in [ax1, ax2, ax3, ax4]:
            ax.set_xlim(time_series.index.min(), time_series.index.max())

        # Add COVID-19 period annotation if requested
        if covid:
            # Define COVID-19 period
            covid_start = pd.to_datetime("2020-03-01")
            covid_end = pd.to_datetime("2022-03-01")
            covid_start_num = float(mdates.date2num(covid_start))
            covid_end_num = float(mdates.date2num(covid_end))

            # First pass to adjust y-limits to fit all data
            for ax in [ax1, ax2, ax3, ax4]:
                # Force matplotlib to calculate the limits
                ax.relim()
                ax.autoscale_view()

            # Add the COVID-19 reference boxes to each subplot
            for ax in [ax1, ax2, ax3, ax4]:
                # Get the ylim after we've forced matplotlib to calculate them
                ymin, ymax = ax.get_ylim()
                # Add the shaded region with proper height
                rect = Rectangle(
                    (covid_start_num, ymin),  # Start at the bottom of the plot
                    covid_end_num - covid_start_num,
                    ymax - ymin,  # Span the entire height
                    facecolor = "green",
                    alpha = 0.2,
                    zorder = 0,  # Ensure it's behind the data
                )
                ax.add_patch(rect)

                # Add the reference lines
                ax.axvline(x = covid_start_num, linewidth = 0.5, linestyle = "dashed", color = "darkgreen")
                ax.axvline(x = covid_end_num, linewidth = 0.5, linestyle = "dashed", color = "darkgreen")

            # Add the annotation text only to the first subplot
            covid_mid_dt = covid_start + (covid_end - covid_start) / 2
            covid_mid_num = float(mdates.date2num(covid_mid_dt))
            ax1.annotate(
                "COVID-19\nRestrictions",
                xy = (covid_mid_num, ax1.get_ylim()[1] * 0.85),
                xycoords = "data",
                ha = "center",
                fontsize = 10,
                fontweight = "bold",
                fontstyle = "italic",
                color = "darkgreen",
            )

        # Adjust layout
        plt.tight_layout()

        # Add footnote at the bottom left of the plot
        # if footnote is a single string, add it to the figure
        if footnote is not None and isinstance(footnote, str):
            fig.text(0.01, 0, footnote, fontsize = 10, style = "italic", ha = "left")

        # Return the decomposition result and the figure
        return decomposition, fig
    

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 16. Format Collision Time ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def format_coll_time(self, x: int) -> str:
        """Convert the coll_time to a formatted time string in HH:MM:SS format.
        Args:
            x (int): The collision time in seconds.
        Returns:
            str: The formatted time string in HH:MM:SS format.
        Raises:
            ValueError: If x is not an integer.
            ValueError: If x is not a positive integer.
            ValueError: If x is not less than 2400.
        Examples:
            >>> format_coll_time(3600)
            '01:00:00'
            >>> format_coll_time(2400)
            '00:00:00'
        Notes:
            This function converts the collision time in seconds to a formatted time string in HH:MM:SS format.
        """
        # Set time_out to a default value
        time_out = "00:00:00"
        # if x has only one digit
        if len(str(x)) == 1:
            time_out = f"00:0{str(x)}:00"
        # if x has two digits
        elif len(str(x)) == 2:
            time_out = f"00:{str(x)}:00"
        # if x has three digits
        elif len(str(x)) == 3:
            time_out = f"0{str(x)[0]}:{str(x)[1:]}:00"
        # if x has four digits and is less than 2400
        elif len(str(x)) == 4 and int(str(x)) < 2400:
            time_out = f"{str(x)[:2]}:{str(x)[2:]}:00"
        else:
            time_out = "00:00:00"
        # Return the formatted time string
        return time_out
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 17. Quarter to Date ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def quarter_to_date(self, row: pd.Series, ts: bool = True) -> pd.Timestamp:
        """Convert the quarter to a datetime object representing the first day of the quarter.
        Args:
            row (pd.Series): The row of the DataFrame containing the dt_year and dt_quarter columns.
            ts (bool, optional): If True, return a Timestamp object. If False, return a quarter string. Defaults to True.
        Returns:
            pd.Timestamp or str: The datetime object or quarter string representing the first day of the quarter.
        Raises:
            ValueError: If dt_year or dt_quarter is missing or invalid.
        Examples:
            >>> quarter_to_date(pd.Series({"dt_year": 2023, "dt_quarter": 1}))
            Timestamp('2023-01-01 00:00:00')
            >>> quarter_to_date(pd.Series({"dt_year": 2023, "dt_quarter": 1}), ts=False)
            '2023-Q1'
        Notes:
            This function converts the quarter to a datetime object representing the first day of the quarter.
        """
        if pd.isna(row["dt_year"]) or pd.isna(row["dt_quarter"]):
            return pd.NaT
        # Map quarter to month (1→Jan, 2→Apr, 3→Jul, 4→Oct)
        month = 1 + (row["dt_quarter"] - 1) * 3
        if ts:
            # Return a Timestamp object for the first day of the quarter
            return pd.Timestamp(year = row["dt_year"], month = month, day = 1)
        elif not ts:
            # Return a quarter string (e.g., "2023-Q1")
            return f"{int(row['dt_year'])}-Q{int(row['dt_quarter'])}"
        else:
            raise ValueError("ts must be True or False")
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 18. Get Collision Severity Rank ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def get_coll_severity_rank(self, row: pd.Series) -> int:
        """Get the severity rank of a collision based on the number of killed and severe injured.
        Args:
            row (pd.Series): The row of the DataFrame containing the number_killed and count_severe_inj columns.
        Returns:
            int: The severity rank of the collision.
        Raises:
            ValueError: If number_killed or count_severe_inj is missing or invalid.
        Examples:
            >>> get_coll_severity_rank(pd.Series({"number_killed": 0, "count_severe_inj": 0}))
            0
            >>> get_coll_severity_rank(pd.Series({"number_killed": 0, "count_severe_inj": 1}))
            1
            >>> get_coll_severity_rank(pd.Series({"number_killed": 0, "count_severe_inj": 2}))
            2
        Notes:
            This function returns the severity rank of a collision based on the number of killed and severe injured.
        """
        if row["number_killed"] == 0 and row["count_severe_inj"] == 0:
            return 0
        elif row["number_killed"] == 0 and row["count_severe_inj"] == 1:
            return 1
        elif row["number_killed"] == 0 and row["count_severe_inj"] > 1:
            return 2
        elif row["number_killed"] == 1 and row["count_severe_inj"] == 0:
            return 3
        elif row["number_killed"] == 1 and row["count_severe_inj"] == 1:
            return 4
        elif row["number_killed"] == 1 and row["count_severe_inj"] > 1:
            return 5
        elif row["number_killed"] > 1 and row["count_severe_inj"] == 0:
            return 6
        elif row["number_killed"] > 1 and row["count_severe_inj"] == 1:
            return 7
        elif row["number_killed"] > 1 and row["count_severe_inj"] > 1:
            return 8
        else:
            return np.nan
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 19. Get Counts by Year ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def counts_by_year(self, df: pd.DataFrame, year: int) -> int:
        """
        Get data counts for a specific year.
        
        Args:
            df (pd.DataFrame): DataFrame containing collision data with a 'date_datetime' column.
            year (int): The year to filter data by.
        
        Returns:
            int: The number of valid rows in the specified year.
        Raises:
            ValueError: If date_datetime is missing or invalid.
        Examples:
            >>> counts_by_year(pd.DataFrame({"date_datetime": pd.to_datetime(["2023-01-01", "2024-01-01"])}), 2023)
            1
        Notes:
            This function returns the number of valid rows in the specified year.
        """
        return len(df[df['date_datetime'].dt.year == year].copy())
    

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 20. Time Series Aggregate ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def ts_aggregate(self, dt: str, df: pd.DataFrame, cb: dict) -> pd.DataFrame:
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
            ts_year_crashes = ts_aggregate(dt = "date_year", df = crashes, cb = cb)
        Notes:
            This function aggregates a dataframe by a specified date column and returns a new dataframe with aggregated statistics.
        """
        # Helper function to aggregate a dataframe by a specified date column
        def _aggregate_helper(cols, agg_type, suffix):
            agg_df = ts[[dt] + cols].copy()
            for col in cols:
                if col in agg_df.columns:
                    agg_df.rename(columns = {col: f"{col}_{suffix}"}, inplace = True)
            for col in agg_df.columns:
                if agg_df[col].dtype.name == "category":
                    agg_df[col] = agg_df[col].cat.codes
            agg_df = agg_df.groupby(dt).agg(agg_type).reset_index()
            return agg_df

        # Get the name of the dataframe
        df_name = df.attrs["name"]
        ts = df.copy()
        
        # Get the list of columns to aggregate
        df_list_sum = [col for col in df.columns if cb[col]["ts"][df_name] == 1 and cb[col]["stats"]["sum"] == 1]
        df_list_mean = [
            col for col in df.columns if cb[col]["ts"][df_name] == 1 and cb[col]["stats"]["mean"] == 1
        ]
        df_list_median = [
            col for col in df.columns if cb[col]["ts"][df_name] == 1 and cb[col]["stats"]["median"] == 1
        ]
        df_list = list(set(df_list_sum + df_list_mean + df_list_median))
        ta = _aggregate_helper(df_list_sum, "sum", "sum")
        tb = _aggregate_helper(df_list_mean, "mean", "mean")
        tc = _aggregate_helper(df_list_median, "median", "median")
        td = _aggregate_helper(df_list, "min", "min")
        te = _aggregate_helper(df_list, "max", "max")
        tf = _aggregate_helper(df_list, "std", "sd")
        tg = _aggregate_helper(df_list, "sem", "se")
        ts_aggregated = ta.merge(tb, on = dt, how = "outer")
        ts_aggregated = ts_aggregated.merge(tc, on = dt, how = "outer")
        ts_aggregated = ts_aggregated.merge(td, on = dt, how = "outer")
        ts_aggregated = ts_aggregated.merge(te, on = dt, how = "outer")
        ts_aggregated = ts_aggregated.merge(tf, on = dt, how = "outer")
        ts_aggregated = ts_aggregated.merge(tg, on = dt, how = "outer")
        
        # Sort the aggregated dataframe by the date column
        ts_aggregated.sort_values(by = dt, inplace = True)
        
        # Return the aggregated dataframe
        return ts_aggregated
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 21. Plot a histogram of victim counts ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def plot_victim_count_histogram(self, df: pd.DataFrame, fig = None, ax = None) -> tuple:
        """
        Plots a histogram for the top 10 victim frequency counts.
        Args:
            df (pd.DataFrame): DataFrame containing a 'victim_count' column.
            fig (matplotlib.figure.Figure, optional): Figure object to plot on.
            ax (matplotlib.axes.Axes, optional): Axes object to plot on.
        Returns:
            tuple: (fig, ax) Figure and Axes objects with the plot.
        Raises:
            KeyError: If 'victim_count' column is not present in df.
        Examples:
            plot_victim_count_histogram(df)
        Notes:
            This function plots a histogram for the top 10 victim frequency counts.
        """
        if "victim_count" not in df.columns:
            raise KeyError("The DataFrame must contain a 'victim_count' column.")

        filtered = df[(df["victim_count"] >= 1) & (df["victim_count"] <= 10)]

        if fig is None or ax is None:
            fig, ax = plt.subplots(figsize = (12, 8))
        sns.histplot(
            filtered["victim_count"],
            bins = 10,
            binrange = (1, 11),
            color = "darkred",
            edgecolor = "white",
            linewidth = 1,
            discrete = True,
            ax = ax,
        )

        # Annotate bars with counts
        for p in ax.patches:
            if hasattr(p, "get_height"):
                height = p.get_height()
                if height > 0:
                    ax.annotate(
                        f"{int(height):,}",
                        (p.get_x() + p.get_width() / 2, height),
                        ha = "center",
                        va = "bottom",
                        fontsize = 16,
                        fontweight = "normal",
                    )

        ax.set_xlabel("Victim Count", fontsize = 20, color = "black")
        ax.set_ylabel("Number of Victims in Crash Incidents", fontsize = 20, color = "black")
        ax.set_title("")
        ax.set_xticks(range(1, 11))
        ax.set_yticks(range(0, 120001, 20000))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax.tick_params(axis = "x", labelsize = 18, colors = "black")
        ax.tick_params(axis = "y", labelsize = 18, colors = "black")
        fig.text(0.01, 0.01, "Note: Top 10 victim frequency counts", ha = "left", fontsize = 16, color = "black", style = "italic")
        sns.despine()
        fig.tight_layout()
        return fig, ax


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 22. Plot a bar graph of collision types ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def plot_collision_type_bar(self, df: pd.DataFrame, fig = None, ax = None) -> tuple:
        """
        Plots a bar graph of collision types from the crashes DataFrame.
        Args:
            df (pd.DataFrame): DataFrame containing a 'type_of_coll' column.
            fig (matplotlib.figure.Figure, optional): Figure object to plot on.
            ax (matplotlib.axes.Axes, optional): Axes object to plot on.
        Returns:
            tuple: (fig, ax) Figure and Axes objects with the plot.
        Raises:
            KeyError: If 'type_of_coll' column not found in DataFrame.
        Examples:
            >>> plot_collision_type_bar(df)
        Notes:
            This function plots a bar graph of collision types from the crashes DataFrame.
        """
        if "type_of_coll" not in df.columns:
            raise KeyError("'type_of_coll' column not found in DataFrame.")
        # Prepare the data for plotting
        fig2_data = df["type_of_coll"].value_counts().reset_index()
        fig2_data.columns = ["CollisionType", "Count"]
        fig2_data = fig2_data[fig2_data["CollisionType"] != "Not Stated"]
        fig2_data = fig2_data.sort_values("Count", ascending = False)
        fig2_data["CollisionType"] = fig2_data["CollisionType"].astype(str)
        fig2_data["CollisionType_wrapped"] = fig2_data["CollisionType"].apply(
            lambda x: "\n".join(textwrap.wrap(x, width=10))
        )
        if fig is None or ax is None:
            fig, ax = plt.subplots(figsize = (12, 8))
        sns.barplot(
            data = fig2_data,
            x = "CollisionType_wrapped",
            y = "Count",
            hue = "CollisionType_wrapped",
            palette = "Dark2",
            legend = True,
            ax = ax,
        )
        for i, count in enumerate(fig2_data["Count"]):
            ax.text(i, count, f"{count:,}", ha = "center", va = "bottom", fontsize = 16)
        ax.set_xlabel("Crash Type", fontsize = 20)
        ax.set_ylabel("Number of Collisions", fontsize = 20)
        ax.set_title("Type of Collision by Count", fontsize = 22)
        plt.xticks(rotation = 0, ha = "center", fontsize = 18)
        plt.yticks(fontsize = 18)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax.yaxis.grid(True, linestyle = ":", color = "gray", zorder = 0)
        ax.set_axisbelow(True)
        fig.tight_layout()
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(True)
        ax.spines["bottom"].set_visible(True)
        legend = ax.legend(title = "Crash Type", fontsize = 16, title_fontsize = 18, ncol = 1)
        if legend is not None:
            legend.get_frame().set_edgecolor("white")
            legend.get_frame().set_facecolor("white")
        return fig, ax
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 23. Plot a stacked bar chart of fatalities by type and year ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def plot_fatalities_by_type_and_year(self, df: pd.DataFrame, fig = None, ax = None) -> tuple:
        """Plot a stacked bar chart of fatalities by type and year.
        Args:
            df (pd.DataFrame): DataFrame with columns ["Year", "Type", "Killed"]
            fig (matplotlib.figure.Figure, optional): Figure object to plot on.
            ax (matplotlib.axes.Axes, optional): Axes object to plot on.
        Returns:
            tuple: (fig, ax) Figure and Axes objects with the plot.
        Raises:
            ValueError: If required columns are missing.
        Examples:
            plot_fatalities_by_type_and_year(fig3_data)
        Notes:
            This function plots a stacked bar chart of fatalities by type and year.
        """
        fig3_data = df[
            ["date_year", "count_car_killed_sum", "count_ped_killed_sum", "count_bic_killed_sum", "count_mc_killed_sum"]
        ].copy()
        fig3_data["date_year"] = fig3_data["date_year"].dt.year
        fig3_data = fig3_data.rename(
            columns = {
                "date_year": "Year",
                "count_car_killed_sum": "Car",
                "count_ped_killed_sum": "Pedestrian",
                "count_bic_killed_sum": "Bicycle",
                "count_mc_killed_sum": "Motorcycle",
            }
        )
        fig3_data = fig3_data[["Year", "Car", "Pedestrian", "Bicycle", "Motorcycle"]]
        fig3_data = pd.melt(
            fig3_data,
            id_vars = ["Year"],
            value_vars = ["Car", "Pedestrian", "Bicycle", "Motorcycle"],
            var_name = "Type",
            value_name = "Killed",
        )
        pivot_df = fig3_data.pivot(index = "Year", columns = "Type", values = "Killed").fillna(0)
        pivot_df = pivot_df.sort_index()
        palette = sns.color_palette("Set2")
        if fig is None or ax is None:
            fig, ax = plt.subplots(figsize = (12, 8))
        pivot_df.plot(kind = "bar", stacked = True, color = palette, edgecolor = "black", width = 0.9, ax = ax)
        for c_idx, col in enumerate(pivot_df.columns):
            if c_idx > 0:
                y_offset = pivot_df.iloc[:, :c_idx].sum(axis = 1)
            else:
                y_offset = pd.Series([0] * len(pivot_df), index = pivot_df.index)
            for i, (y, val) in enumerate(zip(y_offset, pivot_df[col])):
                if val > 0:
                    ax.text(i, y + val / 2, int(val), ha = "center", va = "center", fontsize = 13)
        ax.set_xlabel("Year", fontsize = 18)
        ax.set_ylabel("Number of Fatalities", fontsize = 18)
        ax.tick_params(axis = "y", labelsize = 16)
        ax.set_title("Number of Fatalities by Type and Year", fontsize = 20, fontweight = "normal")
        ax.legend(title = None, loc = "upper left", bbox_to_anchor = (0, 1.0), ncol = pivot_df.shape[1], fontsize = 14, frameon = True)
        legend = ax.get_legend()
        legend.get_frame().set_edgecolor("white")
        legend.get_frame().set_facecolor("white")
        ax.set_xticklabels(ax.get_xticklabels(), rotation = 0, fontsize = 16)
        ax.grid(axis = "y", linestyle = "--", alpha = 0.7)
        fig.text(
            0.01,
            0.01,
            "Notes: (a) Stacked bars of number of fatal accidents; (b) bar labels represent category counts",
            ha = "left",
            fontsize = 14,
            style = "italic",
        )
        fig.tight_layout(rect = (0, 0.05, 1, 1))
        return fig, ax
    

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 24. Compute monthly statistics ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def compute_monthly_stats(self, ts_month: pd.DataFrame) -> pd.DataFrame:
        """
        Compute monthly statistics for the input DataFrame.
        Args:
            ts_month (pd.DataFrame): Input DataFrame with monthly data.
        Returns:
            pd.DataFrame: DataFrame with computed statistics and additional columns.
        Raises:
            ValueError: If input is not a DataFrame.
        Examples:
            >>> df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> compute_monthly_stats(df)
        Notes:
            This function computes monthly statistics for the input DataFrame.
        """
        if not isinstance(ts_month, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")

        # Only use numeric columns for statistics
        numeric_cols = ts_month.select_dtypes(include = [np.number]).columns
        ts_month_numeric = ts_month[numeric_cols]

        desc = ts_month_numeric.describe(percentiles = [0.5, 0.95]).T
        desc["sum"] = ts_month_numeric.sum()
        desc["var"] = ts_month_numeric.var()
        # Prevent division by zero for coef.var
        mean_nozero = desc["mean"].where(desc["mean"] != 0, np.nan)
        desc["coef.var"] = desc["std"] / mean_nozero
        desc["skewness"] = ts_month_numeric.skew()
        desc["kurtosis"] = ts_month_numeric.kurtosis()
        desc["nbr.null"] = ts_month_numeric.isnull().sum()
        desc["nbr.na"] = ts_month_numeric.isna().sum()
        desc["nbr.val"] = ts_month_numeric.count()
        desc["range"] = desc["max"] - desc["min"]
        # Prevent division by zero for SE.mean
        desc["SE.mean"] = desc["std"] / desc["count"].apply(lambda x: np.sqrt(x) if pd.notnull(x) and x > 0 else np.nan)
        # CI.mean.0.95 only where count > 1 and SE.mean is not NaN
        ci_mask = (desc["count"] > 1) & desc["count"].notna() & desc["SE.mean"].notna()
        df_for_t = (desc["count"] - 1).where(ci_mask, np.nan).astype(float)
        t_val = stats.t.ppf(0.975, df_for_t)
        desc["CI.mean.0.95"] = (desc["mean"] + t_val * desc["SE.mean"]).where(ci_mask, np.nan)
        desc["normtest.W"] = ts_month_numeric.apply(lambda x: stats.shapiro(x.dropna())[0] if x.count() > 3 else np.nan)
        desc["normtest.p"] = ts_month_numeric.apply(lambda x: stats.shapiro(x.dropna())[1] if x.count() > 3 else np.nan)

        desc = desc.reset_index().rename(columns = {"index": "var.name", "std": "std.dev", "50%": "median"})

        def get_stat_type(var_name):
            if var_name.endswith("mean"):
                return "mean"
            if var_name.endswith("median"):
                return "median"
            if var_name.endswith("sum"):
                return "sum"
            if var_name.endswith("std.dev"):
                return "sd"
            if var_name.endswith("min"):
                return "min"
            if var_name.endswith("max"):
                return "max"
            if var_name.startswith("dt_"):
                return "datetime"
            return np.nan

        desc["stat.type"] = desc["var.name"].apply(get_stat_type)
        # Filter to only keep sum, mean, or median
        desc = desc[desc["stat.type"].isin(["sum", "mean", "median"])]

        col_order = [
            "stat.type",
            "var.name",
            "nbr.val",
            "nbr.null",
            "nbr.na",
            "sum",
            "min",
            "max",
            "range",
            "median",
            "mean",
            "SE.mean",
            "CI.mean.0.95",
            "std.dev",
            "var",
            "coef.var",
            "skewness",
            "skewness",
            "kurtosis",
            "kurtosis",
            "normtest.W",
            "normtest.p",
        ]
        # Remove duplicates in col_order
        col_order = list(dict.fromkeys(col_order))
        desc = desc[[col for col in col_order if col in desc.columns]]
        return desc

    
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Main ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    pass

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of File ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
