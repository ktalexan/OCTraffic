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
import os, datetime, pickle
import textwrap
from typing import Union, List, Optional
import json, pytz
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
from scipy import stats
import statsmodels.api as sm
from statsmodels.nonparametric.smoothers_lowess import lowess
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle, Patch
import seaborn as sns
import arcpy
import codebook.cbl as cbl


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Class containing OCTraffic data processing functions ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
class OCTraffic:
    """
    Class containing OCTraffic data processing functions.
    Attributes:
        None
    Methods:
        1. project_metadata(self, part: int, version: float, silent: bool = False) -> dict
        2. export_tims_metadata(self, metadata: dict) -> None
        3. update_tims_metadata(self, year: int, type: str = "reported", data_counts = None) -> None
        4. project_directories(self, base_path: str, silent: bool = False) -> dict
        5. load_cb(self) -> dict
        6. relocate_column(self, df: pd.DataFrame, col_name: Union[str, List[str]], ref_col_name: str, position: str = "after") -> pd.DataFrame
        7. categorical_series(self, var_series: pd.Series, var_name: str, cb_dict: dict) -> pd.Series
        8. is_dst(self, dt_series: pd.Series, tz_name: str = "America/Los_Angeles") -> pd.Series
        9. add_attributes(self, df: pd.DataFrame, cb: dict) -> pd.DataFrame
        10. save_to_disk(self, dir_list: dict, local_vars: dict = locals(), global_vars: dict = globals()) -> None
        11. graphics_entry(self, gr_type: int, gr_id: int, gr_attr: dict, gr_list: Optional[dict] = None, gr_dirs: Optional[dict] = None) -> None
        12. chi2_test(self, df: pd.DataFrame, col1: str, col2: str) -> dict
        13. chi2_gof_test(self, df: pd.DataFrame, col: str) -> dict
        14. kruskal_test(self, df: pd.DataFrame, col1: str, col2: str) -> dict
        15. p_value_display(self, p_value: float) -> str
        16. create_stl_plot(self, time_series, season, model="additive", label=None, covid=False, robust=True) -> tuple
        17. format_coll_time(self, x: int) -> str
        18. quarter_to_date(self, row: pd.Series, ts: bool = True) -> pd.Timestamp
        19. get_coll_severity_rank(self, row: pd.Series) -> int
        20. counts_by_year(self, df: pd.DataFrame, year: int) -> int
        21. ts_aggregate(self, dt: str, df: pd.DataFrame, cb: dict = cb) -> pd.DataFrame
        22. plot_victim_count_histogram(self, df: pd.DataFrame, fig: matplotlib.figure.Figure=None, ax: matplotlib.axes.Axes=None) -> tuple
        23. plot_collision_type_bar(self, df: pd.DataFrame, fig: matplotlib.figure.Figure=None, ax: matplotlib.axes.Axes=None) -> tuple
        24. plot_fatalities_by_type_and_year(self, df: pd.DataFrame, fig: matplotlib.figure.Figure=None, ax: matplotlib.axes.Axes=None) -> tuple
        25. compute_monthly_stats(self, ts_month: pd.DataFrame) -> pd.DataFrame
        26. create_monthly_fatalities_figure(self, ts_month: pd.DataFrame) -> tuple
        27. create_victims_severity_plot(self,data: pd.DataFrame, save_path: str = None, show_plot: bool = True) -> tuple
        28. create_age_pyramid_plot(self, collisions: pd.DataFrame) -> tuple
        29. export_cim(self, cim_type: str, cim_object: object, cim_name: str) -> None
        30. set_layer_time(self, layer: arcpy.mapping.Layer) -> None
        31. layout_configuration(self, nmf: int) -> dict
        32. delete_feature_class(self, fc_name: str, gdb_path: Optional[str] = None, dataset: Optional[str] = None) -> None
        33. load_aprx(self, add_to_map: bool = True) -> tuple
    Examples:
        >>> from octraffic import octraffic
        >>> ocs = octraffic()
        >>> ocs.load_aprx(add_to_map=True)
    """

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 0. Class initialization ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self, part: int, version: float):
        self.part = part
        self.version = version
        self.base_path = os.getcwd()

        # Create an prj_meta variable calling the function using the part and version variables from the initialization
        self.prj_meta = self.project_metadata(silent = False)

        # Create an prj_dir variable calling the function using the part and version variables from the initialization
        self.prj_dirs = self.project_directories(silent = False)

        # Load the codebook
        self.cb_path = os.path.join(self.prj_dirs["codebook"], "cb.json")
        self.cb, self.df_cb = self.load_cb(silent = False)


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 1. Project metadata function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def project_metadata(self, silent: bool = False) -> dict:
        """
        Function to generate project metadata for the OCTraffic data processing project.
        Args:
            silent (bool): If True, suppresses the print output. Default is False.
        Returns:
            metadata (dict): A dictionary containing the project metadata. The dictionary includes: name, title, description, version, author, years, date_start, date_end, date_updated, and TIMS metadata.
        Raises:
            ValueError: If part is not an integer, or if version is not numeric.
        Example:
            >>> metadata = self.project_metadata()
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
        with open(metadata_file, "r", encoding = "utf-8") as f:
            tims_metadata = json.load(f)
        
        # Get the first and last dates from the TIMS metadata
        start_date = datetime.date.fromisoformat(tims_metadata[list(tims_metadata.keys())[0]]["date_start"])
        end_date = datetime.date.fromisoformat(tims_metadata[list(tims_metadata.keys())[-1]]["date_end"])

        # Check if the part is integer
        if not isinstance(self.part, int):
            raise ValueError("Part must be an integer.")

        # Check if the version is numeric
        if not isinstance(self.version, (int, float)):
            raise ValueError("Version must be a number.")
        
        # Set dateUpdated to the current date
        date_updated = datetime.date.today()
        
        # Match the part to a specific step and description (with default case)
        match self.part:
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
            "version": self.version,
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
                f"\nProject Metadata:\n- Name: {metadata['name']}\n- Title: {metadata['title']}\n- Description: {metadata['description']}\n- Version: {metadata['version']}\n- Author: {metadata['author']}\n- Start Date: {metadata['date_start'].strftime('%B %d, %Y')}\n- End Date: {metadata['date_end'].strftime('%B %d, %Y')}\n- Years: {list(metadata['years'])}\n- Last Updated: {metadata['date_updated'].strftime('%B %d, %Y')}"
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
        with open(tims_path, 'w', encoding="utf-8") as f:
            json.dump(tims_metadata, f, indent = 4)
        
        # if successful, print a message
        print("- TIMS metadata exported to disk successfully.")
        
        return None


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 3. Update TIMS Metadata Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def update_tims_metadata(self, year: int, data_type: str = "reported", data_counts = None) -> None:
        """
        Update the TIMS metadata file with the counts of crashes, parties, and victims for a given year and type.
        Args:
            year (int): The year for which the metadata is being updated.
            data_type (str): The type of data being updated. Valid values are "reported", "geocoded", or "excluded".
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
        if data_type not in types:
            raise ValueError(f"Invalid data_type '{data_type}'. Valid types are: {', '.join(types)}")
        
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
        with open(metadata_file, 'r', encoding="utf-8") as f:
            tims_metadata = json.load(f)
        
        # Update the metadata for the specified type
        if data_type == "reported":
            tims_metadata[str(year)]["reported"]["crashes"] = count_crashes
        elif data_type == "geocoded":
            tims_metadata[str(year)]["geocoded"]["parties"] = count_parties
        elif data_type == "excluded":
            tims_metadata[str(year)]["excluded"]["victims"] = count_victims
        
        # Save the updated metadata back to the file
        with open(metadata_file, 'w', encoding="utf-8") as f:
            json.dump(tims_metadata, f, indent=4)
        
        print(f"TIMS metadata for {year} ({data_type}) updated successfully:\nCrashes: {count_crashes:,}, Parties: {count_parties:,}, Victims: {count_victims:,}")


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 4. Project Directories function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def project_directories(self, silent: bool = False) -> dict:
        """
        Function to generate project directories for the OCTraffic data processing project.
        Args:
            silent (bool): If True, suppresses the print output. Default is False.
        Returns:
            prj_dirs (dict): A dictionary containing the project directories.
        Raises:
            ValueError: If base_path is not a string.
        Example:
            >>> prj_dirs = self.project_directories()
        Notes:
            This function creates a dictionary of project directories based on the base path.
            The function also checks if the base path exists and raises an error if it does not.
        """
        prj_dirs = {
            "root": self.base_path,
            "admin": os.path.join(self.base_path, "admin"),
            "agp": os.path.join(self.base_path, "octagp"),
            "agp_aprx": os.path.join(self.base_path, "gis", "octagp", "octagp.aprx"),
            "agp_gdb": os.path.join(self.base_path, "gis", "octagp", "octagp.gdb"),
            "agp_gdb_raw": os.path.join(self.base_path, "gis", "octagp", "octagp.gdb", "raw"),
            "agp_gdb_supporting": os.path.join(self.base_path, "gis", "octagp", "octagp.gdb", "supporting"),
            "agp_gdb_analysis": os.path.join(self.base_path, "gis", "octagp", "octagp.gdb", "analysis"),
            "agp_gdb_hotspots": os.path.join(self.base_path, "gis", "octagp", "octagp.gdb", "hotspots"),
            "gdbmain": os.path.join(self.base_path, "gis", "octmain.gdb"),
            "gdbmain_raw": os.path.join(self.base_path, "gis", "octmain.gdb", "raw"),
            "gdbmain_supporting": os.path.join(self.base_path, "gis", "octmain.gdb", "supporting"),
            "analysis": os.path.join(self.base_path, "analysis"),
            "codebook": os.path.join(self.base_path, "codebook"),
            "data": os.path.join(self.base_path, "data"),
            "data_ago": os.path.join(self.base_path, "data", "ago"),
            "data_archived": os.path.join(self.base_path, "data", "archived"),
            "data_processed": os.path.join(self.base_path, "data", "processed"),
            "data_python": os.path.join(self.base_path, "data", "python"),
            "data_raw": os.path.join(self.base_path, "data", "raw"),
            "gis": os.path.join(self.base_path, "gis"),
            "gis_layers": os.path.join(self.base_path, "gis", "layers"),
            "gis_layers_templates": os.path.join(self.base_path, "gis", "layers", "templates"),
            "gis_layouts": os.path.join(self.base_path, "gis", "layouts"),
            "gis_maps": os.path.join(self.base_path, "gis", "maps"),
            "gis_styles": os.path.join(self.base_path, "gis", "styles"),
            "graphics": os.path.join(self.base_path, "graphics"),
            "graphics_gis": os.path.join(self.base_path, "graphics", "gis"),
            "metadata": os.path.join(self.base_path, "metadata"),
            "notebooks": os.path.join(self.base_path, "notebooks"),
            "notebooks_archived": os.path.join(self.base_path, "notebooks", "archived"),
            "scripts": os.path.join(self.base_path, "scripts"),
            "scripts_archived": os.path.join(self.base_path, "scripts", "archived")
        }
        # Print the project directories
        if not silent:
            print("\nProject Directories:")
            for key, value in prj_dirs.items():
                print(f"- {key}: {value}")
        # Return the project directories
        return prj_dirs    

    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 5. Load Codebook Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def load_cb(self, silent: bool = False) -> tuple:
        """
        Load the codebook.
        Args:
            silent (bool): If True, suppresses the print output. Default is False.
        Returns:
            cb (dict): The codebook.
            df_cb (pd.DataFrame): The codebook data frame.
        Raises:
            Nothing
        Example:
            >>>cb, df_cb = load_cb()
        Notes:
            This function loads the codebook from the codebook path.
        """
        with open(self.cb_path, encoding = "utf-8") as json_file:
            cb = json.load(json_file)
        
        # Create a codebook data frame
        df_cb = pd.DataFrame(cb).transpose()
        # Add attributes to the codebook data frame
        df_cb.attrs["name"] = "Codebook"
        
        if not silent:
            print("\nCodebook:\n", df_cb)
        return cb, df_cb
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 6. Relocate Dataframe Column Function ----
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
    ## 7. Categorical Pandas Series function ----
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
    ## 8. Determine Daylight Saving Time function ----
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
    ## 9. Add Codebook Attributes Function ----
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
    ## 10. Save to Disk Function ----
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
    ## 11. Graphics Entry Function ----
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
    ## 12. Chi-squared Independence Test Function ----
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
    ## 13. Chi-squared Goodness-of-fit Function ----
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
    ## 14. Kruskal-Wallis H-test Function ----
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
    ## 15. P-Value Display Function ----
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
    ## 16. Create STL Plot Function ----
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
                    f"{x:,.0f}" if abs(x) == 0 else f"{x:,.2f}" if abs(x) < 1 else f"{x:,.0f}"
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
                    f"{x:,.0f}" if abs(x) == 0 else f"{x:,.2f}" if abs(x) < 1 else f"{x:,.0f}"
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
                    f"{x:,.0f}" if abs(x) == 0 else f"{x:,.2f}" if abs(x) < 1 else f"{x:,.0f}"
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
                    f"{x:,.0f}" if abs(x) == 0 else f"{x:,.2f}" if abs(x) < 1 else f"{x:,.0f}"
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
    ## 17. Format Collision Time ----
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
    ## 18. Quarter to Date ----
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
    ## 19. Get Collision Severity Rank ----
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
    ## 20. Get Counts by Year ----
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
    ## 21. Time Series Aggregate ----
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
    ## 22. Plot a histogram of victim counts ----
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
    ## 23. Plot a bar graph of collision types ----
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
    ## 24. Plot a stacked bar chart of fatalities by type and year ----
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
    ## 25. Compute Monthly Statistics ----
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

    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 26. Create Monthly Fatalities Figure ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def create_monthly_fatalities_figure(self, ts_month: pd.DataFrame) -> tuple:
        """
        Creates a time series plot of monthly fatal crashes with LOESS smoothing and CI.
        Args:
            ts_month (pd.DataFrame): DataFrame containing monthly time series data with crashes
        Returns:
            fig, ax: The figure and axes objects for further customization if needed
        Raises:
            ValueError: If the input data is not in the expected format
        Example:
            >>> fig, ax = create_monthly_fatalities_figure(ts_month)
            >>> fig.show()
            >>> ax.show()
        Notes:
            This function creates a time series plot of monthly fatal crashes with LOESS smoothing and CI.
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

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 27. Create Victims vs Severity Overlay Plot Function ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def create_victims_severity_plot(self,data: pd.DataFrame, save_path: str = None, show_plot: bool = True) -> tuple:
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
        Example:
            >>> create_victims_severity_plot(data)
        Notes:
            The function creates a dual-axis plot showing the number of victims and
            mean collision severity over time, including LOESS trend lines and
            COVID-19 period highlighting.
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
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 28. Age Pyramid Plot ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def create_age_pyramid_plot(self, collisions:pd.DataFrame) -> plt.Figure:
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
        Notes:
            The age pyramid plot is a horizontal bar chart that shows the distribution of parties and victims by age.
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
            result = f"{abs(x):,}" 
            return result

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
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 29. Export CIM Object to JSON ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def export_cim(self, aprx:object, cim_type:str, cim_object:object, cim_name:str) -> None:
        """Export a CIM object to a file in both native (MAPX, PAGX, LYRX) and JSON CIM formats.
        Args:
            cim_type (str): Type of CIM object to export ("map", "layout", or "style").
            cim_object (object): CIM object to export.
            cim_name (str): Name of the CIM object to export.
        Returns:
            None
        Raises:
            ValueError: If cim_type is not "map", "layout", or "style".
        Examples:
            >>> export_cim("map", map_object, "map_name")
            >>> export_cim("layout", layout_object, "layout_name")
            >>> export_cim("style", style_object, "style_name")
        Notes:
            The CIM object must be a valid CIM object.
        """
        match cim_type:
            # When the CIM object is a map
            case "map":
                # Export the CIM object to a MAPX file
                print(f"Exporting {cim_name} map to MAPX...")
                cim_object.exportToMAPX(os.path.join(self.prj_dirs.get("gis_maps", ""), cim_name + ".mapx"))
                print(arcpy.GetMessages())
                # Export the CIM object to a JSON file
                print(f"Exporting {cim_name} map to JSON...\n")
                with open(os.path.join(self.prj_dirs.get("gis_maps", ""), cim_name + ".mapx"), "r", encoding = "utf-8") as f:
                    data = f.read()
                with open(os.path.join(self.prj_dirs.get("gis_maps", ""), cim_name + ".json"), "w", encoding = "utf-8") as f:
                    f.write(data)
            # When the CIM object is a layout
            case "layout":
                # Export the CIM object to a PAGX file
                print(f"Exporting {cim_name} layout to PAGX...")
                cim_object.exportToPAGX(os.path.join(self.prj_dirs.get("gis_layouts", ""), cim_name + ".pagx"))
                print(arcpy.GetMessages())
                # Export the CIM object to a JSON file
                print(f"Exporting {cim_name} layout to JSON...\n")
                with open(os.path.join(self.prj_dirs.get("gis_layouts", ""), cim_name + ".pagx"), "r", encoding = "utf-8") as f:
                    data = f.read()
                with open(os.path.join(self.prj_dirs.get("gis_layouts", ""), cim_name + ".json"), "w", encoding = "utf-8") as f:
                    f.write(data)
            # When the CIM object is a layer
            case "layer":
                # Export the CIM object to a LYRX file
                print(f"Exporting {cim_name} layer to LYRX...")
                # Reformat the name of the output file
                cim_new_name = "default_layer_name"  # Initialize cim_new_name with a default value
                for m in aprx.listMaps():
                    for l in m.listLayers():
                        if l == cim_object:
                            cim_new_name = (
                                m.name.title() + "Map-" + l.name.replace("OCTraffic ", "")
                            )
                # Save the layer to a LYRX file
                arcpy.management.SaveToLayerFile(
                    cim_object, os.path.join(self.prj_dirs.get("gis_layers", ""), cim_new_name + ".lyrx")
                )
                print(arcpy.GetMessages())
                # Export the CIM object to a JSON file
                print(f"Exporting {cim_name} layer to JSON...\n")
                with open(os.path.join(self.prj_dirs.get("gis_layers", ""), cim_new_name + ".lyrx"), "r", encoding = "utf-8") as f:
                    data = f.read()
                with open(os.path.join(self.prj_dirs.get("gis_layers", ""), cim_new_name + ".json"), "w", encoding = "utf-8") as f:
                    f.write(data)
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 30. Set Layer Time ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def set_layer_time(self, time_settings: dict, layer) -> None:
        """
        Set the time properties for a layer in the map.
        Args:
            time_settings (dict): A dictionary containing the time settings for the layer.
            layer: The layer to set the time properties for.
        Returns:
            None
        Raises:
            ValueError: If the layer is not time-enabled.
        Examples:
            >>> set_layer_time(time_settings, layer)
        Notes:
            The layer must be a valid layer in the map.
        """
        # Check if the layer is time-enabled
        if not layer.isTimeEnabled:
            # Enable time for the layer
            layer.enableTime("date_datetime", "", "TRUE", None)
            # Set the start time for the layer
            layer.time.startTime = time_settings["st"]
            # Set the end time for the layer
            layer.time.endTime = time_settings["et"]
            # Set the time field for the layer
            layer.time.startTimeField = time_settings["stf"]
            # Set the time step interval for the layer
            layer.time.timeStepInterval = time_settings["tsi"]
            # Set the time step interval units for the layer
            layer.time.timeStepIntervalUnits = time_settings["tsiu"]
            # Set the time zone for the layer
            layer.time.timeZone = time_settings["tz"]
        # Re assign step interval and time units
        if layer.isTimeEnabled:
            layer.time.timeStepInterval = 1.0
            layer.time.timeStepIntervalUnits = "months"


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 31. Layout Configuration ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def layout_configuration(self, nmf:int) -> dict:
        """
        Set the layout configuration based on the number of map frames.
        Args:
            nmf (int): The number of map frames in the layout.
        Returns:
            dict: The layout configuration.
        Raises:
            ValueError: If the number of map frames is not supported.
        Examples:
            >>> layout_configuration(1)
        Notes:
            The layout configuration is set based on the number of map frames.
        """
        # Match the number of map frames in layout
        lyt_config = {}
        # Set the layout configuration based on the number of map frames
        match nmf:
            case 1:
                lyt_config = {
                    "page_width": 11.0,
                    "page_height": 8.5,
                    "page_units": "INCH",
                    "rows": 1,
                    "cols": 1,
                    "nmf": 1,
                    "mf1": {
                        "coords": [(0.0, 8.5), (11.0, 8.5), (0.0, 0.0), (11.0, 0.0)],
                        "width": 11.0,
                        "height": 8.5,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.0,
                        "coordY": 0.0,
                        "geometry": arcpy.Point(0.0, 0.0),
                    },
                    "t1": {
                        "width": 1.9184,
                        "height": 0.3414,
                        "anchor": "TOP_LEFT_CORNER",
                        "coordX": 0.25,
                        "coordY": 8.25,
                        "geometry": arcpy.Point(0.25, 8.25),
                    },
                    "na": {
                        "width": 0.3606,
                        "height": 0.75,
                        "anchor": "BOTTOM_RIGHT_CORNER",
                        "coordX": 10.75,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(10.75, 0.25),
                    },
                    "sb": {
                        "width": 4.5,
                        "height": 0.5,
                        "anchor": "BOTTOM_MID_POINT",
                        "coordX": 5.5,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(5.5, 0.25),
                    },
                    "cr": {
                        "width": 0.0,
                        "height": 0.0,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.0,
                        "coordY": 0.0,
                        "geometry": arcpy.Point(0.0, 0.0),
                    },
                    "lg1": {
                        "width": 4.5,
                        "height": 2.0,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.25,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(0.25, 0.25),
                    },
                }
            case 2:
                lyt_config = {
                    "page_width": 22.0,
                    "page_height": 8.5,
                    "page_units": "INCH",
                    "rows": 1,
                    "cols": 2,
                    "nmf": 2,
                    "mf1": {
                        "coords": [(0.0, 8.5), (11.0, 8.5), (0.0, 0.0), (11.0, 0.0)],
                        "width": 11.0,
                        "height": 8.5,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.0,
                        "coordY": 0.0,
                        "geometry": arcpy.Point(0.0, 0.0),
                    },
                    "mf2": {
                        "coords": [(11.0, 8.5), (22.0, 8.5), (11.0, 0.0), (22.0, 0.0)],
                        "width": 11.0,
                        "height": 8.5,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 11.0,
                        "coordY": 0.0,
                        "geometry": arcpy.Point(11.0, 0.0),
                    },
                    "t1": {
                        "width": 1.9184,
                        "height": 0.3414,
                        "anchor": "TOP_LEFT_CORNER",
                        "coordX": 0.25,
                        "coordY": 8.25,
                        "geometry": arcpy.Point(0.25, 8.25),
                    },
                    "t2": {
                        "width": 1.9184,
                        "height": 0.3414,
                        "anchor": "TOP_LEFT_CORNER",
                        "coordX": 11.25,
                        "coordY": 8.25,
                        "geometry": arcpy.Point(11.25, 8.25),
                    },
                    "na": {
                        "width": 0.3606,
                        "height": 0.75,
                        "anchor": "BOTTOM_RIGHT_CORNER",
                        "coordX": 21.75,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(21.75, 0.25),
                    },
                    "sb": {
                        "width": 4.5,
                        "height": 0.5,
                        "anchor": "BOTTOM_MID_POINT",
                        "coordX": 16.5,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(16.5, 0.25),
                    },
                    "cr": {
                        "width": 0.0,
                        "height": 0.0,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.0,
                        "coordY": 0.0,
                        "geometry": arcpy.Point(0.0, 0.0),
                    },
                    "lg1": {
                        "width": 4.5,
                        "height": 2.0,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.25,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(0.25, 0.25),
                    },
                    "lg2": {
                        "width": 4.5,
                        "height": 2.0,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 11.25,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(11.25, 0.25),
                    },
                }
            case 4:
                lyt_config = {
                    "page_width": 22.0,
                    "page_height": 17.0,
                    "page_units": "INCH",
                    "rows": 2,
                    "cols": 2,
                    "nmf": 4,
                    "mf1": {
                        "coords": [(0.0, 17.0), (11.0, 17.0), (0.0, 8.5), (11.0, 8.5)],
                        "width": 11.0,
                        "height": 8.5,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.0,
                        "coordY": 8.5,
                        "geometry": arcpy.Point(0.0, 8.5),
                    },
                    "mf2": {
                        "coords": [(11.0, 17.0), (22.0, 17.0), (11.0, 8.5), (22.0, 8.5)],
                        "width": 11.0,
                        "height": 8.5,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 11.0,
                        "coordY": 8.5,
                        "geometry": arcpy.Point(11.0, 8.5),
                    },
                    "mf3": {
                        "coords": [(0.0, 8.5), (11.0, 8.5), (0.0, 0.0), (11.0, 0.0)],
                        "width": 11.0,
                        "height": 8.5,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.0,
                        "coordY": 0.0,
                        "geometry": arcpy.Point(0.0, 0.0),
                    },
                    "mf4": {
                        "coords": [(11.0, 8.5), (22.0, 8.5), (11.0, 0.0), (22.0, 0.0)],
                        "width": 11.0,
                        "height": 8.5,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 11.0,
                        "coordY": 0.0,
                        "geometry": arcpy.Point(11.0, 0.0),
                    },
                    "t1": {
                        "width": 1.9184,
                        "height": 0.3414,
                        "anchor": "TOP_LEFT_CORNER",
                        "coordX": 0.25,
                        "coordY": 16.75,
                        "geometry": arcpy.Point(0.25, 16.75),
                    },
                    "t2": {
                        "width": 1.9184,
                        "height": 0.3414,
                        "anchor": "TOP_LEFT_CORNER",
                        "coordX": 11.25,
                        "coordY": 16.75,
                        "geometry": arcpy.Point(11.25, 16.75),
                    },
                    "t3": {
                        "width": 1.9184,
                        "height": 0.3414,
                        "anchor": "TOP_LEFT_CORNER",
                        "coordX": 0.25,
                        "coordY": 8.25,
                        "geometry": arcpy.Point(0.25, 8.25),
                    },
                    "t4": {
                        "width": 1.9184,
                        "height": 0.3414,
                        "anchor": "TOP_LEFT_CORNER",
                        "coordX": 11.25,
                        "coordY": 8.25,
                        "geometry": arcpy.Point(11.25, 8.25),
                    },
                    "na": {
                        "width": 0.3606,
                        "height": 0.75,
                        "anchor": "BOTTOM_RIGHT_CORNER",
                        "coordX": 21.75,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(21.75, 0.25),
                    },
                    "sb": {
                        "width": 4.5,
                        "height": 0.5,
                        "anchor": "BOTTOM_MID_POINT",
                        "coordX": 16.75,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(16.75, 0.25),
                    },
                    "cr": {
                        "width": 0.5,
                        "height": 0.5,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.0,
                        "coordY": 0.0,
                        "geometry": arcpy.Point(0.0, 0.0),
                    },
                    "lg1": {
                        "width": 4.5,
                        "height": 2.0,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.25,
                        "coordY": 8.75,
                        "geometry": arcpy.Point(0.25, 8.75),
                    },
                    "lg2": {
                        "width": 4.5,
                        "height": 2.0,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 11.25,
                        "coordY": 8.75,
                        "geometry": arcpy.Point(11.25, 8.75),
                    },
                    "lg3": {
                        "width": 4.5,
                        "height": 2.0,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 0.25,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(0.25, 0.25),
                    },
                    "lg4": {
                        "width": 4.5,
                        "height": 2.0,
                        "anchor": "BOTTOM_LEFT_CORNER",
                        "coordX": 11.25,
                        "coordY": 0.25,
                        "geometry": arcpy.Point(11.25, 0.25),
                    },
                }
        return lyt_config


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 32. Delete Feature Class ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def delete_feature_class(self, fc_name: str, gdb_path: Optional[str] = None, dataset: Optional[str] = None) -> None:
        """
        Deletes a feature class from a geodatabase.
        Args:
            fc_name (str): Name of the feature class to delete.
            gdb_path (str, optional): Path to the geodatabase. If None, uses the project geodatabase.
            dataset (str, optional): Name of the feature dataset.
        Returns:
            None
        Raises:
            FileNotFoundError: If the geodatabase does not exist.
            ValueError: If the geodatabase path is invalid.
        Examples:
            >>> delete_feature_class("my_feature_class", "C:/path/to/gdb.gdb", "my_dataset")
        Returns:
            None
        Notes:
            - If the feature class does not exist, the function will print a message and return without doing anything.
            - If the geodatabase does not exist, the function will raise a FileNotFoundError.
            - If the geodatabase path is invalid, the function will raise a ValueError.
        """
        # Determine GDB path
        if gdb_path is None:
            gdb_path = self.prj_dir.get("agp_gdb", "")
        
        # Validate geodatabase path
        if not gdb_path:
            raise ValueError("Geodatabase path is not provided and could not be determined from project directories.")
        if not os.path.exists(gdb_path):
            raise FileNotFoundError(f"Geodatabase not found: {gdb_path}")
        
        # Construct full path
        if dataset:
            fc_path = os.path.join(gdb_path, dataset, fc_name)
        else:
            fc_path = os.path.join(gdb_path, fc_name)
        
        # Check usage of arcpy
        if not arcpy.Exists(fc_path):
            print(f"Feature class '{fc_name}' does not exist at {fc_path}. Skipping deletion.")
            return

        try:
            # Delete the feature class
            arcpy.management.Delete(fc_path)
            print(f"Successfully deleted feature class: {fc_name}")
        except Exception as e:
            print(f"Error deleting feature class {fc_name}: {e}")
            raise e


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ## 33. Load ArcGIS Pro Project ----
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def load_aprx(self, aprx_path: str, gdb_path: str, add_to_map: bool = False) -> tuple:
        """
        Loads an ArcGIS Pro project and sets the workspace to the geodatabase.
        Args:
            aprx_path (str): Path to the ArcGIS Pro project.
            gdb_path (str): Path to the geodatabase.
            add_to_map (bool): Whether to add outputs to the map.
        Raises:
            FileNotFoundError: If the ArcGIS Pro project or geodatabase does not exist.
            ValueError: If the ArcGIS Pro project or geodatabase path is invalid.
        Examples:
            >>> aprx, workspace = load_aprx(aprx_path, gdb_path, add_to_map=True)
        Returns:
            tuple: A tuple containing the ArcGIS Pro project object and the workspace.
        Notes:
            - The ArcGIS Pro project will be closed before loading.
            - The workspace will be set to the geodatabase path.
            - The ArcGIS Pro project will be closed after loading.
        """
        # ArcGIS Pro Project object
        aprx = arcpy.mp.ArcGISProject(aprx_path)
        # Current ArcGIS workspace (arcpy)
        arcpy.env.workspace = gdb_path
        workspace = arcpy.env.workspace
        # Enable overwriting existing outputs
        arcpy.env.overwriteOutput = True
        # Disable adding outputs to map
        arcpy.env.addOutputsToMap = add_to_map
        return aprx, workspace


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Main ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    pass

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# End of File ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
