# Import the class
from octraffic import octraffic
import pandas as pd

# 1. Initialize the class
ocs = octraffic()

# 2. Example: Using project_metadata
# Note: You can now call methods using 'ocs.' 
meta = ocs.project_metadata(part=1, version=2025.2)

# 3. Example: Using project_directories
# You can pass arguments just like before
dirs = ocs.project_directories(base_path=r"C:\Path\To\Project")

# 4. Example: Using relocate_column
# Create a dummy dataframe for demonstration
df = pd.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6]})

# Use the class method to modify the dataframe
ocs.relocate_column(df, col_name='A', ref_col_name='C', position='after')
print(df)