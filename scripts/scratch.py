# Import the class
from octraffic import octraffic
import pandas as pd
import os

# 1. Initialize the class
ocs = octraffic()

# 2. Example: Using project_metadata
meta = ocs.project_metadata(part=1, version=2025.2)

# 3. Example: Using project_directories
dirs = ocs.project_directories(base_path=os.getcwd())

# 4. Example: Using relocate_column
df = pd.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6]})
ocs.relocate_column(df, col_name='A', ref_col_name='B', position='after')
print(df)