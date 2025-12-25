# Print the keys of the codebook dictionary 
print(cb.keys())

# Print the columns of the df_cb DataFrame
print(df_cb.columns)

# Print the dtypes of the df_cb DataFrame
print(df_cb.dtypes)

# Unique values of the df_cb var_class column with counts
df_cb["var_class"].value_counts()
df_cb["var_type"].value_counts()
df_cb["var_cat"].value_counts()

list_sel = df_cb[(df_cb["raw_data"] == 1) & (df_cb["in_crashes"] == 1)].index.tolist()

# Get the first row of the df_cb["fc"]
df_cb["fc"].iloc[0]["crashes"]

# Get all the rows of the df_cb["fc"] where the value of the "crashes" key inside the subdictionary of the  "fc" column is 1
df_cb[df_cb["fc"].apply(lambda x: x["crashes"] == 1)]

list_sel = df_cb[df_cb["fc"].apply(lambda x: x["crashes"] == 1) & (df_cb["raw"] == 1)].index.tolist()

list_sel