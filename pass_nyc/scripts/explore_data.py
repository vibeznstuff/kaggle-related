import pandas as pd
import numpy as np


def import_data(filename):
    common_dir = r"C:\Users\Richard\Documents\open_data\kaggle\pass_nyc"
    return pd.read_csv("{0}\{1}".format(common_dir, filename))


# List of filename and file ref tuples
file_list = [("2016 School Explorer.csv", "SCH_EXP"),
             ("D5 SHSAT Registrations and Testers.csv", "SHSAT"),
             ("nyc-wi-fi-hotspot-locations.csv", "WIFI"),
             ("safe-routes-to-schools-priority-schools.csv", "SROUTE"),
             ("universal-pre-k-upk-school-locations.csv", "PREK")]

# Create dictionary of data frames for analysis
df_dict = {}
for file in file_list:
    df_dict[file[1]] = import_data(file[0])

df_dict["SCH_EXP"]["Income Est"] = np.where(df_dict["SCH_EXP"]["School Income Estimate"] == "N/A",
                                            np.NaN,
                                            df_dict["SCH_EXP"]["School Income Estimate"].replace('[\$,]', '', regex=True).astype(float))
df_dict["SCH_EXP"]["N/A Incomes"] = np.where(df_dict["SCH_EXP"]["School Income Estimate"] == "N/A", 1, 0)
x = df_dict["SCH_EXP"].groupby('Zip').agg({'Income Est': 'sum', 'N/A Incomes': 'count'}).sort_values('Income Est')

print(x.head())

