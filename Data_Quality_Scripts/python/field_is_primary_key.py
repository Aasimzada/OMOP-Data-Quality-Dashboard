# Check name - isPrimaryKey

#The number and percent of records that have a duplicate value in the @cdmFieldName field of the @cdmTableName.

import pandas as pd

# Read data from CSV files 
df_your_table = pd.read_csv('path to your file/your_table.csv')

# Check if the field is a primary key
violated_rows = df_your_table[df_your_table.duplicated(subset=['field'], keep=False)]

# Calculate number of violated rows
num_violated_rows = len(violated_rows)

# Calculate denominator
num_denominator_rows = len(df_your_table)

# Calculate percentage of violated rows
pct_violated_rows = 0 if num_denominator_rows == 0 else num_violated_rows / num_denominator_rows

# Output results
print("Number of violated rows:", num_violated_rows)
print("Percentage of violated rows:", pct_violated_rows)
print("Total number of rows in the your_table:", num_denominator_rows)
