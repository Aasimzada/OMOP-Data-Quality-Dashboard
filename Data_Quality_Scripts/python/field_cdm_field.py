# Check name - cdmField

#A yes or no value indicating if field is present in the  table as expected based on the specification.

import pandas as pd

# Read data from CSV files 
df_your_table =  pd.read_csv('path to your file/your_table.csv')

# Check if the field exists
num_violated_rows = 1 if 'field' not in df_your_table.columns else 0

# Calculate denominator
num_denominator_rows = 1

# Calculate percentage of violated rows
pct_violated_rows = num_violated_rows / num_denominator_rows

# Output results
print("Number of violated rows:", num_violated_rows)
print("Percentage of violated rows:", pct_violated_rows)
print("Total number of rows in the PERSON table:", num_denominator_rows)
