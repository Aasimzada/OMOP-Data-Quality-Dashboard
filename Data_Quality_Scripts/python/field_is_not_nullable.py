# Check name - isRequired

#The number and percent of records with a NULL value in the @cdmFieldName of the @cdmTableName that is considered not nullable.

import pandas as pd

# Read data from CSV files 
df_your_table = pd.read_csv('path to your file/your_table.csv')

# Count the number of violated rows where field is NULL
num_violated_rows = df_your_table['field'].isnull().sum()

# Count the total number of rows in the table
num_denominator_rows = len(df_your_table)

# Calculate the percentage of violated rows
pct_violated_rows = 0 if num_denominator_rows == 0 else num_violated_rows / num_denominator_rows

# Output the results
print("Number of violated rows:", num_violated_rows)
print("Percentage of violated rows:", pct_violated_rows)
print("Total number of rows in PERSON table:", num_denominator_rows)
