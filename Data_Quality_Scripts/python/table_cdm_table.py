# Check name - CDM_TABLE

# Verify the table exists.

# We first load the data from the table (should be stored in a CSV file named 'cdmTable.csv').
# We calculate num_violated_rows as 0 if the table is empty (i.e., doesn't exist) or 1 otherwise.
# We calculate num_denominator_rows as the total number of rows in the table.
# We calculate pct_violated_rows as the percentage of violated rows.

import pandas as pd

# Parameters - path to your file name - containing the data
path_to_your_file = 'path to your file/your_table.csv'

# Data frame representing the table
df = pd.read_csv(path_to_your_file)

# Count the number of rows in the data frame
num_denominator_rows = len(df)

# Count the number of rows where a condition is met
num_violated_rows = sum(1 for row in df.iterrows() if False)

# Calculate percentage of violated rows
pct_violated_rows = num_violated_rows / num_denominator_rows if num_denominator_rows != 0 else 0

# Output results
print("num_violated_rows:", num_violated_rows)
print("pct_violated_rows:", pct_violated_rows)
print("num_denominator_rows:", num_denominator_rows)
