# Check name - measurePersonCompleteness

#The number and percent of persons in the CDM that do not have at least one record in the table

import pandas as pd

# Read data from CSV files 
df_person = pd.read_csv('path to your file/person.csv')
df_your_table = pd.read_csv('path to your file/your_table.csv')

# Merge person and your_table data using an inner join
merged_df = pd.merge(df_person, df_your_table, on='person_id', how='inner')

# Identify persons without at least one record in your_table
violated_rows = df_person[~df_person['person_id'].isin(merged_df['person_id'])]

# Calculate number of violated rows
num_violated_rows = len(violated_rows)

# Calculate denominator
num_denominator_rows = len(df_person)

# Calculate percentage of violated rows
pct_violated_rows = 0 if num_denominator_rows == 0 else num_violated_rows / num_denominator_rows

# Output results
print("Number of persons without at least one record in your_table:", num_violated_rows)
print("Percentage of persons without at least one record in your_table:", pct_violated_rows)
print("Total number of persons in the CDM:", num_denominator_rows)

# Stop the Spark session
spark.stop()