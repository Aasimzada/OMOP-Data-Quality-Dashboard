# Check name - measureConditionEraCompleteness

#The number and Percent of persons that does not have condition_era built successfully for all persons in condition_occurrence

import pandas as pd

# Read data from CSV files 
df_condition_occurrence = pd.read_csv('path to your file/condition_occurrence.csv')
df_condition_era = pd.read_csv('path to your file/condition_era.csv')

# Merge condition_occurrence and condition_era  data using an inner join
merged_df = pd.merge(df_condition_occurrence, df_condition_era, on='person_id', how='inner')

# Identify persons without condition_era built successfully
violated_rows = df_condition_occurrence[~df_condition_occurrence['person_id'].isin(merged_df['person_id'])]

# Calculate number of violated rows
num_violated_rows = len(violated_rows)

# Calculate denominator
num_denominator_rows = len(df_condition_occurrence['person_id'].unique())

# Calculate percentage of violated rows
pct_violated_rows = 0 if num_denominator_rows == 0 else num_violated_rows / num_denominator_rows

# Output results
print("Number of persons without condition_era built successfully:", num_violated_rows)
print("Percentage of persons without condition_era built successfully:", pct_violated_rows)
print("Total number of persons in the condition_occurrence table:", num_denominator_rows)

# Stop the Spark session
spark.stop()