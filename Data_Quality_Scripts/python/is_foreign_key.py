# Check name - IS_FOREIGN_KEY

#The number and percent of records that have a value in the @cdmFieldName field in the @cdmTableName table that does not exist in the @fkTableName table.

import pandas as pd

# Read data from CSV files 
foreign_table = pd.read_csv('path to your file/your_foreign_table.csv')
primary_table = pd.read_csv('path to your file/your_primary_table.csv')

# Performing left join to check foreign key constraint
merged_df = pd.merge(primary_table, foreign_table, on='field', how='left', indicator=True)

# Filtering rows where PERSON_ID exists in PROCEDURE_OCCURRENCE but not in PERSON
violated_rows = merged_df.loc[(merged_df['_merge'] == 'left_only') & (merged_df['field'].notnull())]

# Counting the number of violated rows
num_violated_rows = len(violated_rows)

# Total number of rows in PROCEDURE_OCCURRENCE table
num_denominator_rows = len(primary_table)

# Calculating percentage of violated rows
pct_violated_rows = 0 if num_denominator_rows == 0 else num_violated_rows / num_denominator_rows

# Outputting the results
print("Number of violated rows:", num_violated_rows)
print("Percentage of violated rows:", pct_violated_rows)
print("Total number of rows in your_table:", num_denominator_rows)

# Stop the Spark session
spark.stop()