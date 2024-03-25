# Check name - measurePersonCompleteness

#The number and percent of persons in the CDM that do not have at least one record in the table

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Person Completeness Check") \
    .getOrCreate()

# Read data from CSV files
df_person = spark.read.csv('path_to_your_file/person.csv', header=True, inferSchema=True)
df_your_table = spark.read.csv('path_to_your_file/your_table.csv', header=True, inferSchema=True)

# Merge person and your_table data using an inner join
merged_df = df_person.join(df_your_table, 'person_id', 'inner')

# Identify persons without at least one record in your_table
violated_rows = df_person.join(merged_df.select('person_id'), 'person_id', 'left_anti')

# Calculate number of violated rows
num_violated_rows = violated_rows.count()

# Calculate denominator
num_denominator_rows = df_person.count()

# Calculate percentage of violated rows
pct_violated_rows = 0 if num_denominator_rows == 0 else num_violated_rows / num_denominator_rows

# Output results
print("Number of persons without at least one record in your_table:", num_violated_rows)
print("Percentage of persons without at least one record in your_table:", pct_violated_rows)
print("Total number of persons in the CDM:", num_denominator_rows)

# Stop the Spark session
spark.stop()