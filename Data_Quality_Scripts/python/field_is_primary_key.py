# Check name - isPrimaryKey

#The number and percent of records that have a duplicate value in the @cdmFieldName field of the @cdmTableName.

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Primary Key Check") \
    .getOrCreate()

# Read data from CSV files
df_your_table = spark.read.csv('path_to_your_file/your_table.csv', header=True, inferSchema=True)

# Check if the field is a primary key
violated_rows = df_your_table.groupBy('field').count().filter("count > 1")

# Calculate number of violated rows
num_violated_rows = violated_rows.count()

# Calculate denominator
num_denominator_rows = df_your_table.count()

# Calculate percentage of violated rows
pct_violated_rows = 0 if num_denominator_rows == 0 else num_violated_rows / num_denominator_rows

# Output results
print("Number of violated rows:", num_violated_rows)
print("Percentage of violated rows:", pct_violated_rows)
print("Total number of rows in your_table:", num_denominator_rows)

