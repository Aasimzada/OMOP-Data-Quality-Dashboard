# Check name - measureConditionEraCompleteness

#The number and Percent of persons that does not have condition_era built successfully for all persons in condition_occurrence

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Condition Era Completeness Check") \
    .getOrCreate()

# Read data from CSV files
df_condition_occurrence = spark.read.csv('path_to_your_file/condition_occurrence.csv', header=True, inferSchema=True)
df_condition_era = spark.read.csv('path_to_your_file/condition_era.csv', header=True, inferSchema=True)

# Merge condition_occurrence and condition_era data using an inner join
merged_df = df_condition_occurrence.join(df_condition_era, 'person_id', 'inner')

# Identify persons without condition_era built successfully
violated_rows = df_condition_occurrence.join(merged_df.select('person_id'), 'person_id', 'left_anti')

# Calculate number of violated rows
num_violated_rows = violated_rows.count()

# Calculate denominator
num_denominator_rows = df_condition_occurrence.select('person_id').distinct().count()

# Calculate percentage of violated rows
pct_violated_rows = 0 if num_denominator_rows == 0 else num_violated_rows / num_denominator_rows

# Output results
print("Number of persons without condition_era built successfully:", num_violated_rows)
print("Percentage of persons without condition_era built successfully:", pct_violated_rows)
print("Total number of persons in the condition_occurrence table:", num_denominator_rows)

# Stop the Spark session
spark.stop()

