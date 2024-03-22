# Check name - cdmField

#A yes or no value indicating if field is present in the  table as expected based on the specification.

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CDM Field Check").getOrCreate()

# Load data from a CSV file
df_your_table = spark.read.option("header", "true").csv('path to your file/your_table.csv')

# Check if the field exists
field_name = 'field'  # Replace 'field' with the actual column name you're checking
num_violated_rows = 0 if field_name in df_your_table.columns else 1

# Since we're checking for the existence of a column, the denominator is conceptually 1 (the column's presence or absence)
num_denominator_rows = 1

# Calculate percentage of violated rows
pct_violated_rows = num_violated_rows / num_denominator_rows * 100  # Multiplying by 100 to get a percentage

# Output results
print("Number of violated rows:", num_violated_rows)
print("Percentage of violated rows: {:.2f}%".format(pct_violated_rows))
print("Total number of rows in consideration:", num_denominator_rows)

# Stop the Spark session
spark.stop()

