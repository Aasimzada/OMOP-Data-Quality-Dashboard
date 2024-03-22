# Check name - isRequired

#The number and percent of records with a NULL value in the @cdmFieldName of the @cdmTableName that is considered not nullable.

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Null Field Check").getOrCreate()

# Load data from a CSV file
df_your_table = spark.read.option("header", "true").csv('path to your file/your_table.csv')

# Specify the column name for checking NULL values
field_name = 'field'  # Replace 'field' with the actual column name

# Count the number of violated rows where the specified field is NULL
num_violated_rows = df_your_table.filter(df_your_table[field_name].isNull()).count()

# Count the total number of rows in the DataFrame
num_denominator_rows = df_your_table.count()

# Calculate the percentage of violated rows
pct_violated_rows = 0 if num_denominator_rows == 0 else (num_violated_rows / num_denominator_rows)

# Output the results
print("Number of violated rows:", num_violated_rows)
print(f"Percentage of violated rows: {pct_violated_rows * 100:.2f}%")  # Converted to percentage format
print("Total number of rows in the table:", num_denominator_rows)

# Stop the Spark session
spark.stop()

