# Check name - CDM_TABLE

# Verify the table exists.

# We first load the data from the table (should be stored in a CSV file named 'cdmTable.csv').
# We calculate num_violated_rows as 0 if the table is empty (i.e., doesn't exist) or 1 otherwise.
# We calculate num_denominator_rows as the total number of rows in the table.
# We calculate pct_violated_rows as the percentage of violated rows.

from pyspark.sql import SparkSession
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("File Existence Check") \
    .getOrCreate()

# Parameters - path to your file
file_path = "path_to_your_file/your_file.csv"

# Check if the file exists
if os.path.exists(file_path):
    # Read data from the file
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Count the number of rows
    num_denominator_rows = df.count()

    # Assign a value of 0 to num_violated_rows if the file exists
    num_violated_rows = 0

    # Calculate percentage of violated rows
    pct_violated_rows = 0 if num_denominator_rows == 0 else num_violated_rows / num_denominator_rows

    # Output results
    print("num_violated_rows:", num_violated_rows)
    print("pct_violated_rows:", pct_violated_rows)
    print("num_denominator_rows:", num_denominator_rows)
else:
    print("Error: File does not exist at the specified path.")

# Stop the Spark session
spark.stop()

