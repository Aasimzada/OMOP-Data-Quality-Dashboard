# Check name - IS_FOREIGN_KEY

#The number and percent of records that have a value in the @cdmFieldName field in the @cdmTableName table that does not exist in the @fkTableName table.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col  # Import the col function

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Foreign Key Check") \
    .getOrCreate()

# Read data from CSV files
foreign_df = spark.read.csv('path_to_your_file/your_foreign_table.csv', header=True, inferSchema=True)
primary_df = spark.read.csv('path_to_your_file/your_primary_table.csv', header=True, inferSchema=True)

# Performing left join to check foreign key constraint
merged_df = primary_df.join(foreign_df, 'field', 'left')

# Filtering rows where the foreign key does not exist in the primary table
violated_rows = merged_df.filter(col('field').isNull())

# Counting the number of violated rows
num_violated_rows = violated_rows.count()

# Total number of rows in the primary table
num_denominator_rows = primary_df.count()

# Calculating percentage of violated rows
pct_violated_rows = 0 if num_denominator_rows == 0 else num_violated_rows / num_denominator_rows

# Outputting the results
print("Number of violated rows:", num_violated_rows)
print("Percentage of violated rows:", pct_violated_rows)
print("Total number of rows in your_table:", num_denominator_rows)

# Stop the Spark session
spark.stop()



