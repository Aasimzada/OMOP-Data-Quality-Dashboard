# Check name - isRequired

#The number and percent of records with a NULL value in the @cdmFieldName of the @cdmTableName that is considered not nullable.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def not_nullable_check(spark, cdm_Table, cdm_field):
    """
    
    Parameters:
    - spark: SparkSession object
    - cdm_Table: Path to the CDM table CSV file
    - cdm_field: Name of the field in the CDM table
    
    Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of rows in the CDM table
    """
    
    # Read CSV file into DataFrame
    cdm_table =  spark.read.csv(cdm_Table, header=True)

    # Count the number of violated rows
    num_violated_rows = cdm_table.filter(cdm_table[cdm_field].isNull()).count()

    # Count the total number of rows in the CDM table
    num_denominator_rows = cdm_table.count()

    # Calculate the percentage of violated rows
    pct_violated_rows = num_violated_rows / num_denominator_rows if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows
