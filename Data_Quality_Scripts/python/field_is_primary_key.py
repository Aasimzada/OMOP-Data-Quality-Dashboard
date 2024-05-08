# Check name - isPrimaryKey

#The number and percent of records that have a duplicate value in the @cdmFieldName field of the @cdmTableName.

from pyspark.sql import SparkSession

def primary_key_check(spark, cdm_Table, cdm_field):
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

    # Perform the primary key check
    violated_rows_df = cdm_table.groupBy(cdm_field).count().filter("count > 1")

    # Count the number of violated rows
    num_violated_rows = violated_rows_df.count()

    # Count the total number of rows in the CDM table
    num_denominator_rows = cdm_table.count()

    # Calculate the percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows




