# Check name - measureValueCompleteness

#The number and percent of records with a NULL value in the cdmFieldName of the cdmTableName.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count

def check_measure_Value_Completeness(spark, cdm_Table, cdm_field):
    """

    Parameters:
    - spark : SparkSession object
    - cdm_Table : Path to the CSV file containing the CDM table data
    - cdm_field_name : Field in the CDM table to check for NULL values

    Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of rows in the CDM table
    """

    # Load the CDM table
    cdm_table = spark.read.csv(cdm_Table, header=True)

    # Count rows where the specified field is NULL
    num_violated_rows = cdm_table.filter(col(cdm_field).isNull()).count()

    # Count the total rows in the CDM table
    num_denominator_rows = cdm_table.count()

    # Calculate the percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows * 100) if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows