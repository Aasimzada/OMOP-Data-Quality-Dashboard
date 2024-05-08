# Check name - PLAUSIBLE_TEMPORAL_AFTER

#The number of records and the proportion to total number of eligible records with datetimes that do not occur on or after their corresponding datetimes

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, coalesce
import os

def check_plausible_temporal_after(spark, cdm_Table, cdm_field, plausible_temporal_after_table, plausible_temporal_after_field):
    """
    Check plausible temporal after for a given scenario.

    Parameters:
    - spark: SparkSession object
    - cdm_Table Path to the CSV file containing the CDM table data
    - cdm_field: Name of the field in the CDM table
    - plausible_temporal_after_table: Path to the CSV file containing the plausible temporal after table data
    - plausible_temporal_after_field: Name of the field in the plausible temporal after table

    Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of rows in the CDM table
    """
    # Extract table names from file paths
    cdm_table_name = os.path.splitext(os.path.basename(cdm_Table))[0].upper()
    plausible_temporal_after_table_name = os.path.splitext(os.path.basename(plausible_temporal_after_table))[0].upper()

    # Load CDM table and plausible temporal after table
    cdm_table = spark.read.csv(cdm_Table, header=True)
    plausible_temporal_table = spark.read.csv(plausible_temporal_after_table, header=True)

    # Check if plausible temporal after table name matches 'PERSON'
    if plausible_temporal_after_table_name == 'PERSON':
        plausible_temporal_field = coalesce(col(plausible_temporal_after_field), concat_ws('-', col("year_of_birth"), lit('06'), lit('01')))
        cdm_table = cdm_table.join(plausible_temporal_table, on='person_id', how='inner')
    else:
        plausible_temporal_field = col(plausible_temporal_after_field)

    # Filter the DataFrame to find violating records
    violating_records = cdm_table.filter(plausible_temporal_field.cast("date") > col(cdm_field).cast("date"))

    # Count the number of violating rows
    num_violated_rows = violating_records.count()

    # Count the total number of rows in the CDM table
    num_denominator_rows = cdm_table.count()

    # Calculate the percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) * 100 if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows