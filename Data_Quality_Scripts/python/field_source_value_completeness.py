# Check name - sourceValueCompleteness

#The number and percent of distinct source values in the cdmFieldName field of the cdmTableName table mapped to 0.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

def check_source_value_completeness(spark, cdm_Table, cdm_field, standard_concept_field_name):
    """
    Check the completeness of source values mapped to 0 in the specified field of the CDM table.
    
    Parameters:
    - spark : SparkSession object
    - cdm_Table : Path to the CSV file containing the CDM table data
    - cdm_field : The source field to check for distinct source values
    - standard_concept_field_name : The mapped field where the value should be 0 to count as a violation

    Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of distinct source values in the specified field
    """
    
    # Load the table
    cdm_table = spark.read.csv(cdm_Table, header=True)

    # Count the total distinct source values in the field
    num_denominator_rows = cdm_table.select(countDistinct(cdm_field)).first()[0]

    # Select distinct rows where the standard concept field equals 0
    violating_rows_df = cdm_table.filter(col(standard_concept_field_name) == 0).select(cdm_field).distinct()

    # Count the number of violating rows
    num_violated_rows = violating_rows_df.count()

    # Calculate the percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows * 100) if num_denominator_rows > 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows

    