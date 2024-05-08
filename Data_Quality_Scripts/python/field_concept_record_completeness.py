# Check name - CONCEPT_RECORD_COMPLETENESS

#The number and percent of records with a value of 0 in the source concept field cdmFieldName in the cdmTableName table.

from pyspark.sql import SparkSession

def check_concept_record_completeness(spark, cdm_Table , cdm_field):
    """

    Parameters:
    - spark: SparkSession object
    - cdm_Table: Path to the CDM table CSV file
    - cdm_field: Name of the field to check for zero values

    Returns:
    - num_violated_rows: Number of persons without a record in cdmTable
    - pct_violated_rows: Percentage of persons without a record in cdmTable
    - num_denominator_rows: Total number of persons
    """
    
    # Read CSV file into DataFrame
    cdm_table =  spark.read.csv(cdm_Table, header=True)

    # Count the number of rows where the specified field is zero
    num_violated_rows = cdm_table.filter(cdm_table[cdm_field] == 0).count()

    # Total number of rows
    num_denominator_rows = cdm_table.count()

    # Calculate percentage of violated rows
    pct_violated_rows = num_violated_rows / num_denominator_rows * 100 if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows
