# Check name - cdmField

#A yes or no value indicating if field is present in the  table as expected based on the specification.

from pyspark.sql import SparkSession

def cdm_field_check(spark, cdm_Table, cdm_field):
    """

    Parameters:
    - spark: SparkSession object
    - cdm_Table: Path to the CDM table CSV file
    - cdm_field: Name of the field to check for existence

    Returns:
    - num_violated_rows: Number of persons without a record in cdmTable
    - pct_violated_rows: Percentage of persons without a record in cdmTable
    - num_denominator_rows: Total number of persons
    """
    
    # Load data from a CSV file into a DataFrame
    cdm_table =  spark.read.csv(cdm_Table, header=True)

    # Check if the field exists in the DataFrame
    num_violated_rows = 0 if cdm_field.lower() in [column.lower() for column in cdm_table.columns] else 1

    # Since we're checking for the existence of a column, the denominator is conceptually 1
    num_denominator_rows = 1

    # Calculate percentage of violated rows
    pct_violated_rows = num_violated_rows / num_denominator_rows * 100  # Multiplying by 100 to get a percentage

    return num_violated_rows, pct_violated_rows, num_denominator_rows


