# Check name - cdmDatatype

#A yes or no value indicating if the cdmFieldName in the cdmTableName is the expected data type based on the specification.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def field_cdm_datatype_check(spark, cdm_Table, cdm_field):
    """

    Parameters:
    - spark: SparkSession object
    - cdm_Table: Name of the CDM table
    - cdm_field: Name of the field to check for integer datatype

    Returns:
    - num_violated_rows: Number of persons without a record in cdmTable
    - pct_violated_rows: Percentage of persons without a record in cdmTable
    - num_denominator_rows: Total number of persons
    """
    
    # Read CSV file into DataFrame
    cdm_table =  spark.read.csv(cdm_Table, header=True)

    # Check if the field is an integer
    num_violated_rows = cdm_table.filter(~col(cdm_field).cast("bigint").isNotNull() |
                                       (col(cdm_field).cast("bigint") != col(cdm_field))).count()

    # Total number of rows
    num_denominator_rows = cdm_table.count()

    # Calculate percentage of violated rows
    pct_violated_rows = num_violated_rows / num_denominator_rows * 100 if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows