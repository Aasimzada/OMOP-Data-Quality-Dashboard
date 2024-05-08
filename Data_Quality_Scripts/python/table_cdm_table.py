# Check name - CDM_TABLE
#Verify the existence of the CDM table.

from pyspark.sql import SparkSession

def verify_cdm_table_existence(spark, cdm_Table):
    """
    
    Parameters:
    - spark: SparkSession object
    - cdm_Table: Path to the CDM table CSV file
    
    Returns:
    - num_violated_rows: Number of files that do not exist (should be either 0 or 1)
    - pct_violated_rows: Percentage of files that do not exist (should be either 0 or 100)
    - num_denominator_rows: Total number of files checked (always 1)
    """

    try:
        # Try reading the CSV file
        df = spark.read.option("header", "true").csv(cdm_Table)

        # If reading is successful, consider the file exists
        num_violated_rows = 0
    except:
        # If reading fails, consider the file does not exist
        num_violated_rows = 1

    # Total number of files checked (always 1)
    num_denominator_rows = 1

    # Calculate the percentage of files that do not exist
    pct_violated_rows = (num_violated_rows / num_denominator_rows) * 100

    return num_violated_rows, pct_violated_rows, num_denominator_rows
