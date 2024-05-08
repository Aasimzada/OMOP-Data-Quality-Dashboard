# Check name - IS_FOREIGN_KEY

#The number and percent of records that have a value in the @cdmFieldName field in the @cdmTableName table that does not exist in the @fkTableName table.

from pyspark.sql import SparkSession

def foreign_key_check(spark, cdm_Table, fk_Table, cdm_field, fk_field):
    """
    Perform a foreign key check using DataFrames from CSV files.
    
    Parameters:
    - spark: SparkSession object
    - cdm_Table: Path to the CDM table CSV file
    - fk_Table: Path to the foreign key table  CSV file
    - cdm_field: Name of the field in the CDM table
    - fk_field: Name of the field in the foreign key table
    
    Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of rows in the CDM table
    """

    # Read CDM table and vocabulary table
    cdm_table = spark.read.csv(cdm_Table, header=True)
    fk_table = spark.read.csv(fk_Table, header=True) 


    # Perform left join to find violating rows
    violated_rows_df = cdm_table.join(fk_table, cdm_table[cdm_field] == fk_table[fk_field], "left_outer") \
                              .filter(fk_table[fk_field].isNull() & cdm_table[cdm_field].isNotNull()) \
                              .select(cdm_table[cdm_field])

    # Count the number of violated rows
    num_violated_rows = violated_rows_df.count()

    # Count the total number of rows in the CDM table
    num_denominator_rows = cdm_table.count()

    # Calculate the percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows



