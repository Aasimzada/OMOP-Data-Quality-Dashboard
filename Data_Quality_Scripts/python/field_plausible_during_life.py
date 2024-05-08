# Check name - plausibleDuringLife

#The number and percent of records with a date value in the cdmFieldName field of the cdmTableName table that occurs after death.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, date_add

def check_plausible_During_Life(spark, cdm_Table, death_Table, cdm_field):
    """

    Parameters:
    - spark : SparkSession object
    - cdm_Table : Path to the CSV file containing the CDM table data
    - death_Table : Path to the CSV file containing the death data
    - cdm_field_name : Date field in the CDM table to check against death date

    Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of rows in the CDM table
    """

    # Load the CDM table and death table
    cdm_table = spark.read.csv(cdm_Table, header=True)
    death_table = spark.read.csv(death_Table, header=True) 

    # Join the CDM table with the death table on person_id
    condition = "CAST(cdm.{} AS DATE) > date_add(CAST(death.death_date AS DATE), 60)".format(cdm_field)
    join_df = cdm_table.alias("cdm").join(death_table.alias("death"), "person_id", "inner")\
        .filter(expr(condition))

    # Count the number of violating rows
    num_violated_rows = join_df.count()

    # Count the total rows in the subset from the CDM table that are also in the death table
    death_person_ids = death_table.select("person_id").distinct()
    denominator_df = cdm_table.join(death_person_ids, "person_id", "inner")
    num_denominator_rows = denominator_df.count()

    # Calculate the percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows * 100) if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows