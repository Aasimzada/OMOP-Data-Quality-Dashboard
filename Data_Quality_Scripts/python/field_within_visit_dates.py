# Check name - WITHIN_VISIT_DATES 

#find events that occur one week before the corresponding visit_start_date or one week after the corresponding visit_end_date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count

def check_within_visit_dates(spark, cdm_Table, visit_occurrence, cdm_field):
    """

    Parameters:
    - spark : SparkSession object
    - cdm_Table : Path to the CSV file containing the CDM table data
    - cdm_field_name : Date field in the CDM table to check against visit dates

    Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of rows in the CDM table
    """

    # Load the CDM table and visit occurrence table
    cdm_table = spark.read.csv(cdm_Table, header=True)
    visit_occurrence_table = spark.read.csv(visit_occurrence, header=True) 

    # Join the tables on visit_occurrence_id
    join_condition = cdm_table["visit_occurrence_id"] == visit_occurrence_table["visit_occurrence_id"]
    joined_df = cdm_table.join(visit_occurrence_table, join_condition, "inner")

    # Filter to find violating rows
    condition = (
        (col(cdm_field) < expr("date_add(visit_start_date, -7)")) | 
        (col(cdm_field) > expr("date_add(visit_end_date, 7)"))
    )
    violated_rows_df = joined_df.filter(condition)

    # Count the number of violating rows
    num_violated_rows = violated_rows_df.count()

    # Count the total number of rows after join for denominator
    num_denominator_rows = joined_df.count()

    # Calculate the percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows * 100) if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows