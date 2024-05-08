# Check name - measurePersonCompleteness

#The number and percent of persons in the CDM that do not have at least one record in the table

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def measure_person_completeness(spark, person , cdm_Table):
    """
    
    Parameters:
    - spark: SparkSession object
    - person : Path to the CSV file containing the person data
    - cdm_Table: Path to the CSV file for the cdmTable data
    
    Returns:
    - num_violated_rows: Number of persons without a record in cdmTable
    - pct_violated_rows: Percentage of persons without a record in cdmTable
    - num_denominator_rows: Total number of persons
    """
    
    # Load CSV files into DataFrames
    person_table = spark.read.csv(person, header=True)
    cdm_table = spark.read.csv(cdm_Table, header=True)

    # Perform left join and filter where there are no corresponding records in cdmTable
    violated_rows_df = person_table.join(cdm_table, person_table.person_id == cdm_table.person_id, "left_outer") \
                                 .filter(cdm_table.person_id.isNull())

    # Count the number of violated rows
    num_violated_rows = violated_rows_df.count()

    # Count the total number of rows in the person table
    num_denominator_rows = person_table.count()

    # Calculate the percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows