# Check name - plausibleValueLow

#For the combination of CONCEPT_ID conceptId (conceptName) and UNIT_CONCEPT_ID unitConceptId (unitConceptName), the number and percent of records that have a value less than plausibleValueLow.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from datetime import datetime

def check_plausible_Value_Low(spark, cdm_Table, cdm_field, plausible_value_low):
    """
    Parameters:
    - spark: SparkSession object
    - cdm_Table: Path to the CSV file containing the CDM table data
    - cdm_field: Name of the field to check
    - plausible_value_low: Plausible value bound (low)

    Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of rows in the CDM table
    """
    
    # Read the CDM table CSV file into a DataFrame
    cdm_table = spark.read.csv(cdm_Table, header=True)

    # Check if plausible_value_low is a float or an integer
    if isinstance(plausible_value_low, float) or isinstance(plausible_value_low, int):
        # Filter rows where the BIRTH_DATETIME is below the plausible value low
        violated_df = cdm_table.filter(col(cdm_field) < plausible_value_low)
    else:
        # Convert plausible_value_low to a date object
        plausible_date = datetime.strptime(str(plausible_value_low), '%Y%m%d').date()
        # Filter rows where the BIRTH_DATETIME is below the plausible value low
        violated_df = cdm_table.filter(expr(f"CAST({cdm_field} AS DATE) < '{plausible_date}'"))

    # Count the number of violating rows
    num_violated_rows = violated_df.count()

    # Count the total number of rows
    num_denominator_rows = cdm_table.filter(col(cdm_field).isNotNull()).count()

    # Calculate the percentage of violating rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) * 100 if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows


