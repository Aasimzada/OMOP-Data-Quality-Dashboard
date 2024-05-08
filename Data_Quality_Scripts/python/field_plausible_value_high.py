# Check name - plausibleValueHigh

#For the combination of CONCEPT_ID conceptId (conceptName) and UNIT_CONCEPT_ID unitConceptId (unitConceptName), the number and percent of records that have a value higher than plausibleValueHigh.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from datetime import datetime, timedelta

def check_plausible_Value_High(spark, cdm_Table, cdm_field, plausible_value_high):
    """
    Parameters:
    - spark: SparkSession object
    - cdm_Table: Path to the CSV file containing the CDM table data
    - cdm_field: Name of the field to check
    - plausible_value_high: Plausible value bound (high)

    Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of rows in the CDM table
    """
    
    # Read the CDM table CSV file into a DataFrame
    cdm_table = spark.read.csv(cdm_Table, header=True)

    # Apply conditions to filter rows based on the type of plausible value
    if plausible_value_high == 'DATEADD(dd,1,GETDATE())':
        plausible_date = datetime.now() + timedelta(days=1)
        violated_df = cdm_table.filter(col(cdm_field) > plausible_date)
    elif plausible_value_high == 'YEAR(GETDATE())+1':
        plausible_year = datetime.now().year + 1
        violated_df = cdm_table.filter(expr(f"YEAR({cdm_field}) > {plausible_year}"))
    else:
        plausible_value = float(plausible_value_high) if '.' in plausible_value_high else int(plausible_value_high)
        violated_df = cdm_table.filter(col(cdm_field) > plausible_value)

    # Count the number of violating rows
    num_violated_rows = violated_df.count()

    # Count the total number of rows
    num_denominator_rows = cdm_table.count()

    # Calculate the percentage of violating rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) * 100 if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows


