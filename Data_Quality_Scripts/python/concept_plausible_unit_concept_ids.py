from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def check_plausible_unit_concept_ids(spark, cdm_Table, cdm_field, concept_id, plausible_Unit_Concept_Ids):
    """
    Parameters:
    - spark: SparkSession object
    - cdm_Table: Path to the CSV file for the cdmTable data
    - cdm_field: Name of the field to check for integer datatype
    - concept_id: ID of the concept to be evaluated 
    - plausible_unit_concept_id: Plausible unit concept IDs as a string

    Returns:
    - num_violated_rows: Number of records with implausible unit concept IDs
    - pct_violated_rows: Percentage of records with implausible unit concept IDs
    - num_denominator_rows: Total number of records
    """

  # Read CSV file into DataFrame
    cdm_table = spark.read.csv(cdm_Table, header=True, inferSchema=True)

    # Convert plausible_unit_concept_ids from comma-separated string to a list of integers
    plausible_ids_list = list(map(int, plausible_Unit_Concept_Ids.split(',')))

    # Filter CDM table to select records matching the specified concept ID
    filtered_cdm_table = cdm_table.filter(
        (col(cdm_field) == concept_id) &
        cdm_table['value_as_number'].isNotNull() &
        (cdm_table['unit_source_value'].isNotNull() | (cdm_table['unit_source_value'] != ""))
    )

    # Identify records where unit_concept_id is not in the list of plausible IDs
    violated_records = filtered_cdm_table.filter(~col("unit_concept_id").isin(plausible_ids_list))
    
    # Count the number of violated rows
    num_violated_rows = violated_records.count()

    # Count the total number of rows in the CDM table for the denominator
    num_denominator_rows = filtered_cdm_table.count()

    # Calculate percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) * 100 if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows