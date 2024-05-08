# Check name - plausibleGender

#For a CONCEPT_ID conceptId (conceptName), the number and percent of records associated with patients with an implausible gender (correct gender = plausibleGender).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def check_plausible_gender(spark, cdm_Table, person, cdm_field, concept_id, plausible_gender):    
    """

    Parameters:
    - spark: SparkSession object
    - person : Path to the CSV file containing the person data
    - cdm_Table: Path to the CSV file for the cdmTable data
    - cdm_field: Name of the field to check for integer datatype
    - concept_id: ID of the concept to be evaluated 
    - plausible_gender: the gender to be checked for

    Returns:
    - num_violated_rows: Number of persons without a record in cdmTable
    - pct_violated_rows: Percentage of persons without a record in cdmTable
    - num_denominator_rows: Total number of persons
    """

    # Read CSV files for CDM table and person table
    cdm_table = spark.read.csv(cdm_Table, header=True)
    person_table = spark.read.csv(person, header=True)
  
    # Filter CDM table to select records with the specified concept ID
    filtered_cdm_table = cdm_table.filter(col(cdm_field) == concept_id)

    # Join filtered CDM table with person table based on person_id
    joined_df = filtered_cdm_table.join(person_table, filtered_cdm_table.person_id == person_table.person_id)

    # Apply condition to check for implausible gender
    if plausible_gender == 'Male':
        violated_records = joined_df.filter(person_table.gender_concept_id != 8507)
    else:
        violated_records = joined_df.filter(person_table.gender_concept_id != 8532)

    # Count the number of violated rows
    num_violated_rows = violated_records.count()

    # Count the total number of rows in the CDM table for the denominator
    num_denominator_rows = cdm_table.filter(col(cdm_field) == concept_id).count()

    # Calculate percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) * 100 if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows
