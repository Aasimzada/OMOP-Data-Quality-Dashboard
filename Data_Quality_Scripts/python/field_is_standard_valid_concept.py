# Check name - isStandardValidConcept

#The number and percent of records that do not have a standard, valid concept in the cdmFieldName field in the cdmTableName table

from pyspark.sql.functions import col, expr, when, count

def check_isStandard_Valid_Concept(spark, cdm_Table, concept, cdm_field):
    """

    Parameters:
    - spark : SparkSession object
    - cdm_Table : Path to the CSV file containing the CDM table data
    - concept : Path to the CSV file containing the concept table data
    - cdm_field : Field in the CDM table to check for violations

    Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of rows in the CDM table
    """
    # Load the CDM and concept tables
    cdm_table = spark.read.csv(cdm_Table, header=True)
    vocab_table_concept = spark.read.csv(concept, header=True)

    # Create a joined DataFrame based on the concept ID
    joined_df = cdm_table.join(
        vocab_table_concept,
        cdm_table[cdm_field] == vocab_table_concept["concept_id"],
        "left"
    )

    # Define the condition for a violating row
    violating_condition = (vocab_table_concept["concept_id"] != 0) & (
        (vocab_table_concept["standard_concept"] != 'S') | (vocab_table_concept["invalid_reason"].isNotNull())
    )

    # Count the violating rows
    num_violated_rows = joined_df.filter(violating_condition).count()

    # Count the total rows in the CDM table
    num_denominator_rows = cdm_table.count()

    # Calculate the percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows * 100) if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows