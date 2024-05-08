# Check name - fkDomain

#The number and percent of records that have a value in the cdmFieldName field in the cdmTableName table that do not conform to the fkDomain domain.

from pyspark.sql import SparkSession

def check_fk_domain(spark, cdm_Table , cdm_field, concept , fk_domain):
    """

    Parameters:
    - spark: SparkSession object
    - cdm_Table: Path to the CDM table CSV file
    - concept: Path to the concept table CSV file
    - cdm_field: Name of the field to check
    - fk_domain: Expected concept domain

      Returns:
    - num_violated_rows: Number of violated rows
    - pct_violated_rows: Percentage of violated rows
    - num_denominator_rows: Total number of rows in the CDM table
    """

    # Read CDM table and concept table from CSV files
    cdm_table = spark.read.csv(cdm_Table, header=True)
    vocab_table_concept = spark.read.csv(concept, header=True)  

    # Join the CDM table with the concept table based on the specified field
    joined_df = cdm_table.join(vocab_table_concept, cdm_table[cdm_field] == vocab_table_concept["concept_id"], "left")

    # Filter the joined DataFrame to find violating records
    violating_records = joined_df.filter((joined_df["concept_id"].isNull()) | (~joined_df["domain_id"].isin(fk_domain)))

    # Count the number of violating rows
    num_violated_rows = violating_records.count()

    # Count the total number of rows
    num_denominator_rows = cdm_table.count()

    # Calculate percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) * 100 if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows
