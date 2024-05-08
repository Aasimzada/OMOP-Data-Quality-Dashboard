# Check name - fkClass

#The number and percent of records that have a value in the cdmFieldName field in the cdmTableName table that do not conform to the fkClass class.

from pyspark.sql import SparkSession

def check_fk_class(spark, cdm_Table , cdm_field, concept , fk_class):
    """

    Parameters:
    - spark: SparkSession object
    - cdm_Table: Name of the CDM table
    - cdm_field: Name of the field to check
    - concept: Path to the concept table CSV file
    - fk_class: Expected concept class

    Returns:
    - num_violated_rows: Number of persons without a record in cdmTable
    - pct_violated_rows: Percentage of persons without a record in cdmTable
    - num_denominator_rows: Total number of persons
    """

    # Read CDM table and concept table
    cdm_table = spark.read.csv(cdm_Table, header=True, inferSchema=True)
    concept_table = spark.read.csv(concept, header=True, inferSchema=True)

    # Join the CDM table with the concept table based on the specified field
    joined_df = cdm_table.join(concept_table, cdm_table[cdm_field] == concept_table["concept_id"], "left")

    # Filter the joined DataFrame to find violating records
    violating_records = joined_df.filter((joined_df["concept_id"].isNull()) | (joined_df["concept_class_id"] != fk_class))

    # Count the number of violating rows
    num_violated_rows = violating_records.count()

    # Count the total number of rows in the CDM table
    num_denominator_rows = cdm_table.count()

    # Calculate the percentage of violating rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) * 100 if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows