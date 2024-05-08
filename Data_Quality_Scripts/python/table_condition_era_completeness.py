# Check name - measureConditionEraCompleteness

#The number and Percent of persons that does not have condition_era built successfully for all persons in condition_occurrence

from pyspark.sql import SparkSession

def measure_condition_era_completeness(spark, condition_occurrence, condition_era):
    """
    Determine the number and percentage of persons with condition era built successfully
    for persons in the condition occurrence table.
    
    Parameters:
    - spark: SparkSession object
    - condition_occurrence_csv: Path to the CSV file containing condition_occurrence data
    - condition_era_csv: Path to the CSV file containing condition_era data
    
    Returns:
    - num_violated_rows: Number of persons without condition era built successfully
    - pct_violated_rows: Percentage of persons without condition era built successfully
    - num_denominator_rows: Total number of persons in the condition occurrence table
    """
    
    # Read condition_occurrence and condition_era CSV files into DataFrames
    condition_occurrence_df =  spark.read.csv(condition_occurrence, header=True)
    condition_era_df =  spark.read.csv(condition_era, header=True)

    # Perform left join and filter where there are no corresponding records in condition_era
    violated_rows_df = condition_occurrence_df.join(condition_era_df, "person_id", "left_outer") \
                                               .filter(condition_era_df.person_id.isNull()) \
                                               .select("person_id").distinct()

    # Count the number of violated rows
    num_violated_rows = violated_rows_df.count()

    # Count the total number of distinct persons in the condition occurrence table
    num_denominator_rows = condition_occurrence_df.select("person_id").distinct().count()

    # Calculate the percentage of violated rows
    pct_violated_rows = (num_violated_rows / num_denominator_rows) if num_denominator_rows != 0 else 0

    return num_violated_rows, pct_violated_rows, num_denominator_rows

