
/*********
FIELD_IS_REQUIRED

For each table, check that the fields in which IS_NOT_NULLABLE == TRUE, there are no null values in that field.

Parameters used in this template:
schema = @schema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName

**********/


SELECT 
  num_violated_rows, 
	CASE 
		WHEN denominator.num_rows = 0 THEN 0 
		ELSE 1.0*num_violated_rows/denominator.num_rows
	END AS pct_violated_rows, 
	denominator.num_rows AS num_denominator_rows
FROM (
	SELECT 
	  COUNT_BIG(violated_rows.violating_field) AS num_violated_rows
	FROM (
		/*violatedRowsBegin*/
		SELECT 
		  '@cdmTableName.@cdmFieldName' AS violating_field, 
		  cdmTable.* 
		FROM @schema.@cdmTableName cdmTable
		  
		WHERE cdmTable.@cdmFieldName IS NULL
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT_BIG(*) AS num_rows
	FROM @schema.@cdmTableName cdmTable
	  
) denominator
;
