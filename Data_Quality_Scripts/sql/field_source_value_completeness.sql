/*********
SOURCE_VALUE_COMPLETENESS
number of source values with 0 standard concept / number of distinct source values

Parameters used in this template:
schema = @schema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
standardConceptFieldName = @standardConceptFieldName

**********/

SELECT num_violated_rows,
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
		SELECT DISTINCT 
		  '@cdmTableName.@cdmFieldName' AS violating_field, 
		  cdmTable.@cdmFieldName
		FROM @schema.@cdmTableName cdmTable
		  
		WHERE cdmTable.@standardConceptFieldName = 0
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
	  COUNT_BIG(distinct cdmTable.@cdmFieldName) + COUNT(DISTINCT CASE WHEN cdmTable.@cdmFieldName IS NULL THEN 1 END) AS num_rows
	FROM @schema.@cdmTableName cdmTable
  	
) denominator
;
