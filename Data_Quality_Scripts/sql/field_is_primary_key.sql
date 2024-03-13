
/*********
FIELD_IS_PRIMARY_KEY

Primary Key - verify those fields where IS_PRIMARY_KEY == Yes, the values in that field are unique

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
FROM
(
	SELECT 
		COUNT_BIG(violated_rows.violating_field) AS num_violated_rows
	FROM
	(
		/*violatedRowsBegin*/
		SELECT 
			'@cdmTableName.@cdmFieldName' AS violating_field, 
			cdmTable.* 
		FROM @schema.@cdmTableName cdmTable
			
		WHERE cdmTable.@cdmTableName IN ( 
			 SELECT 
                @cdmFieldName 
            FROM @schema.@cdmTableName
            GROUP BY @cdmFieldName
            HAVING COUNT_BIG(*) > 1 
		)
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
		COUNT_BIG(*) AS num_rows
	FROM @schema.@cdmTableName cdmTable
		
) denominator
;
