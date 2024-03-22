
/*********
PLAUSIBLE_TEMPORAL_AFTER
get number of records and the proportion to total number of eligible records with datetimes that do not occur on or after their corresponding datetimes

Parameters used in this template:
schema = @schema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
plausibleTemporalAfterTableName = @plausibleTemporalAfterTableName
plausibleTemporalAfterFieldName = @plausibleTemporalAfterFieldName

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
    		
			
    WHERE 
    	{'@plausibleTemporalAfterTableName' == 'PERSON'}?{
			COALESCE(
				CAST(plausibleTable.@plausibleTemporalAfterFieldName AS DATE),
				CAST(CONCAT(plausibleTable.year_of_birth,'-06-01') AS DATE)
			) 
		}:{
			CAST(cdmTable.@plausibleTemporalAfterFieldName AS DATE)
		} > CAST(cdmTable.@cdmFieldName AS DATE)
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
		COUNT_BIG(*) AS num_rows
	FROM @schema.@cdmTableName cdmTable
		
) denominator
;
