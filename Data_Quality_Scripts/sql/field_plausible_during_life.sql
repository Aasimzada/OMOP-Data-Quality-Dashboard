
/*********
PLAUSIBLE_DURING_LIFE
get number of events that occur after death event (PLAUSIBLE_DURING_LIFE == Yes)

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
    		
    	JOIN @Schema.death de ON cdmTable.person_id = de.person_id
    	WHERE CAST(cdmTable.@cdmFieldName AS DATE) > DATEADD(day, 60, CAST(de.death_date AS DATE))
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
		COUNT_BIG(*) AS num_rows
	FROM @schema.@cdmTableName cdmTable
		
	WHERE person_id IN
		(SELECT 
			person_id 
		FROM @schema.death)
) denominator
;
