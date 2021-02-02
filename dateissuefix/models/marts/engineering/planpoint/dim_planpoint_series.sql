{{ config(materialized = 'table',schema = 'engineering',tags=['daily'])}}
{% set username = "venkatk" %}
WITH source_data AS (
	SELECT
	pps.*,
	md5(cast(coalesce(cast("SeriesUID" as varchar), '') as varchar)) AS sk_series_id,
	cs.sk_clinicalsite_id
	FROM
	{{ source('stage','PLANPOINT_SERIES') }} pps
	left JOIN  {{ ref('dim_clinicalsite') }} cs ON trim(pps."SiteName", '"') = cs.clinicalsite
)
SELECT
	sk_series_id,
	trim("SeriesUID",'"') series_uid,
	"ProcedureID" as procedure_id,
	{{ dbt_utils.current_timestamp() }}
	currentdate,
	'{{username}}' currentuser,
	{{ dbt_utils.current_timestamp() }}
	modifieddate,
	'{{username}}' modifieduser
FROM
source_data