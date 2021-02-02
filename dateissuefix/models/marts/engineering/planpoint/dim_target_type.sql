{{ config(materialized = 'table',tags=['daily']) }}
{% set username = "venkatk" %}
WITH source_data AS (
	SELECT
	"TargetType",
	md5(cast(coalesce(cast("TargetType" as varchar), '') as varchar)) AS sk_target_type_id
	FROM
	{{ source(
	'stage',
	'PLANPOINT_TARGET'
	) }}
)
SELECT
	sk_target_type_id,
	"TargetType" as TARGET_TYPE,
	{{ dbt_utils.current_timestamp() }}
	currentdate,
	'{{username}}' currentuser,
	{{ dbt_utils.current_timestamp() }}
	modifieddate,
	'{{username}}' modifieduser
FROM
source_data