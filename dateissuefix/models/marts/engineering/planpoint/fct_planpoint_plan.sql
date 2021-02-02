{{ config(materialized = 'table',schema = 'engineering',tags=['daily']) }}
{% set username = "venkatk" %}
WITH source_data AS (
	SELECT
	ppp.*,
	md5(cast(coalesce(cast("PlanID" as varchar), '') as varchar)) AS sk_plan_id,
	cs.sk_clinicalsite_id, p.sk_procedure_id
	FROM
	{{ source('stage','PLANPOINT_PLAN') }}
	ppp
	left JOIN  {{ ref('dim_clinicalsite') }}	cs	ON trim(ppp."SiteName", '"') = cs.clinicalsite
	left JOIN  {{ ref('fct_system_procedure') }}	p ON trim(ppp."procedureID", '"') = p.procedure_id
)
SELECT
	sk_plan_id, sk_clinicalsite_id,sk_procedure_id,
	"QMLActions" as qml_actions,
	"PathDefaults" as path_defaults,
	"ExportTime" as export_date_time,
	"CTTime" as ct_date_time,
	"PlanID" as plan_id,
	"SeriesUID" as series_uid,
	"BootDate" as boot_date,
	to_timestamp_ntz(concat(   TO_VARCHAR(TO_DATE("BootDate",'YYYYMMDD'),'YYYY-MM-DD'),   ' ', TO_VARCHAR(TO_TIME("BootTime",'HH24MISS'),'HH24:MI:SS')   )) as boot_date_time,
	{{ dbt_utils.current_timestamp() }}
	currentdate,
	'{{username}}' currentuser,
	{{ dbt_utils.current_timestamp() }}
	modifieddate,
	'{{username}}' modifieduser
FROM
source_data