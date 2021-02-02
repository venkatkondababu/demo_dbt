{{ config(
	materialized = 'table',
	schema = 'engineering',
	tags=['daily']
) }}
{% set username = "venkatk" %}
WITH source_data AS (
	SELECT
	ppt.*,
	cs.sk_clinicalsite_id,
	//t.sk_target_type_id,
	s.sk_series_id
	FROM
	{{ source('stage','PLANPOINT_TARGET') }}
	ppt left JOIN  {{ ref('dim_clinicalsite')}} 	cs 	ON trim(ppt."SiteName",'"') = cs.clinicalsite
	//left JOIN  {{ ref('dim_target_type') }} t ON trim(ppt."TargetType",'"') = t."TARGET_TYPE"
	left JOIN  {{ ref('fct_planpoint_series') }} s ON trim(ppt."SeriesUID",'"') = s.series_uid
)
SELECT
	sk_clinicalsite_id,sk_series_id,
	trim("TargetUID",'"') as target_uid,
	"TargetID" as target_id,
	"TargetLabel" as target_label,
	"TargetPosition" as target_position_array,
	trim("TargetAxes",'"') as target_axes_array,
	"ProperContainmentFlag" as proper_containment_flag,
	"IntersectionFlag" as intersection_flag,
	trim("TargetQuadrant",'"') as target_quadrant,
	"TargetLobe" as target_lobe,
	"ParkingLobes" as parking_lobes_array,
	"ParkingGenerationsOut" as parking_generations_out,
	"VolumeDirectory" as volumes_directory,
	trim("ThroughLineLengths",'"') as through_line_length_array,
	trim("PullDate",'"') as pull_date,
	{{ dbt_utils.current_timestamp() }}
	currentdate,
	'{{username}}' currentuser,
	{{ dbt_utils.current_timestamp() }}
	modifieddate,
	'{{username}}' modifieduser
FROM
source_data