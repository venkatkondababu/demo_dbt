
{{ config(materialized = 'table',schema = 'engineering',tags=['daily']) }}
{% set username = "venkatk" %}
WITH source_data AS (
	SELECT
	ppp.*,
	md5(cast(coalesce(cast("PathID" as varchar), '') as varchar)) AS sk_path_id, cs.sk_clinicalsite_id, s.sk_series_id
	FROM
	{{ source('stage','PLANPOINT_PATH') }}
	ppp
	left JOIN  {{ ref('dim_clinicalsite') }}	cs	ON trim(ppp."SiteName", '"') = cs.clinicalsite
	left JOIN  {{ ref('fct_planpoint_series') }} s	ON trim(ppp."SeriesUID", '"') = s.series_uid
)
SELECT
	sk_path_id,
	sk_clinicalsite_id,
	sk_series_id,
	trim("PathID", '"') path_id,
	"PathLabel" path_label,
	trim("PathUID",'"') path_uid,
	"FinalParkingDistance" final_parking_distance,
	"DefaultParkingDistance" default_parking_distance,
	"FinalParkingAngle" final_parking_angle,
	"DefaultParkingAngle" default_parking_angle,
	"ParkingPosition" parking_position,
	"TargetPosition" target_position,
	"OriginPosition" origin_position,
	"CumulativeCurvature" cumulative_curvature,
	trim("ParkingQuadrant",'"') parking_quadrant,
	"ParkingLobe" parking_lobe,
	"ParkingGenerationsOut" parking_generations_out,
	"ParkingRadius" parking_radius,
	trim("PullDate",'"') pull_date,
	"VolumeDirectory" volume_directory,
	"ThroughLineLength" through_line_length,
	{{ dbt_utils.current_timestamp() }}
	currentdate,
	'{{username}}' currentuser,
	{{ dbt_utils.current_timestamp() }}
	modifieddate,
	'{{username}}' modifieduser
FROM
source_data