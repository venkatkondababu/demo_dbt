{{ config(
    materialized = 'view',
    schema = 'reporting',
) }}
{% set username = "venkatk" %}
SELECT
cs.clinicalsite as sitename,
path_uid as pathuid,
parking_quadrant as parkingquadrant,
pull_date:: TIMESTAMP  as pulldate,
final_parking_distance::FLOAT  as finalparkingdistance,
final_parking_angle::FLOAT as finalparkingangle,
default_parking_distance::FLOAT as defaultparkingdistance,
default_parking_angle::FLOAT as defaultparkingangle
FROM
{{ ref('fct_planpoint_path') }} fpp
left JOIN {{ ref('dim_clinicalsite') }} cs ON fpp.sk_clinicalsite_id = cs.sk_clinicalsite_id
left JOIN {{ ref('dim_planpoint_series') }} fps ON fpp.sk_series_id = fps.sk_series_id

