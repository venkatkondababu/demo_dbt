{{ config(
    materialized = 'view',
    schema = 'reporting',
) }}
{% set username = "venkatk" %}
SELECT
cs.clinicalsite as sitename,
plan_id as planid,
ct_date_time:: TIMESTAMP as cttime,
export_date_time:: TIMESTAMP as EXPORTTIME,
series_uid as seriesuid	,
boot_date as bootdate
FROM
{{ ref('fct_planpoint_plan') }} fpp
left JOIN {{ ref('dim_clinicalsite') }} cs
ON fpp.sk_clinicalsite_id = cs.sk_clinicalsite_id


