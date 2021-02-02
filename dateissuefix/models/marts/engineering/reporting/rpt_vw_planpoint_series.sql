{{ config(
    materialized = 'view',
    schema = 'reporting',
) }}
{% set username = "venkatk" %}
SELECT
site_name as sitename,
series_uid as seriesuid,
cumulative_planning_time::FLOAT as cumulativeplanningtime,
cumulative_productive_plan_time::FLOAT  as cumulativeproductiveplantime,
plan as plans,
procedure_id as procedureid

FROM
{{ ref('fct_planpoint_series') }} fps




