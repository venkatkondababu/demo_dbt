{{ config(
    materialized = 'view',
    schema = 'reporting',
) }}
{% set username = "venkatk" %}
SELECT distinct
 cs.clinicalsite as sitename,
fps.series_uid as seriesuid,
target_uid as targetuid,
target_axes_array as targetaxes,
target_quadrant as targetquadrant,
through_line_length_array as throughlinelengths,
pull_date:: TIMESTAMP as pulldate

FROM
{{ ref('fct_planpoint_target') }} fpt
  JOIN {{ ref('dim_clinicalsite') }} cs ON fpt.sk_clinicalsite_id = cs.sk_clinicalsite_id
JOIN {{ ref('dim_planpoint_series') }} fps ON fpt.sk_series_id = fps.sk_series_id
