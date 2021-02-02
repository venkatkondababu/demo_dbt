{{ config(
    materialized = 'view',
    schema = 'reporting',
) }}
{% set username = "venkatk" %}
SELECT
procedure_id as procedureid,
software_version as swversion,
cs.clinicalsite::VARCHAR as clinicalsite
FROM
{{ ref('fct_system_procedure') }} fsp 
JOIN {{ ref('dim_clinicalsite') }}
cs
ON fsp.sk_clinicalsite_id = cs.sk_clinicalsite_id 