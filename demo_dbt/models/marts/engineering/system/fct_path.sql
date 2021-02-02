{{ config(
    materialized = 'table',
    tags=['daily']
) }}

{% set username = "venkatk" %}
WITH source_data AS (

    SELECT
        p.*
    FROM
         {{ source(
            'ion',
            'PlanPointSeries'
        ) }}
        p

)
SELECT
"Plans" as "PLANS_ARRAY","ExposureMAS" as EXPOSURE_MAS, "ExposureTimeMS" as EXPOSURE_TIME_MAS,
"Resolution" as RESOLUTION, "Slices" as SLICES, "SliceValidation" as SLICE_VALIDATION,
"SlicesRemoved" as SLICES_REMOVED,"BootPath" as BOOTPATH,
 {{ dbt_utils.current_timestamp() }}
    currentdate,
    '{{username}}' currentuser,
    {{ dbt_utils.current_timestamp() }}
    modifieddate,
    '{{username}}' modifieduser
FROM
    source_data
