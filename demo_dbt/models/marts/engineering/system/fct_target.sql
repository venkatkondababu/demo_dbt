{{ config(
    materialized = 'table',
    tags=['daily']
) }}

{% set username = "venkatk" %}
WITH source_data AS (

    SELECT
        p."QMLActions","PathDefaults","ExportTime","CTTime","PlanID","BootDate", "BootTime"
    FROM
         {{ source(
            'ion',
            'PlanPointPlans'
        ) }}
        p

)
SELECT
    "QMLActions" as "QML_ACTIONS","PathDefaults" as "PATH_DEFAULTS","ExportTime" as "EXPORT_DATE_TIME","CTTime" as CT_DATE_TIME,
    "PlanID" as PLAN_ID,
    to_timestamp_ntz(concat(   TO_VARCHAR(TO_DATE("BootDate",'YYYYMMDD'),'YYYY-MM-DD'),   ' ', TO_VARCHAR(TO_TIME("BootTime",'HH24MISS'),'HH24:MI:SS')   )) as BOOT_DATE_TIME,
    {{ dbt_utils.current_timestamp() }}
    currentdate,
    '{{username}}' currentuser,
    {{ dbt_utils.current_timestamp() }}
    modifieddate,
    '{{username}}' modifieduser
FROM
    source_data
