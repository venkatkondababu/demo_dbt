{{ config(
    materialized = 'table',
    tags=['daily']
) }}

{% set username = "sponram" %}
WITH source_data AS (

    SELECT
        s.sk_session_id,
        A.*
    FROM
        (
            SELECT
                p.procedureid,
                cs.sk_clinicalsite_id,
                sys.sk_system_id,
                p.starttime procedure_starttime,
                p.endtime procedure_endtime,
                p.ismpr,
                p.issfhu,
                p.paths,
                p.frls
            FROM
                {{ source(
                    'ion',
                    'ion_procedures'
                ) }}
                p
                JOIN {{ ref('dim_clinicalsite') }}
                cs
                ON p.clinicalsite = cs.clinicalsite
                JOIN {{ ref('dim_system') }}
                sys
                ON p.robot = sys.system
        ) A
        JOIN {{ ref('fct_system_session') }}
        s
        ON s.sk_system_id = A.sk_system_id
        AND s.sk_clinicalsite_id = A.sk_clinicalsite_id
        AND s.path = A.paths [0]
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['procedureid']
    ) }} AS sk_procedure_id,
    sk_session_id,
    sk_clinicalsite_id,
    sk_system_id,
    procedureid,
    procedure_starttime,
    procedure_endtime,
    ismpr is_mpr,
    issfhu is_sfhu,
    paths,
    frls,
    {{ dbt_utils.current_timestamp() }}
    currentdate,
    '{{username}}' currentuser,
    {{ dbt_utils.current_timestamp() }}
    modifieddate,
    '{{username}}' modifieduser
FROM
    source_data
