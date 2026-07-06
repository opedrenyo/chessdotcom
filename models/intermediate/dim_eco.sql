{{ config(materialized='incremental', incremental_strategy='merge', unique_key='ECO_ID') }}


SELECT DISTINCT
    MD5(ECO_CODE || ECO_URL) AS ECO_ID,
    ECO_CODE::VARCHAR AS ECO_CODE,
    ECO_URL::VARCHAR AS ECO_URL
FROM {{ ref('stg_chess_matches') }}

{% if is_incremental() %}
WHERE ECO_ID NOT IN (SELECT ECO_ID FROM {{ this }})
{% endif %}