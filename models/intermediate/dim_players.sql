{{ config(materialized='incremental', incremental_strategy='merge', unique_key='player_id') }}

SELECT
    player_id::VARCHAR AS player_id
FROM {{ ref('players') }}

{% if is_incremental() %}
WHERE player_id NOT IN (SELECT player_id FROM {{ this }})
{% endif %}