{{ config(materialized='incremental', incremental_strategy='merge', unique_key='GAME_TYPE_SECS') }}

SELECT DISTINCT
    GAME_TYPE_SECS::INT AS GAME_TYPE_SECS,
    GAME_TYPE_SECS::INT/60 AS GAME_TYPE_MINS,
    CASE 
        WHEN GAME_TYPE_SECS < 60 THEN 'Bullet'
        WHEN GAME_TYPE_SECS >= 60 AND GAME_TYPE_SECS < 300 THEN 'Blitz'
        WHEN GAME_TYPE_SECS >= 300 AND GAME_TYPE_SECS < 900 THEN 'Rapid'
        ELSE 'Classical'
    END AS GAME_TYPE_CATEGORY
FROM {{ ref('stg_chess_matches') }}


{% if is_incremental() %}
WHERE GAME_TYPE_SECS NOT IN (SELECT GAME_TYPE_SECS FROM {{ this }})
{% endif %}