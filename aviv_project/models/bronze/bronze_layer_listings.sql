{{ config(
    materialized='table',
    schema='bronze'
) }}

SELECT * FROM bronze_layer_listings