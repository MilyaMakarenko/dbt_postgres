{{ config(
    materialized='ephemeral',
    schema='bronze'
) }}

SELECT * FROM bronze_layer_listings