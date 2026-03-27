-- models/bronze/bronze_layer_leads.sql
{{ config(
    materialized='ephemeral',
    schema='bronze'
) }}

SELECT * 
FROM bronze_layer_leads
