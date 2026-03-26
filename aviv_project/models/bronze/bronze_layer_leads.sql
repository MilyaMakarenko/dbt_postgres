-- models/bronze/bronze_layer_leads.sql
{{ config(
    materialized='table',
    schema='bronze'
) }}

SELECT * FROM bronze_layer_leads