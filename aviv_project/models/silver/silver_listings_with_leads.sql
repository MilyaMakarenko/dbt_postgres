{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT 
    l.listing_id,
    l.property_type,
    l.city,
    l.region,
    l.price,
    l.created_at,
    COUNT(lc.contact_id) as total_leads,
    COUNT(DISTINCT lc.contact_source) as unique_sources,
    MIN(lc.contact_timestamp) as first_lead,
    MAX(lc.contact_timestamp) as last_lead
FROM {{ ref('bronze_layer_listings') }} l
LEFT JOIN {{ ref('bronze_layer_leads') }} lc 
    ON l.listing_id = lc.listing_id
GROUP BY 
    l.listing_id,
    l.property_type,
    l.city,
    l.region,
    l.price,
    l.created_at
    