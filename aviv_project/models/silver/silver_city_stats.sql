-- models/silver/silver_city_stats.sql
{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT 
    l.city,
    l.region,
    COUNT(DISTINCT l.listing_id) as total_listings,
    AVG(l.price) as avg_price,
    COUNT(lc.contact_id) as total_leads,
    COUNT(DISTINCT lc.contact_source) as lead_sources_count
FROM {{ ref('bronze_layer_listings') }} l
LEFT JOIN {{ ref('bronze_layer_leads') }} lc 
    ON l.listing_id = lc.listing_id
GROUP BY l.city, l.region
ORDER BY total_leads DESC