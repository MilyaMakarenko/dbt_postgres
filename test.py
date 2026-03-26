import duckdb

sql_query = "SHOW TABLES"

print("="*50)
print("TABLES IN DATABASE")
print("="*50)

with duckdb.connect('data/my_db.db') as con:
    result = con.execute(sql_query).fetchall()
    for table in result:
        print(table[0])  

with duckdb.connect('data/my_db.db') as con:
    result = con.execute(sql_query).fetchall()
    
    if result:
        for table in result:
            print(f"  - {table[0]}")
    else:
        print("  No tables found")

print("="*60)
print("TOP LISTINGS BY LEADS")
print("="*60)

with duckdb.connect('data/my_db.db') as con:
    result = con.execute("""
        SELECT 
            l.listing_id,
            l.property_type,
            l.city,
            l.price,
            COUNT(lc.contact_id) as total_leads
        FROM bronze_layer_listings l
        LEFT JOIN bronze_layer_leads lc ON l.listing_id = lc.listing_id
        GROUP BY l.listing_id, l.property_type, l.city, l.price
        ORDER BY total_leads DESC
        LIMIT 10
    """).fetchall()
    
    # Выводим заголовки
    print(f"{'Listing ID':<15} {'Type':<15} {'City':<20} {'Price':<12} {'Leads':<6}")
    print("-"*70)
    
    # Выводим данные
    for row in result:
        listing_id, prop_type, city, price, leads = row
        print(f"{listing_id:<15} {prop_type:<15} {city:<20} {price:<12,} {leads:<6}")