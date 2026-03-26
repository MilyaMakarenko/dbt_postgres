# postgres_test.py
import psycopg2
import pandas as pd
import yaml

# Загружаем конфигурацию из YAML файла
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Берем параметры PostgreSQL
pg_config = config['postgresql']

print("="*50)
print("CONNECTING TO POSTGRESQL")
print("="*50)



print("\n" + "-"*50)

try:
    # Подключаемся
    conn = psycopg2.connect(**pg_config)
    print("✓ Connected to PostgreSQL!")
    
    # Проверяем версию
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print(f"✓ Version: {version[0][:60]}...")
    
    # Показываем список таблиц
    print("\n📋 TABLES:")
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'bronze_layer'
        ORDER BY table_name
    """)
    
    tables = cur.fetchall()
    if tables:
        for table in tables:
            print(f"  - {table[0]}")
    else:
        print("  No tables found")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"✗ Error: {e}")
    print("\nCheck your connection parameters in config.yaml")