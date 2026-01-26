"""
Explore PostgreSQL - Batch Database
Run: python python_scripts/explore_postgres.py
"""

import psycopg2
import json
from tabulate import tabulate

def connect_postgres():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="pipeline_db",
        user="pipeline_user",
        password="pipeline_pass"
    )

def explore_tables(conn):
    """See what tables exist"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("EXPLORING POSTGRESQL DATABASE")
    print("="*60)
    
    # List all tables
    cursor.execute("""
        SELECT table_name, 
               pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    
    tables = cursor.fetchall()
    print("\n📊 TABLES IN DATABASE:")
    print(tabulate(tables, headers=['Table Name', 'Size'], tablefmt='grid'))
    
    cursor.close()

def explore_raw_data(conn):
    """Look at raw (messy) data"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("RAW PRODUCTS (Messy Data)")
    print("="*60)
    
    cursor.execute("SELECT id, source, raw_data, processed FROM raw_products;")
    rows = cursor.fetchall()
    
    for row in rows:
        print(f"\nID: {row[0]} | Source: {row[1]} | Processed: {row[3]}")
        print(f"Raw JSON: {json.dumps(row[2], indent=2)}")
        print("-" * 60)
    
    cursor.close()

def explore_clean_data(conn):
    """Look at clean (validated) data"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("CLEAN PRODUCTS (Validated Data)")
    print("="*60)
    
    cursor.execute("""
        SELECT product_id, name, price, stock, source 
        FROM clean_products 
        ORDER BY price DESC;
    """)
    
    rows = cursor.fetchall()
    print(tabulate(rows, 
                   headers=['Product ID', 'Name', 'Price', 'Stock', 'Source'],
                   tablefmt='grid',
                   floatfmt='.2f'))
    
    cursor.close()

def run_analytics(conn):
    """Run some analytical queries"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("ANALYTICS QUERIES")
    print("="*60)
    
    # Total inventory value
    cursor.execute("""
        SELECT 
            SUM(price * stock) as total_inventory_value,
            COUNT(*) as total_products,
            AVG(price) as avg_price
        FROM clean_products;
    """)
    
    result = cursor.fetchone()
    print(f"\n💰 Total Inventory Value: ${result[0]:,.2f}")
    print(f"📦 Total Products: {result[1]}")
    print(f"📊 Average Price: ${result[2]:,.2f}")
    
    # Products by price range
    cursor.execute("""
        SELECT 
            CASE 
                WHEN price < 20 THEN 'Budget (<$20)'
                WHEN price < 60 THEN 'Mid-range ($20-60)'
                ELSE 'Premium (>$60)'
            END as price_category,
            COUNT(*) as product_count,
            SUM(stock) as total_stock
        FROM clean_products
        GROUP BY price_category
        ORDER BY product_count DESC;
    """)
    
    rows = cursor.fetchall()
    print("\n📈 Products by Price Category:")
    print(tabulate(rows, 
                   headers=['Category', 'Count', 'Total Stock'],
                   tablefmt='grid'))
    
    cursor.close()

def interactive_query(conn):
    """Let user run custom SQL"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("INTERACTIVE SQL (Type 'exit' to quit)")
    print("="*60)
    
    while True:
        query = input("\nSQL> ").strip()
        
        if query.lower() == 'exit':
            break
        
        try:
            cursor.execute(query)
            
            # If SELECT query, show results
            if query.lower().startswith('select'):
                rows = cursor.fetchall()
                if cursor.description:
                    headers = [desc[0] for desc in cursor.description]
                    print(tabulate(rows, headers=headers, tablefmt='grid'))
            else:
                conn.commit()
                print(f"✅ Query executed successfully!")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            conn.rollback()
    
    cursor.close()

if __name__ == "__main__":
    conn = connect_postgres()
    
    try:
        explore_tables(conn)
        explore_raw_data(conn)
        explore_clean_data(conn)
        run_analytics(conn)

        print("\n✨ Try running your own queries!")
        print("Example: SELECT * FROM clean_products WHERE price > 10.0 LIMIT 5;")

        interactive_query(conn)
        
    finally:
        conn.close()
        print("\n👋 Connection closed!")