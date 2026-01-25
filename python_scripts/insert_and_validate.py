"""
Practice inserting and validating data
Run: python python_scripts/insert_and_validate.py
"""

import psycopg2
import json
from datetime import datetime

def connect_postgres():
    return psycopg2.connect(
        host="localhost", port=5432, database="pipeline_db",
        user="pipeline_user", password="pipeline_pass"
    )

def insert_raw_product(conn, product_data, source):
    """Insert messy product into raw zone"""
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO raw_products (source, raw_data)
        VALUES (%s, %s)
        RETURNING id;
    """, (source, json.dumps(product_data)))
    
    new_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    
    print(f"✅ Inserted raw product with ID: {new_id}")
    return new_id

def validate_and_clean(conn, raw_id):
    """Validate a raw product and move to clean zone"""
    cursor = conn.cursor()
    
    # Fetch raw data
    cursor.execute("SELECT raw_data FROM raw_products WHERE id = %s", (raw_id,))
    raw_json = cursor.fetchone()[0]
    
    print(f"\n🔍 Validating: {raw_json}")
    
    issues = []
    
    # Validation checks
    if 'product_id' not in raw_json:
        issues.append("Missing product_id")
    if 'name' not in raw_json or raw_json['name'] is None:
        issues.append("Missing name")
    
    # Clean price
    price_str = raw_json.get('price', '')
    try:
        price = float(str(price_str).replace('$', '').replace(',', ''))
        if price <= 0:
            issues.append("Price must be positive")
    except ValueError:
        issues.append(f"Invalid price format: {price_str}")
        price = None
    
    # Clean stock
    try:
        stock = int(raw_json.get('stock', 0))
        if stock < 0:
            issues.append("Stock cannot be negative")
    except ValueError:
        issues.append("Invalid stock format")
        stock = None
    
    # Decision: Clean or Quarantine?
    if issues:
        print(f"❌ REJECTED: {', '.join(issues)}")
        cursor.execute("""
            INSERT INTO quarantine_products (raw_data, issues)
            VALUES (%s, %s);
        """, (raw_json, ', '.join(issues)))
        
        cursor.execute("UPDATE raw_products SET processed = TRUE WHERE id = %s", (raw_id,))
        conn.commit()
        print("📦 Moved to quarantine")
    else:
        print("✅ VALID - Moving to clean zone")
        cursor.execute("""
            INSERT INTO clean_products (product_id, name, price, stock, source)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE 
            SET price = EXCLUDED.price, stock = EXCLUDED.stock;
        """, (raw_json['product_id'], raw_json['name'], price, stock, 'manual_insert'))
        
        cursor.execute("UPDATE raw_products SET processed = TRUE WHERE id = %s", (raw_id,))
        conn.commit()
        print("✨ Moved to clean zone")
    
    cursor.close()

if __name__ == "__main__":
    conn = connect_postgres()
    
    print("="*60)
    print("DATA VALIDATION DEMO")
    print("="*60)
    
    # Test Case 1: Valid product
    print("\n--- Test 1: Valid Product ---")
    valid_product = {
        "product_id": "P999",
        "name": "Test Widget",
        "price": "$29.99",
        "stock": "50"
    }
    raw_id = insert_raw_product(conn, valid_product, "test_source")
    validate_and_clean(conn, raw_id)
    
    # Test Case 2: Missing name
    print("\n--- Test 2: Missing Name ---")
    invalid_product = {
        "product_id": "P998",
        "name": None,
        "price": "19.99",
        "stock": "10"
    }
    raw_id = insert_raw_product(conn, invalid_product, "test_source")
    validate_and_clean(conn, raw_id)
    
    # Test Case 3: Negative stock
    print("\n--- Test 3: Negative Stock ---")
    invalid_stock = {
        "product_id": "P997",
        "name": "Broken Gadget",
        "price": "15.00",
        "stock": "-5"
    }
    raw_id = insert_raw_product(conn, invalid_stock, "test_source")
    validate_and_clean(conn, raw_id)
    
    # Test Case 4: Invalid price
    print("\n--- Test 4: Invalid Price ---")
    invalid_price = {
        "product_id": "P996",
        "name": "Mystery Item",
        "price": "CALL FOR PRICE",
        "stock": "3"
    }
    raw_id = insert_raw_product(conn, invalid_price, "test_source")
    validate_and_clean(conn, raw_id)
    
    conn.close()
    print("\n" + "="*60)
    print("Demo complete! Check the database:")
    print("SELECT * FROM quarantine_products;")
    print("="*60)