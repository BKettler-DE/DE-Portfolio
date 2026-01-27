"""
Product Batch Pipeline DAG
Orchestrates: Scrape → Load Raw → Validate → Load Clean/Quarantine → Report

Schedule: Daily at 2 AM
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import sys
import os
import json

# Add batch_pipeline to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'batch_pipeline'))

from scraper import ProductScraper
from validator import ProductValidator

# Default args for all tasks
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'product_batch_pipeline',
    default_args=default_args,
    description='Daily product data extraction, validation, and loading',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['batch', 'products', 'validation'],
)


def extract_products(**context):
    """
    Task 1: Extract products from sources
    Simulates scraping multiple vendors
    """
    print("Starting product extraction...")
    
    scraper = ProductScraper()
    
    # Scrape from multiple sources
    sources = ["vendor_a", "vendor_b", "vendor_c"]
    all_products = []
    
    for source in sources:
        products = scraper.scrape_source(source, num_products=15)
        all_products.extend(products)
        print(f"Scraped {len(products)} products from {source}")
    
    print(f"\nTotal products extracted: {len(all_products)}")
    
    # Push to XCom for next task
    context['task_instance'].xcom_push(key='raw_products', value=all_products)
    
    return len(all_products)


def load_to_raw_zone(**context):
    """
    Task 2: Load extracted data to raw zone
    Stores data exactly as received
    """
    print("Loading data to raw zone...")
    
    # Pull from XCom
    products = context['task_instance'].xcom_pull(
        task_ids='extract_products',
        key='raw_products'
    )
    
    if not products:
        raise ValueError("No products to load!")
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Insert into raw_products
    batch_id = context['dag_run'].run_id
    loaded_count = 0
    
    for product in products:
        try:
            cursor.execute("""
                INSERT INTO raw_products (source, ingestion_time, raw_data, batch_id, processed)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                product.get('source', 'unknown'),
                datetime.now(),
                json.dumps(product),
                batch_id,
                False
            ))
            loaded_count += 1
        except Exception as e:
            print(f"Error loading product: {e}")
            # Continue with other products
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Loaded {loaded_count} products to raw zone")
    
    # Push batch_id for next tasks
    context['task_instance'].xcom_push(key='batch_id', value=batch_id)
    
    return loaded_count


def validate_products(**context):
    """
    Task 3: Validate products from raw zone
    Applies quality checks and separates clean vs quarantine
    """
    print("Validating products...")
    
    # Get batch_id
    batch_id = context['task_instance'].xcom_pull(
        task_ids='load_to_raw_zone',
        key='batch_id'
    )
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Fetch unprocessed products from raw zone
    cursor.execute("""
        SELECT id, raw_data
        FROM raw_products
        WHERE batch_id = %s AND processed = FALSE
    """, (batch_id,))
    
    raw_records = cursor.fetchall()
    print(f"Found {len(raw_records)} records to validate")
    
    if not raw_records:
        print("No records to process")
        cursor.close()
        conn.close()
        return 0
    
    # Extract products - raw_data is already a dict!
    products = [record[1] for record in raw_records]
    raw_ids = [record[0] for record in raw_records]
    
    # Validate
    validator = ProductValidator()
    clean_products, quarantined = validator.validate_batch(products)
    
    # Print validation report
    validator.print_report()
    
    # Store results in XCom
    context['task_instance'].xcom_push(key='clean_products', value=clean_products)
    context['task_instance'].xcom_push(key='quarantined_products', value=quarantined)
    context['task_instance'].xcom_push(key='validation_stats', value=validator.get_stats())
    context['task_instance'].xcom_push(key='raw_ids', value=raw_ids)
    
    cursor.close()
    conn.close()
    
    return len(clean_products)


def load_to_clean_zone(**context):
    """
    Task 4: Load validated products to clean zone
    """
    print("Loading clean products...")
    
    # Get clean products from XCom
    clean_products = context['task_instance'].xcom_pull(
        task_ids='validate_products',
        key='clean_products'
    )
    
    if not clean_products:
        print("No clean products to load")
        return 0
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    loaded_count = 0
    
    for product in clean_products:
        try:
            cursor.execute("""
                INSERT INTO clean_products 
                (product_id, name, price, stock, source, category, loaded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE
                SET price = EXCLUDED.price,
                    stock = EXCLUDED.stock,
                    loaded_at = EXCLUDED.loaded_at
            """, (
                product['product_id'],
                product['name'],
                product['price'],
                product['stock'],
                product.get('source', 'unknown'),
                product.get('category', 'Unknown'),
                datetime.now()
            ))
            loaded_count += 1
        except Exception as e:
            print(f"Error loading clean product: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Loaded {loaded_count} clean products")
    
    return loaded_count


def load_to_quarantine(**context):
    """
    Task 5: Load rejected products to quarantine
    """
    print("Loading quarantined products...")
    
    # Get quarantined products from XCom
    quarantined = context['task_instance'].xcom_pull(
        task_ids='validate_products',
        key='quarantined_products'
    )
    
    if not quarantined:
        print("No products to quarantine")
        return 0
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    loaded_count = 0
    
    for item in quarantined:
        try:
            cursor.execute("""
                INSERT INTO quarantine_products (raw_data, issues, quarantined_at)
                VALUES (%s, %s, %s)
            """, (
                json.dumps(item['raw_data']),
                ', '.join(item['issues']),
                item['quarantined_at']
            ))
            loaded_count += 1
        except Exception as e:
            print(f"Error loading quarantine record: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Quarantined {loaded_count} invalid products")
    
    return loaded_count


def mark_processed(**context):
    """
    Task 6: Mark raw records as processed
    """
    print("Marking records as processed...")
    
    # Get raw IDs from XCom
    raw_ids = context['task_instance'].xcom_pull(
        task_ids='validate_products',
        key='raw_ids'
    )
    
    if not raw_ids:
        print("No IDs to mark")
        return 0
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Mark as processed
    cursor.execute("""
        UPDATE raw_products
        SET processed = TRUE
        WHERE id = ANY(%s)
    """, (raw_ids,))
    
    updated = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Marked {updated} records as processed")
    
    return updated


def generate_report(**context):
    """
    Task 7: Generate pipeline execution report
    """
    print("\n" + "="*60)
    print("PIPELINE EXECUTION REPORT")
    print("="*60)
    
    # Get stats from XCom
    stats = context['task_instance'].xcom_pull(
        task_ids='validate_products',
        key='validation_stats'
    )
    
    extracted = context['task_instance'].xcom_pull(task_ids='extract_products')
    loaded_raw = context['task_instance'].xcom_pull(task_ids='load_to_raw_zone')
    loaded_clean = context['task_instance'].xcom_pull(task_ids='load_to_clean_zone')
    loaded_quarantine = context['task_instance'].xcom_pull(task_ids='load_to_quarantine')
    
    print(f"\nExtraction:")
    print(f"  Products extracted: {extracted}")
    print(f"  Loaded to raw zone: {loaded_raw}")
    
    print(f"\nValidation:")
    if stats:
        print(f"  Total validated: {stats.get('total', 0)}")
        print(f"  ✅ Valid: {stats.get('valid', 0)}")
        print(f"  ❌ Invalid: {stats.get('invalid', 0)}")
        print(f"  🔄 Duplicates: {stats.get('duplicates', 0)}")
        
        if stats.get('total', 0) > 0:
            quality_pct = (stats.get('valid', 0) / stats.get('total', 0)) * 100
            print(f"  📈 Data Quality: {quality_pct:.1f}%")
    
    print(f"\nLoading:")
    print(f"  Clean products loaded: {loaded_clean}")
    print(f"  Products quarantined: {loaded_quarantine}")
    
    print("="*60 + "\n")
    
    return "Report generated"


# Define tasks
extract_task = PythonOperator(
    task_id='extract_products',
    python_callable=extract_products,
    dag=dag,
)

load_raw_task = PythonOperator(
    task_id='load_to_raw_zone',
    python_callable=load_to_raw_zone,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_products',
    python_callable=validate_products,
    dag=dag,
)

load_clean_task = PythonOperator(
    task_id='load_to_clean_zone',
    python_callable=load_to_clean_zone,
    dag=dag,
)

load_quarantine_task = PythonOperator(
    task_id='load_to_quarantine',
    python_callable=load_to_quarantine,
    dag=dag,
)

mark_processed_task = PythonOperator(
    task_id='mark_processed',
    python_callable=mark_processed,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# Define dependencies
extract_task >> load_raw_task >> validate_task
validate_task >> [load_clean_task, load_quarantine_task] >> mark_processed_task >> report_task