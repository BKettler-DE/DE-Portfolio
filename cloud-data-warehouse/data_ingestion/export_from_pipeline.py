"""
Data Export Bridge - Connects Pipeline Project to Warehouse

This script extracts data from the pipeline project databases and exports
to parquet files, simulating a data lake pattern (like S3 in production).

WHAT THIS DOES:
1. Connects to PostgreSQL (your batch pipeline data)
2. Connects to TimescaleDB (your streaming pipeline data)
3. Runs SQL queries to extract the data
4. Saves results as parquet files in data_lake/ folder
5. Creates metadata about the export

WHY PARQUET?
- Industry standard for data lakes
- Columnar format (fast for analytics)
- Built-in compression (smaller files)
- Used by Snowflake, BigQuery, Redshift, Spark, etc.

IN PRODUCTION:
This would be an Airflow DAG running daily, writing to S3 instead of local files.
"""

import os
import psycopg2
import pandas as pd
from datetime import datetime
from pathlib import Path
import yaml


class PipelineDataExporter:
    """Exports data from pipeline project databases to parquet data lake"""
    
    def __init__(self, config_path='config.yaml'):
        """
        Initialize exporter with configuration
        
        This reads your config.yaml file to get database connection details
        """
        # Get the directory where THIS script is located
        script_dir = Path(__file__).parent
        
        # If config_path is just a filename (not absolute path), look in script directory
        if not Path(config_path).is_absolute():
            config_path = script_dir / config_path
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.data_lake_path = Path(self.config['data_lake']['base_path'])
        self.setup_data_lake_structure()
    
    def setup_data_lake_structure(self):
        """
        Create data lake folder structure (mimics S3 bucket structure)
        
        WHY THIS STRUCTURE?
        - raw/ = data straight from source systems (immutable)
        - batch/ = data from batch pipelines
        - streaming/ = data from streaming pipelines
        - metadata/ = information about the data
        
        This mirrors how companies organize S3 buckets
        """
        paths = [
            self.data_lake_path / 'raw' / 'batch',
            self.data_lake_path / 'raw' / 'streaming',
            self.data_lake_path / 'raw' / 'metadata',
        ]
        for path in paths:
            path.mkdir(parents=True, exist_ok=True)
        
        print(f"✅ Data lake structure created at: {self.data_lake_path}")
    
    def get_postgres_connection(self):
        """
        Connect to PostgreSQL (batch pipeline data)
        
        This connects to the database from your first project where
        your Airflow pipeline stores clean product data
        """
        return psycopg2.connect(
            host=self.config['databases']['postgres']['host'],
            port=self.config['databases']['postgres']['port'],
            database=self.config['databases']['postgres']['database'],
            user=self.config['databases']['postgres']['user'],
            password=self.config['databases']['postgres']['password']
        )
    
    def get_timescaledb_connection(self):
        """
        Connect to TimescaleDB (streaming pipeline data)
        
        This connects to the database from your first project where
        your Kafka consumer stores sensor readings
        """
        return psycopg2.connect(
            host=self.config['databases']['timescaledb']['host'],
            port=self.config['databases']['timescaledb']['port'],
            database=self.config['databases']['timescaledb']['database'],
            user=self.config['databases']['timescaledb']['user'],
            password=self.config['databases']['timescaledb']['password']
        )
    
    def export_batch_data(self):
        """
        Export batch pipeline data (products from PostgreSQL)
        
        WHAT THIS DOES:
        1. Connects to PostgreSQL
        2. Queries the clean_products table (validated data)
        3. Queries the quarantine_products table (for quality analysis)
        4. Saves both as parquet files with today's date in filename
        
        WHY BOTH CLEAN AND QUARANTINE?
        - Clean data = for analytics
        - Quarantine data = to analyze data quality trends
        
        NOTE: Schema matches your actual PostgreSQL tables from init.sql:
        - clean_products: product_id, name, price, stock, source, category, loaded_at
        - quarantine_products: id, raw_data, issues, quarantined_at
        """
        print("\n📦 Exporting batch pipeline data...")
        
        conn = self.get_postgres_connection()
        
        # Export clean products
        # Columns match your actual schema from init.sql
        query = """
        SELECT 
            product_id,
            name,
            price,
            stock,
            source,
            category,
            loaded_at
        FROM clean_products
        ORDER BY loaded_at DESC
        """
        
        df = pd.read_sql(query, conn)
        output_path = self.data_lake_path / 'raw' / 'batch' / f'clean_products_{datetime.now().strftime("%Y%m%d")}.parquet'
        df.to_parquet(output_path, index=False)
        print(f"  ✅ Exported {len(df)} clean products to {output_path.name}")
        
        # Export quarantined data (for quality analysis)
        # These are records that failed validation
        query = """
        SELECT 
            id,
            raw_data::text as raw_data,
            issues,
            quarantined_at
        FROM quarantine_products
        ORDER BY quarantined_at DESC
        """
        
        df = pd.read_sql(query, conn)
        
        # Note: raw_data is JSONB in PostgreSQL, which we cast to text
        # In the warehouse, we'll parse it back to JSON when needed
        
        output_path = self.data_lake_path / 'raw' / 'batch' / f'quarantine_products_{datetime.now().strftime("%Y%m%d")}.parquet'
        df.to_parquet(output_path, index=False)
        print(f"  ✅ Exported {len(df)} quarantined products to {output_path.name}")
        
        conn.close()
    
    def export_streaming_data(self):
        """
        Export streaming pipeline data (IoT sensors from TimescaleDB)
        
        WHAT THIS DOES:
        1. Connects to TimescaleDB
        2. Queries sensor_readings (valid data)
        3. Queries sensor_readings_invalid (failed validations)
        4. Exports last 7 days by default (configurable)
        
        WHY LAST 7 DAYS?
        - Streaming data grows quickly
        - For demo purposes, we don't need years of data
        - In production, you'd adjust based on requirements
        - Keeps parquet files manageable size
        
        NOTE: Schema matches your actual TimescaleDB tables:
        - sensor_readings: time, sensor_id, temperature, humidity, pressure, location
        - sensor_readings_invalid: time, sensor_id, raw_data, issues
        """
        print("\n🌊 Exporting streaming pipeline data...")
        
        conn = self.get_timescaledb_connection()
        
        # Export sensor readings (last 7 days by default)
        # Columns match your actual schema from init.sql
        query = """
        SELECT 
            time,
            sensor_id,
            temperature,
            humidity,
            pressure,
            location
        FROM sensor_readings
        WHERE time >= NOW() - INTERVAL '7 days'
        ORDER BY time DESC
        """
        
        df = pd.read_sql(query, conn)
        output_path = self.data_lake_path / 'raw' / 'streaming' / f'sensor_readings_{datetime.now().strftime("%Y%m%d")}.parquet'
        df.to_parquet(output_path, index=False)
        print(f"  ✅ Exported {len(df)} sensor readings to {output_path.name}")
        
        # Export invalid sensor readings
        # These are readings that failed validation
        query = """
        SELECT 
            time,
            sensor_id,
            raw_data::text as raw_data,
            issues
        FROM sensor_readings_invalid
        WHERE time >= NOW() - INTERVAL '7 days'
        ORDER BY time DESC
        """
        
        df = pd.read_sql(query, conn)
        
        # Note: raw_data is JSONB in TimescaleDB, which we cast to text
        # In the warehouse, we'll parse it back to JSON when needed
        
        output_path = self.data_lake_path / 'raw' / 'streaming' / f'sensor_readings_invalid_{datetime.now().strftime("%Y%m%d")}.parquet'
        df.to_parquet(output_path, index=False)
        print(f"  ✅ Exported {len(df)} invalid sensor readings to {output_path.name}")
        
        conn.close()
    
    def export_metadata(self):
        """
        Export pipeline metadata for analysis
        
        WHY METADATA?
        - Track when exports happened
        - Debug issues ("what version exported this?")
        - Data lineage (tracing data back to source)
        - Good practice in data engineering
        """
        print("\n📊 Exporting pipeline metadata...")
        
        # Create metadata about this export
        metadata = {
            'export_timestamp': datetime.now().isoformat(),
            'source_project': 'data-pipeline-exploration',
            'exporter_version': '1.0.0',
            'data_lake_path': str(self.data_lake_path)
        }
        
        # Save as parquet (consistent with rest of data lake)
        metadata_df = pd.DataFrame([metadata])
        output_path = self.data_lake_path / 'raw' / 'metadata' / f'export_metadata_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        metadata_df.to_parquet(output_path, index=False)
        print(f"  ✅ Exported metadata to {output_path.name}")
    
    def run_full_export(self):
        """
        Execute complete export process
        
        This is the main method that runs everything in order
        """
        print("=" * 60)
        print("🚀 Starting Pipeline Data Export")
        print("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # Export data from both pipelines
            self.export_batch_data()
            self.export_streaming_data()
            self.export_metadata()
            
            duration = (datetime.now() - start_time).total_seconds()
            
            print("\n" + "=" * 60)
            print(f"✅ Export completed successfully in {duration:.2f} seconds")
            print("=" * 60)
            print(f"\n📁 Data exported to: {self.data_lake_path}")
            # print("\nNext steps:")
            # print("  1. cd dbt_warehouse")
            # print("  2. dbt run")
            # print("  3. dbt test")
            
        except Exception as e:
            print(f"\n❌ Export failed: {e}")
            raise


def main():
    """
    Main execution function
    
    This is what runs when you execute: python export_from_pipeline.py
    """
    exporter = PipelineDataExporter()
    exporter.run_full_export()


# Standard Python pattern - only runs if script is executed directly
# (not imported as a module)
if __name__ == "__main__":
    main()