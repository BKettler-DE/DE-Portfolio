"""
Offline Feature Store
Exports features from PostgreSQL to Parquet files for ML training
"""

import psycopg2
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OfflineFeatureStore:
    """
    Manages offline features stored as Parquet files
    Provides point-in-time feature retrieval for ML training
    """
    
    def __init__(self, base_path: str = "offline_features"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # PostgreSQL connection (source)
        self.pg_conn_params = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'pipeline_db',
            'user': 'pipeline_user',
            'password': 'pipeline_pass'
        }
        
        logger.info(f"Offline feature store initialized at {self.base_path}")
    
    def export_features(self, feature_date: date = None):
        """
        Export features from PostgreSQL to Parquet
        
        Args:
            feature_date: Date to export (defaults to today)
        """
        if feature_date is None:
            feature_date = date.today()
        
        logger.info(f"Exporting features for date: {feature_date}")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(**self.pg_conn_params)
        
        # Read features into pandas
        query = """
        SELECT 
            equipment_id,
            equipment_type,
            location,
            manufacturer,
            model,
            age_category,
            equipment_age_days,
            total_operating_hours,
            days_since_maintenance,
            maintenance_count_30d,
            failure_count_90d,
            avg_downtime_hours_90d,
            total_repair_cost_90d,
            avg_severity_score_90d,
            equipment_type_risk_score,
            risk_tier,
            feature_date,
            feature_timestamp
        FROM features.batch_equipment_features
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        logger.info(f"Read {len(df)} feature records from PostgreSQL")
        
        # Create date-partitioned directory
        partition_path = self.base_path / "equipment_features" / f"date={feature_date}"
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Export to Parquet
        output_file = partition_path / "features.parquet"
        df.to_parquet(output_file, compression='snappy', index=False)
        
        logger.info(f"✅ Exported {len(df)} records to {output_file}")
        logger.info(f"   File size: {output_file.stat().st_size / 1024:.2f} KB")
        
        return output_file
    
    def get_features(self, 
                     equipment_ids: list = None, 
                     as_of_date: date = None,
                     feature_names: list = None) -> pd.DataFrame:
        """
        Retrieve features for ML training with point-in-time correctness
        
        Args:
            equipment_ids: List of equipment IDs (None = all)
            as_of_date: Get features as they existed on this date (None = latest)
            feature_names: Specific features to retrieve (None = all)
        
        Returns:
            DataFrame with requested features
        """
        # Use DuckDB to query Parquet files
        con = duckdb.connect(':memory:')
        
        # Build query
        table_path = str(self.base_path / "equipment_features" / "*" / "*.parquet")
        
        # Start with base query
        query_parts = [f"SELECT * FROM read_parquet('{table_path}')"]
        
        # Add filters
        filters = []
        
        if as_of_date:
            filters.append(f"feature_date <= DATE '{as_of_date}'")
        
        if equipment_ids:
            ids_str = "', '".join(equipment_ids)
            filters.append(f"equipment_id IN ('{ids_str}')")
        
        if filters:
            query_parts.append("WHERE " + " AND ".join(filters))
        
        # Build the complete query
        base_query = ' '.join(query_parts)
        
        # Get latest features per equipment (point-in-time)
        query = f"""
        WITH base_features AS (
            {base_query}
        ),
        ranked_features AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY equipment_id 
                    ORDER BY feature_date DESC
                ) as rn
            FROM base_features
        )
        SELECT * EXCLUDE (rn) FROM ranked_features WHERE rn = 1
        """
        
        # Execute query
        df = con.execute(query).df()
        con.close()
        
        # Select specific columns if requested
        if feature_names:
            available_cols = [c for c in feature_names if c in df.columns]
            if 'equipment_id' not in available_cols:
                available_cols = ['equipment_id'] + available_cols
            df = df[available_cols]
        
        logger.info(f"Retrieved {len(df)} feature records")
        
        return df
    
    def list_feature_dates(self) -> list:
        """List all available feature dates"""
        equipment_path = self.base_path / "equipment_features"
        
        if not equipment_path.exists():
            return []
        
        dates = []
        for date_dir in equipment_path.iterdir():
            if date_dir.is_dir() and date_dir.name.startswith('date='):
                date_str = date_dir.name.replace('date=', '')
                dates.append(date_str)
        
        return sorted(dates)
    
    def get_feature_stats(self) -> dict:
        """Get statistics about the offline feature store"""
        con = duckdb.connect(':memory:')
        
        table_path = str(self.base_path / "equipment_features" / "*" / "*.parquet")
        
        try:
            stats_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT equipment_id) as unique_equipment,
                COUNT(DISTINCT feature_date) as feature_dates,
                MIN(feature_date) as earliest_date,
                MAX(feature_date) as latest_date
            FROM read_parquet('{table_path}')
            """
            
            result = con.execute(stats_query).fetchone()
            con.close()
            
            return {
                'total_records': result[0],
                'unique_equipment': result[1],
                'feature_dates': result[2],
                'earliest_date': result[3],
                'latest_date': result[4]
            }
        except:
            con.close()
            return {
                'total_records': 0,
                'unique_equipment': 0,
                'feature_dates': 0,
                'earliest_date': None,
                'latest_date': None
            }


if __name__ == '__main__':
    # Demo usage
    print("="*60)
    print("OFFLINE FEATURE STORE - DEMO")
    print("="*60)
    
    store = OfflineFeatureStore()
    
    # Export today's features
    print("\n1. Exporting features to Parquet...")
    store.export_features()
    
    # List available dates
    print("\n2. Available feature dates:")
    dates = store.list_feature_dates()
    for d in dates:
        print(f"   - {d}")
    
    # Get feature stats
    print("\n3. Feature store statistics:")
    stats = store.get_feature_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    # Retrieve features
    print("\n4. Retrieving features for all equipment...")
    features = store.get_features()
    print(f"   Retrieved {len(features)} records")
    print(f"   Columns: {list(features.columns)}")
    
    # Point-in-time retrieval example
    print("\n5. Point-in-time feature retrieval:")
    print(f"   Getting features as of today...")
    features_pit = store.get_features(
        equipment_ids=['PUMP-001', 'MOTOR-001'],
        as_of_date=date.today()
    )
    print(f"   Retrieved {len(features_pit)} records")
    print(features_pit[['equipment_id', 'equipment_type', 'failure_count_90d', 'risk_tier']])
    
    print("\n" + "="*60)
    print("✅ Offline feature store demo complete!")
    print("="*60)