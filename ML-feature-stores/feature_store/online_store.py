"""
Online Feature Store
Stores latest features in Redis for real-time inference
"""

import redis
import json
import psycopg2
from datetime import datetime
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OnlineFeatureStore:
    """
    Manages online features stored in Redis
    Provides sub-10ms feature retrieval for real-time inference
    """
    
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        """Initialize Redis connection"""
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=0,
            decode_responses=True  # Automatically decode bytes to strings
        )
        
        # Test connection
        try:
            self.redis_client.ping()
            logger.info(f"✅ Connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            raise
        
        # PostgreSQL connection (source)
        self.pg_conn_params = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'pipeline_db',
            'user': 'pipeline_user',
            'password': 'pipeline_pass'
        }
    
    def materialize_batch_features(self, ttl_seconds: int = 86400):
        """
        Materialize (sync) batch features from PostgreSQL to Redis
        
        This is the key operation that makes features available for inference.
        Should be run daily after dbt builds fresh features.
        
        Args:
            ttl_seconds: Time-to-live for features in Redis (default 24 hours)
        
        Returns:
            Number of features materialized
        """
        logger.info("Starting batch feature materialization to Redis...")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(**self.pg_conn_params)
        cur = conn.cursor()
        
        # Read latest batch features
        query = """
        SELECT 
            equipment_id,
            equipment_type,
            location,
            manufacturer,
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
        
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        
        logger.info(f"Read {len(rows)} equipment feature records from PostgreSQL")
        
        # Materialize to Redis
        pipeline = self.redis_client.pipeline()
        materialized_count = 0
        
        for row in rows:
            # Convert row to dictionary
            features = dict(zip(columns, row))
            equipment_id = features['equipment_id']
            
            # Convert datetime/date objects to strings for JSON
            for key, value in features.items():
                if isinstance(value, (datetime, )):
                    features[key] = value.isoformat()
            
            # Add metadata
            features['_updated_at'] = datetime.now().isoformat()
            features['_source'] = 'batch_features'
            
            # Store in Redis with key pattern: equipment:{equipment_id}
            key = f"equipment:{equipment_id}"
            pipeline.setex(
                key,
                ttl_seconds,
                json.dumps(features, default=str)
            )
            materialized_count += 1
        
        # Execute all Redis commands in batch
        pipeline.execute()
        
        cur.close()
        conn.close()
        
        logger.info(f"✅ Materialized {materialized_count} equipment features to Redis")
        logger.info(f"   TTL: {ttl_seconds} seconds ({ttl_seconds/3600:.1f} hours)")
        
        return materialized_count
    
    def get_features(self, equipment_id: str) -> Optional[Dict]:
        """
        Get features for a single equipment from Redis
        
        Args:
            equipment_id: Equipment identifier (e.g., 'PUMP-001')
        
        Returns:
            Dictionary of features or None if not found
        """
        key = f"equipment:{equipment_id}"
        
        try:
            data = self.redis_client.get(key)
            if data:
                return json.loads(data)
            else:
                logger.warning(f"Features not found for {equipment_id}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving features for {equipment_id}: {e}")
            return None
    
    def get_batch_features(self, equipment_ids: List[str]) -> Dict[str, Dict]:
        """
        Get features for multiple equipment (batch retrieval)
        
        Args:
            equipment_ids: List of equipment identifiers
        
        Returns:
            Dictionary mapping equipment_id to features
        """
        keys = [f"equipment:{eq_id}" for eq_id in equipment_ids]
        
        # Use pipeline for efficient batch retrieval
        pipeline = self.redis_client.pipeline()
        for key in keys:
            pipeline.get(key)
        
        values = pipeline.execute()
        
        # Build result dictionary
        result = {}
        for equipment_id, value in zip(equipment_ids, values):
            if value:
                result[equipment_id] = json.loads(value)
        
        logger.info(f"Retrieved features for {len(result)}/{len(equipment_ids)} equipment")
        return result
    
    def list_available_equipment(self) -> List[str]:
        """
        List all equipment that have features in Redis
        
        Returns:
            List of equipment IDs
        """
        keys = self.redis_client.keys("equipment:*")
        equipment_ids = [key.replace("equipment:", "") for key in keys]
        return sorted(equipment_ids)
    
    def get_store_stats(self) -> Dict:
        """
        Get statistics about the online feature store
        
        Returns:
            Dictionary with store statistics
        """
        equipment_count = len(self.redis_client.keys("equipment:*"))
        
        # Get Redis memory info
        info = self.redis_client.info('memory')
        memory_used_mb = info['used_memory'] / (1024 * 1024)
        
        # Sample one key to get feature count
        sample_keys = self.redis_client.keys("equipment:*")
        feature_count = 0
        if sample_keys:
            sample_data = self.redis_client.get(sample_keys[0])
            if sample_data:
                feature_count = len(json.loads(sample_data))
        
        return {
            'equipment_count': equipment_count,
            'features_per_equipment': feature_count,
            'memory_used_mb': round(memory_used_mb, 2),
            'redis_db': 0
        }
    
    def clear_all_features(self):
        """
        Clear all features from Redis (use with caution!)
        """
        keys = self.redis_client.keys("equipment:*")
        if keys:
            self.redis_client.delete(*keys)
            logger.info(f"Cleared {len(keys)} feature keys from Redis")
        else:
            logger.info("No features to clear")


if __name__ == '__main__':
    # Demo usage
    print("="*70)
    print("ONLINE FEATURE STORE - DEMO")
    print("="*70)
    
    store = OnlineFeatureStore()
    
    # Clear any existing features
    print("\n1. Clearing existing features...")
    store.clear_all_features()
    
    # Materialize batch features to Redis
    print("\n2. Materializing batch features to Redis...")
    count = store.materialize_batch_features(ttl_seconds=86400)  # 24 hour TTL
    
    # List available equipment
    print("\n3. Available equipment in online store:")
    equipment_list = store.list_available_equipment()
    for eq_id in equipment_list[:5]:  # Show first 5
        print(f"   - {eq_id}")
    if len(equipment_list) > 5:
        print(f"   ... and {len(equipment_list) - 5} more")
    
    # Get features for single equipment
    print("\n4. Retrieving features for PUMP-001:")
    features = store.get_features('PUMP-001')
    if features:
        print(f"   Equipment Type: {features.get('equipment_type')}")
        print(f"   Age (days): {features.get('equipment_age_days')}")
        print(f"   Days Since Maintenance: {features.get('days_since_maintenance')}")
        print(f"   Failure Count (90d): {features.get('failure_count_90d')}")
        print(f"   Risk Tier: {features.get('risk_tier')}")
        print(f"   Updated At: {features.get('_updated_at')}")
    
    # Batch retrieval
    print("\n5. Batch retrieval for multiple equipment:")
    batch_features = store.get_batch_features(['PUMP-001', 'MOTOR-001', 'COMPRESSOR-001'])
    print(f"   Retrieved features for {len(batch_features)} equipment")
    for eq_id, feats in batch_features.items():
        print(f"   - {eq_id}: risk_tier={feats.get('risk_tier')}")
    
    # Store statistics
    print("\n6. Online store statistics:")
    stats = store.get_store_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    print("\n" + "="*70)
    print("✅ Online feature store demo complete!")
    print("="*70)
    print("\nFeatures are now available in Redis for real-time inference!")
    print("Inference latency: <10ms per lookup")
    print("="*70)