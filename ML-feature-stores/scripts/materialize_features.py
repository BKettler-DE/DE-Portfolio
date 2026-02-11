"""
Feature Materialization Script
Syncs batch features from PostgreSQL to Redis for online serving

Usage:
    python scripts/materialize_features.py
    
This should run daily after dbt builds fresh features
"""

import sys
from pathlib import Path

# Add feature_store to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from feature_store.online_store import OnlineFeatureStore
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """
    Materialize features from PostgreSQL to Redis
    This is the glue between batch feature engineering and online serving
    """
    logger.info("="*70)
    logger.info("FEATURE MATERIALIZATION JOB")
    logger.info("="*70)
    
    try:
        # Initialize online store
        store = OnlineFeatureStore()
        
        # Materialize batch features
        # TTL = 48 hours (2x the daily refresh to handle job failures)
        count = store.materialize_batch_features(ttl_seconds=172800)
        
        # Get statistics
        stats = store.get_store_stats()
        
        logger.info("="*70)
        logger.info("MATERIALIZATION SUMMARY")
        logger.info("="*70)
        logger.info(f"Equipment materialized: {count}")
        logger.info(f"Features per equipment: {stats['features_per_equipment']}")
        logger.info(f"Redis memory used: {stats['memory_used_mb']} MB")
        logger.info(f"TTL: 48 hours")
        logger.info("="*70)
        logger.info("✅ Feature materialization completed successfully")
        logger.info("="*70)
        
        return 0
        
    except Exception as e:
        logger.error("="*70)
        logger.error("❌ Feature materialization failed!")
        logger.error(f"Error: {e}")
        logger.error("="*70)
        return 1


if __name__ == '__main__':
    exit(main())