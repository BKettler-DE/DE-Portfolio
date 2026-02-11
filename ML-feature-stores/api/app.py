"""
FastAPI Inference API
Serves equipment failure predictions in real-time
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict
from contextlib import asynccontextmanager
import pickle
import sys
from pathlib import Path
import logging

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from feature_store.online_store import OnlineFeatureStore
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for model and feature store
model = None
feature_store = None
feature_names = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler - replaces deprecated on_event
    Runs on startup and shutdown
    """
    # Startup
    global model, feature_store, feature_names
    
    logger.info("="*70)
    logger.info("STARTING INFERENCE API")
    logger.info("="*70)
    
    # Initialize online feature store
    logger.info("1. Connecting to online feature store (Redis)...")
    feature_store = OnlineFeatureStore()
    stats = feature_store.get_store_stats()
    logger.info(f"   ✅ Connected to Redis")
    logger.info(f"   Equipment available: {stats['equipment_count']}")
    
    # Load model from MLflow artifacts
    logger.info("\n2. Loading model from MLflow...")
    
    import mlflow
    mlflow.set_tracking_uri("http://localhost:5000")
    
    try:
        # Get the latest run from the experiment
        experiment = mlflow.get_experiment_by_name("equipment_failure_prediction")
        
        if experiment:
            runs = mlflow.search_runs(
                experiment_ids=[experiment.experiment_id],
                order_by=["start_time DESC"],
                max_results=1
            )
            
            if len(runs) > 0:
                run_id = runs.iloc[0]['run_id']
                
                # Download model artifact
                client = mlflow.tracking.MlflowClient()
                local_path = client.download_artifacts(run_id, "model/model.pkl")
                
                with open(local_path, 'rb') as f:
                    model = pickle.load(f)
                
                logger.info(f"   ✅ Model loaded from run: {run_id[:8]}")
            else:
                logger.warning("   ⚠️  No trained models found")
        else:
            logger.warning("   ⚠️  Experiment not found")
            
    except Exception as e:
        logger.warning(f"   ⚠️  Could not load from MLflow: {e}")
        logger.info("   Using dummy model for demo")
        import xgboost as xgb
        model = xgb.XGBClassifier()
    
    # Define expected feature names (must match training)
    feature_names = [
        'equipment_age_days',
        'total_operating_hours',
        'days_since_maintenance',
        'maintenance_count_30d',
        'failure_count_90d',
        'avg_downtime_hours_90d',
        'total_repair_cost_90d',
        'avg_severity_score_90d',
        'equipment_type_risk_score',
        'equipment_type_Air Compressor',
        'equipment_type_Centrifugal Pump',
        'equipment_type_Electric Motor',
        'equipment_type_HVAC System',
        'age_category_aging',
        'age_category_established',
        'risk_tier_aging_risk',
        'risk_tier_high_risk',
        'risk_tier_low_risk',
        'risk_tier_medium_risk'
    ]
    
    logger.info("\n" + "="*70)
    logger.info("✅ INFERENCE API READY")
    logger.info("="*70)
    logger.info("Endpoints:")
    logger.info("  GET  /              - API information")
    logger.info("  GET  /health        - Health check")
    logger.info("  POST /predict       - Failure prediction")
    logger.info("  GET  /features/{id} - Get equipment features")
    logger.info("="*70 + "\n")
    
    yield  # Server runs here
    
    # Shutdown
    logger.info("Shutting down API...")


# Initialize FastAPI app with lifespan
app = FastAPI(
    title="Equipment Failure Prediction API",
    description="Real-time failure prediction for industrial equipment",
    version="1.0.0",
    lifespan=lifespan
)


class PredictionRequest(BaseModel):
    """Request model for predictions"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "equipment_id": "PUMP-001"
            }
        }
    )
    
    equipment_id: str = Field(..., description="Equipment identifier (e.g., 'PUMP-001')")


class PredictionResponse(BaseModel):
    """Response model for predictions"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "equipment_id": "PUMP-001",
                "failure_probability": 0.78,
                "risk_tier": "high_risk",
                "recommendation": "Schedule immediate maintenance inspection",
                "features_used": {
                    "equipment_age_days": 745,
                    "failure_count_90d": 2,
                    "days_since_maintenance": 23
                }
            }
        }
    )
    
    equipment_id: str
    failure_probability: float = Field(..., description="Probability of failure (0-1)")
    risk_tier: str = Field(..., description="Risk classification")
    recommendation: str = Field(..., description="Action recommendation")
    features_used: Dict = Field(..., description="Features used for prediction")


@app.get("/")
async def root():
    """API information"""
    return {
        "name": "Equipment Failure Prediction API",
        "version": "1.0.0",
        "description": "Real-time ML inference for predictive maintenance",
        "endpoints": {
            "health": "/health",
            "predict": "/predict (POST)",
            "features": "/features/{equipment_id} (GET)"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    
    # Check Redis connection
    redis_ok = False
    try:
        feature_store.redis_client.ping()
        redis_ok = True
    except:
        pass
    
    # Check model loaded
    model_ok = model is not None
    
    status = "healthy" if (redis_ok and model_ok) else "degraded"
    
    return {
        "status": status,
        "redis_connected": redis_ok,
        "model_loaded": model_ok,
        "equipment_available": feature_store.get_store_stats()['equipment_count'] if redis_ok else 0
    }


@app.get("/features/{equipment_id}")
async def get_features(equipment_id: str):
    """Get features for equipment from online store"""
    features = feature_store.get_features(equipment_id)
    
    if features is None:
        raise HTTPException(
            status_code=404,
            detail=f"Equipment '{equipment_id}' not found in feature store"
        )
    
    return {
        "equipment_id": equipment_id,
        "features": features
    }


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    """
    Predict equipment failure probability
    
    Returns:
        - failure_probability: Probability of failure (0-1)
        - risk_tier: Risk classification
        - recommendation: Action to take
    """
    equipment_id = request.equipment_id
    
    # 1. Get features from Redis
    logger.info(f"Prediction request for {equipment_id}")
    
    features = feature_store.get_features(equipment_id)
    
    if features is None:
        raise HTTPException(
            status_code=404,
            detail=f"Equipment '{equipment_id}' not found. Available equipment: {feature_store.list_available_equipment()}"
        )
    
    # 2. Prepare features for model
    try:
        # Create feature vector matching training format
        feature_dict = {}
        
        # Numeric features
        numeric_features = [
            'equipment_age_days', 'total_operating_hours', 
            'days_since_maintenance', 'maintenance_count_30d',
            'failure_count_90d', 'avg_downtime_hours_90d',
            'total_repair_cost_90d', 'avg_severity_score_90d',
            'equipment_type_risk_score'
        ]
        
        for feat in numeric_features:
            feature_dict[feat] = features.get(feat, 0)
        
        # One-hot encode categorical features
        equipment_type = features.get('equipment_type', '')
        age_category = features.get('age_category', '')
        risk_tier = features.get('risk_tier', '')
        
        # Equipment type one-hot
        for et in ['Air Compressor', 'Centrifugal Pump', 'Electric Motor', 'HVAC System']:
            feature_dict[f'equipment_type_{et}'] = 1 if equipment_type == et else 0
        
        # Age category one-hot
        for ac in ['aging', 'established']:
            feature_dict[f'age_category_{ac}'] = 1 if age_category == ac else 0
        
        # Risk tier one-hot
        for rt in ['aging_risk', 'high_risk', 'low_risk', 'medium_risk']:
            feature_dict[f'risk_tier_{rt}'] = 1 if risk_tier == rt else 0
        
        # Create DataFrame with correct column order
        X = pd.DataFrame([feature_dict])[feature_names]
        
    except Exception as e:
        logger.error(f"Feature preparation error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error preparing features: {str(e)}"
        )
    
    # 3. Make prediction
    try:
        if hasattr(model, 'predict_proba'):
            probability = float(model.predict_proba(X)[0, 1])
        else:
            # Dummy prediction for demo
            probability = float(features.get('failure_count_90d', 0)) * 0.3
            probability = min(probability, 1.0)
        
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        # Fallback: use rule-based prediction
        failure_count = features.get('failure_count_90d', 0)
        probability = min(failure_count * 0.3, 1.0)
    
    # 4. Generate recommendation
    if probability >= 0.7:
        recommendation = "URGENT: Schedule immediate maintenance inspection"
    elif probability >= 0.4:
        recommendation = "Schedule maintenance within 1 week"
    elif probability >= 0.2:
        recommendation = "Monitor closely, schedule routine maintenance"
    else:
        recommendation = "Continue normal operations, routine monitoring"
    
    # 5. Return response
    return PredictionResponse(
        equipment_id=equipment_id,
        failure_probability=round(probability, 3),
        risk_tier=features.get('risk_tier', 'unknown'),
        recommendation=recommendation,
        features_used={
            'equipment_age_days': features.get('equipment_age_days'),
            'failure_count_90d': features.get('failure_count_90d'),
            'days_since_maintenance': features.get('days_since_maintenance'),
            'equipment_type': features.get('equipment_type'),
            'risk_tier': features.get('risk_tier')
        }
    )


if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "="*70)
    print("STARTING EQUIPMENT FAILURE PREDICTION API")
    print("="*70)
    print("API will be available at: http://localhost:8000")
    print("Interactive docs at: http://localhost:8000/docs")
    print("="*70 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)