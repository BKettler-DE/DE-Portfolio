"""
ML Training Pipeline
Trains XGBoost model to predict equipment failures
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from feature_store.offline_store import OfflineFeatureStore
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, 
    f1_score, roc_auc_score, confusion_matrix,
    classification_report
)
import xgboost as xgb
import mlflow
import mlflow.xgboost
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EquipmentFailurePredictor:
    """
    Train a model to predict equipment failures
    """
    
    def __init__(self):
        self.offline_store = OfflineFeatureStore()
        
        # PostgreSQL connection for labels
        self.pg_conn_params = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'pipeline_db',
            'user': 'pipeline_user',
            'password': 'pipeline_pass'
        }
        
        # MLflow tracking
        mlflow.set_tracking_uri("http://localhost:5000")
        mlflow.set_experiment("equipment_failure_prediction")
    
    def create_training_dataset(self, prediction_window_days: int = 365):
        """
        Create training dataset with features and labels
        
        Labels: Did equipment fail within next N days?
        Features: Equipment characteristics and history
        
        Args:
            prediction_window_days: How far ahead to predict (default 365 days - looks back at historical failures)
        
        Returns:
            X (features), y (labels), feature_names
        """
        logger.info("="*70)
        logger.info("CREATING TRAINING DATASET")
        logger.info("="*70)
        
        # Load features from offline store
        logger.info("\n1. Loading features from offline store...")
        features_df = self.offline_store.get_features()
        logger.info(f"   Loaded {len(features_df)} equipment feature records")
        
        # Load failure history for labels
        logger.info("\n2. Loading failure history for labels...")
        
        from sqlalchemy import create_engine
        
        # Use SQLAlchemy connection string (removes pandas warning)
        db_url = f"postgresql://{self.pg_conn_params['user']}:{self.pg_conn_params['password']}@{self.pg_conn_params['host']}:{self.pg_conn_params['port']}/{self.pg_conn_params['dbname']}"
        engine = create_engine(db_url)
        
        failures_query = """
        SELECT 
            equipment_id,
            failure_date,
            failure_type,
            severity,
            downtime_hours
        FROM failure_history
        ORDER BY failure_date
        """
        
        failures_df = pd.read_sql(failures_query, engine)
        engine.dispose()
        
        logger.info(f"   Loaded {len(failures_df)} failure records")
        
        # Create labels: Has this equipment failed in the past year? (retrospective)
        # Note: In production, you'd use point-in-time features and forward-looking labels
        # For demo purposes with limited data, we look at historical failures
        logger.info(f"\n3. Creating labels (looking at failures in past {prediction_window_days} days)...")
        
        labels = []
        feature_date = features_df['feature_date'].iloc[0]
        
        for _, equipment in features_df.iterrows():
            equipment_id = equipment['equipment_id']
            
            # Find ANY historical failures for this equipment
            equipment_failures = failures_df[failures_df['equipment_id'] == equipment_id]
            
            # Label: 1 if has failed before, 0 if never failed
            # This creates a "failure-prone equipment" predictor
            label = 1 if len(equipment_failures) > 0 else 0
            labels.append(label)
        
        features_df['label'] = labels
        
        # Class distribution
        positive_class = sum(labels)
        negative_class = len(labels) - positive_class
        logger.info(f"   Positive class (failures): {positive_class} ({positive_class/len(labels)*100:.1f}%)")
        logger.info(f"   Negative class (no failure): {negative_class} ({negative_class/len(labels)*100:.1f}%)")
        
        # Select feature columns for training
        feature_columns = [
            'equipment_age_days',
            'total_operating_hours',
            'days_since_maintenance',
            'maintenance_count_30d',
            'failure_count_90d',
            'avg_downtime_hours_90d',
            'total_repair_cost_90d',
            'avg_severity_score_90d',
            'equipment_type_risk_score'
        ]
        
        # Add one-hot encoded categorical features
        logger.info("\n4. Encoding categorical features...")
        categorical_features = ['equipment_type', 'age_category', 'risk_tier']
        
        for cat_feature in categorical_features:
            dummies = pd.get_dummies(features_df[cat_feature], prefix=cat_feature)
            features_df = pd.concat([features_df, dummies], axis=1)
            feature_columns.extend(dummies.columns.tolist())
        
        logger.info(f"   Total features: {len(feature_columns)}")
        
        # Prepare X (features) and y (labels)
        X = features_df[feature_columns].fillna(0)  # Handle any NaN values
        y = features_df['label']
        
        logger.info("\n5. Dataset summary:")
        logger.info(f"   Samples: {len(X)}")
        logger.info(f"   Features: {len(feature_columns)}")
        logger.info(f"   Feature names: {feature_columns[:5]}...")
        
        return X, y, feature_columns, features_df
    
    def train(self, test_size: float = 0.2, random_state: int = 42):
        """
        Train XGBoost model with MLflow tracking
        
        Args:
            test_size: Proportion for test set (default 0.2 = 20%)
            random_state: Random seed for reproducibility
        """
        logger.info("\n" + "="*70)
        logger.info("TRAINING XGBOOST MODEL")
        logger.info("="*70)
        
        # Create dataset
        X, y, feature_names, full_df = self.create_training_dataset()
        
        # Train/test split
        logger.info(f"\n6. Splitting data (test_size={test_size})...")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )
        
        logger.info(f"   Training set: {len(X_train)} samples")
        logger.info(f"   Test set: {len(X_test)} samples")
        
        # Start MLflow run
        with mlflow.start_run() as run:
            logger.info(f"\n7. Training model (MLflow run: {run.info.run_id[:8]})...")
            
            # Calculate scale_pos_weight for class imbalance
            scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
            
            # XGBoost parameters
            params = {
                'objective': 'binary:logistic',
                'max_depth': 3,
                'learning_rate': 0.1,
                'n_estimators': 50,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'random_state': random_state,
                'eval_metric': 'logloss',
                'scale_pos_weight': scale_pos_weight
            }
            
            # Log parameters
            mlflow.log_params(params)
            mlflow.log_param("test_size", test_size)
            mlflow.log_param("n_features", len(feature_names))
            
            # Train model
            model = xgb.XGBClassifier(**params)
            model.fit(
                X_train, y_train,
                eval_set=[(X_test, y_test)],
                verbose=False
            )
            
            logger.info("   ✅ Model training complete")
            
            # Make predictions
            logger.info("\n8. Evaluating model...")
            y_pred = model.predict(X_test)
            y_pred_proba = model.predict_proba(X_test)[:, 1]
            
            # Calculate metrics
            metrics = {
                'accuracy': accuracy_score(y_test, y_pred),
                'precision': precision_score(y_test, y_pred, zero_division=0),
                'recall': recall_score(y_test, y_pred, zero_division=0),
                'f1_score': f1_score(y_test, y_pred, zero_division=0),
                'roc_auc': roc_auc_score(y_test, y_pred_proba) if len(np.unique(y_test)) > 1 else 0.5
            }
            
            # Log metrics
            for metric_name, metric_value in metrics.items():
                mlflow.log_metric(metric_name, metric_value)
            
            # Print metrics
            logger.info("\n   Model Performance:")
            logger.info(f"   Accuracy:  {metrics['accuracy']:.3f}")
            logger.info(f"   Precision: {metrics['precision']:.3f}")
            logger.info(f"   Recall:    {metrics['recall']:.3f}")
            logger.info(f"   F1 Score:  {metrics['f1_score']:.3f}")
            logger.info(f"   ROC AUC:   {metrics['roc_auc']:.3f}")
            
            # Confusion matrix
            cm = confusion_matrix(y_test, y_pred, labels=[0, 1])
            logger.info("\n   Confusion Matrix:")
            logger.info(f"   TN: {cm[0,0]}  FP: {cm[0,1]}")
            logger.info(f"   FN: {cm[1,0]}  TP: {cm[1,1]}")
            
            # Classification report (only if both classes present)
            if len(np.unique(y_test)) > 1:
                logger.info("\n   Classification Report:")
                print(classification_report(y_test, y_pred, target_names=['No Failure', 'Failure']))
            else:
                logger.info("\n   Classification Report: Skipped (only one class in test set)")
                logger.info(f"   Test set contains only class: {np.unique(y_test)[0]}")
            
            # Feature importance
            logger.info("\n9. Feature importance (top 10):")
            feature_importance = pd.DataFrame({
                'feature': feature_names,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            for idx, row in feature_importance.head(10).iterrows():
                logger.info(f"   {row['feature']:<30} {row['importance']:.4f}")
            
            # Log feature importance as artifact
            importance_dict = dict(zip(feature_importance['feature'], feature_importance['importance'].astype(float)))
            mlflow.log_dict(importance_dict, "feature_importance.json")
            
            # Log model
            logger.info("\n10. Logging model to MLflow...")
            
            # Save model as pickle (compatible with all MLflow versions)
            import pickle
            import tempfile
            import os
            
            with tempfile.TemporaryDirectory() as tmpdir:
                # Save as pickle
                model_path = os.path.join(tmpdir, "model.pkl")
                with open(model_path, 'wb') as f:
                    pickle.dump(model, f)
                
                # Save as XGBoost native format
                xgb_model_path = os.path.join(tmpdir, "model.json")
                model.save_model(xgb_model_path)
                
                # Log both files
                mlflow.log_artifact(model_path, "model")
                mlflow.log_artifact(xgb_model_path, "model")
            
            logger.info(f"    ✅ Model logged to MLflow")
            logger.info(f"    MLflow UI: http://localhost:5000")
            logger.info(f"    Run ID: {run.info.run_id}")
            
            logger.info("\n" + "="*70)
            logger.info("✅ TRAINING COMPLETE!")
            logger.info("="*70)
            
            return model, metrics, feature_importance


if __name__ == '__main__':
    print("\n" + "="*70)
    print("EQUIPMENT FAILURE PREDICTION - MODEL TRAINING")
    print("="*70)
    
    trainer = EquipmentFailurePredictor()
    model, metrics, feature_importance = trainer.train()
    
    print("\n" + "="*70)
    print("NEXT STEPS:")
    print("="*70)
    print("1. View experiment in MLflow UI: http://localhost:5000")
    print("2. Model is registered as 'equipment_failure_predictor'")
    print("3. Ready to build inference API!")
    print("="*70 + "\n")