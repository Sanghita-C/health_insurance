# Feature engineering module using SQLAlchemy ORM

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hashlib
from datetime import datetime
from sqlalchemy.exc import IntegrityError

from src.models import get_session, HealthFeature


# --------------------------------------------------
# MAIN FEATURE PERSISTENCE FUNCTION (using ORM)
# --------------------------------------------------
def persist_features(
    health_data_id: str,
    llm_output: dict,
    extraction_method: str = "gemini-llm",
    model_version: str = "gemini-2.0-flash-lite"
):
    """
    Converts LLM output into normalized HEALTH_FEATURES rows
    and inserts them into Postgres using SQLAlchemy ORM.
    """

    session = get_session()
    features_to_add = []
    now = datetime.utcnow()

    def add_feature(feature_type, feature_name, feature_value, unit=None):
        # Generate a short feature_id (20 chars max) from health_data_id + feature info
        feature_key = f"{health_data_id}_{feature_type}_{feature_name}_{feature_value}"
        hash_obj = hashlib.md5(feature_key.encode())
        feature_id = hash_obj.hexdigest()[:20]  # Use first 20 chars of MD5 hash
        
        # Check if feature already exists
        existing = session.query(HealthFeature).filter(
            HealthFeature.feature_id == feature_id
        ).first()
        
        if existing:
            return  # Skip if already exists
        
        feature = HealthFeature(
            feature_id=feature_id,
            health_data_id=health_data_id,
            feature_type=feature_type,
            feature_name=feature_name,
            feature_value=str(feature_value),
            unit=unit,
            extraction_method=extraction_method,
            embedding_vector=None,  # Optional
            model_version=model_version,
            created_timestamp=now
        )
        features_to_add.append(feature)

    # --------------------------------------------------
    # DEMOGRAPHIC FEATURES
    # --------------------------------------------------
    if llm_output.get("Age"):
        add_feature("demographic", "age", llm_output["Age"], "years")

    if llm_output.get("Gender"):
        add_feature("demographic", "gender", llm_output["Gender"])

    if llm_output.get("Height"):
        add_feature("demographic", "height", llm_output["Height"])

    if llm_output.get("Weight"):
        add_feature("demographic", "weight", llm_output["Weight"])

    # --------------------------------------------------
    # SYMPTOMS
    # --------------------------------------------------
    symptoms = llm_output.get("symptoms", {})
    if isinstance(symptoms, dict):
        for symptom, severity in symptoms.items():
            add_feature(
                feature_type="symptom",
                feature_name=symptom.lower().replace(" ", "_"),
                feature_value=severity,
                unit="severity_score"
            )

    # --------------------------------------------------
    # DIAGNOSIS
    # --------------------------------------------------
    if llm_output.get("diagnosis"):
        add_feature(
            "diagnosis",
            "primary_diagnosis",
            llm_output["diagnosis"]
        )

    # --------------------------------------------------
    # RISK SCORE
    # --------------------------------------------------
    if llm_output.get("risk_score") is not None:
        add_feature(
            "risk",
            "risk_score",
            llm_output["risk_score"],
            unit="probability"
        )

    # --------------------------------------------------
    # MEDICAL DEPARTMENT
    # --------------------------------------------------
    if llm_output.get("medical_department"):
        add_feature(
            "category",
            "medical_department",
            llm_output["medical_department"]
        )

    if not features_to_add:
        print(f"⚠️ No features generated for health_data_id={health_data_id}")
        session.close()
        return

    # --------------------------------------------------
    # INSERT INTO DB USING ORM
    # --------------------------------------------------
    try:
        for feature in features_to_add:
            session.add(feature)
        session.commit()
        print(f"✅ Inserted {len(features_to_add)} features for {health_data_id} using ORM")
    except IntegrityError as e:
        session.rollback()
        print(f"⚠️ Some features may already exist for {health_data_id}: {e}")
    except Exception as e:
        session.rollback()
        print(f"❌ Error inserting features for {health_data_id}: {e}")
        raise
    finally:
        session.close()
