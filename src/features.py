# Feature engineering module

import uuid
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime


# --------------------------------------------------
# DB CONNECTION (Cloud SQL – Postgres)
# --------------------------------------------------
DB_CONFIG = {
    "host": "YOUR_CLOUDSQL_PRIVATE_IP_OR_PUBLIC_IP",
    "dbname": "postgres",
    "user": "postgres",
    "password": "YOUR_PASSWORD",
    "port": 5432
}


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


# --------------------------------------------------
# MAIN FEATURE PERSISTENCE FUNCTION
# --------------------------------------------------
def persist_features(
    health_data_id: str,
    llm_output: dict,
    extraction_method: str = "gemini-llm",
    model_version: str = "gemini-2.0-flash-lite"
):
    """
    Converts LLM output into normalized HEALTH_FEATURES rows
    and inserts them into Postgres.
    """

    rows = []
    now = datetime.utcnow()

    def add_feature(feature_type, feature_name, feature_value, unit=None):
        rows.append((
            str(uuid.uuid4()),           # feature_id
            health_data_id,
            feature_type,
            feature_name,
            str(feature_value),
            unit,
            extraction_method,
            None,                         # embedding_vector (optional)
            model_version,
            now
        ))

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

    if not rows:
        print(f"⚠️ No features generated for health_data_id={health_data_id}")
        return

    # --------------------------------------------------
    # INSERT INTO DB
    # --------------------------------------------------
    insert_sql = """
        INSERT INTO HEALTH_FEATURES (
            feature_id,
            health_data_id,
            feature_type,
            feature_name,
            feature_value,
            unit,
            extraction_method,
            embedding_vector,
            model_version,
            created_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, rows)
        conn.commit()
        print(f"✅ Inserted {len(rows)} features for {health_data_id}")
    finally:
        conn.close()