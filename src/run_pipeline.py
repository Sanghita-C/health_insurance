# Pipeline runner module using SQLAlchemy ORM

import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import storage
from sqlalchemy.exc import IntegrityError

from src.models import get_session, HealthData, HealthFeature
from src.llm_extract import extract_features
from src.features import persist_features


# --------------------------------------------------
# GCS HELPERS
# --------------------------------------------------
def load_note_from_gcs(gcs_uri: str) -> str:
    """
    Reads JSON file from GCS and returns clinical note text
    """
    assert gcs_uri.startswith("gs://")

    path = gcs_uri.replace("gs://", "")
    bucket_name, blob_path = path.split("/", 1)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    content = blob.download_as_text()
    data = json.loads(content)

    return data.get("text", "")


# --------------------------------------------------
# FETCH UNPROCESSED NOTES (using ORM)
# --------------------------------------------------
def fetch_unprocessed_notes(limit=20):
    """
    Fetch unprocessed notes using SQLAlchemy ORM.
    A note is considered unprocessed if no health_features exist for it.
    This is more reliable than using a processed_flag column.
    """
    session = get_session()
    try:
        # Get all clinical notes
        all_notes = session.query(HealthData).filter(
            HealthData.data_type == 'clinical_note'
        ).all()
        
        # Filter out notes that already have features
        unprocessed = []
        for note in all_notes:
            feature_count = session.query(HealthFeature).filter(
                HealthFeature.health_data_id == note.health_data_id
            ).count()
            
            if feature_count == 0:
                unprocessed.append((note.health_data_id, note.file_path))
            
            # Stop when we have enough
            if len(unprocessed) >= limit:
                break
        
        return unprocessed
    finally:
        session.close()


# --------------------------------------------------
# MARK NOTE AS PROCESSED (using ORM)
# --------------------------------------------------
def mark_processed(health_data_id: str):
    """
    Mark a note as processed.
    Since we check for features existence, this function is optional.
    We can use it to set processed_flag if the column exists, but it's not required.
    """
    # No-op function - processing is determined by feature existence
    # This function is kept for compatibility but doesn't need to do anything
    pass


# --------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------
def process_notes(batch_size=10):
    rows = fetch_unprocessed_notes(limit=batch_size)

    if not rows:
        print("✅ No unprocessed notes found.")
        return

    print(f"🔄 Processing {len(rows)} notes...")

    for health_data_id, file_path in rows:
        try:
            print(f"➡️ Processing {health_data_id}")

            # 1. Load note text
            note_text = load_note_from_gcs(file_path)

            if not note_text.strip():
                print(f"⚠️ Empty note for {health_data_id}, skipping")
                # Skip empty notes - they won't have features, so they'll be picked up again
                # if needed, but we don't want to process empty content
                continue

            # 2. LLM extraction
            llm_output = extract_features(note_text)

            # 3. Persist features
            persist_features(
                health_data_id=health_data_id,
                llm_output=llm_output
            )

            # 4. Mark processed
            mark_processed(health_data_id)

            print(f"✅ Done {health_data_id}")

        except Exception as e:
            print(f"❌ Error processing {health_data_id}: {e}")
            import traceback
            traceback.print_exc()


# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------
if __name__ == "__main__":
    process_notes(batch_size=200)
