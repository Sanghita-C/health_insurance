# Data ingestion module

from datasets import load_dataset
from google.cloud import storage
import pandas as pd
import json
import random
from datetime import datetime, timedelta


BUCKET_NAME = "insurance_health_data"
CLINICAL_NOTES_PREFIX = "clinical_notes"


def generate_customer_id(i: int) -> str:
    return f"CUST_{i:06d}"


def generate_random_date(start="2018-01-01", end="2023-12-31") -> datetime:
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    delta = end_dt - start_dt
    return start_dt + timedelta(days=random.randint(0, delta.days))


def upload_clinical_notes(limit: int = None):
    # Load dataset
    dataset = load_dataset("AGBonnet/augmented-clinical-notes", split="train")
    df = dataset.to_pandas()

    df = df[['idx', 'note']]
    df = df.drop_duplicates(subset='idx', keep='first')
    df = df.reset_index(drop=True)

    if limit:
        df = df.head(limit)

    # Generate synthetic metadata
    df["customer_id"] = [generate_customer_id(i) for i in range(len(df))]
    df["note_date"] = df.apply(lambda _: generate_random_date(), axis=1)

    # Init GCS client
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    for _, row in df.iterrows():
        customer_id = row["customer_id"]
        note_date_str = row["note_date"].strftime("%Y%m%d")

        blob_path = (
            f"{CLINICAL_NOTES_PREFIX}/"
            f"{customer_id}_note_{note_date_str}.json"
        )

        payload = {
            "customer_id": customer_id,
            "note_date": row["note_date"].strftime("%Y-%m-%d"),
            "text": row["note"]
        }

        blob = bucket.blob(blob_path)
        blob.upload_from_string(
            json.dumps(payload),
            content_type="application/json"
        )

    print(f"✅ Uploaded {len(df)} clinical notes to GCS")


if __name__ == "__main__":
    upload_clinical_notes(limit=200)  # start small