# Database module using SQLAlchemy ORM

from google.cloud import storage
from datetime import datetime
import os
import hashlib
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

from models import (
    get_session, 
    Customer, 
    DataSource, 
    HealthData,
    Base,
    get_engine
)

# ==============================
# CONFIG
# ==============================

BUCKET_NAME = "insurance_health_data"
NOTES_PREFIX = "clinical_notes/"

SOURCE_ID = "SRC_002"
DATA_TYPE = "clinical_note"
FILE_FORMAT = "json"

# ==============================
# LIST NOTES FROM GCS
# ==============================

def list_clinical_notes():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blobs = bucket.list_blobs(prefix=NOTES_PREFIX)
    return [blob for blob in blobs if blob.name.endswith(".json")]

# ==============================
# PARSE METADATA FROM FILENAME
# ==============================

def parse_metadata(blob_name: str):
    """
    Expected format:
    clinical_notes/<customer_id>_note_<date>.json
    where date is in format YYYYMMDD
    """
    file_name = os.path.basename(blob_name)
    base = file_name.replace(".json", "")

    # Split by "_note_" to separate customer_id from date
    if "_note_" in base:
        parts = base.split("_note_")
        customer_id = parts[0]
        date_str = parts[1]
    else:
        # Fallback: try to parse old format
        parts = base.split("_")
        customer_id = parts[0]
        date_str = parts[-1] if len(parts) > 1 else ""

    try:
        # Try YYYYMMDD format first (from ingest.py)
        collection_date = datetime.strptime(date_str, "%Y%m%d").date()
    except:
        try:
            # Try YYYY-MM-DD format as fallback
            collection_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except:
            collection_date = datetime.today().date()

    return customer_id, collection_date, file_name

# ==============================
# INITIALIZE SCHEMA
# ==============================

def init_schema():
    """Create database tables using SQLAlchemy"""
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("✅ Database schema initialized using ORM")

# ==============================
# ENSURE CUSTOMER EXISTS (ORM)
# ==============================

def ensure_customer_exists(customer_id: str, session):
    """Create customer if it doesn't exist using ORM"""
    # Check if customer exists
    customer = session.query(Customer).filter(Customer.customer_id == customer_id).first()
    
    if not customer:
        customer = Customer(
            customer_id=customer_id,
            first_name="jane",
            last_name="doe"
        )
        session.add(customer)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            # Customer might have been created by another process
            pass
    
    return customer

# ==============================
# ENSURE DATA SOURCE EXISTS (ORM)
# ==============================

def ensure_data_source_exists(source_id: str, session):
    """Create data source if it doesn't exist using ORM"""
    # Check if data source exists
    data_source = session.query(DataSource).filter(DataSource.source_id == source_id).first()
    
    if not data_source:
        data_source = DataSource(
            source_id=source_id,
            source_name="Clinical Notes Source"
        )
        session.add(data_source)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            # Data source might have been created by another process
            pass
    
    return data_source

# ==============================
# INSERT INTO HEALTH_DATA (ORM)
# ==============================

def insert_health_data():
    """Insert health data using SQLAlchemy ORM"""
    session = get_session()
    
    try:
        blobs = list_clinical_notes()
        
        # Collect unique customer IDs and ensure they exist
        customer_ids = set()
        for blob in blobs:
            customer_id, _, _ = parse_metadata(blob.name)
            customer_ids.add(customer_id)
        
        # Create customers if they don't exist (using ORM)
        for customer_id in customer_ids:
            ensure_customer_exists(customer_id, session)
        
        # Ensure data source exists (using ORM)
        ensure_data_source_exists(SOURCE_ID, session)
        
        # Refresh session to ensure objects are available
        session.commit()
        
        inserted_count = 0
        skipped_count = 0
        
        for blob in blobs:
            # Generate a short ID (20 chars max) from file path hash
            file_path = f"gs://{BUCKET_NAME}/{blob.name}"
            hash_obj = hashlib.md5(file_path.encode())
            health_data_id = hash_obj.hexdigest()[:20]  # Use first 20 chars of MD5 hash

            customer_id, collection_date, file_name = parse_metadata(blob.name)
            
            # Get file size in KB
            file_size_kb = blob.size / 1024.0 if blob.size else None
            
            # Check if record already exists using ORM
            existing = session.query(HealthData).filter(
                HealthData.health_data_id == health_data_id
            ).first()
            
            if existing:
                skipped_count += 1
                continue
            
            # Create new HealthData record using ORM
            health_data = HealthData(
                health_data_id=health_data_id,
                customer_id=customer_id,
                source_id=SOURCE_ID,
                data_type=DATA_TYPE,
                file_name=file_name,
                file_path=file_path,
                file_format=FILE_FORMAT,
                file_size_kb=file_size_kb,
                collection_date=collection_date
            )
            
            session.add(health_data)
            inserted_count += 1
        
        # Commit all inserts
        session.commit()
        print(f"✅ Inserted {inserted_count} rows into HEALTH_DATA using ORM")
        if skipped_count > 0:
            print(f"⏭️  Skipped {skipped_count} existing records")
            
    except Exception as e:
        session.rollback()
        print(f"❌ Error inserting health data: {e}")
        raise
    finally:
        session.close()

# ==============================
# QUERY EXAMPLES USING ORM
# ==============================

def get_customer_health_data(customer_id: str):
    """Query health data for a customer using ORM"""
    session = get_session()
    try:
        customer = session.query(Customer).filter(Customer.customer_id == customer_id).first()
        if customer:
            return customer.health_data
        return []
    finally:
        session.close()

def get_health_data_by_date_range(start_date: datetime.date, end_date: datetime.date):
    """Query health data by date range using ORM"""
    session = get_session()
    try:
        results = session.query(HealthData).filter(
            HealthData.collection_date >= start_date,
            HealthData.collection_date <= end_date
        ).all()
        return results
    finally:
        session.close()

def get_health_data_count_by_customer():
    """Get count of health data records grouped by customer using ORM"""
    session = get_session()
    try:
        results = session.query(
            HealthData.customer_id,
            func.count(HealthData.health_data_id).label('count')
        ).group_by(HealthData.customer_id).all()
        return results
    finally:
        session.close()

# ==============================
# ENTRY POINT
# ==============================

if __name__ == "__main__":
    insert_health_data()
