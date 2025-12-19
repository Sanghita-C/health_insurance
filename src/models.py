# SQLAlchemy ORM Models

from sqlalchemy import create_engine, Column, String, Integer, Float, Date, DateTime, Text, CHAR, ForeignKey, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSON
from datetime import datetime
import os

Base = declarative_base()

# ==============================
# DATABASE CONNECTION
# ==============================

def get_database_url():
    """Construct database URL from environment variables or defaults"""
    from urllib.parse import quote_plus
    
    host = os.getenv("DB_HOST", "136.119.21.177")
    dbname = os.getenv("DB_NAME", "insurance_hybrid")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "santaClaude@876")
    port = os.getenv("DB_PORT", "5432")
    
    # URL-encode password to handle special characters like @
    password_encoded = quote_plus(password)
    
    return f"postgresql://{user}:{password_encoded}@{host}:{port}/{dbname}"

def get_engine():
    """Create SQLAlchemy engine"""
    return create_engine(get_database_url(), echo=False)

def get_session():
    """Create SQLAlchemy session"""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()

# ==============================
# ORM MODELS
# ==============================

class Customer(Base):
    __tablename__ = 'customer'
    
    customer_id = Column(String(20), primary_key=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    dob = Column(Date, nullable=True)
    gender = Column(CHAR(1), nullable=True)
    email = Column(String(100), nullable=True)
    phone = Column(String(20), nullable=True)
    status = Column(String(20), nullable=True)
    
    # Relationships
    health_data = relationship("HealthData", back_populates="customer", cascade="all, delete-orphan")
    contracts = relationship("Contract", back_populates="customer")
    account_members = relationship("AccountMember", back_populates="customer")
    
    def __repr__(self):
        return f"<Customer(customer_id='{self.customer_id}', name='{self.first_name} {self.last_name}')>"


class DataSource(Base):
    __tablename__ = 'data_source'
    
    source_id = Column(String(20), primary_key=True)
    source_name = Column(String(100), nullable=False)
    source_type = Column(String(50), nullable=True)
    data_domain = Column(String(50), nullable=True)
    storage_location = Column(String(255), nullable=True)
    format = Column(String(20), nullable=True)
    size_gb = Column(Float, nullable=True)
    record_count = Column(Integer, nullable=True)
    collection_date = Column(Date, nullable=True)
    license_info = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    
    # Relationships
    health_data = relationship("HealthData", back_populates="data_source")
    
    def __repr__(self):
        return f"<DataSource(source_id='{self.source_id}', source_name='{self.source_name}')>"


class HealthData(Base):
    __tablename__ = 'health_data'
    
    health_data_id = Column(String(20), primary_key=True)
    customer_id = Column(String(20), ForeignKey('customer.customer_id', ondelete='CASCADE'), nullable=False)
    source_id = Column(String(20), ForeignKey('data_source.source_id'), nullable=True)
    data_type = Column(String(20), nullable=True)
    file_name = Column(String(255), nullable=True)
    file_path = Column(String(255), nullable=True)
    file_format = Column(String(20), nullable=True)
    file_size_kb = Column(Float, nullable=True)
    diagnosis = Column(String(100), nullable=True)
    disease_category = Column(String(100), nullable=True)
    age = Column(Integer, nullable=True)
    sex = Column(CHAR(1), nullable=True)
    collection_date = Column(Date, nullable=True)
    ingestion_timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    annotation_status = Column(String(50), nullable=True)
    risk_level = Column(String(20), nullable=True)
    notes_summary = Column(Text, nullable=True)
    data_quality_score = Column(Float, nullable=True)
    embedding_path = Column(String(255), nullable=True)
    processed_flag = Column(String(10), default='FALSE', nullable=True)  # Track processing status
    
    # Relationships
    customer = relationship("Customer", back_populates="health_data")
    data_source = relationship("DataSource", back_populates="health_data")
    health_features = relationship("HealthFeature", back_populates="health_data", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<HealthData(health_data_id='{self.health_data_id}', customer_id='{self.customer_id}')>"


class HealthFeature(Base):
    __tablename__ = 'health_features'
    
    feature_id = Column(String(20), primary_key=True)
    health_data_id = Column(String(20), ForeignKey('health_data.health_data_id', ondelete='CASCADE'), nullable=False)
    feature_type = Column(String(50), nullable=True)
    feature_name = Column(String(100), nullable=True)
    feature_value = Column(String(100), nullable=True)
    unit = Column(String(20), nullable=True)
    extraction_method = Column(String(100), nullable=True)
    embedding_vector = Column(JSON, nullable=True)
    model_version = Column(String(50), nullable=True)
    created_timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    health_data = relationship("HealthData", back_populates="health_features")
    
    def __repr__(self):
        return f"<HealthFeature(feature_id='{self.feature_id}', health_data_id='{self.health_data_id}')>"


# Additional models for completeness (referenced but not fully implemented)
class Account(Base):
    __tablename__ = 'account'
    
    account_id = Column(String(20), primary_key=True)
    account_name = Column(String(100), nullable=True)
    account_type = Column(String(50), nullable=True)
    status = Column(String(20), nullable=True)
    region = Column(String(50), nullable=True)


class AccountMember(Base):
    __tablename__ = 'account_member'
    
    account_member_id = Column(String(20), primary_key=True)
    customer_id = Column(String(20), ForeignKey('customer.customer_id', ondelete='CASCADE'), nullable=False)
    account_id = Column(String(20), ForeignKey('account.account_id', ondelete='CASCADE'), nullable=False)
    role = Column(String(50), nullable=True)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    
    customer = relationship("Customer", back_populates="account_members")


class Contract(Base):
    __tablename__ = 'contract'
    
    contract_id = Column(String(20), primary_key=True)
    customer_id = Column(String(20), ForeignKey('customer.customer_id'), nullable=False)
    account_id = Column(String(20), ForeignKey('account.account_id'), nullable=True)
    associate_id = Column(String(20), ForeignKey('associate.associate_id'), nullable=True)
    contract_type = Column(String(50), nullable=True)
    issue_date = Column(Date, nullable=True)
    expiry_date = Column(Date, nullable=True)
    premium_amount = Column(Float, CheckConstraint('premium_amount >= 0'), nullable=True)
    status = Column(String(20), nullable=True)
    
    customer = relationship("Customer", back_populates="contracts")


class Associate(Base):
    __tablename__ = 'associate'
    
    associate_id = Column(String(20), primary_key=True)
    name = Column(String(100), nullable=True)
    license_no = Column(String(50), nullable=True)
    region = Column(String(50), nullable=True)
    status = Column(String(20), nullable=True)

