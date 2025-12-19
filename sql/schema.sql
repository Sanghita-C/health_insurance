-- Database schema

CREATE TABLE CUSTOMER (
    customer_id        VARCHAR(20) PRIMARY KEY,
    first_name         VARCHAR(50) NOT NULL,
    last_name          VARCHAR(50) NOT NULL,
    dob                DATE,
    gender             CHAR(1),
    email              VARCHAR(100),
    phone              VARCHAR(20),
    status             VARCHAR(20)
);

CREATE TABLE ACCOUNT (
    account_id         VARCHAR(20) PRIMARY KEY,
    account_name       VARCHAR(100),
    account_type       VARCHAR(50),
    status             VARCHAR(20),
    region             VARCHAR(50)
);

CREATE TABLE ACCOUNT_MEMBER (
    account_member_id  VARCHAR(20) PRIMARY KEY,
    customer_id        VARCHAR(20) REFERENCES CUSTOMER(customer_id) ON DELETE CASCADE,
    account_id         VARCHAR(20) REFERENCES ACCOUNT(account_id) ON DELETE CASCADE,
    role               VARCHAR(50),
    start_date         DATE,
    end_date           DATE
);

CREATE TABLE ACCOUNT_ALIAS (
    alias_id           VARCHAR(20) PRIMARY KEY,
    account_id         VARCHAR(20) REFERENCES ACCOUNT(account_id),
    alias_name         VARCHAR(100),
    alias_type         VARCHAR(50)
);

CREATE TABLE BILLING_ACCOUNT (
    billing_account_id VARCHAR(20) PRIMARY KEY,
    account_id         VARCHAR(20) REFERENCES ACCOUNT(account_id),
    billing_address    VARCHAR(255),
    invoice_cycle      VARCHAR(50),
    status             VARCHAR(20)
);

CREATE TABLE ASSOCIATE (
    associate_id       VARCHAR(20) PRIMARY KEY,
    name               VARCHAR(100),
    license_no         VARCHAR(50),
    region             VARCHAR(50),
    status             VARCHAR(20)
);

CREATE TABLE MANAGER_CONTRACT (
    manager_contract_id VARCHAR(20) PRIMARY KEY,
    associate_id        VARCHAR(20) REFERENCES ASSOCIATE(associate_id),
    account_id          VARCHAR(20) REFERENCES ACCOUNT(account_id),
    sit_code            VARCHAR(20),
    region              VARCHAR(50),
    status              VARCHAR(20)
);

CREATE TABLE ACCOUNT_ADMIN (
    account_admin_id   VARCHAR(20) PRIMARY KEY,
    name               VARCHAR(100),
    email              VARCHAR(100),
    phone              VARCHAR(20),
    department         VARCHAR(50)
);

CREATE TABLE CONTRACT (
    contract_id        VARCHAR(20) PRIMARY KEY,
    customer_id        VARCHAR(20) REFERENCES CUSTOMER(customer_id),
    account_id         VARCHAR(20) REFERENCES ACCOUNT(account_id),
    associate_id       VARCHAR(20) REFERENCES ASSOCIATE(associate_id),
    contract_type      VARCHAR(50),
    issue_date         DATE,
    expiry_date        DATE,
    premium_amount     FLOAT CHECK (premium_amount >= 0),
    status             VARCHAR(20)
);

CREATE TABLE CONTRACT_BENEFIT (
    contract_benefit_id VARCHAR(20) PRIMARY KEY,
    contract_id         VARCHAR(20) REFERENCES CONTRACT(contract_id) ON DELETE CASCADE,
    benefit_type        VARCHAR(100),
    benefit_amount      FLOAT
);

CREATE TABLE CONTRACT_PREMIUM (
    contract_premium_id VARCHAR(20) PRIMARY KEY,
    contract_id         VARCHAR(20) REFERENCES CONTRACT(contract_id) ON DELETE CASCADE,
    billing_cycle       VARCHAR(50),
    premium_amount      FLOAT,
    status              VARCHAR(20)
);

CREATE TABLE ASSOCIATE_RELATIONSHIP (
    relationship_id     VARCHAR(20) PRIMARY KEY,
    parent_associate_id VARCHAR(20) REFERENCES ASSOCIATE(associate_id),
    child_associate_id  VARCHAR(20) REFERENCES ASSOCIATE(associate_id),
    relation_type       VARCHAR(50),
    start_date          DATE,
    end_date            DATE,
    status              VARCHAR(20)
);

CREATE TABLE ASSOCIATE_COMMISSION_BENEFICIARY (
    id                        VARCHAR(20) PRIMARY KEY,
    associate_id              VARCHAR(20) REFERENCES ASSOCIATE(associate_id),
    beneficiary_customer_id   VARCHAR(20) REFERENCES CUSTOMER(customer_id),
    percentage_share          FLOAT CHECK (percentage_share BETWEEN 0 AND 100),
    effective_date            DATE,
    end_date                  DATE,
    status                    VARCHAR(20)
);

CREATE TABLE DATA_SOURCE (
    source_id        VARCHAR(20) PRIMARY KEY,
    source_name      VARCHAR(100) NOT NULL,
    source_type      VARCHAR(50),
    data_domain      VARCHAR(50),
    storage_location VARCHAR(255),
    format           VARCHAR(20),
    size_gb          FLOAT,
    record_count     INT,
    collection_date  DATE,
    license_info     VARCHAR(100),
    description      TEXT
);
-- Health Data table
CREATE TABLE HEALTH_DATA (
    health_data_id       VARCHAR(20) PRIMARY KEY,
    customer_id          VARCHAR(20) REFERENCES CUSTOMER(customer_id) ON DELETE CASCADE,
    source_id            VARCHAR(20) REFERENCES DATA_SOURCE(source_id),
    data_type            VARCHAR(20),
    file_name            VARCHAR(255),
    file_path            VARCHAR(255),
    file_format          VARCHAR(20),
    file_size_kb         FLOAT,
    diagnosis            VARCHAR(100),
    disease_category     VARCHAR(100),
    age                  INT,
    sex                  CHAR(1),
    collection_date      DATE,
    ingestion_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    annotation_status    VARCHAR(50),
    risk_level           VARCHAR(20),
    notes_summary        TEXT,
    data_quality_score   FLOAT,
    embedding_path       VARCHAR(255),
    processed_flag       VARCHAR(10) DEFAULT 'FALSE'
);

CREATE TABLE HEALTH_FEATURES (
    feature_id         VARCHAR(20) PRIMARY KEY,
    health_data_id     VARCHAR(20) REFERENCES HEALTH_DATA(health_data_id) ON DELETE CASCADE,
    feature_type       VARCHAR(50),
    feature_name       VARCHAR(100),
    feature_value      VARCHAR(100),
    unit               VARCHAR(20),
    extraction_method  VARCHAR(100),
    embedding_vector   JSON,
    model_version      VARCHAR(50),
    created_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
