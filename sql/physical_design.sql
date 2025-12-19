-- ============================================================
-- NYU DATABASE SYSTEMS — PROJECT PART 3
-- Physical Database Design Script
-- Members: Arushi Srivastava(as19341), Sanghita Chakraborty(sc11072)
-- ============================================================
-- 1. INDEXING STRATEGY
-- ============================================================

-- Index 1: Frequently used join and lookup (Customer → Health Data)
CREATE INDEX idx_health_customer 
ON health_data(customer_id);

-- Index 2: Diagnosis-based cohort queries (ML + analytics)
CREATE INDEX idx_health_diagnosis 
ON health_data(diagnosis);

-- Index 3: Filter by feature types (symptom, risk, embedding, etc.)
CREATE INDEX idx_feature_type 
ON health_features(feature_type);

-- ============================================================
-- 2. PARTITIONING STRATEGY
-- LIST PARTITIONING ON feature_type
-- ============================================================

-- Drop and recreate parent partitioned table (if required)
DROP TABLE IF EXISTS health_features CASCADE;

CREATE TABLE health_features (
    feature_id         VARCHAR(20) PRIMARY KEY,
    health_data_id     VARCHAR(20) REFERENCES health_data(health_data_id) ON DELETE CASCADE,
    feature_type       VARCHAR(100) NOT NULL,
    feature_name       VARCHAR(100),
    feature_value      VARCHAR(100),
    unit               VARCHAR(20),
    extraction_method  VARCHAR(100),
    embedding_vector   JSON,
    model_version      VARCHAR(50),
    created_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PARTITION BY LIST (feature_type);

-- ------------------------------------------------------------
-- Partitions
-- ------------------------------------------------------------

-- Partition 1: Symptom features
CREATE TABLE health_features_symptoms 
PARTITION OF health_features
FOR VALUES IN ('symptom');

-- Partition 2: ML risk scores
CREATE TABLE health_features_risk 
PARTITION OF health_features
FOR VALUES IN ('risk_score');

-- Partition 3: Embeddings (large, JSON-heavy)
CREATE TABLE health_features_embeddings 
PARTITION OF health_features
FOR VALUES IN ('embedding');

-- Partition 4: Catch-all for future feature types
CREATE TABLE health_features_other 
PARTITION OF health_features
DEFAULT;


-- ============================================================
-- 3. CLUSTERING (PHYSICAL ORDERING)
-- ============================================================

-- Cluster HEALTH_DATA by customer_id to colocate patient records
CREATE INDEX idx_cluster_healthdata_customer 
ON health_data(customer_id);

CLUSTER health_data USING idx_cluster_healthdata_customer;


-- Cluster CONTRACT by account_id for faster contract retrieval
CREATE INDEX idx_cluster_contract_account 
ON contract(account_id);

CLUSTER contract USING idx_cluster_contract_account;


-- ============================================================
-- 4. MATERIALIZED VIEWS
-- ============================================================

-- MV #1: Summary of symptoms + risk for each customer
DROP MATERIALIZED VIEW IF EXISTS mv_patient_symptom_summary;

CREATE MATERIALIZED VIEW mv_patient_symptom_summary AS
SELECT
    hd.customer_id,
    COUNT(*) FILTER (WHERE hf.feature_type = 'symptom') AS symptom_count,
    AVG(CASE WHEN hf.feature_type = 'risk_score' 
             THEN hf.feature_value::float END) AS avg_risk_score,
    MAX(hf.created_timestamp) AS last_update
FROM health_data hd
LEFT JOIN health_features hf 
       ON hd.health_data_id = hf.health_data_id
GROUP BY hd.customer_id;

-- Index to speed up searches on MV
CREATE INDEX idx_mv_symptom_customer 
ON mv_patient_symptom_summary(customer_id);


-- ============================================================
-- 5. PERFORMANCE SETTINGS / OPTIMIZATIONS
-- ============================================================

-- Enable parallel queries for materialized view
SET max_parallel_workers = 8;
SET max_parallel_workers_per_gather = 4;

-- Increase work memory for analytical feature extraction
SET work_mem = '256MB';

