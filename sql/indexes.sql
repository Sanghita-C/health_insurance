-- Database indexes

-- Health Data indexes
CREATE INDEX IF NOT EXISTS idx_health_customer ON HEALTH_DATA(customer_id);
CREATE INDEX IF NOT EXISTS idx_health_diagnosis ON HEALTH_DATA(diagnosis);

-- Health Features indexes
CREATE INDEX IF NOT EXISTS idx_health_features_health_data
    ON HEALTH_FEATURES(health_data_id);

CREATE INDEX IF NOT EXISTS idx_health_features_type
    ON HEALTH_FEATURES(feature_type);

CREATE INDEX IF NOT EXISTS idx_health_features_name
    ON HEALTH_FEATURES(feature_name);
