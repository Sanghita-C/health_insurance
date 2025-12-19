# Analytics module

# src/analytics.py

import pandas as pd
import psycopg2
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from datetime import datetime
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "136.119.21.177"),
    "dbname": os.getenv("DB_NAME", "insurance_hybrid"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "santaClaude@876"),
    "port": int(os.getenv("DB_PORT", "5432")),
}

# Figures directory
FIGURES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports", "figures")
os.makedirs(FIGURES_DIR, exist_ok=True)

N_CLUSTERS = 4
MODEL_VERSION = "kmeans_v1"

logging.basicConfig(level=logging.INFO)

# --------------------------------------------------
# DB CONNECTION
# --------------------------------------------------

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# --------------------------------------------------
# LOAD FEATURES
# --------------------------------------------------

def load_feature_matrix():
    """
    Produces one row per health_data_id with numeric features
    """
    query = """
    SELECT
        hd.health_data_id,
        MAX(CASE 
            WHEN hf.feature_name = 'age' 
            THEN CASE 
                WHEN hf.feature_value ~ '^[0-9]+[.]?[0-9]*$' 
                THEN hf.feature_value::FLOAT 
                ELSE NULL 
            END
        END) AS age,
        MAX(CASE 
            WHEN hf.feature_name = 'risk_score' 
            THEN CASE 
                WHEN hf.feature_value ~ '^[0-9]+[.]?[0-9]*$' 
                THEN hf.feature_value::FLOAT 
                ELSE NULL 
            END
        END) AS risk_score,
        MAX(CASE WHEN hf.feature_name = 'gender' THEN
            CASE
                WHEN LOWER(hf.feature_value) = 'male' THEN 1
                WHEN LOWER(hf.feature_value) = 'female' THEN 0
                ELSE -1
            END
        END) AS gender_encoded,
        hd.disease_category
    FROM health_data hd
    JOIN health_features hf
      ON hd.health_data_id = hf.health_data_id
    GROUP BY hd.health_data_id, hd.disease_category
    """

    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()

    return df

# --------------------------------------------------
# PREPROCESS
# --------------------------------------------------

def preprocess(df: pd.DataFrame):
    """
    Encode department and normalize features
    """
    # Drop rows with missing critical features
    df = df.dropna(subset=["age", "risk_score"]).copy()
    
    # Fill missing gender_encoded with -1 (unknown)
    df["gender_encoded"] = df["gender_encoded"].fillna(-1)
    
    # Encode department (fill NaN with 'Unknown' first)
    df["disease_category"] = df["disease_category"].fillna("Unknown")
    df["department_encoded"] = df["disease_category"].astype("category").cat.codes

    features = df[
        ["age", "risk_score", "gender_encoded", "department_encoded"]
    ]

    # Ensure no NaN values remain
    if features.isna().any().any():
        features = features.fillna(0)  # Fill any remaining NaN with 0

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    return df, X_scaled

# --------------------------------------------------
# CLUSTERING
# --------------------------------------------------

def run_kmeans(X_scaled):
    model = KMeans(
        n_clusters=N_CLUSTERS,
        random_state=42,
        n_init="auto"
    )
    clusters = model.fit_predict(X_scaled)
    return clusters

# --------------------------------------------------
# PERSIST RESULTS
# --------------------------------------------------

def persist_clusters(df, clusters):
    conn = get_connection()
    cur = conn.cursor()

    for idx, cluster_id in zip(df["health_data_id"], clusters):
        # Convert numpy types to Python native types
        cluster_id_int = int(cluster_id)
        cur.execute(
            """
            UPDATE health_data
            SET annotation_status = 'CLUSTERED',
                data_quality_score = %s
            WHERE health_data_id = %s
            """,
            (cluster_id_int, str(idx))
        )

    conn.commit()
    cur.close()
    conn.close()

# --------------------------------------------------
# VISUALIZATIONS
# --------------------------------------------------

def generate_plots(df: pd.DataFrame, clusters):
    """
    Generate and save various analytics plots to the figures directory
    """
    df_with_clusters = df.copy()
    df_with_clusters['cluster'] = clusters
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Cluster distribution
    plt.figure(figsize=(10, 6))
    cluster_counts = pd.Series(clusters).value_counts().sort_index()
    plt.bar(cluster_counts.index, cluster_counts.values, color=sns.color_palette("husl", len(cluster_counts)))
    plt.xlabel('Cluster ID')
    plt.ylabel('Number of Records')
    plt.title('Distribution of Records Across Clusters')
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, f'cluster_distribution_{timestamp}.png'), dpi=300, bbox_inches='tight')
    plt.close()
    logging.info(f"✅ Saved cluster distribution plot")
    
    # 2. Age vs Risk Score by Cluster
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(df_with_clusters['age'], df_with_clusters['risk_score'], 
                         c=df_with_clusters['cluster'], cmap='viridis', 
                         alpha=0.6, s=50, edgecolors='black', linewidth=0.5)
    plt.colorbar(scatter, label='Cluster ID')
    plt.xlabel('Age')
    plt.ylabel('Risk Score')
    plt.title('Age vs Risk Score by Cluster')
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, f'age_vs_risk_by_cluster_{timestamp}.png'), dpi=300, bbox_inches='tight')
    plt.close()
    logging.info(f"✅ Saved age vs risk score plot")
    
    # 3. Risk Score Distribution by Cluster
    plt.figure(figsize=(12, 6))
    for cluster_id in sorted(df_with_clusters['cluster'].unique()):
        cluster_data = df_with_clusters[df_with_clusters['cluster'] == cluster_id]['risk_score']
        plt.hist(cluster_data, alpha=0.6, label=f'Cluster {cluster_id}', bins=20)
    plt.xlabel('Risk Score')
    plt.ylabel('Frequency')
    plt.title('Risk Score Distribution by Cluster')
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, f'risk_score_distribution_{timestamp}.png'), dpi=300, bbox_inches='tight')
    plt.close()
    logging.info(f"✅ Saved risk score distribution plot")
    
    # 4. Age Distribution by Cluster
    plt.figure(figsize=(12, 6))
    for cluster_id in sorted(df_with_clusters['cluster'].unique()):
        cluster_data = df_with_clusters[df_with_clusters['cluster'] == cluster_id]['age']
        plt.hist(cluster_data, alpha=0.6, label=f'Cluster {cluster_id}', bins=20)
    plt.xlabel('Age')
    plt.ylabel('Frequency')
    plt.title('Age Distribution by Cluster')
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, f'age_distribution_{timestamp}.png'), dpi=300, bbox_inches='tight')
    plt.close()
    logging.info(f"✅ Saved age distribution plot")
    
    # 5. Cluster Summary Statistics
    plt.figure(figsize=(14, 8))
    cluster_stats = df_with_clusters.groupby('cluster').agg({
        'age': ['mean', 'std'],
        'risk_score': ['mean', 'std'],
        'gender_encoded': 'mean'
    }).round(2)
    
    # Create a heatmap of cluster statistics
    sns.heatmap(cluster_stats, annot=True, fmt='.2f', cmap='YlOrRd', cbar_kws={'label': 'Value'})
    plt.title('Cluster Summary Statistics (Mean and Std)')
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, f'cluster_statistics_{timestamp}.png'), dpi=300, bbox_inches='tight')
    plt.close()
    logging.info(f"✅ Saved cluster statistics heatmap")
    
    # 6. Disease Category Distribution by Cluster
    if 'disease_category' in df_with_clusters.columns:
        plt.figure(figsize=(14, 8))
        cluster_disease = pd.crosstab(df_with_clusters['cluster'], df_with_clusters['disease_category'])
        cluster_disease.plot(kind='bar', stacked=True, colormap='Set3')
        plt.xlabel('Cluster ID')
        plt.ylabel('Number of Records')
        plt.title('Disease Category Distribution by Cluster')
        plt.legend(title='Disease Category', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=0)
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(FIGURES_DIR, f'disease_category_by_cluster_{timestamp}.png'), dpi=300, bbox_inches='tight')
        plt.close()
        logging.info(f"✅ Saved disease category distribution plot")
    
    logging.info(f"📊 All plots saved to {FIGURES_DIR}")

# --------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------

def run_analytics():
    logging.info("Loading features...")
    df = load_feature_matrix()

    logging.info("Preprocessing...")
    df, X_scaled = preprocess(df)

    logging.info("Running KMeans clustering...")
    clusters = run_kmeans(X_scaled)

    logging.info("Persisting cluster assignments...")
    persist_clusters(df, clusters)

    logging.info("Generating visualizations...")
    generate_plots(df, clusters)

    logging.info("Analytics pipeline completed.")

# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":
    run_analytics()
