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
from google.cloud import storage
from pathlib import Path
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

# GCS Configuration
BUCKET_NAME = "insurance_health_data"
PROCESSED_ANALYTICS_PREFIX = "processed_analytics"

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
# DEPARTMENT-BASED ANALYSIS
# --------------------------------------------------

def load_department_data():
    """
    Load data with medical department information
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
        MAX(CASE WHEN hf.feature_name = 'medical_department' THEN hf.feature_value END) AS medical_department,
        hd.disease_category
    FROM health_data hd
    JOIN health_features hf
      ON hd.health_data_id = hf.health_data_id
    WHERE hf.feature_name = 'medical_department' OR hf.feature_name IN ('age', 'risk_score', 'gender')
    GROUP BY hd.health_data_id, hd.disease_category
    HAVING MAX(CASE WHEN hf.feature_name = 'medical_department' THEN 1 ELSE 0 END) = 1
    """

    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()

    return df

def cluster_by_department(df: pd.DataFrame):
    """
    Perform clustering within each medical department
    """
    department_clusters = {}
    department_stats = []
    
    # Get unique departments
    departments = df['medical_department'].dropna().unique()
    
    for dept in departments:
        dept_df = df[df['medical_department'] == dept].copy()
        
        if len(dept_df) < N_CLUSTERS:
            logging.info(f"Skipping {dept}: insufficient data ({len(dept_df)} records)")
            continue
        
        # Preprocess department data
        dept_df = dept_df.dropna(subset=["age", "risk_score"]).copy()
        dept_df["gender_encoded"] = dept_df["gender_encoded"].fillna(-1)
        dept_df["disease_category"] = dept_df["disease_category"].fillna("Unknown")
        dept_df["department_encoded"] = dept_df["disease_category"].astype("category").cat.codes
        
        if len(dept_df) < N_CLUSTERS:
            continue
        
        features = dept_df[["age", "risk_score", "gender_encoded", "department_encoded"]]
        features = features.fillna(0)
        
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(features)
        
        # Cluster within department
        model = KMeans(n_clusters=min(N_CLUSTERS, len(dept_df)), random_state=42, n_init="auto")
        dept_clusters = model.fit_predict(X_scaled)
        
        dept_df['cluster'] = dept_clusters
        department_clusters[dept] = dept_df
        
        # Calculate statistics
        for cluster_id in range(len(set(dept_clusters))):
            cluster_data = dept_df[dept_df['cluster'] == cluster_id]
            department_stats.append({
                'department': dept,
                'cluster': cluster_id,
                'count': len(cluster_data),
                'avg_age': cluster_data['age'].mean(),
                'avg_risk': cluster_data['risk_score'].mean(),
                'std_age': cluster_data['age'].std(),
                'std_risk': cluster_data['risk_score'].std()
            })
    
    return department_clusters, pd.DataFrame(department_stats)

def generate_department_plots(department_clusters: dict, department_stats: pd.DataFrame):
    """
    Generate visualizations for department-based clustering
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Department Cluster Counts
    plt.figure(figsize=(14, 8))
    dept_counts = {}
    for dept, df in department_clusters.items():
        dept_counts[dept] = df['cluster'].value_counts().sort_index()
    
    dept_df_counts = pd.DataFrame(dept_counts).T
    dept_df_counts.plot(kind='bar', stacked=False, colormap='tab10', figsize=(14, 8))
    plt.xlabel('Medical Department')
    plt.ylabel('Number of Records')
    plt.title('Cluster Distribution by Medical Department')
    plt.legend(title='Cluster ID', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, f'department_cluster_distribution_{timestamp}.png'), dpi=300, bbox_inches='tight')
    plt.close()
    logging.info(f"✅ Saved department cluster distribution plot")
    
    # 2. Average Risk Score by Department and Cluster
    if not department_stats.empty:
        plt.figure(figsize=(14, 8))
        pivot_risk = department_stats.pivot(index='department', columns='cluster', values='avg_risk')
        sns.heatmap(pivot_risk, annot=True, fmt='.3f', cmap='RdYlGn_r', cbar_kws={'label': 'Average Risk Score'})
        plt.title('Average Risk Score by Department and Cluster')
        plt.xlabel('Cluster ID')
        plt.ylabel('Medical Department')
        plt.tight_layout()
        plt.savefig(os.path.join(FIGURES_DIR, f'department_risk_heatmap_{timestamp}.png'), dpi=300, bbox_inches='tight')
        plt.close()
        logging.info(f"✅ Saved department risk heatmap")
        
        # 3. Average Age by Department and Cluster
        plt.figure(figsize=(14, 8))
        pivot_age = department_stats.pivot(index='department', columns='cluster', values='avg_age')
        sns.heatmap(pivot_age, annot=True, fmt='.1f', cmap='Blues', cbar_kws={'label': 'Average Age'})
        plt.title('Average Age by Department and Cluster')
        plt.xlabel('Cluster ID')
        plt.ylabel('Medical Department')
        plt.tight_layout()
        plt.savefig(os.path.join(FIGURES_DIR, f'department_age_heatmap_{timestamp}.png'), dpi=300, bbox_inches='tight')
        plt.close()
        logging.info(f"✅ Saved department age heatmap")
    
    # 4. Risk Score Comparison Across Departments
    plt.figure(figsize=(16, 10))
    all_dept_data = []
    for dept, df in department_clusters.items():
        for cluster_id in df['cluster'].unique():
            cluster_data = df[df['cluster'] == cluster_id]
            all_dept_data.append({
                'department': dept,
                'cluster': f'Cluster {cluster_id}',
                'risk_score': cluster_data['risk_score'].values
            })
    
    if all_dept_data:
        dept_risk_df = pd.DataFrame(all_dept_data)
        dept_risk_df = dept_risk_df.explode('risk_score')
        dept_risk_df['risk_score'] = pd.to_numeric(dept_risk_df['risk_score'], errors='coerce')
        dept_risk_df = dept_risk_df.dropna()
        
        if not dept_risk_df.empty:
            sns.boxplot(data=dept_risk_df, x='department', y='risk_score', hue='cluster')
            plt.title('Risk Score Distribution by Department and Cluster')
            plt.xlabel('Medical Department')
            plt.ylabel('Risk Score')
            plt.xticks(rotation=45, ha='right')
            plt.legend(title='Cluster', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.grid(axis='y', alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(FIGURES_DIR, f'department_risk_boxplot_{timestamp}.png'), dpi=300, bbox_inches='tight')
            plt.close()
            logging.info(f"✅ Saved department risk boxplot")
    
    # 5. Department Summary Statistics
    if not department_stats.empty:
        plt.figure(figsize=(16, 10))
        summary_stats = department_stats.groupby('department').agg({
            'count': 'sum',
            'avg_risk': 'mean',
            'avg_age': 'mean'
        }).round(2)
        
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        
        # Total records per department
        summary_stats['count'].plot(kind='bar', ax=axes[0], color='steelblue')
        axes[0].set_title('Total Records per Department')
        axes[0].set_ylabel('Number of Records')
        axes[0].tick_params(axis='x', rotation=45)
        axes[0].grid(axis='y', alpha=0.3)
        
        # Average risk per department
        summary_stats['avg_risk'].plot(kind='bar', ax=axes[1], color='coral')
        axes[1].set_title('Average Risk Score per Department')
        axes[1].set_ylabel('Risk Score')
        axes[1].tick_params(axis='x', rotation=45)
        axes[1].grid(axis='y', alpha=0.3)
        
        # Average age per department
        summary_stats['avg_age'].plot(kind='bar', ax=axes[2], color='lightgreen')
        axes[2].set_title('Average Age per Department')
        axes[2].set_ylabel('Age (years)')
        axes[2].tick_params(axis='x', rotation=45)
        axes[2].grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(FIGURES_DIR, f'department_summary_stats_{timestamp}.png'), dpi=300, bbox_inches='tight')
        plt.close()
        logging.info(f"✅ Saved department summary statistics")
    
    logging.info(f"📊 All department analysis plots saved to {FIGURES_DIR}")

# --------------------------------------------------
# UPLOAD FIGURES TO GCS
# --------------------------------------------------

def upload_figures_to_gcs(date_partition=None, time_partition=None):
    """
    Upload all figures from local figures directory to GCS bucket
    partitioned by date and time.
    
    Args:
        date_partition: Date string in YYYY-MM-DD format. If None, uses today's date.
        time_partition: Time string in HHMMSS format. If None, uses current time.
    """
    if date_partition is None:
        date_partition = datetime.now().strftime("%Y-%m-%d")
    
    if time_partition is None:
        time_partition = datetime.now().strftime("%H%M%S")
    
    # Initialize GCS client
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    # Get all PNG files from figures directory
    figures_path = Path(FIGURES_DIR)
    figure_files = list(figures_path.glob("*.png"))
    
    if not figure_files:
        logging.warning(f"No figure files found in {FIGURES_DIR}")
        return
    
    uploaded_count = 0
    failed_count = 0
    
    # Create partition path: processed_analytics/YYYY-MM-DD/HHMMSS/
    partition_path = f"{PROCESSED_ANALYTICS_PREFIX}/{date_partition}/{time_partition}"
    
    logging.info(f"Uploading {len(figure_files)} figures to GCS...")
    logging.info(f"Destination: gs://{BUCKET_NAME}/{partition_path}/")
    
    for figure_file in figure_files:
        try:
            # Create GCS blob path: processed_analytics/YYYY-MM-DD/HHMMSS/filename.png
            blob_path = f"{partition_path}/{figure_file.name}"
            
            # Upload file
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(str(figure_file))
            
            # Set content type for proper display in browser
            blob.content_type = "image/png"
            blob.patch()
            
            uploaded_count += 1
            logging.info(f"  ✅ Uploaded: {figure_file.name}")
            
        except Exception as e:
            failed_count += 1
            logging.error(f"  ❌ Failed to upload {figure_file.name}: {e}")
    
    logging.info(f"📤 Upload complete: {uploaded_count} successful, {failed_count} failed")
    logging.info(f"📍 Figures available at: gs://{BUCKET_NAME}/{partition_path}/")

def run_department_analysis():
    """
    Run department-based clustering and analysis
    """
    logging.info("Loading department data...")
    df = load_department_data()
    
    if df.empty or df['medical_department'].isna().all():
        logging.warning("No department data available. Skipping department analysis.")
        return
    
    logging.info(f"Found {len(df)} records with department information")
    logging.info(f"Departments: {df['medical_department'].dropna().unique()}")
    
    logging.info("Clustering by department...")
    department_clusters, department_stats = cluster_by_department(df)
    
    if not department_clusters:
        logging.warning("No valid department clusters created. Skipping visualizations.")
        return
    
    logging.info(f"Created clusters for {len(department_clusters)} departments")
    logging.info("Generating department visualizations...")
    generate_department_plots(department_clusters, department_stats)
    
    # Print summary statistics
    logging.info("\n=== DEPARTMENT CLUSTERING SUMMARY ===")
    for dept, dept_df in department_clusters.items():
        logging.info(f"\n{dept}:")
        logging.info(f"  Total records: {len(dept_df)}")
        for cluster_id in sorted(dept_df['cluster'].unique()):
            cluster_data = dept_df[dept_df['cluster'] == cluster_id]
            logging.info(f"  Cluster {cluster_id}: {len(cluster_data)} records, "
                        f"Avg Age: {cluster_data['age'].mean():.1f}, "
                        f"Avg Risk: {cluster_data['risk_score'].mean():.3f}")

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

    logging.info("Running department-based analysis...")
    run_department_analysis()

    logging.info("Uploading figures to GCS...")
    upload_figures_to_gcs()

    logging.info("Analytics pipeline completed.")

# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":
    run_analytics()
