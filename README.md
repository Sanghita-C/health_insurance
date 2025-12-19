# Insurance Health ML- NYU Project
# Course: Database Systems (CSCI-GA.2433-001)
# Contributors: Sanghita Chakraborty, Arushi Srivastava

This project implements an end-to-end data management and analytics pipeline for processing unstructured clinical notes and transforming them into structured, queryable health insights. Built on Google Cloud Platform (GCP), the system integrates data ingestion, cloud storage, relational databases, large language models (LLMs), and analytical workflows to support downstream risk analysis and patient stratification.

1. Key Features

Clinical Data Ingestion: Secure ingestion of raw clinical notes into Google Cloud Storage with metadata tracking.

OLTP / ODS Layer: Structured storage of health records, clinical notes, and derived features in Cloud SQL (PostgreSQL).

LLM-Based Feature Extraction: Automated nightly processing using Vertex AI to extract medical entities, risk indicators, and semantic embeddings from clinical notes.

Feature Engineering Pipeline: Normalization and persistence of extracted features into a dedicated HEALTH_FEATURES table for analytics.

Analytics & Clustering: Application of machine learning techniques (e.g., K-Means clustering) to group patients by risk profiles.

ORM-Backed Data Access: Use of an ORM layer to ensure maintainable, optimized, and secure database interactions.

Scalable Architecture: Modular design separating ingestion, processing, feature extraction, and analytics for scalability and maintainability.

2. Architecture Overview

The solution follows a layered reference architecture spanning:

Business Layer: Healthcare risk analysis and decision support use cases

Application Layer: Ingestion services, LLM pipelines, feature processors, analytics modules

Data Layer (DIKW): Raw data → structured information → analytical insights

Infrastructure Layer: GCP (Cloud Storage, Cloud SQL, Vertex AI, scheduled jobs)

3. Use Cases

Clinical risk stratification

Patient segmentation based on extracted health indicators

Structured analytics over unstructured medical text

Demonstration of end-to-end data governance and optimization techniques

4. Technologies

Python, PostgreSQL, Google Cloud Storage, Cloud SQL, Vertex AI, ORM (SQLAlchemy), Machine Learning, LLMs

