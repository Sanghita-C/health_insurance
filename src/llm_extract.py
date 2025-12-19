# LLM extraction module

"""
LLM Extraction Module
--------------------
Uses Vertex AI Gemini to extract structured clinical features
from unstructured clinical notes.

This module is intentionally stateless and DB-agnostic.
"""

import json
import vertexai
from vertexai.preview.generative_models import GenerativeModel

# ==============================
# CONFIG
# ==============================

PROJECT_ID = "rapid-rite-478023-j8"
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.0-flash-lite"   # cheap + fast, good for batch jobs

# Initialize Vertex AI
vertexai.init(project=PROJECT_ID, location=LOCATION)

model = GenerativeModel(MODEL_NAME)

# ==============================
# PROMPT (STRICT JSON)
# ==============================

PROMPT = """
You are a clinical NLP model. Extract structured information from the clinical note.

Return STRICT JSON ONLY with EXACT format:

{
  "Age": "string",
  "Gender": "string",
  "Height": "string",
  "Weight": "string",
  "symptoms": {
    "symptom_1": float,
    "symptom_2": float,
    ...
  },
  "diagnosis": "string",
  "risk_score": float,
  "medical_department": "string"
}

Rules:
- Capture ALL symptoms mentioned
- Symptom severity must be between 0.0 and 1.0
- risk_score must be between 0.0 and 1.0
- Medical department must be ONE of:
  Cardiology, Neurology, Orthopedics / Musculoskeletal,
  Pulmonology, Gastroenterology, Endocrinology, Oncology,
  Dermatology, Infectious Diseases, Psychiatry / Behavioral,
  Obstetrics / Gynecology, Radiology
- Choose the most probable diagnosis
- If information is missing, return null
- DO NOT return any text outside the JSON
"""

# ==============================
# MAIN EXTRACTION FUNCTION
# ==============================

def extract_features(note_text: str) -> dict:
    """
    Sends clinical note text to Gemini and returns structured JSON.
    """

    response = model.generate_content(
        PROMPT + "\n\nClinical Note:\n" + note_text,
        generation_config={
            "response_mime_type": "application/json",
            "temperature": 0.2,
            "max_output_tokens": 1024
        }
    )

    try:
        return json.loads(response.text)
    except json.JSONDecodeError:
        # Hard fallback to prevent pipeline crash
        return {
            "Age": None,
            "Gender": None,
            "Height": None,
            "Weight": None,
            "symptoms": {},
            "diagnosis": None,
            "risk_score": None,
            "medical_department": None
        }