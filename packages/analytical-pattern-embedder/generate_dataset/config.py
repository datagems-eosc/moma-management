from pathlib import Path

HERE = Path(__file__).parent.parent

# User input directory
INPUT_DIR = HERE / "dataset" / "inputs"

# Hand-labelled questions: MathE and weather ones asked by users, and the tasks of
# AP Management's tests and demo. Evaluated on, never trained on.
REAL_TASKS_FILE = INPUT_DIR / "real_tasks.jsonl"

# Real APs from the actual use cases.
REAL_APS_DIR = INPUT_DIR / "real_aps"

# name -> what the LLM is told the domain covers. The first three mirror what the
# platform serves today (SQL, MathE, ERA5 weather), the rest widen the catalogue.
DOMAINS = {
    "sql_analytics": "querying relational databases and tabular datasets: text-to-SQL, query execution, schema inspection, summarising result rows, provenance and explanation of query results",
    "education_analytics": "learning platforms: student performance, recommending questions or study material, estimating difficulty, building study plans",
    "meteorology_climate": "weather and climate records: aggregating readings, detecting extremes, comparing periods and regions, climate indicators",
    "time_series": "time series: resampling, forecasting, anomaly detection, seasonality, change points",
    "geospatial": "geographic data: geocoding, spatial joins, distance and area computation, map layers",
    "text_nlp": "free text: summarisation, classification, entity extraction, sentiment, keyword extraction",
    "document_processing": "PDF and scanned documents: OCR, layout analysis, table extraction, citation extraction",
    "information_retrieval": "searching document collections and answering questions from the passages found",
    "data_quality": "profiling datasets, detecting missing or inconsistent values, validating against rules, deduplication",
    "data_integration": "schema matching, entity resolution, joining and harmonising heterogeneous datasets",
    "visualization_reporting": "charts, dashboards and written reports produced from data",
    "statistics": "descriptive statistics, hypothesis tests, correlation, regression",
    "machine_learning": "feature engineering, model training, inference, evaluation, explanation of predictions",
    "image_analysis": "images: classification, object detection, captioning, similarity search",
    "audio_speech": "audio: transcription, speaker separation, language identification",
    "knowledge_graphs": "building, querying and enriching knowledge graphs; linking entities to a graph",
    "privacy_anonymisation": "detecting personal data, anonymising or pseudonymising datasets, measuring re-identification risk",
    "dataset_discovery": "finding, recommending, profiling and comparing datasets in a catalogue",
    "translation_multilingual": "translating text, detecting language, aligning multilingual content",
    "finance_economics": "economic indicators, price series, budgets, risk measures",
    "health_biomedical": "clinical and biomedical data: cohort selection, coding to medical vocabularies, survival and outcome analysis",
    "occupations_surveys": "survey responses and occupational taxonomies: coding free-text job titles to ISCO, weighting, cross-tabulation",
}
