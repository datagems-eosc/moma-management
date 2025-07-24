# MoMa API with FastAPI and Neo4j (Dockerized)

This project provides a containerized API for interacting with the MoMa Property Graph, which is implemented using Neo4j. It includes:

- A **FastAPI** application that exposes two endpoints
- A **Neo4j** database instance to store property graph data
- A `docker-compose.yml` file to manage both services

---
.
├── app/
│ ├── main.py
│ ├── converters.py
│ └── manager.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md

## 📦 Run App
```bash
git clone https://github.com/datagems-eosc/moma-management.git MoMaGateway
cd MoMaGateway
```

**Build and start services**
```bash
docker-compose build
docker-compose up
```

**Services will be available at:**
FastAPI: http://localhost:8000
Neo4j Browser: http://localhost:7474

**Environment Variables**
The FastAPI app reads Neo4j credentials and connection info from environment variables set in docker-compose.yml.
environment:
  NEO4J_URI: bolt://neo4j:7687
  NEO4J_USER: neo4j
  NEO4J_PASSWORD: datagems

---
## 📦 Services Overview

### 1. `/ingestProfile2MoMa` (POST)

**Purpose:**  
Ingest profiling metadata into the MoMa property graph.

**Details:**  
- Accepts input JSON in the **Croissant** format.
- Converts it to **PG-JSON** based on the **MoMa structure**
- Stores the data into **Neo4j**
- Returns:
	- {"status": "success"} – if the data was ingested successfully
	- {"status": "An error occurred: <message>"} – if an error occurred during processing


**Usage:**
```bash
POST /ingestProfile2MoMa
Content-Type: application/json
```

### 2. `/retrieveMoMaMetadata` (POST)

**Purpose:**  
Retrieve metadata from the MoMa property graph.

**Details:**  
- Accepts a dataset UUID
- Returns: PG-JSON containing metadata of the requested dataset (MoMa node)

**Usage:**
```bash
POST /retrieveMoMaMetadata?id=<your_id>
```
