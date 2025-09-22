# MoMa API with FastAPI and Neo4j (Dockerized)

This project provides a containerized API for interacting with the MoMa Property Graph, which is implemented using Neo4j. It includes:

- A **FastAPI** application that exposes two endpoints
- A **Neo4j** database instance to store property graph data
- A `docker-compose.yml` file to manage both services

---
**Project Structure**
```
.
├── app/
│   ├── main.py
│   ├── converters.py
│   └── manager.py
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

## 📦 Run App
```bash
git clone https://github.com/datagems-eosc/moma-management.git
cd MoMaGateway
```
**Installation**
- Install Docker
```bash
sudo apt update
sudo apt install -y docker.io
sudo systemctl start docker
sudo systemctl enable docker
```
- Install Docker Compose
```bash
sudo apt install -y docker-compose
```

**Build and start services**
```bash
docker-compose build
docker-compose up -d
```

**Services will be available at:**
- FastAPI: http://localhost:8000
- Neo4j Browser: http://localhost:7474

**Stop services**
```bash
docker-compose down
```

**Environment Variables:**
The FastAPI app reads Neo4j credentials and connection info from environment variables set in docker-compose.yml.
  - NEO4J_URI: bolt://neo4j:7687
  - NEO4J_USER: neo4j
  - NEO4J_PASSWORD: datagems

---
## 📦 Services Overview

### 1. `/ingestProfile2MoMa` (POST)

**Purpose:**  
Ingest profiling data into the MoMa property graph database.

**Details:**  
- Accepts input JSON in the **Croissant** format.
- Converts it to **PG-JSON** based on the **MoMa structure**
- Stores the data into **Neo4j**
- Returns:
	- {"status": "success", "metadata": PG-JSON} – if the data was ingested successfully
	- {"status": "An error occurred: <message>"} – if an error occurred during processing


**Usage:**
```bash
POST /ingestProfile2MoMa
Content-Type: application/json
```

### 2. `/getMoMaObject` (GET)

**Purpose:**  
Retrieve the metadata of a MoMa node from the MoMa property graph.

**Details:**  
- Accepts a UUID of a MoMa node
- Returns: PG-JSON containing metadata of the requested MoMa node
	- {"metadata": PG-JSON} – returned if the process executes successfully

**Usage:**
```bash
GET /getMoMaObject?id=<your_id>
```

### 3. `/getDataset` (GET)

**Purpose:**  
Retrieve the metadata of a Dataset node and all nodes (data) transitively connected to it that belong to this Dataset.

**Details:**  
- Accepts a Dataset UUID
- Returns: PG-JSON containing metadata of the requested Dataset and all nodes transitively connected to it
	- {"metadata": PG-JSON} – returned if the process executes successfully

**Usage:**
```bash
GET /getDataset?id=<your_id>
```

### 4. `/listDatasets` (GET)

**Purpose:**  
Retrieve the metadata of the Datasets stored in the MoMA property graph.

**Details:**  
- Returns: PG-JSON containing metadata of the Datasets stored in the MoMA
	- {"metadata": PG-JSON} – returned if the process executes successfully
	
**Usage:**
```bash
GET /listDatasets
```

### 5. `/listDatasetsOrderedBy` (GET)

**Purpose:**  
Retrieve the metadata of the Datasets stored in the MoMA property graph, ordered by a specific property

**Details:**  
- Accepts a property of Dataset label. Accepted properties: [“datePublished”]
- Returns: PG-JSON containing metadata of the Collections, ordered by the specified property in the parameter
	- {"metadata": PG-JSON} – returned if the process executes successfully
	- {"metadata": "status - wrong parameter"} – returned if the parameter is wrong

**Usage:**
```bash
GET /listDatasetsOrderedBy?orderBy=<your_id>
```

### 6. `/listDatasetsByType` (GET)

**Purpose:**  
Retrieve the metadata of Dataset nodes that contain a specific type of dataset.

**Details:**  
- Accepts a type of a dataset. Accepted Types: ["PDF", "RelationalDatabase", "CSV", "ImageSet", "TextSet", "Table"]
- Returns: PG-JSON containing metadata of the Datasets, containing this data type.
	- {"metadata": PG-JSON} – returned if the process executes successfully
	- {"metadata": "status - wrong parameter"} – returned if the parameter is wrong

**Usage:**
```bash
GET /listDatasetsByType?type=<your_id>
```

