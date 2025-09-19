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
	- {"status": "success"} – if the data was ingested successfully
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

**Usage:**
```bash
GET /getMoMaObject?id=<your_id>
```

### 3. `/getCollection` (GET)

**Purpose:**  
Retrieve the metadata of a Collection node and all nodes transitively connected to it that belong to this Collection.

**Details:**  
- Accepts a Collection UUID
- Returns: PG-JSON containing metadata of the requested Collection and all nodes transitively connected to it

**Usage:**
```bash
GET /getCollection?id=<your_id>
```

### 4. `/listCollections` (GET)

**Purpose:**  
Retrieve the metadata of the Collections stored in the MoMA property graph.

**Details:**  
- Returns: PG-JSON containing metadata of the Collections stored in the MoMA

**Usage:**
```bash
GET /listCollections
```

### 5. `/listCollectionsOrderedBy` (GET)

**Purpose:**  
Retrieve the metadata of the Collections stored in the MoMA property graph, ordered by a specific property

**Details:**  
- Accepts a property of Collection label. Accepted properties: [“datePublished”]
- Returns: PG-JSON containing metadata of the Collections, ordered by the specified property in the parameter
	- {"metadata": "status: wrong parameter"} – returned if the parameter is wrong

**Usage:**
```bash
GET /listCollectionsOrderedBy?orderBy=<your_id>
```

### 6. `/listCollectionsByType` (GET)

**Purpose:**  
Retrieve the metadata of Collection nodes that contain a specific type of dataset.

**Details:**  
- Accepts a type of a dataset. Accepted Types: ["PDF", "RelationalDatabase", "CSV", "ImageSet", "TextSet", "Table"]
- Returns: PG-JSON containing metadata of the Collections, containing this dataset type.
	- {"metadata": "status: wrong parameter"} – returned if the parameter is wrong

**Usage:**
```bash
GET /listCollectionsByType?type=<your_id>
```

