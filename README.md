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
Ingest profiling data into the MoMa property graph stored in the Neo4j database.

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
Retrieve the metadata of a MoMa node from the MoMa property graph stored in the Neo4j database.

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
Retrieve the metadata of Dataset nodes and all nodes (data) transitively connected to them that belong to each Dataset based on filtering criteria.

**Details:**  
- Accepts filtering parameters such as nodeIds, properties, types, orderBy, published date range, direction, and status of a dataset.
- Returns a JSON containing metadata of the requested Dataset(s) and and all nodes transitively connected to it according to the criteria defined by the values of the parameters.
	- {"metadata": PG-JSON} – returned if the process executes successfully

**Parameters:**
- nodeIds (List[str], optional): Filter datasets by their UUIDs. Defaults to an empty list [], which returns all datasets in the repository.
- properties (List[str], optional): List of Dataset properties to include. Special values "distribution" and "recordSet" include connected nodes. Default [], which includes all properties.
	- propetries: ["type", "name", "archivedAt", "description", "conformsTo", "citeAs", "license", "url", "version", "headline",  "keywords",  "fieldOfScience",  "inLanguage", "country", "datePublished", "access", "uploadedBy", "distribution", "recordSet"]
- types (List[str], optional): Filter datasets connected to nodes with these labels. Special values are "FileObject" and "FileSet", which essentially encapsulate multiple types based on MoMa types. Default [].
	- types: ["TextSet", "ImageSet", "CSV", "Table", "RelationalDatabase", "PDF", "Column", "FileObject", "FileSet"]
- orderBy (List[str], optional): List of Dataset properties to sort results. Default [].
	- orderBy: ["id", "type", "name", "archivedAt", "description", "conformsTo", "citeAs", "license", "url", "version", "headline",  "keywords",  "fieldOfScience",  "inLanguage", "country", "datePublished", "access", "uploadedBy"]
- publishedDateFrom (date, optional): Minimum published date (YYYY-MM-DD). Default None.
- publishedDateTo (date, optional): Maximum published date (YYYY-MM-DD). Default None.
- direction (int, optional):  Traversal direction. Determines the sort order of the values in the orderBy parameter: 1 for ascending (increasing), -1 for descending (decreasing). Default is 1.
- status (str, optional): Dataset status to filter on. Default "ready".

**Usage:**
```bash
# Get specific datasets with filters
GET /getDataset?nodeIds=123&nodeIds=456&properties=url&properties=country&types=RelationalDatabase&orderBy=name&direction=1&publishedDateFrom=2025-01-01&publishedDateTo=2025-11-20&status=ready

# Get all datasets without filters
GET /getDataset
```

### 4. `/deleteDatasets` (GET)

**Purpose:**  
Delete all Dataset nodes specified in the list of UUIDs provided in the ids parameter, along with all nodes transitively connected to them. If the list is empty, all Dataset nodes in the repository will be deleted.

**Details:**  
- Accepts a list of Dataset UUIDs to delete. If the list is empty, all Dataset nodes in the repository will be deleted.
- Returns: JSON containing metadata about the deletion process, such as the number of nodes deleted.
	- { "metadata": {"status": "all" | "selected", "deletedNodes": <number_of_deleted_nodes> } }
	
**Usage:**
```bash
# Delete specific datasets
GET /deleteDataset?ids=123&ids=456

# Delete all datasets
GET /deleteDataset
```

### 5. `/listDatasets` (GET)

**Purpose:**  
Retrieve the metadata of the Datasets stored in the MoMa property graph in the Neo4

**Details:**  
- Returns: PG-JSON containing metadata of the Datasets stored in the MoMA
	- {"metadata": PG-JSON} – returned if the process executes successfully
	
**Usage:**
```bash
GET /listDatasets
```

### 6. `/listDatasetsOrderedBy` (GET)

**Purpose:**  
Retrieve the metadata of the Datasets stored into the MoMa graph in Neo4j, ordered by a specific property

**Details:**  
- Accepts a property of Dataset label. Accepted properties: [“datePublished”]
- Returns: PG-JSON containing metadata of the Collections, ordered by the specified property in the parameter
	- {"metadata": PG-JSON} – returned if the process executes successfully
	- {"metadata": "status - wrong parameter"} – returned if the parameter is wrong

**Usage:**
```bash
GET /listDatasetsOrderedBy?orderBy=<your_id>
```

### 7. `/listDatasetsByType` (GET)

**Purpose:**  
Retrieve the metadata of Dataset nodes that contain a specific type of data.

**Details:**  
- Accepts a data type. Accepted Types: ["PDF", "RelationalDatabase", "CSV", "ImageSet", "TextSet", "Table"]
- Returns: PG-JSON containing metadata of the Datasets, containing this data type.
	- {"metadata": PG-JSON} – returned if the process executes successfully
	- {"metadata": "status - wrong parameter"} – returned if the parameter is wrong

**Usage:**
```bash
GET /listDatasetsByType?type=<your_id>
```

