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
Ingest the entire profiling (basic, light, heavy) or only the basic part to the MoMa repository.

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


### 2. `/ingestLightProfiling` (POST)

**Purpose:**  
Ingest light profiling data into the MoMa property graph stored in the Neo4j database.

**Details:**  
- Accepts input JSON in the **Croissant** format, containing the light profiling data (distribution part).
- The JSON must contain the dataset part, which includes only the dataset type and identifier ("@type": "sc:Dataset" and "@id": "<dataset-id>"). Additionally, the corresponding Dataset node must already exist in the repository.
- Converts it to **PG-JSON** based on the **MoMa structure**
- Stores the data into **Neo4j**
- Returns:
	- {"status": "success", "metadata": PG-JSON} – if the data was ingested successfully
	- {"status": "An error occurred: <message>"} – if an error occurred during processing


**Usage:**
```bash
POST /ingestLightProfiling
Content-Type: application/json
```

### 3. `/ingestHeavyProfiling` (POST)

**Purpose:**  
Ingest heavy profiling data into the MoMa property graph stored in the Neo4j database.

**Details:**  
- Accepts input JSON in the **Croissant** format, containing the heavy profiling data (recordSet part).
- Converts it to **PG-JSON** based on the **MoMa structure**
- Stores the data into **Neo4j**
- Returns:
	- {"status": "success", "metadata": PG-JSON} – if the data was ingested successfully
	- {"status": "An error occurred: <message>"} – if an error occurred during processing


**Usage:**
```bash
POST /ingestHeavyProfiling
Content-Type: application/json
```

### 4. `/convertLightProfiling2PGjson` (POST)

**Purpose:**  
The service receives light profiling data in Croissant format and returns it as PG-JSON formatted according to the MoMa schema.

**Details:**  
- Accepts input JSON in the **Croissant** format, containing the light profiling data (distribution part), including only the dataset type and identifier ("@type": "sc:Dataset" and "@id": "<dataset-id>").
- Converts it to **PG-JSON** based on the **MoMa structure**
- Returns light profiling in PG-JSON based on MoMa schema:
	- {"metadata": PG-JSON} 

**Usage:**
```bash
POST /convertLightProfiling2PGjson
Content-Type: application/json
```

### 5. `/convertHeavyProfiling2PGjson` (POST)

**Purpose:**  
The service receives heavy profiling data in Croissant format and returns it as PG-JSON formatted according to the MoMa schema.

**Details:**  
- Accepts input JSON in the **Croissant** format, containing the heavy profiling data (RecordSet part).
- Converts it to **PG-JSON** based on the **MoMa structure**
- Returns heavy profiling in PG-JSON based on MoMa schema:
	- {"metadata": PG-JSON}


**Usage:**
```bash
POST /convertHeavyProfiling2PGjson
Content-Type: application/json
```


### 6. `/addMoMaNodes` (POST)

**Purpose:**  
Add MoMa nodes to the MoMa property graph stored in the Neo4j database.

**Details:**  
- Accepts input JSON in the **PG-JSON** format based on the **MoMa nodes** containing only MoMa nodes information.
- Stores the data into **Neo4j**
- For the definition of valid node types and allowed label combinations, refer to the /validatePGjson service.
- Returns:
	- The service always returns a validation report about the labeling of the nodes. The upload behavior depends on the validation result:
	```python
	{"status": "success/error", 
	"report": {
    	"is_valid": true/false,
    	"total_nodes": number,
    	"invalid_nodes": [],
    	"unknown_labels": [],
    	"nodes_without_labels": [] 
		}
	}
	```

**Parameters:**
-validation (boolean, optional): Determines whether the PG-JSON input should be validated before uploading to Neo4j. Default True.

**Usage:**
```bash
POST /addMoMaNodes?validation=False
Content-Type: application/json
```


### 7. `/addMoMaEdjes` (POST)

**Purpose:**  
Add MoMa edges to the MoMa property graph stored in the Neo4j database.

**Details:**  
- Accepts input JSON in the **PG-JSON** format based on the **MoMa structure** containing only MoMa edges information.
- Stores the data into **Neo4j**
- Returns:
	- {"status": "success"} – if the MoMa edges were added successfully
	- {"status": "An error occurred: <message>"} – if an error occurred during processing

**Usage:**
```bash
POST /addMoMaEdjes
Content-Type: application/json
```


### 8. `/addMoMaGraph` (POST)

**Purpose:**  
Add MoMa graph to the MoMa stored in the Neo4j database.

**Details:**  
- Accepts input JSON in the **PG-JSON** format containing  **MoMa nodes** and the relationships (**edges**) between them.
- Stores the data into **Neo4j**
- For the definition of valid node types and allowed label combinations, refer to the /validatePGjson service.
- Returns:
	- The service always returns a validation report about the labeling of the nodes. The upload behavior depends on the validation result:
	```python
	{"status": "success/error", 
	"report": {
    	"is_valid": true/false,
    	"total_nodes": number,
    	"invalid_nodes": [],
    	"unknown_labels": [],
    	"nodes_without_labels": [] 
		}
	}
	```

**Usage:**
```bash
POST /addMoMaGraph
Content-Type: application/json
```


### 9. `/validatePGjson` (POST)

**Purpose:**  
Validate a Property Graph JSON (PG-JSON) against the MoMa graph schema to ensure that all nodes use valid labels and valid label combinations. 

**Details:**  
- Accepts input JSON in the **PG-JSON** format and supports strict validation (invalid nodes cause failure).
- Validates:
	- That every node has at least one label
	- That all node labels belong to the allowed MoMa label set
	- That each node's label combination matches at least one valid MoMa node type
- Each MoMa node type is defined by a specific set of labels. A node is considered valid if its labels exactly match one of the following combinations:
 	```python
	 VALID_NODE_TYPES = {
    	"Dataset": {"sc:Dataset"},
    	"RecordSet": {"cr:RecordSet"},
    	"RelationalDatabase": {"RelationalDatabase", "dg:DatabaseConnection", "Data"},
    	"TextSet": {"TextSet", "Data", "cr:FileSet"},
    	"ImageSet": {"ImageSet", "Data", "cr:FileSet"},
    	"Table": {"Table", "cr:FileObject"},
    	"CSV": {"CSV", "cr:FileObject"},
    	"PDF": {"PDF", "cr:Field"},
    	"Column": {"Column", "cr:Field"},
    	"Statistics": {"Statistics"},
    	"User": {"User"},
    	"Task": {"Task"},
    	"Analytical_Pattern": {"Analytical_Pattern"},
    	"Operator": {"Operator"}
	}
	```
- Returns:
	- The service always returns a JSON object with the following structure:
	{
		"is_valid": true/false,
 	 	"total_nodes": <number>,
  		"invalid_nodes": [],
  		"unknown_labels": [],
  		"nodes_without_labels": []
	}

**Usage:**
```bash
POST /validatePGjson
Content-Type: application/json
```

### 10. `/updateNodes` (POST)

**Purpose:**  
Update property values on existing nodes in the MoMa property graph stored in the Neo4j database.

**Details:**  
- Accepts input JSON in the **PG-JSON** format. Specifically, it matches the nodes contained in the JSON by its id and updates only the listed properties with the provided values. JSON has the following form:
	- {"nodes": [{"id":"", "properties": {"property_name":"property_value}}]}
- Returns:
	- {"status": "success", "updated": "<number_of_updated_nodes>"} – if the data was updated successfully
	- {"error": "An error occurred: <message>", "updated": "0"} – if an error occurred during processing


**Usage:**
```bash
POST /ingestProfile2MoMa
Content-Type: application/json
```

### 11. `/getMoMaObject` (GET)

**Purpose:**  
Retrieve the metadata of a MoMa node from the MoMa property graph stored in the Neo4j database.

**Details:**  
- Accepts a UUID of a MoMa node
- Returns: PG-JSON containing metadata of the requested MoMa node
	- {"metadata": PG-JSON} – returned if the process executes successfully
	- {"metadata": {"error": "<message>"}} – if an error occurred during processing

**Usage:**
```bash
GET /getMoMaObject?id=<your_id>
```

### 12. `/getDatasets` (GET)

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
	- types: ["TextSet", "ImageSet", "CSV", "Table", "RelationalDatabase", "PDF", "Column"]
- orderBy (List[str], optional): List of Dataset properties to sort results. Default [].
	- orderBy: ["id", "type", "name", "archivedAt", "description", "conformsTo", "citeAs", "license", "url", "version", "headline",  "keywords",  "fieldOfScience",  "inLanguage", "country", "datePublished", "access", "uploadedBy"]
- publishedDateFrom (date, optional): Minimum published date (YYYY-MM-DD). Default None.
- publishedDateTo (date, optional): Maximum published date (YYYY-MM-DD). Default None.
- direction (int, optional):  Traversal direction. Determines the sort order of the values in the orderBy parameter: 1 for ascending (increasing), -1 for descending (decreasing). Default is 1.
- status (str, optional): Filter datasets based on their status. Default None.

**Usage:**
```bash
# Get specific datasets with filters
GET /getDatasets?nodeIds=123&nodeIds=456&properties=url&properties=country&types=RelationalDatabase&orderBy=name&direction=1&publishedDateFrom=2025-01-01&publishedDateTo=2025-11-20&status=ready

# Get all datasets without filters
GET /getDatasets
```

### 13. `/deleteDatasets` (GET)

**Purpose:**  
Delete all Dataset nodes specified in the list of UUIDs provided in the ids parameter, along with all nodes transitively connected to them. If the list is empty, all Dataset nodes in the repository will be deleted.

**Details:**  
- Accepts a list of Dataset UUIDs to delete. If the list is empty, all Dataset nodes in the repository will be deleted.
- Returns: JSON containing metadata about the deletion process, such as the number of nodes deleted.
	- {"status": "success", "deletedRows": <number_of_deleted_rows> } – returned if the process executes successfully
	- {"error": "An error occurred: <message>", "deletedRows": "0"} – if an error occurred during processing
	
**Usage:**
```bash
# Delete specific datasets
GET /deleteDatasets?ids=123&ids=456

# Delete all datasets
GET /deleteDatasets
```