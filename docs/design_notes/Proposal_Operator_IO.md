# Design Proposal : Operator Input / Output

## Definition

**MoMa** : Hybrid ETL pipelines storage system. Represent data, action and Machine learning in a single storage.

**Analytical Pattern (AP)** : Heart of the MoMa model. A reusable workflow/dataflow that serves as a communication languages between building blocks of Datagems.

**Operator** : High-level function transforming a set of input into a set of outputs 

## Motivation

As more APs are created, the need to properly specify what an Operator consumes and produces becomes critical. This document discusses the structured definition of Operator inputs and outputs, and how data flows between Operators through Datasets.

## Current Design Limitation

Today, neither the Operator schema nor the Dataset schema capture what data actually flows through the pipeline:

<details>

<summary> Operator Specification </summary>
```jsonc
// Operator_schema.spec.json
// Source : https://github.com/datagems-eosc/moma-management/blob/d4e4600eb85c5edd9228d4d87c8f62f6c1a03014/moma_management/domain/schema/nodes/ap/operator.schema.json
// Viewed on 2026/04/13
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "operator.schema.json",
    "title": "Operator",
    "allOf": [
        {
            "$ref": "../node.schema.json"
        }
    ],
    "type": "object",
    "description": "An Operator node represents a single processing step within an AnalyticalPattern. Specialisations (e.g. SQL_Operator) add further properties.",
    "properties": {
        "id": {
            "type": "string",
            "format": "uuid"
        },
        "type": {
            "type": "string",
            "enum": [
                "Operator"
            ]
        },
        "name": {
            "type": "string",
            "description": "Human-readable name of the operator"
        },
        "description": {
            "type": "string",
            "description": "What this operator does"
        },
        "command": {
            "type": "string",
            "description": "Executable command or function name"
        },
        "step": {
            "type": "integer",
            "description": "Ordinal position in a sequence of operators"
        },
        "startTime": {
            "type": "string",
            "description": "Optional operator-level start time"
        }
    },
    "required": [
        "id",
        "type",
        "name"
    ]
}
```

</details>

<details>

<summary> Dataset Specification </summary>

```jsonc
// Dataset_schema.spec.json
// Source : https://github.com/datagems-eosc/moma-management/blob/d4e4600eb85c5edd9228d4d87c8f62f6c1a03014/moma_management/domain/schema/nodes/dataset/dataset.schema.json
// Viewed on 2026/04/21
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Dataset",
    "type": "object",
    "properties": {
        "id": {
            "type": "string",
            "description": "Unique identifier of the dataset"
        },
        "type": {
            "type": "string",
            "enum": ["sc:Dataset"],
            "description": "Node type discriminator"
        },
        "name": {
            "type": "string",
            "description": "Human-readable name of the dataset"
        },
        "description": {
            "type": "string",
            "description": "Full description of the dataset"
        },
        "datePublished": {
            "type": "string",
            "format": "date",
            "description": "Publication date (ISO 8601: YYYY-MM-DD)"
        },
        "version": {
            "type": "string",
            "description": "Version string of the dataset"
        },
        "license": {
            "type": "string",
            "description": "License under which the dataset is published"
        },
        "url": {
            "type": "string",
            "format": "uri",
            "description": "Canonical URL of the dataset"
        },
        "keywords": {
            "type": "array",
            "items": { "type": "string" },
            "description": "Descriptive keywords for discovery and search"
        },
        "status": {
            "type": "string",
            "enum": ["draft", "published", "loaded", "archived"],
            "description": "Lifecycle status of the dataset"
        },
        "archivedAt": {
            "type": "string",
            "description": "Storage location of the archived dataset (e.g. S3 URI)"
        }
    },
    "required": [
        "id",
        "type"
    ]
}
```

</details>

We don't see anything about :
- What's the **input** of an Operator ?
- What's the **output** of an Operator ?
- What's the structure of data contained in a **Dataset** ?

This creates questions on :
- How to pass **structured** results between Operators ?
- Is the generic **Dataset** type enough to represent Data passing ?

## Proposed Changes

### Updated Graph Data Model

The diagram below extends the MoMa schema with the `ResultType` node. `Data` and each JSON Schema type (`string`, `boolean`, `number`, `array`, `object`) specialise `ResultType` (dashed "is-a" arrows), and Operator input/output edges now point to `ResultType` rather than `Data` directly.

```mermaid
graph LR
  %% ── Styling ──────────────────────────────────────────────
  classDef dataset fill:#1565C0,stroke:#0D47A1,color:#fff
  classDef data fill:#1E88E5,stroke:#1565C0,color:#fff
  classDef dataChild fill:#42A5F5,stroke:#1E88E5,color:#fff
  classDef dataLeaf fill:#64B5F6,stroke:#42A5F5,color:#fff
  classDef record fill:#90CAF9,stroke:#42A5F5,color:#000
  classDef ap fill:#E53935,stroke:#C62828,color:#fff
  classDef operator fill:#EF5350,stroke:#E53935,color:#fff
  classDef ml fill:#43A047,stroke:#2E7D32,color:#fff
  classDef misc fill:#78909C,stroke:#546E7A,color:#fff
  classDef resulttype fill:#7B1FA2,stroke:#4A148C,color:#fff
  classDef scalar fill:#CE93D8,stroke:#7B1FA2,color:#000

  %% ── ResultType ───────────────────────────────────────────
  ResultType:::resulttype
  StringResult["string"]:::scalar
  BooleanResult["boolean"]:::scalar
  NumberResult["number"]:::scalar
  ArrayResult["array"]:::scalar
  ObjectResult["object"]:::scalar

  %% ── Dataset subgraph ────────────────────────────────────
  Dataset["sc:Dataset"]:::dataset
  Data:::data
  StructuredData["Structured Data"]:::dataChild
  UnstructuredData["Unstructured Data"]:::dataChild
  RelDB["Relational Database"]:::dataChild
  CsvSet["CSV Set"]:::dataChild
  JSONSet["JSON Set"]:::dataChild
  XMLSet["XML Set"]:::dataChild
  TextSet["Text Set"]:::dataChild
  VideoSet["Video Set"]:::dataChild
  ImageSet["Image Set"]:::dataChild
  Table:::dataLeaf
  CSV:::dataLeaf
  JSON:::dataLeaf
  XML:::dataLeaf
  Text:::dataLeaf
  Video:::dataLeaf
  Image:::dataLeaf
  ColumnTable["Column Table"]:::dataLeaf
  ColumnCSV["Column CSV"]:::dataLeaf
  Column:::dataLeaf
  PDF:::dataLeaf
  Statistics:::dataLeaf
  RecordSet["cr:RecordSet"]:::record

  Data -. "is-a" .-> ResultType
  StringResult -. "is-a" .-> ResultType
  BooleanResult -. "is-a" .-> ResultType
  NumberResult -. "is-a" .-> ResultType
  ArrayResult -. "is-a" .-> ResultType
  ObjectResult -. "is-a" .-> ResultType
  Dataset -- distribution --> Data
  Dataset -- recordSet --> RecordSet
  Data -- linkTo --> Data
  StructuredData -. "is-a" .-> Data
  UnstructuredData -. "is-a" .-> Data
  RelDB -. "is-a" .-> StructuredData
  CsvSet -. "is-a" .-> StructuredData
  JSONSet -. "is-a" .-> StructuredData
  XMLSet -. "is-a" .-> StructuredData
  TextSet -. "is-a" .-> UnstructuredData
  PDFSet -. "is-a" .-> UnstructuredData
  VideoSet -. "is-a" .-> UnstructuredData
  ImageSet -. "is-a" .-> UnstructuredData
  Table -- containedIn --> RelDB
  CSV -- containedIn --> CsvSet
  JSON -- containedIn --> JSONSet
  XML -- containedIn --> XMLSet
  Text -- containedIn --> TextSet
  Video -- containedIn --> VideoSet
  Image -- containedIn --> ImageSet
  ColumnTable -- containedIn --> Table
  ColumnCSV -- containedIn --> CSV
  RecordSet -- field --> Column
  RecordSet -- field --> PDF
  Column -- "source/fileObject" --> Data
  PDF -- "source/fileSet" --> Data
  Column -- statistics --> Statistics

  %% ── Analytical-Pattern subgraph ─────────────────────────
  AP["Analytical Pattern"]:::ap
  Operator:::operator
  Task:::misc
  User:::misc

  AP -- consist_of --> Operator
  Operator -- input --> ResultType
  Operator -- output --> ResultType
  Operator -- follows --> Operator
  User -- uses --> Operator
  Task -- is_accomplished_by --> AP

  %% ── ML Model subgraph ──────────────────────────────────
  MLModel["ML_Model"]:::ml

  Operator -- perform_inference --> MLModel
```

| Colour          | Domain                               |
| --------------- | ------------------------------------ |
| 🟦 Blue shades   | Dataset & Data nodes                 |
| 🟥 Red shades    | Analytical Pattern & Operators       |
| 🟩 Green         | ML Model                             |
| ⬜ Grey          | Task & User                          |
| 🟣 Purple shades | ResultType & scalar subtypes *(new)* |

### JSON Schema type system

Parameters and result values are typed using JSON Schema types.

| Type      | Description                                                                                                                                                                                                |
| --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `string`  | -                                                                                                                                                                                                          |
| `boolean` | -                                                                                                                                                                                                          |
| `number`  | -                                                                                                                                                                                                          |
| `array`   | List of values. Use `items` to type homogeneous lists (`"items": "string"`). Untyped `array` is valid for heterogeneous lists.                                                                             |
| `object`  | Structured value. Can carry an inline JSON Schema (`properties`, `required`, `additionalProperties`) in the Operator parameter definition. Opaque use (no `properties`) is valid for untyped pass-through. |

### Operator input and output parameters 

Each Operator is modelled as a function with a typed signature:

$f(x_1, ..., x_n) \rightarrow  y_1...y_m$

Where:
- $x_1...x_n$ are the input parameters of the Operator *f*
- $y_1...y_m$ are the output parameters of the Operator *f*

As in a programming language, the **AP template** must define for each operator and for each parameter :
- The **name** of the parameter, used within the operator
- The **type** of the parameter, expressed as a JSON Schema type (see [JSON Schema type system](#json-schema-type-system))
- Whether the parameter is **required** (Allowing pseudo-[variadic](https://en.cppreference.com/w/c/variadic.html) input)
- The optional **default value** of the parameter.


```jsonc
{
    "id": "<uuid>",
    "labels": ["Operator", "Op_1"],
    "properties": {
        // Operator signature versioning
        "version": "1.0.0",
        ...
        "inputs":[
            {
                "name": "p1",
                "type": "string",
                "required": true,
                "default": ""
            }
        ],
        "outputs":[
            {
                "name": "r1",
                "type": "string",
                "required": true,
                "default": ""
            }
        ]
    }
}
```

### ResultType node

A `ResultType` is the base node type for any value exchanged between Operators. Both `Data` and JSON Schema types (`string`, `boolean`, `number`, `array`, `object`) are concrete subtypes of `ResultType`  each identified by its second label in the graph.

**When to use each subtype:**

| Subtype                                                 | Use when                                                                                                                                                                                           |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **scalar `ResultType`** (`string`, `boolean`, `number`) | Non-persistent, single primitive value. Each output parameter gets its own node.                                                                                                                   |
| **`array` `ResultType`**                                | Non-persistent, small homogeneous list.                                                                                                                                                            |
| **`object` `ResultType`**                               | Non-persistent, small structured value with inline JSON Schema.                                                                                                                                    |
| **`Data`**                                              | Any persistent or large data (existing behavior) |

A `ResultType` node carries:
- `id` : UUID
- `type` : one of `"string"`, `"boolean"`, `"number"`, `"array"`, `"object"`, `"Data"`
- `name`  the logical name of the value in the graph (required for concrete subtypes, optional on the base)

The concrete type is encoded as the second label for efficient graph filtering. Only `array` and `object` subtypes carry an additional `schema` property (a JSON Schema fragment describing their structure). Scalar subtypes (`string`, `boolean`, `number`) have no `schema` field.

```jsonc
// scalar
{
    "id": "<uuid>",
    "labels": ["ResultType", "string"],
    "properties": {
        "name": "sql_query"
    }
}
```

```jsonc
// typed array 
{
    "id": "<uuid>",
    "labels": ["ResultType", "array"],
    "properties": {
        "name": "results",
        "schema": { "type": "array", "items": { "type": "string" } }
    }
}
```

```jsonc
// structured object 
{
    "id": "<uuid>",
    "labels": ["ResultType", "object"],
    "properties": {
        "name": "metadata",
        "schema": {
            "type": "object",
            "properties": {
                "code": { "type": "string" },
                "message": { "type": "string" }
            },
            "required": ["code"]
        }
    }
}
```

This makes each data channel in the pipeline explicit and type-safe:

```mermaid
graph LR
    OP1["TEXT_TO_SQL [Operator]"]
    RT1["sql_query [ResultType; string]"]
    RT2["is_safe [ResultType; boolean]"]
    OP2["EXEC_SQL [Operator]"]

    OP1 -->|"output [to['sql_query']: from['query']]"| RT1
    OP1 -->|"output [to['is_safe']: from['is_read_only']]"| RT2
    OP2 -->|"input [to['sql']: from['sql_query']]"| RT1
    OP2 -->|"input [to['is_safe']: from['is_safe']]"| RT2

    style OP1 fill:#FFB347,stroke:#333,stroke-width:2px
    style OP2 fill:#FFB347,stroke:#333,stroke-width:2px
    style RT1 fill:#CE93D8,stroke:#7B1FA2,stroke-width:2px
    style RT2 fill:#CE93D8,stroke:#7B1FA2,stroke-width:2px
```

### Data nodes

```mermaid

graph LR
    %% Operations (execution flow)
    OP1[Operator 1]
    OP2[Operator 2]

    %% Data
    DS1[Data]

    %% Data flow
    OP1 -->|output| DS1
    OP2 -->|input| DS1

    %% Styling
    style OP1 fill:#FFB347,stroke:#333,stroke-width:2px
    style OP2 fill:#FFB347,stroke:#333,stroke-width:2px

    style DS1 fill:#87CEFA,stroke:#333,stroke-width:2px
```

**Operators** use **`Data`** nodes as intermediates when exchanging persistent, structured, or large data (relational databases, streaming sources, files). A `Data` node is a subtype of `ResultType` 

**NOTE**: `sc:Dataset` is the *root node* of a dataset graph  it is not a `ResultType` subtype and should not be used as a direct input/output of an Operator.

### Input and output links

Mappings define how data flows between Operators and ResultType nodes.

The mapping syntax follows [JSONPath (RFC 9535)](https://www.rfc-editor.org/rfc/rfc9535), using `from` and `to` to refer to the source and target nodes respectively. Mappings are stored as a JSON object where **each key is the target path and the corresponding value is the source path**:

```json
{
    "to['<target_key>']": "from['<jsonpath-expression>']"
}
```

Standard JSONPath expressions are supported inside `from[...]`  dot notation, bracket notation, array indexing, filter expressions, etc. Additional transform functions may be applied to a `from` expression.

Ideally, mappings should be validated at AP design time. A type mismatch between source and target (as declared on the respective Operator `outputs` / `inputs`) is a design-time error. However, validating against Data node is nearly impossible. 

**Edge direction**: Both `input` and `output` edges go **from the Operator** to the Data/ResultType node. The edge label (`input` or `output`) indicates the role from the Operator's perspective.

```jsonc
// Operator → [output] → ResultType node
// The operator writes its output named 'query' into the 'sql_query' node
{
    "from": "<Op1>",
    "to": "<ResultType_node>",
    "labels": ["output"],
    "properties": {
        "mapping": {
            "to['sql_query']": "from['query']"
        }
    }
}
```

```jsonc
// Operator → [input] → ResultType node
// The operator reads 'sql_query' node into its 'sql' input
{
    "from": "<OP_2>",
    "to": "<ResultType_node>",
    "labels": ["input"],
    "properties": {
        "mapping": {
            "to['sql']": "from['sql_query']"
        }
    }
}
```

## Example : A NL Query execution AP

The purpose of this AP is to take in a natural language query and execute it as an SQL statement.

```mermaid

graph TD

    %% Top level
    AP[Ap]

    %% Operations (execution flow)
    OP1[Text to SQL]
    OP2[SQL execution]

    %% Data
    RT_NL["nl_query [ResultType; string]"]
    RT_SQL["sql_query [ResultType; string]"]
    DS_DB[DB - RelationalDatabase]
    DS_RESULT[Result - RelationalDatabase]

    %% Execution order (vertical flow)
    AP -->|consist_of| OP1
    AP -->|consist_of| OP2
    OP2 -->|follows| OP1

    %% Data flow (all edges go FROM Operator TO data node)
    OP1 -->|input| RT_NL
    OP1 -->|output| RT_SQL
    OP2 -->|input| RT_SQL
    OP2 -->|input| DS_DB
    OP2 -->|output| DS_RESULT

    %% Styling
    style AP fill:#E53935,stroke:#C62828,color:#fff
    style OP1 fill:#EF5350,stroke:#E53935,color:#fff
    style OP2 fill:#EF5350,stroke:#E53935,color:#fff

    style RT_NL fill:#CE93D8,stroke:#7B1FA2,color:#000
    style RT_SQL fill:#CE93D8,stroke:#7B1FA2,color:#000
    style DS_DB fill:#1565C0,stroke:#0D47A1,color:#fff
    style DS_RESULT fill:#1565C0,stroke:#0D47A1,color:#fff
```

### Operators definitions

Operators must define the shape of their inputs and outputs so the execution layer can map properties between them.

#### Text To SQL Operator

```jsonc
{
    "id": "<uuid>",
    "labels": ["Text_To_SQL_Operator", "Operator"],
    "properties": {
        "version": "1.0.0",
        // Input of the Operator, what must be passed to it for it to funciton properly
        "inputs":[
            {
                "name": "nl",
                "type": "string",
                "required": true
            }
        ],
        // What the output of operator looks like
        "outputs":[
            {
                "name": "query",
                "type": "string",
                "required": true
            }
        ]
    }
}
```

#### SQL Execution Operator

```jsonc
{
    "id": "<uuid>",
    "labels": ["SQL_Query_Operator", "Operator"],
    "properties": {
        "version": "1.0.0",
        // Input of the Operator, what must be passed to it for it to funciton properly
        "inputs":[
            {
                "name": "sql",
                "type": "string",
                "required": true
            },
            {
                "name": "db",
                "type": "RelationalDatabase",
                "required": true
            }
        ],
        // What the output of operator looks like
        "outputs":[
            {
                "name": "result",
                "type": "RelationalDatabase",
                "required": true
            }
        ]
    }
}
```

### Nodes

There are four nodes exchanged in this AP.

#### nl_query (ResultType; string)

The natural language question entered by the user. Modelled as a scalar `ResultType` — it is a transient string value, not a persistent dataset.

```jsonc
{
    "id": "<uuid_rt_nl>",
    "labels": ["ResultType", "string"],
    "properties": {
        "name": "nl_query"
    }
}
```

#### sql_query (ResultType; string)

The SQL statement produced by `Text to SQL` and consumed by `SQL Execution`. Also a transient scalar `ResultType`.

```jsonc
{
    "id": "<uuid_rt_sql>",
    "labels": ["ResultType", "string"],
    "properties": {
        "name": "sql_query"
    }
}
```

#### DB (Data; RelationalDatabase)

The relational database against which the SQL query is executed. This is a persistent `Data` node — it exists independently of the AP.

```jsonc
{
    "id": "<uuid_ds_db>",
    "labels": ["RelationalDatabase", "Data"],
    "properties": {
        "name": "DB"
    }
}
```

#### Result (Data; RelationalDatabase)

The output of `SQL Execution` — the query result set, stored as a relational `Data` node.

```jsonc
{
    "id": "<uuid_ds_result>",
    "labels": ["RelationalDatabase", "Data"],
    "properties": {
        "name": "Result"
    }
}
```

### Mapping definitions 

Mappings define how data flows:
  - From ResultType node → Operator input (scalar: mapping required)
  - From Operator output → ResultType node (scalar: mapping required)
  - From/to Data node → Operator (no mapping: the whole node is passed)

In a real use case, this mapping could be done during AP design, for example via drag-and-drop, using parameter types to ensure compatibility.

All edges go **from the Operator** to the Data/ResultType node. The label (`input` / `output`) describes the role from the Operator's perspective.

#### input edge (Text to SQL → nl_query ResultType)

```jsonc
// Text to SQL reads the 'nl_query' value from the ResultType node into its 'nl' input
{
    "from": "<Text_To_SQL>",
    "to": "<nl_query_RT>",
    "labels": ["input"],
    "properties": {
        "mapping": {
            "to['nl']": "from['nl_query']"
        }
    }
}
```

#### output edge (Text to SQL → sql_query ResultType)

```jsonc
// Text to SQL writes its 'query' output into the sql_query node
{
    "from": "<Text_To_SQL>",
    "to": "<sql_query_RT>",
    "labels": ["output"],
    "properties": {
        "mapping": {
            "to['sql_query']": "from['query']"
        }
    }
}
```

#### input edge (SQL Execution → sql_query ResultType)

```jsonc
// SQL Execution reads the sql_query node into its 'sql' parameter
{
    "from": "<SQL_Execution>",
    "to": "<sql_query_RT>",
    "labels": ["input"],
    "properties": {
        "mapping": {
            "to['sql']": "from['sql_query']"
        }
    }
}
```

#### input edge (SQL Execution → DB)

```jsonc
// No mapping needed — the whole Data node is passed as 'db'
{
    "from": "<SQL_Execution>",
    "to": "<DS_DB>",
    "labels": ["input"]
}
```

#### output edge (SQL Execution → Result)

```jsonc
// No mapping needed — the whole Data node is written as output
{
    "from": "<SQL_Execution>",
    "to": "<DS_RESULT>",
    "labels": ["output"]
}
```

## Limitations && Open Questions
- Mapping definition could have special functions like `aggregate`, `collapse`. The set of supported transform functions is not yet defined. Candidates, semantics, and validation rules need to be specified.
- Error handling is not yet specified. What to do when an opertaor fails ?
    + Proposition: every Operator has an implicit reserved `_error` output (never declared, always present) of type `object` with fields `code: string` and `message: string` (`code` required). An AP could route `_error` to a dedicated error-handling Operator to enable conditional error branches without special graph constructs.

## Appendix

<details>

<summary> Full Example AP </summary>

```jsonc
{
  "nodes": [
    {
      "id": "<ap_template_uuid>",
      "labels": [
        "Analytical_Pattern"
      ],
      "properties": {
        "name": "NL Query Execution",
        "description": "AP Template to transform a natural language query into SQL and execute it"
      }
    },
    {
      "id": "<op1_uuid>",
      "labels": [
        "Operator",
        "Text_To_SQL"
      ],
      "properties": {
        "name": "Text to SQL",
        "version": "1.0.0",
        "step": 1,
        "inputs": [
          {
            "name": "nl",
            "type": "string",
            "required": true
          }
        ],
        "outputs": [
          {
            "name": "query",
            "type": "string",
            "required": true
          }
        ]
      }
    },
    {
      "id": "<op2_uuid>",
      "labels": [
        "Operator",
        "SQL_Execution"
      ],
      "properties": {
        "name": "SQL Execution",
        "version": "1.0.0",
        "step": 2,
        "inputs": [
          {
            "name": "sql",
            "type": "string",
            "required": true
          },
          {
            "name": "db",
            "type": "RelationalDatabase",
            "required": true
          }
        ],
        "outputs": [
          {
            "name": "result",
            "type": "RelationalDatabase",
            "required": true
          }
        ]
      }
    },
    {
      "id": "<rt_nl_uuid>",
      "labels": ["ResultType", "string"],
      "properties": {
        "name": "nl_query"
      }
    },
    {
      "id": "<rt_sql_uuid>",
      "labels": ["ResultType", "string"],
      "properties": {
        "name": "sql_query"
      }
    },
    {
      "id": "<ds_db_uuid>",
      "labels": ["RelationalDatabase", "Data"],
      "properties": {
        "name": "DB"
      }
    },
    {
      "id": "<ds_result_uuid>",
      "labels": ["RelationalDatabase", "Data"],
      "properties": {
        "name": "Result"
      }
    }
  ],
  "edges": [
    {
      "from": "<ap_template_uuid>",
      "to": "<op1_uuid>",
      "labels": ["consist_of"]
    },
    {
      "from": "<ap_template_uuid>",
      "to": "<op2_uuid>",
      "labels": ["consist_of"]
    },
    {
      "from": "<op2_uuid>",
      "to": "<op1_uuid>",
      "labels": ["follows"]
    },
    {
      "from": "<op1_uuid>",
      "to": "<rt_nl_uuid>",
      "labels": ["input"],
      "properties": {
        "mapping": {
          "to['nl']": "from['nl_query']"
        }
      }
    },
    {
      "from": "<op1_uuid>",
      "to": "<rt_sql_uuid>",
      "labels": ["output"],
      "properties": {
        "mapping": {
          "to['sql_query']": "from['query']"
        }
      }
    },
    {
      "from": "<op2_uuid>",
      "to": "<rt_sql_uuid>",
      "labels": ["input"],
      "properties": {
        "mapping": {
          "to['sql']": "from['sql_query']"
        }
      }
    },
    {
      "from": "<op2_uuid>",
      "to": "<ds_db_uuid>",
      "labels": ["input"]
    },
    {
      "from": "<op2_uuid>",
      "to": "<ds_result_uuid>",
      "labels": ["output"]
    }
  ]
}
```
</details>
