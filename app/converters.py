def addEdge(edges, start, end, label):
    edges.append({
        "from": start,
        "to": end,
        "labels": [label],
        "properties": {}
    })

def Croissant2PGjson(data: dict) -> dict:
    edges = []
    nodes = []

    dataset_id = data.get("@id")
    # Extract Dataset metadata
    metadata = {
        "type": data.get("@type"),
        "name": data.get("name"),
        "archivedAt": data.get("archivedAt"),
        "description": data.get("description"),
        "conformsTo": data.get("conformsTo"),
        "license": data.get("license"),
        "url": data.get("url"),
        "version": data.get("version"),
        "headline": data.get("headline"),
        "keywords": data.get("keywords"),
        "fieldOfScience": data.get("fieldOfScience"),
        "inLanguage": data.get("inLanguage"),
        "country": data.get("country"),
        "datePublished": data.get("datePublished"),
        "status": data.get("status")
    }
    # remove nulls
    metadata = {k: v for k, v in metadata.items() if v is not None}
    # Dataset node
    nodes.append({
        "id": dataset_id,
        "labels": [data.get("@type")],
        "properties": metadata
    })

    # --- Light profiling (distribution) ---
    light_graph = lightProfiling2PGjson(data)
    nodes.extend(light_graph.get("nodes", []))
    edges.extend(light_graph.get("edges", []))

    # --- Heavy profiling (recordSet) ---
    heavy_graph = heavyProfiling2PGjson(data)
    nodes.extend(heavy_graph.get("nodes", []))
    edges.extend(heavy_graph.get("edges", []))

    graph = {"nodes": nodes, "edges": edges}
    return graph

def lightProfiling2PGjson(data: dict) -> dict:
    nodes = []
    edges = []

    # Dataset root (ID only)
    dataset_id = data.get("@id")
    if not dataset_id:
        return {"nodes": [], "edges": []}

    # Distribution only
    for dist in data.get("distribution", []):
        dist_id = dist.get("@id")
        if not dist_id:
            continue

        encoding = dist.get("encodingFormat", "").lower()
        safeType = dist.get("@type", "")

        # Base properties for all
        properties = {
            "type": dist.get("@type", ""),
            "name": dist.get("name", ""),
            "description": dist.get("description", ""),
            "contentSize": dist.get("contentSize", ""),
            "contentUrl": dist.get("contentUrl", ""),
            "encodingFormat": dist.get("encodingFormat", ""),
        }

        # ---------- Text Set (PDF, DOCX, PPTX) ----------
        if encoding in {
            "application/pdf",
            "application/docx",
            "application/pptx",
            "application/x-ipynb+json"
        }:
            labels = ["TextSet", "Data", safeType]
            # includes exists ONLY for FileSet
            if "includes" in dist:
                properties["includes"] = dist.get("includes")

        # ---------- Image Set ----------
        elif encoding == "image/jpg":
            labels = ["ImageSet", "Data", safeType]
            if "includes" in dist:
                properties["includes"] = dist.get("includes")

        # ---------- CSV ----------
        elif encoding == "text/csv":
            labels = ["CSV", "Data", safeType]
            properties["sha256"] = dist.get("sha256", "")

        # ---------- SQL ----------
        elif encoding == "text/sql":
            if "containedIn" in dist:
                # Table
                labels = ["Table", "Data", safeType]
                source = dist.get("containedIn", {}).get("@id", {})
                addEdge(edges, dist_id, source, "containedIn")
            else:
                # Relational database
                labels = ["RelationalDatabase", "Data", safeType]

        # ---------- Fallback ----------
        else:
            labels = ["Distribution"]

        # remove nulls
        properties = {k: v for k, v in properties.items() if v is not None}
        # Add node
        nodes.append({
            "id": dist_id,
            "labels": labels,
            "properties": properties
        })

        # Dataset -> Distribution edge
        addEdge(edges, dataset_id, dist_id, "distribution")

    graph = {"nodes": nodes, "edges": edges}
    return graph


def heavyProfiling2PGjson(data: dict) -> dict:
    nodes = []
    edges = []

    # Dataset root (ID only)
    dataset_id = data.get("@id")
    if not dataset_id:
        return {"nodes": [], "edges": []}

    # Process recordSet
    for record in data.get("recordSet", []):
        # RecordSet Node
        record_id = record.get("@id", "")
        if not record_id:
            continue

        # cr:RecordSet
        record_type = record.get("@type", "")
        record_properties = {
            "type": record.get("@type", ""),
            "name": record.get("name", ""),
            "description": record.get("description", ""),
            "examples": record.get("examples", "")
        }
        # RecordSet node
        nodes.append({
            "id": record_id,
            "labels": [record_type],
            "properties": record_properties
        })

        for field in record.get("field", []):
            field_id = field.get("@id")
            if not field_id:
                continue

            # Determine the source of the field
            source_id = field.get("source", {}).get("fileObject", {}).get("@id")
            if not source_id:  # PDF file
                edge_type = "source/fileSet"
                source_id = field.get("source", {}).get("fileSet", {}).get("@id")
                safeType = field.get("@type", "")  # cr:Field
                props_raw = {
                    "type": field.get("@type", ""),
                    "name": field.get("name", ""),
                    "file_size_bytes": field.get("file_size_bytes", ""),
                    "keywords": field.get("keywords", ""),
                    "summary": field.get("summary", "")
                }
                properties = {k: v for k, v in props_raw.items() if v is not None}
                labels = ["PDF", safeType]
            else:  # Column from CSV or SQL table
                edge_type = "source/fileObject"
                safeType = field.get("@type", "")
                props_raw = {
                    "type": field.get("@type"),
                    "name": field.get("name"),
                    "description": field.get("description"),
                    "dataType": field.get("dataType"),
                    "column": field.get("source", {}).get("extract", {}).get("column"),
                    "sample": field.get("sample")
                }
                properties = {k: v for k, v in props_raw.items() if v is not None}
                labels = ["Column", safeType]

            # Add field node
            nodes.append({
                "id": field_id,
                "labels": labels,
                "properties": properties
            })

            # ---------- Statistics ONLY for columns ----------
            stats = field.get("statistics")
            if stats:
                stats_id = stats.get("@id")
                stats_type = stats.get("@type")

                # Keep only non-null properties excluding @id and @type
                properties_stats = {
                    k: v
                    for k, v in stats.items()
                    if v is not None and k not in ("@id", "@type")
                }

                # Only create the node if there is at least one non-null property
                if properties_stats:
                    nodes.append({
                        "id": stats_id,
                        "labels": ["Statistics", stats_type],
                        "properties": properties_stats
                    })
                    addEdge(edges, field_id, stats_id, "statistics")

            # Add edge from source -> field and record -> field
            if source_id:
                addEdge(edges, field_id, source_id, edge_type)
            if record_id:
                # recordSet --field--> Field
                addEdge(edges, record_id, field_id, "field")
                # Dataset --recordSet--> RecordSet
                addEdge(edges, dataset_id, record_id, "recordSet")

    graph = {"nodes": nodes, "edges": edges}
    return graph
