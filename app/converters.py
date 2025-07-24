def addEdge(edges, start, end, label):
    edges.append({
        "start": start,
        "end": end,
        "labels": [label],
        "properties": {}
    })

def Croissant2PGjson(data: dict) -> dict:
    edges = []
    nodes = []

    # Extract Collection metadata
    metadata = {
        "name": data.get("name"),
        "description": data.get("description"),
        "conformsTo": data.get("conformsTo"),
        "license": data.get("license"),
        "url": data.get("url"),
        "version": data.get("version"),
        "headline": data.get("headline"),
        "keywords": data.get("headline"),
        "fieldOfScience": data.get("fieldOfScience"),
        "inLanguage": data.get("inLanguage"),
        "country": data.get("country"),
        "datePublished": data.get("datePublished")
    }

    # print("metadata_df: ", metadata_df.to_json(orient="records", indent=1))
    nodes.append({"id": data.get("@id"), "labels": ["Collection"], "properties": metadata})
    collectionID = data.get("@id")

    # Extract distribution (fileSets and fileObjects)
    for dist in data.get("distribution", []):
        # properties = []
        id = dist.get("@id")
        encoding = dist.get("encodingFormat", "").lower()
        if encoding == "application/pdf" or encoding == "application/docx" or encoding == "application/pptx" or encoding == "application/x-ipynb+json":
            #print(f"PDF FileSet: {dist.get('name')} with ID: {dist.get('@id')}")
            properties = {
                "type": dist.get("@type"),
                "name": dist.get("name"),
                "description": dist.get("description"),
                "contentSize": dist.get("contentSize"),
                "contentUrl": dist.get("contentUrl"),
                "encodingFormat": dist.get("encodingFormat"),
                "includes": dist.get("includes")
            }
            nodes.append({"id": id, "labels": ["TextSet"], "properties": properties})
            addEdge(edges, id, collectionID, "belong")

        elif encoding == "image/jpg":
            #print(f"Image File: {dist.get('name')} with ID: {dist.get('@id')}")
            properties = {
                "type": dist.get("@type"),
                "name": dist.get("name"),
                "description": dist.get("description"),
                "contentSize": dist.get("contentSize"),
                "contentUrl": dist.get("contentUrl"),
                "encodingFormat": dist.get("encodingFormat"),
                "includes": dist.get("includes")
            }
            nodes.append({"id": id, "labels": ["ImageSet"], "properties": properties})
            addEdge(edges, id, collectionID, "belong")

        elif encoding == "text/csv":
            #print(f"CSV File: {dist.get('name')} with ID: {dist.get('@id')}")
            properties = {
                "type": dist.get("@type"),
                "name": dist.get("name"),
                "description": dist.get("description"),
                "contentSize": dist.get("contentSize"),
                "contentUrl": dist.get("contentUrl"),
                "encodingFormat": dist.get("encodingFormat"),
                "sha256": dist.get("sha256")
            }
            nodes.append({"id": id, "labels": ["CSV"], "properties": properties})
            addEdge(edges, id, collectionID, "belong")

        elif encoding == "text/sql":
            #print(f"SQL File: {dist.get('name')} with ID: {dist.get('@id')}")
            # table
            if "containedIn" in dist:
                properties = {
                    "type": dist.get("@type"),
                    "name": dist.get("name"),
                    "description": dist.get("description"),
                    "containedIn": dist.get("containedIn").get("@id"),
                    "encodingFormat": dist.get("encodingFormat"),
                }
                source = dist.get("containedIn", {}).get("@id", {})
                nodes.append({"id": id, "labels": ["Table"], "properties": properties})
                addEdge(edges, source, id, "contain")
            else:  # relational db
                properties = {
                    "type": dist.get("@type"),
                    "name": dist.get("name"),
                    "description": dist.get("description"),
                    "contentSize": dist.get("contentSize"),
                    "contentUrl": dist.get("contentUrl"),
                    "encodingFormat": dist.get("encodingFormat"),
                    "sha256": dist.get("sha256")
                }
                nodes.append({"id": id, "labels": ["RelationalSet"], "properties": properties})
                addEdge(edges, id, collectionID, "belong")

    # print(json.dumps(nodes, indent=2))
    # print(json.dumps(edge, indent=2))

    # Extract recordSet
    record_sets = []
    for record in data.get("recordSet", []):

        # record_sets.append({
        #     "record_id": record["@id"],
        #     "name": record["name"],
        #     "description": record["description"]
        # })
        # print("name:", record["name"])

        for field in record.get("field", []):
            # properties = []
            id = field.get("@id")
            #print(id)
            source_id = field.get("source", {}).get("fileObject", {}).get("@id")
            if not source_id:  # pdf file
                source_id = field.get("source", {}).get("fileSet", {}).get("@id")
                properties = {
                    "type": field.get("@type"),
                    "name": field.get("name"),
                    "file_size_bytes": field.get("file_size_bytes"),
                    "keywords": field.get("keywords"),
                    "summary": field.get("summary")
                }
                nodes.append({"id": id, "labels": ["PDF"], "properties": properties})
            else:  # coulumn of table or csv
                properties = {
                    "type": field.get("@type"),
                    "name": field.get("name"),
                    "description": field.get("description"),
                    "dataType": field.get("dataType"),
                    "column": field.get("source", {}).get("extract", {}).get("column"),
                    "sample": field.get("sample")
                }
                nodes.append({"id": id, "labels": ["Column"], "properties": properties})

            addEdge(edges, source_id, id, "contain")

    graph = {"nodes": nodes, "edges": edges}
    #print(json.dumps(graph, indent=2))

    return graph
