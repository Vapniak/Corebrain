def extract_schema(self, sample_limit: int = 5, collection_limit: Optional[int] = None, 
                      progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
  '''
  extract schema for MongoDB collections
  Args:
      sample_limit (int): Number of samples to extract from each collection.
      collection_limit (Optional[int]): Maximum number of collections to process.
      progress_callback (Optional[Callable]): Function to call for progress updates.
  '''

    try:
        collections = self.db.list_collection_names()
        if collection_limit is not None and collection_limit > 0:
            collections = collections[:collection_limit]
        total_collections = len(collections)
        for i, collection_name in enumerate(collections):
            if progress_callback:
                progress_callback(i, total_collections, f"Processing collection: {collection_name}")
            collection = self.db[collection_name]

            try:
                doc_count = collection.count_documents({})
                if doc_count <= 0:
                    schema["tables"][collection_name] = {
                            "fields": [],
                            "sample_data": [],
                            "count": 0,
                            "empty": True
                        }
                else:
                    sample_docs = list(collection.find().limit(sample_limit))
                    fields = {}
                    sample_data = []

                for doc in sample_docs:
                    self._extract_document_fields(doc, fields)
                    processed_doc = self._process_document_for_serialization(doc)
                    sample_data.append(processed_doc)

                formatted_fields = [{"name": field, "type": type_name} for field, type_name in fields.items()]

                schema["tables"][collection_name] = {
                        "fields": formatted_fields,
                        "sample_data": sample_data,
                        "count": doc_count,
                        }
            except Exception as e:
                print(f"Error processing collection {collection_name}: {e}")
                schema["tables"][collection_name] = {
                    "fields": [],
                    "error": str(e)
                }
                        # Convert the schema to a list of tables
        table_list = []
        for collection_name, collection_info in schema["tables"].items():
            table_data = {"name": collection_name}
            table_data.update(collection_info)
            table_list.append(table_data)
        schema["tables_list"] = table_list
        return schema
    except Exception as e:
        print(f"Error extracting schema: {e}")
        return {
            "type": "mongodb",
            "tables": {},
            "tabbles_list": []
        }