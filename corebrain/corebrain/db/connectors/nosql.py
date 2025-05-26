'''
NoSQL Database Connector
This module provides a basic structure for connecting to a NoSQL database.
It includes methods for connecting, disconnecting, and executing queries.
'''

import time
import json
import re

from typing import Dict, Any, List, Optional, Callable, Tuple

# Try'ies for imports DB's (for now only mongoDB)
try: 
    import pymongo
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    PYMONGO_IMPORTED = True
except ImportError:
    PYMONGO_IMPORTED = False
# When adding new DB type write a try to it from user

from corebrain.db.connector import DatabaseConnector
class NoSQLConnector(DatabaseConnector):
    '''
    NoSQL Database Connector
    This class provides a basic structure for connecting to a NoSQL database.
    It includes methods for connecting, disconnecting, and executing queries.
    '''
    def __init__(self, config: Dict[str, Any]):
      '''
      Initialize the NoSQL database connector.
      Args:
          engine (str): Name of the database.
          config (dict): Configuration dictionary containing connection parameters.
      '''
      super().__init__(config)
      self.engine = config.get("engine", "").lower()
      self.client = None
      self.db = None
      self.config = config
      self.connection_timeout = 30 # seconds

      match self.engine:
        case "mongodb":
            if not PYMONGO_IMPORTED:
                raise ImportError("pymongo is not installed. Please install it to use MongoDB connector.")
        case _:
            pass
      

    def connect(self) -> bool:
        '''
          Connection with NoSQL DB's
          Args:
            self.engine (str): Name of the database.
        '''

        match self.engine:
            case "mongodb":
                if not PYMONGO_IMPORTED:
                    raise ImportError("Pymongo is not installed. Please install it to use MongoDB connector.")
                try:
                    start_time = time.time()
                    # Check if connection string is provided
                    if "connection_string" in self.config:
                        connection_string = self.config["connection_string"]
                        if "connectTimeoutMS=" not in connection_string:
                            if "?" in connection_string:
                                connection_string += "&connectTimeoutMS=10000"
                            else:
                                connection_string += "?connectTimeoutMS=10000"
                        self.client = pymongo.MongoClient(connection_string)
                    else:

                        # Setup for MongoDB connection parameters

                        mongo_params = {
                            "host": self.config.get("host", "localhost"),
                            "port": int(self.config.get("port", 27017)),
                            "connection_timeoutMS": 10000,
                            "serverSelectionTimeoutMS": 10000,
                        }

                        # Required parameters

                        if self.config.get("user"):
                            mongo_params["username"] = self.config["user"]
                        if self.config.get("password"):
                            mongo_params["password"] = self.config["password"]

                        #Optional parameters

                        if self.config.get("authSource"):
                            mongo_params["authSource"] = self.config["authSource"]
                        if self.config.get("authMechanism"):
                            mongo_params["authMechanism"] = self.config["authMechanism"]

                        # Insert parameters for MongoDB
                        self.client = pymongo.MongoClient(**mongo_params)
                    # Ping test for DB connection
                    self.client.admin.command('ping')

                    db_name = self.config.get("database", "")

                    if not db_name:
                        db_names = self.client.list_database_names()
                        if not db_names:
                            raise ValueError("No database names found in the MongoDB server.")
                        system_dbs = ["admin", "local", "config"]
                        for name in db_names:
                            if name not in system_dbs:
                                db_name = name
                                break
                        if not db_name:
                            db_name = db_names[0]
                        print(f"Not specified database name. Using the first available database: {db_name}")
                    self.db = self.client[db_name]
                    return True
                except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                    if time.time() - start_time > self.connection_timeout:
                        print(f"Connection to MongoDB timed out after {self.connection_timeout} seconds.")
                        time.sleep(2) 
                        self.close()
                        return False
            # Add case when is needed new DB type
            case _ :
              raise ValueError(f"Unsupported NoSQL database: {self.engine}")
        pass

    def extract_schema(self, sample_limit: int = 5, collection_limit: Optional[int] = None, 
                      progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        '''
        Extract schema from the NoSQL database.
        Args:
            sample_limit (int): Number of samples to extract for schema inference.
            collection_limit (int): Maximum number of collections to process.
            progress_callback (Callable): Optional callback function for progress updates.
        Returns:
            Dict[str, Any]: Extracted schema information.
        '''

        match self.engine:
            case "mongodb":
                if not PYMONGO_IMPORTED:
                    raise ImportError("pymongo is not installed. Please install it to use MongoDB connector.")
                if not self.client and not self.connect():
                    return {
                        "type": "mongodb",
                        "tables": {},
                        "tables_list": []
                    }
                schema = {
                    "type": "mongodb",
                    "database": self.db.name,
                    "tables": {}, # In MongoDB, tables are collections
                }
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
            # Add case when is needed new DB type
            case _ :
              raise ValueError(f"Unsupported NoSQL database: {self.engine}")
    def _extract_document_fields(self, doc: Dict[str, Any], fields: Dict[str, str], 
                                prefix: str = "", max_depth: int = 3, current_depth: int = 0) -> None:
            '''
            
            Recursively extract fields from a document and determine their types.
            Args:
                doc (Dict[str, Any]): The document to extract fields from.
                fields (Dict[str, str]): Dictionary to store field names and types.
                prefix (str): Prefix for nested fields.
                max_depth (int): Maximum depth for nested fields.
                current_depth (int): Current depth in the recursion.
            '''
            match self.engine:
                case "mongodb":
                    if not PYMONGO_IMPORTED:
                        raise ImportError("pymongo is not installed. Please install it to use MongoDB connector.")
                    if current_depth >= max_depth:
                        return
                    for field, value in doc.items():
                        if field == "_id":
                            field_type = "ObjectId"
                        elif isinstance(value, dict):
                            if value and current_depth < max_depth - 1:
                                self._extract_document_fields(value, fields, f"{prefix}{field}.", max_depth, current_depth + 1)
                            else:
                                field_type = f"array<{type(value[0]).__name__}>"
                        else:
                            field_type = "array"
                    else:
                        field_type = type(value).__name__

                    field_key = f"{prefix}{field}"
                    if field_key not in fields:
                        fields[field_key] = field_type
                # Add case when is needed new DB type
                case _ :
                  raise ValueError(f"Unsupported NoSQL database: {self.engine}")
                
    def _process_document_for_serialization(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        '''
        Proccesig a document for serialization of a JSON.
        Args:
            doc (Dict[str, Any]): The document to process.
        Returns:
            Procesed document
        '''
        match self.engine:
            case "mongodb":
                if not PYMONGO_IMPORTED:
                    raise ImportError("pymongo is not installed. Please install it to use MongoDB connector.")
                processed_doc = {}
                for field, value in doc.items():
                    if field == "_id":
                        processed_doc[field] = self._process_document_for_serialization(value)
                    elif isinstance(value, list):
                        processed_items = []
                        for item in value:
                            if isinstance(item, dict):
                                processed_items.append(self._process_document_for_serialization(item))
                            elif hasattr(item, "__str__"):
                                processed_items.append(str(item))
                            else:
                                processed_items.append(item)
                        processed_doc[field] = processed_items
                    # Convert fetch to ISO
                    elif hasattr(value, 'isoformat'):
                        processed_doc[field] = value.isoformat()
                    # Convert data 
                    else:
                        processed_doc[field] = value
                return processed_doc
            # Add case when is needed new DB type
            case _ :
              raise ValueError(f"Unsupported NoSQL database: {self.engine}")
            
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Runs a NoSQL (or other) query with improved error handling
        
        Args:
            query: A NoSQL (or other) query in JSON format or query language
            
        Returns:
            List of resulting documents.
        """

        match self.engine:
            case "nosql":
                if not PYMONGO_IMPORTED:
                    raise ImportError("Pymongo is not installed. Please install it to use NoSQL connector.")
                
                if not self.client and not self.connect():
                    raise ConnectionError("Couldn't estabilish a connection with NoSQL")
                
                try:
                    # Determine whether the query is a JSON string or a query in another format
                    filter_dict, projection, collection_name, limit = self._parse_query(query)
                    
                    # Get the collection
                    if not collection_name:
                        raise ValueError("Name of the colletion not specified in the query")
                        
                    collection = self.db[collection_name]
                    
                    # Execute the query
                    if projection:
                        cursor = collection.find(filter_dict, projection).limit(limit or 100)
                    else:
                        cursor = collection.find(filter_dict).limit(limit or 100)
                    
                    # Convert the results to a serializable format
                    results = []
                    for doc in cursor:
                        processed_doc = self._process_document_for_serialization(doc)
                        results.append(processed_doc)
                    
                    return results
                    
                except Exception as e:
                    # Reconnect and retry the query
                    try:
                        self.close()
                        if self.connect():
                            print("Reconnecting and retrying the query...")
                            
                            # Retry the query
                            filter_dict, projection, collection_name, limit = self._parse_query(query)
                            collection = self.db[collection_name]
                            
                            if projection:
                                cursor = collection.find(filter_dict, projection).limit(limit or 100)
                            else:
                                cursor = collection.find(filter_dict).limit(limit or 100)
                            
                            results = []
                            for doc in cursor:
                                processed_doc = self._process_document_for_serialization(doc)
                                results.append(processed_doc)
                            
                            return results
                    except Exception as retry_error:
                        # If retrying fails, show the original error
                        raise Exception(f"Failed to execute the NoSQL query: {str(e)}")
                    
                    # This code is will be executed if the retry fails
                    raise Exception(f"Failed to execute the NoSQL query (after the reconnection): {str(e)}")
                
            # Add case when is needed new DB type
            case _ :
                raise ValueError(f"Unsupported NoSQL database: {self.self.engine}")