"""
Connector for MongoDB databases.
"""

import time
import json
import re

from typing import Dict, Any, List, Optional, Callable, Tuple

try:
    import pymongo
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False

from corebrain.db.connector import DatabaseConnector

class NoSQLConenctor(DatabaseConnector):
    """Optimized connector for MongoDB."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the MongoDB connector with the provided configuration.
        
        Args:
            config: Dictionary with the connection configuration
        """
        super().__init__(config)
        self.client = None
        self.db = None
        self.config = config
        self.connection_timeout = 30  # seconds
        
        if not PYMONGO_AVAILABLE:
            print("Advertencia: pymongo no está instalado. Instálalo con 'pip install pymongo'")
    
    def connect(self) -> bool:
        """
        Establishes a connection with optimized timeout.
        
        Returns:
            True if the connection was successful, False otherwise
        """
        if not PYMONGO_AVAILABLE:
            raise ImportError("pymongo no está instalado. Instálalo con 'pip install pymongo'")
        
        try:
            start_time = time.time()
            
            # Build the connection parameters
            if "connection_string" in self.config:
                connection_string = self.config["connection_string"]
                # Add timeout to the connection string if not present
                if "connectTimeoutMS=" not in connection_string:
                    if "?" in connection_string:
                        connection_string += "&connectTimeoutMS=10000"  # 10 seconds
                    else:
                        connection_string += "?connectTimeoutMS=10000"
                
                # Create MongoDB client with connection string
                self.client = pymongo.MongoClient(connection_string)
            else:
                # Parameter dictionary for MongoClient
                mongo_params = {
                    "host": self.config.get("host", "localhost"),
                    "port": int(self.config.get("port", 27017)),
                    "connectTimeoutMS": 10000,  # 10 seconds
                    "serverSelectionTimeoutMS": 10000
                }
                
                # Añadir credenciales solo si están presentes
                if self.config.get("user"):
                    mongo_params["username"] = self.config.get("user")
                if self.config.get("password"):
                    mongo_params["password"] = self.config.get("password")
                
                # Optionally add authentication options
                if self.config.get("auth_source"):
                    mongo_params["authSource"] = self.config.get("auth_source")
                if self.config.get("auth_mechanism"):
                    mongo_params["authMechanism"] = self.config.get("auth_mechanism")
                
                # Create MongoDB client with parameters
                self.client = pymongo.MongoClient(**mongo_params)
            
            # Verify that the connection works
            self.client.admin.command('ping')
            
            # Select the database
            db_name = self.config.get("database", "")
            if not db_name:
                # If no database is specified, list the available ones
                db_names = self.client.list_database_names()
                if not db_names:
                    raise ValueError("No se encontraron bases de datos disponibles")
                
                # Select the first non-system one
                system_dbs = ["admin", "local", "config"]
                for name in db_names:
                    if name not in system_dbs:
                        db_name = name
                        break
                
                # If we don't find any non-system ones, use the first one
                if not db_name:
                    db_name = db_names[0]
                
                print(f"No se especificó base de datos. Usando '{db_name}'")
            
            # Save the reference to the database
            self.db = self.client[db_name]
            return True
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            # If it is a timeout error, retry
            if time.time() - start_time < self.connection_timeout:
                print(f"Timeout al conectar a MongoDB: {str(e)}. Reintentando...")
                time.sleep(2)  # Wait before retrying
                return self.connect()
            else:
                print(f"Error de conexión a MongoDB después de {self.connection_timeout}s: {str(e)}")
                self.close()
                return False
        except Exception as e:
            print(f"Error al conectar a MongoDB: {str(e)}")
            self.close()
            return False
    
    def extract_schema(self, sample_limit: int = 5, collection_limit: Optional[int] = None, 
                      progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        Extracts the schema with limits and progress to improve performance.
        
        Args:
            sample_limit: Maximum number of sample documents per collection
            collection_limit: Limit of collections to process (None for all)
            progress_callback: Optional function to report progress
            
        Returns:
            Dictionary with the database schema
        """
        # Ensure we are connected
        if not self.client and not self.connect():
            return {"type": "mongodb", "tables": {}, "tables_list": []}
        
        # Initialize the schema
        schema = {
            "type": "mongodb",
            "database": self.db.name,
            "tables": {}  # In MongoDB, "tables" are collections
        }
        
        try:
            # Get the list of collections
            collections = self.db.list_collection_names()
            
            # Limitar colecciones si es necesario
            if collection_limit is not None and collection_limit > 0:
                collections = collections[:collection_limit]
            
            # Process each collection
            total_collections = len(collections)
            for i, collection_name in enumerate(collections):
                # Report progress if there is a callback
                if progress_callback:
                    progress_callback(i, total_collections, f"Procesando colección {collection_name}")
                
                collection = self.db[collection_name]
                
                try:
                    # Count documents
                    doc_count = collection.count_documents({})
                    
                    if doc_count > 0:
                        # Get sample documents
                        sample_docs = list(collection.find().limit(sample_limit))
                        
                        # Extract fields and their types
                        fields = {}
                        for doc in sample_docs:
                            self._extract_document_fields(doc, fields)
                        
                        # Convert to expected format
                        formatted_fields = [{"name": field, "type": type_name} for field, type_name in fields.items()]
                        
                        # Process documents for sample_data
                        sample_data = []
                        for doc in sample_docs:
                            processed_doc = self._process_document_for_serialization(doc)
                            sample_data.append(processed_doc)
                        
                        # Save to outline
                        schema["tables"][collection_name] = {
                            "fields": formatted_fields,
                            "sample_data": sample_data,
                            "count": doc_count
                        }
                    else:
                        # Empty collection
                        schema["tables"][collection_name] = {
                            "fields": [],
                            "sample_data": [],
                            "count": 0,
                            "empty": True
                        }
                        
                except Exception as e:
                    print(f"Error al procesar colección {collection_name}: {str(e)}")
                    schema["tables"][collection_name] = {
                        "fields": [],
                        "error": str(e)
                    }
            
            # Create the list of tables/collections for compatibility
            table_list = []
            for collection_name, collection_info in schema["tables"].items():
                table_data = {"name": collection_name}
                table_data.update(collection_info)
                table_list.append(table_data)
            
            # Also save the list of tables for compatibility
            schema["tables_list"] = table_list
            
            return schema
            
        except Exception as e:
            print(f"Error al extraer el esquema MongoDB: {str(e)}")
            return {"type": "mongodb", "tables": {}, "tables_list": []}
    
    def _extract_document_fields(self, doc: Dict[str, Any], fields: Dict[str, str], 
                                prefix: str = "", max_depth: int = 3, current_depth: int = 0) -> None:
        """
        Recursively extracts fields and types from a MongoDB document.
        
        Args:
            doc: Document to analyze
            fields: Dictionary to store fields and types
            prefix: Prefix for nested fields
            max_depth: Maximum depth for nested fields
            current_depth: Current depth
        """
        if current_depth >= max_depth:
            return
            
        for field, value in doc.items():
            # For _id and other special fields
            if field == "_id":
                field_type = "ObjectId"
            elif isinstance(value, dict):
                if current_depth < max_depth - 1:
                    # Recursion for nested fields
                    self._extract_document_fields(value, fields, 
                                                f"{prefix}{field}.", max_depth, current_depth + 1)
                field_type = "object"
            elif isinstance(value, list):
                if value and current_depth < max_depth - 1:
                    # If we have elements in the list, analyze the first one
                    if isinstance(value[0], dict):
                        self._extract_document_fields(value[0], fields, 
                                                    f"{prefix}{field}[].", max_depth, current_depth + 1)
                    else:
                        # For lists of primitive types
                        field_type = f"array<{type(value[0]).__name__}>"
                else:
                    field_type = "array"
            else:
                field_type = type(value).__name__
            
            # Save the current field type
            field_key = f"{prefix}{field}"
            if field_key not in fields:
                fields[field_key] = field_type
    
    def _process_document_for_serialization(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes a document to be JSON serializable.
        
        Args:
            doc: Document to process
            
        Returns:
            Processed document
        """
        processed_doc = {}
        for field, value in doc.items():
            # Convert ObjectId to string
            if field == "_id":
                processed_doc[field] = str(value)
            # Handling nested objects
            elif isinstance(value, dict):
                processed_doc[field] = self._process_document_for_serialization(value)
            # Handling arrays
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
            # Convert dates to ISO
            elif hasattr(value, 'isoformat'):
                processed_doc[field] = value.isoformat()
            # Other types of data
            else:
                processed_doc[field] = value
                
        return processed_doc
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Executes a MongoDB query with improved error handling.
        
        Args:
            query: MongoDB query in JSON format or query language
            
        Returns:
            List of resulting documents
        """
        if not self.client and not self.connect():
            raise ConnectionError("No se pudo establecer conexión con MongoDB")
        
        try:
            # Determine whether the query is a JSON string or a query in another format
            filter_dict, projection, collection_name, limit = self._parse_query(query)
            
            # Get the collection
            if not collection_name:
                raise ValueError("No se especificó el nombre de la colección en la consulta")
                
            collection = self.db[collection_name]
            
            # Run the query
            if projection:
                cursor = collection.find(filter_dict, projection).limit(limit or 100)
            else:
                cursor = collection.find(filter_dict).limit(limit or 100)
            
            # Convert results to serializable format
            results = []
            for doc in cursor:
                processed_doc = self._process_document_for_serialization(doc)
                results.append(processed_doc)
            
            return results
            
        except Exception as e:
            # Try to reconnect and try again once
            try:
                self.close()
                if self.connect():
                    print("Reconectando y reintentando consulta...")
                    
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
                # If the retry fails, propagate the original error
                raise Exception(f"Error al ejecutar consulta MongoDB: {str(e)}")
            
            # If we get here, there was an error in the retry.
            raise Exception(f"Error al ejecutar consulta MongoDB (después de reconexión): {str(e)}")
    
    def _parse_query(self, query: str) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], str, Optional[int]]:
        """
        Analyzes a query and extracts the necessary components.
        
        Args:
            query: Query in string format
            
        Returns:
            Tuple with (filter, projection, collection name, limit)
        """
        # Trying to parse as JSON
        try:
            query_dict = json.loads(query)
            
            # Extract components from the query
            filter_dict = query_dict.get("filter", {})
            projection = query_dict.get("projection")
            collection_name = query_dict.get("collection")
            limit = query_dict.get("limit")
            
            return filter_dict, projection, collection_name, limit
            
        except json.JSONDecodeError:
            # If not valid JSON, attempt to parse the alternative query format
            collection_match = re.search(r'from\s+([a-zA-Z0-9_]+)', query, re.IGNORECASE)
            collection_name = collection_match.group(1) if collection_match else None
            
            # Try to extract filters
            filter_match = re.search(r'where\s+(.+?)(?:limit|$)', query, re.IGNORECASE | re.DOTALL)
            filter_str = filter_match.group(1).strip() if filter_match else "{}"
            
            # Try to parse the filters as JSON
            try:
                filter_dict = json.loads(filter_str)
            except json.JSONDecodeError:
                # If parsing is not possible, use empty filter
                filter_dict = {}
            
            # Extract limit if it exists
            limit_match = re.search(r'limit\s+(\d+)', query, re.IGNORECASE)
            limit = int(limit_match.group(1)) if limit_match else None
            
            return filter_dict, None, collection_name, limit
    
    def count_documents(self, collection_name: str, filter_dict: Optional[Dict[str, Any]] = None) -> int:
        """
        Counts documents in a collection.
        
        Args:
            collection_name: Name of the collection
            filter_dict: Optional filter
            
        Returns:
            Number of documents
        """
        if not self.client and not self.connect():
            raise ConnectionError("No se pudo establecer conexión con MongoDB")
        
        try:
            collection = self.db[collection_name]
            return collection.count_documents(filter_dict or {})
        except Exception as e:
            print(f"Error al contar documentos: {str(e)}")
            return 0
    
    def list_collections(self) -> List[str]:
        """
        Returns a list of collections in the database.
        
        Returns:
            List of collection names
        """
        if not self.client and not self.connect():
            raise ConnectionError("No se pudo establecer conexión con MongoDB")
        
        try:
            return self.db.list_collection_names()
        except Exception as e:
            print(f"Error al listar colecciones: {str(e)}")
            return []
    
    def close(self) -> None:
        """Closes the MongoDB connection."""
        if self.client:
            try:
                self.client.close()
            except:
                pass
            finally:
                self.client = None
                self.db = None
    
    def __del__(self):
        """Destructor to ensure the connection is closed."""
        self.close()