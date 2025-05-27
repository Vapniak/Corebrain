"""
Components for query handling and analysis.
"""
import os
import json
import time
import re
import sqlite3
import pickle
import hashlib

from typing import Dict, Any, List, Optional, Tuple, Callable
from datetime import datetime
from pathlib import Path

from corebrain.cli.utils import print_colored

class QueryCache:
    """Multilevel cache system for queries."""
    
    def __init__(self, cache_dir: str = None, ttl: int = 86400, memory_limit: int = 100):
        """
        Initializes the cache system.
        
        Args:
            cache_dir: Directory for persistent cache
            ttl: Time-to-live of the cache in seconds (default: 24 hours)
            memory_limit: Memory cache entry limit
        """
        # In-memory cache (faster, but volatile)
        self.memory_cache = {}
        self.memory_timestamps = {}
        self.memory_limit = memory_limit
        self.memory_lru = []  # Least recently used tracking list
        
        # Persistent cache (slower, but permanent)
        self.ttl = ttl
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path.home() / ".corebrain_cache"
            
        # Create cache directory if it does not exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize SQLite database for metadata
        self.db_path = self.cache_dir / "cache_metadata.db"
        self._init_db()
        
        print_colored(f"Caché inicializado en {self.cache_dir}", "blue")
    
    def _init_db(self):
        """Initializes the SQLite database for cache metadata."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        # Create metadata table if it does not exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cache_metadata (
            query_hash TEXT PRIMARY KEY,
            query TEXT,
            config_id TEXT,
            created_at TIMESTAMP,
            last_accessed TIMESTAMP,
            hit_count INTEGER DEFAULT 1
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def _get_hash(self, query: str, config_id: str, collection_name: Optional[str] = None) -> str:
        """Generates a unique hash for the query."""
        # Normalize the query (remove extra spaces, convert to lowercase)
        normalized_query = re.sub(r'\s+', ' ', query.lower().strip())
        
        # Create composite string for the hash
        hash_input = f"{normalized_query}|{config_id}"
        if collection_name:
            hash_input += f"|{collection_name}"
            
        # Generate the hash
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def _get_cache_path(self, query_hash: str) -> Path:
        """Gets the cache file path for a given hash."""
        # Use the first characters of the hash to create subdirectories
        # This prevents having too many files in a single directory
        subdir = query_hash[:2]
        cache_subdir = self.cache_dir / subdir
        cache_subdir.mkdir(exist_ok=True)
        
        return cache_subdir / f"{query_hash}.cache"
    
    def _update_metadata(self, query_hash: str, query: str, config_id: str):
        """Updates the metadata in the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        # Check if the hash already exists
        cursor.execute("SELECT hit_count FROM cache_metadata WHERE query_hash = ?", (query_hash,))
        result = cursor.fetchone()
        
        if result:
            # Update existing entry
            hit_count = result[0] + 1
            cursor.execute('''
            UPDATE cache_metadata 
            SET last_accessed = ?, hit_count = ? 
            WHERE query_hash = ?
            ''', (now, hit_count, query_hash))
        else:
            # Insert new entry
            cursor.execute('''
            INSERT INTO cache_metadata (query_hash, query, config_id, created_at, last_accessed, hit_count)
            VALUES (?, ?, ?, ?, ?, 1)
            ''', (query_hash, query, config_id, now, now))
        
        conn.commit()
        conn.close()
    
    def _update_memory_lru(self, query_hash: str):
        """Updates the LRU (Least Recently Used) list for the in-memory cache."""
        if query_hash in self.memory_lru:
            # Move to end (most recently used)
            self.memory_lru.remove(query_hash)
        
        self.memory_lru.append(query_hash)
        
        # If we exceed the limit, delete the least recently used item
        if len(self.memory_lru) > self.memory_limit:
            oldest_hash = self.memory_lru.pop(0)
            if oldest_hash in self.memory_cache:
                del self.memory_cache[oldest_hash]
                del self.memory_timestamps[oldest_hash]
    
    def get(self, query: str, config_id: str, collection_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieves a cached result if it exists and has not expired.
        
        Args:
            query: Natural language query
            config_id: Database configuration ID
            collection_name: Name of the collection/table (optional)
            
        Returns:
            Cached result or None if it does not exist or has expired
        """
        query_hash = self._get_hash(query, config_id, collection_name)
        
        # 1. Check in-memory cache (faster)
        if query_hash in self.memory_cache:
            timestamp = self.memory_timestamps[query_hash]
            if (time.time() - timestamp) < self.ttl:
                self._update_memory_lru(query_hash)
                self._update_metadata(query_hash, query, config_id)
                print_colored(f"Cache hit (memory): {query[:30]}...", "green")
                return self.memory_cache[query_hash]
            else:
                # Expired in memory
                del self.memory_cache[query_hash]
                del self.memory_timestamps[query_hash]
                if query_hash in self.memory_lru:
                    self.memory_lru.remove(query_hash)
        
        # 2. Check disk cache
        cache_path = self._get_cache_path(query_hash)
        if cache_path.exists():
            # Check file age
            file_age = time.time() - cache_path.stat().st_mtime
            if file_age < self.ttl:
                try:
                    with open(cache_path, 'rb') as f:
                        result = pickle.load(f)
                    
                    # Also save in memory cache
                    self.memory_cache[query_hash] = result
                    self.memory_timestamps[query_hash] = time.time()
                    self._update_memory_lru(query_hash)
                    self._update_metadata(query_hash, query, config_id)
                    
                    print_colored(f"Cache hit (disk): {query[:30]}...", "green")
                    return result
                except Exception as e:
                    print_colored(f"Error al cargar caché: {str(e)}", "red")
                    # If there is an error when uploading, delete the corrupted file
                    cache_path.unlink(missing_ok=True)
            else:
                # Expired file, delete it
                cache_path.unlink(missing_ok=True)
        
        return None
    
    def set(self, query: str, config_id: str, result: Dict[str, Any], collection_name: Optional[str] = None):
        """
        Saves a result in the cache.
        
        Args:
            query: Natural language query
            config_id: Configuration ID
            result: Result to cache
            collection_name: Name of the collection/table (optional)
        """
        query_hash = self._get_hash(query, config_id, collection_name)
        
        # 1. Save to memory cache
        self.memory_cache[query_hash] = result
        self.memory_timestamps[query_hash] = time.time()
        self._update_memory_lru(query_hash)
        
        # 2. Save to persistent cache
        try:
            cache_path = self._get_cache_path(query_hash)
            with open(cache_path, 'wb') as f:
                pickle.dump(result, f)
            
            # 3. Update metadata
            self._update_metadata(query_hash, query, config_id)
            
            print_colored(f"Cached: {query[:30]}...", "green")
        except Exception as e:
            print_colored(f"Error al guardar en caché: {str(e)}", "red")
    
    def clear(self, older_than: int = None):
        """
        Clears the cache.
        
        Args:
            older_than: Only clear entries older than this number of seconds
        """
        # Clear cache in memory
        if older_than:
            current_time = time.time()
            keys_to_remove = [
                k for k, timestamp in self.memory_timestamps.items()
                if (current_time - timestamp) > older_than
            ]
            
            for k in keys_to_remove:
                if k in self.memory_cache:
                    del self.memory_cache[k]
                if k in self.memory_timestamps:
                    del self.memory_timestamps[k]
                if k in self.memory_lru:
                    self.memory_lru.remove(k)
        else:
            self.memory_cache.clear()
            self.memory_timestamps.clear()
            self.memory_lru.clear()
        
        # Clear disk cache
        if older_than:
            cutoff_time = time.time() - older_than
            
            # Using the database to find old files
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            # Convert cutoff_time to ISO format
            cutoff_datetime = datetime.fromtimestamp(cutoff_time).isoformat()
            
            cursor.execute(
                "SELECT query_hash FROM cache_metadata WHERE last_accessed < ?",
                (cutoff_datetime,)
            )
            
            old_hashes = [row[0] for row in cursor.fetchall()]
            
            # Delete old files
            for query_hash in old_hashes:
                cache_path = self._get_cache_path(query_hash)
                if cache_path.exists():
                    cache_path.unlink()
                
                # Delete from the database
                cursor.execute(
                    "DELETE FROM cache_metadata WHERE query_hash = ?",
                    (query_hash,)
                )
            
            conn.commit()
            conn.close()
        else:
            # Delete all cache files
            for subdir in self.cache_dir.iterdir():
                if subdir.is_dir():
                    for cache_file in subdir.glob("*.cache"):
                        cache_file.unlink()
            
            # Restart the database
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            cursor.execute("DELETE FROM cache_metadata")
            conn.commit()
            conn.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """Gets cache statistics."""
        # Count files on disk
        disk_count = 0
        for subdir in self.cache_dir.iterdir():
            if subdir.is_dir():
                disk_count += len(list(subdir.glob("*.cache")))
        
        # Obtaining database statistics
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        # Total entries
        cursor.execute("SELECT COUNT(*) FROM cache_metadata")
        total_entries = cursor.fetchone()[0]
        
        # Most frequent queries
        cursor.execute(
            "SELECT query, hit_count FROM cache_metadata ORDER BY hit_count DESC LIMIT 5"
        )
        top_queries = cursor.fetchall()
        
        # Average age
        cursor.execute(
            "SELECT AVG(strftime('%s', 'now') - strftime('%s', created_at)) FROM cache_metadata"
        )
        avg_age = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "memory_cache_size": len(self.memory_cache),
            "disk_cache_size": disk_count,
            "total_entries": total_entries,
            "top_queries": top_queries,
            "average_age_seconds": avg_age,
            "cache_directory": str(self.cache_dir)
        }

class QueryTemplate:
    """Predefined query template for common patterns."""
    
    def __init__(self, pattern: str, description: str, 
                 sql_template: Optional[str] = None,
                 generator_func: Optional[Callable] = None,
                 db_type: str = "sql",
                 applicable_tables: Optional[List[str]] = None):
        """
        Initializes a query template.
        
        Args:
            pattern: Natural language pattern that matches this template
            description: Description of the template
            sql_template: SQL template with placeholders for parameters
            generator_func: Alternative function to generate the query
            db_type: Database type (sql, mongodb)
            applicable_tables: List of tables to which this template applies
        """
        self.pattern = pattern
        self.description = description
        self.sql_template = sql_template
        self.generator_func = generator_func
        self.db_type = db_type
        self.applicable_tables = applicable_tables or []
        
        # Compile regular expression for the pattern
        self.regex = self._compile_pattern(pattern)
    
    def _compile_pattern(self, pattern: str) -> re.Pattern:
        """Compiles the pattern into a regular expression."""
        # Replace special markers with capture groups
        regex_pattern = pattern
        
        # {table} becomes a capturing group for the table name
        regex_pattern = regex_pattern.replace("{table}", r"(\w+)")
        
        # {field} becomes a capturing group for the field name
        regex_pattern = regex_pattern.replace("{field}", r"(\w+)")
        
        # {value} becomes a capturing group for a value
        regex_pattern = regex_pattern.replace("{value}", r"([^,.\s]+)")
        
        # {number} becomes a capture group for a number
        regex_pattern = regex_pattern.replace("{number}", r"(\d+)")
        
        # Match the entire pattern
        regex_pattern = f"^{regex_pattern}$"
        
        return re.compile(regex_pattern, re.IGNORECASE)
    
    def matches(self, query: str) -> Tuple[bool, List[str]]:
        """
        Checks if a query matches this template.
        
        Args:
            query: Query to check
            
        Returns:
            Tuple of (match, [captured parameters])
        """
        match = self.regex.match(query)
        if match:
            return True, list(match.groups())
        return False, []
    
    def generate_query(self, params: List[str], db_schema: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Generates a query from the captured parameters.
        
        Args:
            params: Captured parameters from the pattern
            db_schema: Database schema
            
        Returns:
            Generated query or None if it cannot be generated
        """
        if self.generator_func:
            # Use custom function
            return self.generator_func(params, db_schema)
        
        if not self.sql_template:
            return None
            
        # Try to apply the SQL template with the parameters
        try:
            sql_query = self.sql_template
            
            # Replace parameters in the template
            for i, param in enumerate(params):
                placeholder = f"${i+1}"
                sql_query = sql_query.replace(placeholder, param)
            
            # Check if there are any unreplaced parameters
            if "$" in sql_query:
                return None
                
            return {"sql": sql_query}
        except Exception:
            return None

class QueryAnalyzer:
    """Analyzes query patterns to suggest optimizations."""
    
    def __init__(self, query_log_path: str = None, template_path: str = None):
        """
        Initializes the query analyzer.
        
        Args:
            query_log_path: Path to the query log file
            template_path: Path to the template file
        """
        self.query_log_path = query_log_path or os.path.join(
            Path.home(), ".corebrain_cache", "query_log.db"
        )
        
        self.template_path = template_path or os.path.join(
            Path.home(), ".corebrain_cache", "templates.json"
        )
        
        # Initialize database
        self._init_db()
        
        # Predefined templates for common queries
        self.templates = self._load_default_templates()
        
        # Upload custom templates
        self._load_custom_templates()
        
        # Common templates for identifying patterns
        self.common_patterns = [
            r"muestra\s+(?:todos\s+)?los\s+(\w+)",
            r"lista\s+(?:de\s+)?(?:todos\s+)?los\s+(\w+)",
            r"busca\s+(\w+)\s+donde",
            r"cu[aá]ntos\s+(\w+)\s+hay",
            r"total\s+de\s+(\w+)"
        ]
    
    def _init_db(self):
        """Initializes the database for query logging."""
        # Ensure that the directory exists
        os.makedirs(os.path.dirname(self.query_log_path), exist_ok=True)
        
        conn = sqlite3.connect(self.query_log_path)
        cursor = conn.cursor()
        
        # Create log table if it does not exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS query_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT,
            config_id TEXT,
            collection_name TEXT,
            timestamp TIMESTAMP,
            execution_time REAL,
            cost REAL,
            result_count INTEGER,
            pattern TEXT
        )
        ''')
        
        # Create table of detected patterns
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS query_patterns (
            pattern TEXT PRIMARY KEY,
            count INTEGER,
            avg_execution_time REAL,
            avg_cost REAL,
            last_updated TIMESTAMP
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def _load_default_templates(self) -> List[QueryTemplate]:
        """Carga las plantillas predefinidas para consultas comunes."""
        templates = []
        
        # List all records in a table
        templates.append(
            QueryTemplate(
                pattern="muestra todos los {table}",
                description="Listar todos los registros de una tabla",
                sql_template="SELECT * FROM $1 LIMIT 100",
                db_type="sql"
            )
        )
        
        # Count records
        templates.append(
            QueryTemplate(
                pattern="cuántos {table} hay",
                description="Contar registros en una tabla",
                sql_template="SELECT COUNT(*) FROM $1",
                db_type="sql"
            )
        )
        
        # Search by ID
        templates.append(
            QueryTemplate(
                pattern="busca el {table} con id {value}",
                description="Buscar registro por ID",
                sql_template="SELECT * FROM $1 WHERE id = $2",
                db_type="sql"
            )
        )
        
         # List sorted
        templates.append(
            QueryTemplate(
                pattern="lista los {table} ordenados por {field}",
                description="Listar registros ordenados por campo",
                sql_template="SELECT * FROM $1 ORDER BY $2 LIMIT 100",
                db_type="sql"
            )
        )
        
        # Search by email
        templates.append(
            QueryTemplate(
                pattern="busca el usuario con email {value}",
                description="Buscar usuario por email",
                sql_template="SELECT * FROM users WHERE email = '$2'",
                db_type="sql"
            )
        )
        
        # Count by field
        templates.append(
            QueryTemplate(
                pattern="cuántos {table} hay por {field}",
                description="Contar registros agrupados por campo",
                sql_template="SELECT $2, COUNT(*) FROM $1 GROUP BY $2",
                db_type="sql"
            )
        )
        
        # Count active users
        templates.append(
            QueryTemplate(
                pattern="cuántos usuarios activos hay",
                description="Contar usuarios activos",
                sql_template="SELECT COUNT(*) FROM users WHERE is_active = TRUE",
                db_type="sql",
                applicable_tables=["users"]
            )
        )
        
        # List users by registration date
        templates.append(
            QueryTemplate(
                pattern="usuarios registrados en los últimos {number} días",
                description="Listar usuarios recientes",
                sql_template="""
                SELECT * FROM users 
                WHERE created_at >= datetime('now', '-$2 days')
                ORDER BY created_at DESC
                LIMIT 100
                """,
                db_type="sql",
                applicable_tables=["users"]
            )
        )
        
        # Search companies
        templates.append(
            QueryTemplate(
                pattern="usuarios que tienen empresa",
                description="Buscar usuarios con empresa asignada",
                sql_template="""
                SELECT u.* FROM users u
                INNER JOIN businesses b ON u.id = b.owner_id
                WHERE u.is_business = TRUE
                LIMIT 100
                """,
                db_type="sql",
                applicable_tables=["users", "businesses"]
            )
        )
        
        # Find businesses
        templates.append(
            QueryTemplate(
                pattern="busca negocios en {value}",
                description="Buscar negocios por ubicación",
                sql_template="""
                SELECT * FROM businesses 
                WHERE address_city LIKE '%$2%' OR address_province LIKE '%$2%'
                LIMIT 100
                """,
                db_type="sql",
                applicable_tables=["businesses"]
            )
        )
        
        # MongoDB: List documents
        templates.append(
            QueryTemplate(
                pattern="muestra todos los documentos de {table}",
                description="Listar documentos en una colección",
                db_type="mongodb",
                generator_func=lambda params, schema: {
                    "collection": params[0],
                    "operation": "find",
                    "query": {},
                    "limit": 100
                }
            )
        )
        
        return templates
    
    def _load_custom_templates(self):
        """Loads custom templates from the file."""
        if not os.path.exists(self.template_path):
            return
            
        try:
            with open(self.template_path, 'r') as f:
                custom_templates = json.load(f)
                
            for template_data in custom_templates:
                # Create template from JSON data
                template = QueryTemplate(
                    pattern=template_data.get("pattern", ""),
                    description=template_data.get("description", ""),
                    sql_template=template_data.get("sql_template"),
                    db_type=template_data.get("db_type", "sql"),
                    applicable_tables=template_data.get("applicable_tables", [])
                )
                
                self.templates.append(template)
                
        except Exception as e:
            print_colored(f"Error al cargar plantillas personalizadas: {str(e)}", "red")
    
    def save_custom_template(self, template: QueryTemplate) -> bool:
        """
        Saves a custom template.
        
        Args:
            template: Template to save
            
        Returns:
            True if saved successfully
        """
        # Load existing templates
        custom_templates = []
        if os.path.exists(self.template_path):
            try:
                with open(self.template_path, 'r') as f:
                    custom_templates = json.load(f)
            except:
                custom_templates = []
        
        # Convert template to dictionary
        template_data = {
            "pattern": template.pattern,
            "description": template.description,
            "sql_template": template.sql_template,
            "db_type": template.db_type,
            "applicable_tables": template.applicable_tables
        }
        
        # Check if a template with the same pattern already exists
        for i, existing in enumerate(custom_templates):
            if existing.get("pattern") == template.pattern:
                # Update existing
                custom_templates[i] = template_data
                break
        else:
            # Add new
            custom_templates.append(template_data)
        
        # Save templates
        try:
            with open(self.template_path, 'w') as f:
                json.dump(custom_templates, f, indent=2)
            
            # Update template list
            self.templates.append(template)
            
            return True
        except Exception as e:
            print_colored(f"Error al guardar plantilla personalizada: {str(e)}", "red")
            return False
    
    def find_matching_template(self, query: str, db_schema: Dict[str, Any]) -> Optional[Tuple[QueryTemplate, List[str]]]:
        """
        Searches for a template that matches the query.
        
        Args:
            query: Natural language query
            db_schema: Database schema
            
        Returns:
            Tuple of (template, parameters) or None if no match is found
        """
        for template in self.templates:
            matches, params = template.matches(query)
            if matches:
                # Check if the template is applicable to existing tables
                if template.applicable_tables:
                    available_tables = set(db_schema.get("tables", {}).keys())
                    if not any(table in available_tables for table in template.applicable_tables):
                        continue
                
                return template, params
                
        return None
    
    def log_query(self, query: str, config_id: str, collection_name: str = None, 
                 execution_time: float = 0, cost: float = 0.09, result_count: int = 0):
        """
        Registers a query for analysis.
        
        Args:
            query: Natural language query
            config_id: Configuration ID
            collection_name: Name of the collection/table
            execution_time: Execution time in seconds
            cost: Estimated cost of the query
            result_count: Number of results obtained
        """
        # Detect pattern
        pattern = self._detect_pattern(query)
        
        # Register in the database
        conn = sqlite3.connect(self.query_log_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO query_log (query, config_id, collection_name, timestamp, execution_time, cost, result_count, pattern)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            query, config_id, collection_name, datetime.now().isoformat(),
            execution_time, cost, result_count, pattern
        ))
        
        # Update pattern statistics
        if pattern:
            cursor.execute(
                "SELECT count, avg_execution_time, avg_cost FROM query_patterns WHERE pattern = ?",
                (pattern,)
            )
            result = cursor.fetchone()
            
            if result:
                # Update existing pattern
                count, avg_exec_time, avg_cost = result
                new_count = count + 1
                new_avg_exec_time = (avg_exec_time * count + execution_time) / new_count
                new_avg_cost = (avg_cost * count + cost) / new_count
                
                cursor.execute('''
                UPDATE query_patterns
                SET count = ?, avg_execution_time = ?, avg_cost = ?, last_updated = ?
                WHERE pattern = ?
                ''', (new_count, new_avg_exec_time, new_avg_cost, datetime.now().isoformat(), pattern))
            else:
                # Insert new pattern
                cursor.execute('''
                INSERT INTO query_patterns (pattern, count, avg_execution_time, avg_cost, last_updated)
                VALUES (?, 1, ?, ?, ?)
                ''', (pattern, execution_time, cost, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    
    def _detect_pattern(self, query: str) -> Optional[str]:
        """
        Detects a pattern in the query.
        
        Args:
            query: Query to analyze
            
        Returns:
            Detected pattern or None
        """
        normalized_query = query.lower()
        
        # Check predefined patterns
        for pattern in self.common_patterns:
            match = re.search(pattern, normalized_query)
            if match:
                # Return the pattern with wildcards
                entity = match.group(1)
                return pattern.replace(r'(\w+)', f"{entity}")
        
        # If no predefined pattern is detected, try to generalize
        words = normalized_query.split()
        if len(words) < 3:
            return None
            
        # Try to generalize simple queries
        if "mostrar" in words or "muestra" in words or "listar" in words or "lista" in words:
            for i, word in enumerate(words):
                if word in ["de", "los", "las", "todos", "todas"]:
                    if i+1 < len(words):
                        return f"lista_de_{words[i+1]}"
        
        return None
    
    def get_common_patterns(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Retrieves the most common query patterns.
        
        Args:
            limit: Maximum number of patterns to return
            
        Returns:
            List of the most common patterns
        """
        conn = sqlite3.connect(self.query_log_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT pattern, count, avg_execution_time, avg_cost
        FROM query_patterns
        ORDER BY count DESC
        LIMIT ?
        ''', (limit,))
        
        patterns = []
        for row in cursor.fetchall():
            pattern, count, avg_time, avg_cost = row
            patterns.append({
                "pattern": pattern,
                "count": count,
                "avg_execution_time": avg_time,
                "avg_cost": avg_cost,
                "estimated_monthly_cost": round(avg_cost * count * 30 / 7, 2)  # Estimación mensual
            })
        
        conn.close()
        return patterns
    
    def suggest_new_template(self, query: str, sql_query: str) -> Optional[QueryTemplate]:
        """
        Suggests a new template based on a successful query.
        
        Args:
            query: Natural language query
            sql_query: Generated SQL query
            
        Returns:
            Suggested template or None
        """
        # Detect pattern
        pattern = self._detect_pattern(query)
        if not pattern:
            return None
            
        # Generalize the SQL query
        generalized_sql = sql_query
        
        # Replace specific values ​​with markers
        # This is a simplification; ideally, you would use an SQL parser
        tokens = query.lower().split()
        
        # Identify possible values ​​to parameterize
        for i, token in enumerate(tokens):
            if token.isdigit():
                # Replace numbers
                generalized_sql = re.sub(r'\b' + re.escape(token) + r'\b', '$1', generalized_sql)
                pattern = pattern.replace(token, "{number}")
            elif '@' in token and '.' in token:
                # Replace emails
                generalized_sql = re.sub(r'\b' + re.escape(token) + r'\b', '$1', generalized_sql)
                pattern = pattern.replace(token, "{value}")
            elif token.startswith('"') or token.startswith("'"):
                # Reemplazar strings
                value = token.strip('"\'')
                if len(value) > 2:  # Avoid replacing very short strings
                    generalized_sql = re.sub(r'[\'"]' + re.escape(value) + r'[\'"]', "'$1'", generalized_sql)
                    pattern = pattern.replace(token, "{value}")
        
        # Create template
        return QueryTemplate(
            pattern=pattern,
            description=f"Plantilla generada automáticamente para: {pattern}",
            sql_template=generalized_sql,
            db_type="sql"
        )
    
    def get_optimization_suggestions(self) -> List[Dict[str, Any]]:
        """
        Generates suggestions to optimize queries.
        
        Returns:
            List of optimization suggestions
        """
        suggestions = []
        
        # Calculate general statistics
        conn = sqlite3.connect(self.query_log_path)
        cursor = conn.cursor()
        
        # Total consultations and cost in the last 30 days
        cursor.execute('''
        SELECT COUNT(*) as query_count, SUM(cost) as total_cost
        FROM query_log
        WHERE timestamp > datetime('now', '-30 day')
        ''')
        
        row = cursor.fetchone()
        if row:
            query_count, total_cost = row
            
            if query_count and query_count > 100:
                # If there are many queries in total, suggest volume plan
                suggestions.append({
                    "type": "volume_plan",
                    "query_count": query_count,
                    "total_cost": round(total_cost, 2) if total_cost else 0,
                    "suggestion": f"Considerar negociar un plan por volumen. Actualmente ~{query_count} consultas/mes."
                })
                
                # Suggest adjusting cache TTL based on frequency
                avg_queries_per_day = query_count / 30
                suggested_ttl = max(3600, min(86400 * 3, 86400 * (100 / avg_queries_per_day)))
                
                suggestions.append({
                    "type": "cache_adjustment",
                    "current_rate": f"{avg_queries_per_day:.1f} consultas/día",
                    "suggestion": f"Ajustar TTL del caché a {suggested_ttl/3600:.1f} horas basado en su patrón de uso"
                })
        
        # Get common patterns
        common_patterns = self.get_common_patterns(10)
        
        for pattern in common_patterns:
            if pattern["count"] >= 5:
                # If a pattern repeats a lot, suggest precompilation
                suggestions.append({
                    "type": "precompile",
                    "pattern": pattern["pattern"],
                    "count": pattern["count"],
                    "estimated_savings": round(pattern["avg_cost"] * pattern["count"] * 0.9, 2),  # 90% savings
                    "suggestion": f"Crear una plantilla SQL para consultas del tipo '{pattern['pattern']}'"
                })
            
            # If a pattern is expensive but rare
            if pattern["avg_cost"] > 0.1 and pattern["count"] < 5:
                suggestions.append({
                    "type": "analyze",
                    "pattern": pattern["pattern"],
                    "avg_cost": pattern["avg_cost"],
                    "suggestion": f"Revisar manualmente consultas del tipo '{pattern['pattern']}' para optimizar"
                })
        
        # Find periods with high load to adjust parameters
        cursor.execute('''
        SELECT strftime('%Y-%m-%d %H', timestamp) as hour, COUNT(*) as count, SUM(cost) as total_cost
        FROM query_log
        WHERE timestamp > datetime('now', '-7 day')
        GROUP BY hour
        ORDER BY count DESC
        LIMIT 5
        ''')
        
        for row in cursor.fetchall():
            hour, count, total_cost = row
            if count > 20:  # If there are more than 20 queries in an hour
                suggestions.append({
                    "type": "load_balancing",
                    "hour": hour,
                    "query_count": count,
                    "total_cost": round(total_cost, 2),
                    "suggestion": f"Alta carga de consultas detectada el {hour} ({count} consultas). Considerar técnicas de agrupación."
                })
        
        # Find redundant queries (same query in a short time)
        cursor.execute('''
        SELECT query, COUNT(*) as count
        FROM query_log
        WHERE timestamp > datetime('now', '-1 day')
        GROUP BY query
        HAVING COUNT(*) > 3
        ORDER BY COUNT(*) DESC
        LIMIT 5
        ''')
        
        for row in cursor.fetchall():
            query, count = row
            suggestions.append({
                "type": "redundant",
                "query": query,
                "count": count,
                "estimated_savings": round(0.09 * (count - 1), 2),  # Ahorro por no repetir
                "suggestion": f"Implementar caché para la consulta '{query[:50]}...' que se repitió {count} veces"
            })
        
        conn.close()
        return suggestions


    