"""
Utilities for testing and validating components.
"""
import json
import random
from typing import Dict, Any, Optional

from corebrain.cli.utils import print_colored
from corebrain.cli.common import DEFAULT_API_URL
from corebrain.network.client import http_session

def generate_test_question_from_schema(schema: Dict[str, Any]) -> str:
    """
    Generates a test question based on the database schema.
    
    Args:
        schema: Database schema
        
    Returns:
        Generated test question
    """
    if not schema or not schema.get("tables"):
        return "¿Cuáles son las tablas disponibles?"
    
    tables = schema["tables"]
    
    if not tables:
        return "¿Cuáles son las tablas disponibles?"
    
    # Select a random table
    table = random.choice(tables)
    table_name = table["name"]
    
    # Determine the type of question
    question_types = [
        f"¿Cuántos registros hay en la tabla {table_name}?",
        f"Muestra los primeros 5 registros de {table_name}",
        f"¿Cuáles son los campos de la tabla {table_name}?",
    ]
    
    # Getting columns by structure (SQL vs NoSQL)
    columns = []
    if "columns" in table and table["columns"]:
        columns = table["columns"]
    elif "fields" in table and table["fields"]:
        columns = table["fields"]
    
    if columns:
        # If we have information from columns/fields
        column_name = columns[0]["name"] if columns else "id"
        
        # Add specific questions with columns
        question_types.extend([
            f"¿Cuál es el valor máximo de {column_name} en {table_name}?",
            f"¿Cuáles son los valores únicos de {column_name} en {table_name}?",
        ])
    
    return random.choice(question_types)

def test_natural_language_query(api_token: str, db_config: Dict[str, Any], api_url: Optional[str] = None, user_data: Optional[Dict[str, Any]] = None) -> bool:
    """
    Tests a natural language query.
    
    Args:
        api_token: API token
        db_config: Database configuration
        api_url: Optional API URL
        user_data: User data

    Returns:
        True if the test is successful, False otherwise
    """
    try:
        print_colored("\nRealizando prueba de consulta en lenguaje natural...", "blue")
        
        # Dynamic import to avoid circular imports
        from db.schema_file import extract_db_schema
        
        # Generate a test question based on the extracted schema directly
        schema = extract_db_schema(db_config)
        print("REcoge esquema: ", schema)
        question = generate_test_question_from_schema(schema)
        print(f"Pregunta de prueba: {question}")
        
        # Prepare the data for the request
        api_url = api_url or DEFAULT_API_URL
        if not api_url.startswith(("http://", "https://")):
            api_url = "https://" + api_url
        
        if api_url.endswith('/'):
            api_url = api_url[:-1]
        
        # Build endpoint for the query
        endpoint = f"{api_url}/api/database/sdk/query"
        
        # Data for consultation
        request_data = {
            "question": question,
            "db_schema": schema,
            "config_id": db_config["config_id"]
        }
        
        # Make the request to the API
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        
        timeout = 15.0  # Reduced maximum waiting time
        
        try:
            print_colored("Enviando consulta al API...", "blue")
            response = http_session.post(
                endpoint,
                headers=headers,
                json=request_data,
                timeout=timeout
            )
            
            # Check the answer
            if response.status_code == 200:
                result = response.json()
                
                # Check if there is an explanation in the result
                if "explanation" in result:
                    print_colored("\nRespuesta:", "green")
                    print(result["explanation"])
                    
                    print_colored("\n✅ Prueba de consulta exitosa!", "green")
                    return True
                else:
                    # If there is no explanation but the API responds, it may be a different format
                    print_colored("\nRespuesta recibida del API (formato diferente al esperado):", "yellow")
                    print(json.dumps(result, indent=2))
                    print_colored("\n⚠️ La API respondió, pero con un formato diferente al esperado.", "yellow")
                    return True
            else:
                print_colored(f"❌ Error en la respuesta: Código {response.status_code}", "red")
                try:
                    error_data = response.json()
                    print(json.dumps(error_data, indent=2))
                except:
                    print(response.text[:500])
                return False
                
        except http_session.TimeoutException:
            print_colored("⚠️ Timeout al realizar la consulta. El API puede estar ocupado o no disponible.", "yellow")
            print_colored("Esto no afecta a la configuración guardada.", "yellow")
            return False
        except http_session.RequestError as e:
            print_colored(f"⚠️ Error de conexión: {str(e)}", "yellow")
            print_colored("Verifica la URL de la API y tu conexión a internet.", "yellow")
            return False
            
    except Exception as e:
        print_colored(f"❌ Error al realizar la consulta: {str(e)}", "red")
        return False