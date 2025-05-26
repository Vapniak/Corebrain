"""
Main commands for the Corebrain CLI.
"""
import argparse
import os
import sys
import webbrowser
import requests
import random 
import string

from typing import Optional, List

from corebrain.cli.common import DEFAULT_API_URL, DEFAULT_SSO_URL, DEFAULT_PORT, SSO_CLIENT_ID, SSO_CLIENT_SECRET
from corebrain.cli.auth.sso import authenticate_with_sso, authenticate_with_sso_and_api_key_request
from corebrain.cli.config import configure_sdk, get_api_credential
from corebrain.cli.utils import print_colored
from corebrain.config.manager import ConfigManager
from corebrain.config.manager import export_config
from corebrain.config.manager import validate_config
from corebrain.lib.sso.auth import GlobodainSSOAuth

def main_cli(argv: Optional[List[str]] = None) -> int:
    """
    Main entry point for the Corebrain CLI.
    
    Args:
        argv: List of command line arguments (defaults to sys.argv[1:])
        
    Returns:
        Exit code (0 for success, other value for error)
    """
        
    # Package version
    __version__ = "0.2.0"

    try:
        print_colored("Corebrain CLI started. Version ", __version__, "blue")
        
        if argv is None:
            argv = sys.argv[1:]
        
        # Argument parser configuration
        parser = argparse.ArgumentParser(description="Corebrain SDK CLI")
        parser.add_argument("--version", action="store_true", help="Show SDK version")
        parser.add_argument("--authentication", action="store_true", help="Authenticate with SSO")
        parser.add_argument("--create-user", action="store_true", help="Create an user and API Key by default")
        parser.add_argument("--configure", action="store_true", help="Configure the Corebrain SDK")
        parser.add_argument("--list-configs", action="store_true", help="List available configurations")

        parser.add_argument("--token", help="Corebrain API token (any type)")
        parser.add_argument("--api-key", help="Specific API Key for Corebrain")
        parser.add_argument("--api-url", help="Corebrain API URL")
        parser.add_argument("--sso-url", help="Globodain SSO service URL")
        parser.add_argument("--login", action="store_true", help="Login via SSO")
        parser.add_argument("--test-auth", action="store_true", help="Test SSO authentication system")
        parser.add_argument("--woami",action="store_true",help="Display information about the current user")
        parser.add_argument("--check-status",action="store_true",help="Checks status of task")
        parser.add_argument("--task-id",help="ID of the task to check status for")
        parser.add_argument("--validate-config",action="store_true",help="Validates the selected configuration without executing any operations")
        parser.add_argument("--test-connection",action="store_true",help="Tests the connection to the Corebrain API using the provide credentials")
        parser.add_argument("--export-config",action="store_true",help="Exports the current configuration to a file")
        parser.add_argument("--gui", action="store_true", help="Check setup and launch the web interface")

        
        args = parser.parse_args(argv)
        
        def authentication():
            sso_url = args.sso_url or os.environ.get("COREBRAIN_SSO_URL") or DEFAULT_SSO_URL
            sso_token, sso_user = authenticate_with_sso(sso_url)
            if sso_token:
                try:
                    print_colored("✅ Returning SSO Token.", "green")
                    print_colored(f"{sso_token}", "blue")
                    print_colored("✅ Returning User data.", "green")
                    print_colored(f"{sso_user}", "blue")
                    return sso_token, sso_user
                
                except Exception as e:
                    print_colored("❌ Could not return SSO Token or SSO User data.", "red")
                    return sso_token, sso_user
                
            else:
                print_colored("❌ Could not authenticate with SSO.", "red")
                return None, None
        

        # Show version
        if args.version:
            """
            Show the library version.
            """
            try:
                from importlib.metadata import version
                sdk_version = version("corebrain")
                print(f"Corebrain SDK version {sdk_version}")
            except Exception:
                print(f"Corebrain SDK version {__version__}")
            return 0

        if args.check_status:
            """
            If you're in development mode:

            Check that all requirements for developing code and performing tests or other functions are accessible:
            - Check that the API Server is runned
            - Check that the Redis is runned on port 6379
            - Check that the SSO Server is active (sso.globodain.com)
            - Check that MongoDB is runned on port 27017
            - Check that the all libraries are installed:         

            httpx>=0.23.0
            pymongo>=4.3.0
            psycopg2-binary>=2.9.5
            mysql-connector-python>=8.0.31
            sqlalchemy>=2.0.0
            cryptography>=39.0.0
            pydantic>=1.10.0


            If you're in production mode:

            Check that the API Server is active (api.etedata.com)
            Check that the SSO Server is active (sso.globodain.com)
            Check that the all libraries are installed:         

            httpx>=0.23.0
            pymongo>=4.3.0
            psycopg2-binary>=2.9.5
            mysql-connector-python>=8.0.31
            sqlalchemy>=2.0.0
            cryptography>=39.0.0
            pydantic>=1.10.0

            """            
        
            import socket
            import subprocess
            import importlib.util
            
            def check_port(host, port, service_name):
                """Check if a service is running on a specific port"""
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(3)
                    result = sock.connect_ex((host, port))
                    sock.close()
                    if result == 0:
                        print_colored(f"✅ {service_name} is running on {host}:{port}", "green")
                        return True
                    else:
                        print_colored(f"❌ {service_name} is not accessible on {host}:{port}", "red")
                        return False
                except Exception as e:
                    print_colored(f"❌ Error checking {service_name}: {str(e)}", "red")
                    return False
            
            def check_url(url, service_name):
                """Check if a service is accessible via HTTP"""
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code < 500:
                        print_colored(f"✅ {service_name} is accessible at {url}", "green")
                        return True
                    else:
                        print_colored(f"❌ {service_name} returned status {response.status_code} at {url}", "red")
                        return False
                except Exception as e:
                    print_colored(f"❌ {service_name} is not accessible at {url}: {str(e)}", "red")
                    return False
            
            def check_library(library_name, min_version):
                """Check if a library is installed with minimum version"""
                # Mapping of PyPI package names to import names
                package_import_mapping = {
                    'psycopg2-binary': 'psycopg2',
                    'mysql-connector-python': 'mysql.connector',
                    'httpx': 'httpx',
                    'pymongo': 'pymongo',
                    'sqlalchemy': 'sqlalchemy',
                    'cryptography': 'cryptography',
                    'pydantic': 'pydantic'
                }
                
                package_name = library_name.split('>=')[0]
                import_name = package_import_mapping.get(package_name, package_name)
                
                try:
                    # Check if the module can be imported
                    if '.' in import_name:
                        # For modules like mysql.connector
                        parts = import_name.split('.')
                        spec = importlib.util.find_spec(parts[0])
                        if spec is None:
                            print_colored(f"❌ {package_name} is not installed", "red")
                            return False
                        # Try to import the full module path
                        try:
                            __import__(import_name)
                        except ImportError:
                            print_colored(f"❌ {package_name} is not installed", "red")
                            return False
                    else:
                        spec = importlib.util.find_spec(import_name)
                        if spec is None:
                            print_colored(f"❌ {package_name} is not installed", "red")
                            return False
                    
                    # Try to get version using different methods
                    try:
                        from importlib.metadata import version
                        # Try with the package name first
                        try:
                            installed_version = version(package_name)
                        except:
                            # If that fails, try with common alternative names
                            alternative_names = {
                                'psycopg2-binary': ['psycopg2', 'psycopg2-binary'],
                                'mysql-connector-python': ['mysql-connector-python', 'mysql-connector']
                            }
                            
                            installed_version = None
                            for alt_name in alternative_names.get(package_name, [package_name]):
                                try:
                                    installed_version = version(alt_name)
                                    break
                                except:
                                    continue
                            
                            if installed_version is None:
                                raise Exception("Version not found")
                        
                        print_colored(f"✅ {package_name} {installed_version} is installed", "green")
                        return True
                        
                    except Exception:
                        # If version check fails, at least we know the module can be imported
                        print_colored(f"✅ {package_name} is installed (version check failed)", "yellow")
                        return True
                    
                except Exception as e:
                    print_colored(f"❌ Error checking {package_name}: {str(e)}", "red")
                    return False
            
            # Determine if in development or production mode
            api_url = args.api_url or os.environ.get("COREBRAIN_API_URL") or DEFAULT_API_URL
            is_development = "localhost" in api_url or "127.0.0.1" in api_url or api_url == DEFAULT_API_URL
            
            print_colored("🔍 Checking system status...", "blue")
            print_colored(f"Mode: {'Development' if is_development else 'Production'}", "blue")
            print_colored(f"API URL: {api_url}", "blue")
            print()
            
            all_checks_passed = True
            
            # Required libraries for both modes
            required_libraries = [
                "httpx>=0.23.0",
                "pymongo>=4.3.0", 
                "psycopg2-binary>=2.9.5",
                "mysql-connector-python>=8.0.31",
                "sqlalchemy>=2.0.0",
                "cryptography>=39.0.0",
                "pydantic>=1.10.0"
            ]
            
            # Check libraries
            print_colored("📚 Checking required libraries:", "blue")
            for library in required_libraries:
                if not check_library(library, library.split('>=')[1] if '>=' in library else None):
                    all_checks_passed = False
            print()
            
            # Check services based on mode
            if is_development:
                print_colored("🔧 Development mode - Checking local services:", "blue")
                
                # Check local API server
                if not check_url(api_url, "API Server"):
                    all_checks_passed = False
                
                # Check Redis
                if not check_port("localhost", 6379, "Redis"):
                    all_checks_passed = False
                
                # Check MongoDB  
                if not check_port("localhost", 27017, "MongoDB"):
                    all_checks_passed = False
                    
            else:
                print_colored("🌐 Production mode - Checking remote services:", "blue")
                
                # Check production API server
                if not check_url("https://api.etedata.com", "API Server (Production)"):
                    all_checks_passed = False
            
            # Check SSO service for both modes
            sso_url = args.sso_url or os.environ.get("COREBRAIN_SSO_URL") or DEFAULT_SSO_URL
            if not check_url(sso_url, "SSO Server"):
                all_checks_passed = False
            
            print()
            if all_checks_passed:
                print_colored("✅ All system checks passed!", "green")
                return 0
            else:
                print_colored("❌ Some system checks failed. Please review the issues above.", "red")
                return 1

        if args.configure or args.list_configs or args.show_schema:
            """
            Configure, list or show schema of the configured database.

            Reuse the same autehntication code for configure, list and show schema.
            """
            # Get URLs
            api_url = args.api_url or os.environ.get("COREBRAIN_API_URL") or DEFAULT_API_URL
            sso_url = args.sso_url or os.environ.get("COREBRAIN_SSO_URL") or DEFAULT_SSO_URL
            
            # Prioritize api_key if explicitly provided
            token_arg = args.api_key if args.api_key else args.token
            
            # Get API credentials
            api_key, user_data, api_token = get_api_credential(token_arg, sso_url)
            
            if not api_key:
                print_colored("Error: An API Key is required. You can generate one at dashboard.corebrain.com", "red")
                print_colored("Or use the 'corebrain --login' command to login via SSO.", "blue")
                return 1
            
            from corebrain.db.schema_file import show_db_schema, extract_schema_to_file
            
            # Execute the selected operation
            if args.configure:
                configure_sdk(api_token, api_key, api_url, sso_url, user_data)
            elif args.list_configs:
                ConfigManager.list_configs(api_key, api_url)
            elif args.remove_config:
                ConfigManager.remove_config(api_key, api_url)
            elif args.show_schema:
                show_db_schema(api_key, args.config_id, api_url)
        
        if args.export_config:
            export_config(args.export_config)
            # --> config/manager.py --> export_config



        # Create an user and API Key by default
        if args.authentication:
            authentication()
            
        if args.create_user:
            sso_token, sso_user = authentication() # Authentica use with SSO
            
            if sso_token and sso_user:
                print_colored("✅ Enter to create an user and API Key.", "green")
                
                # Get API URL from environment or use default
                api_url = os.environ.get("COREBRAIN_API_URL", DEFAULT_API_URL)
                
                """
                Create user data with SSO information.
                If the user wants to use a different password than their SSO account,
                they can specify it here.
                """
                # Ask if user wants to use SSO password or create a new one
                use_sso_password = input("Do you want to use your SSO password? (y/n): ").lower().strip() == 'y'
                
                if use_sso_password:
                    random_password = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
                    password = sso_user.get("password", random_password)
                else:
                    while True:
                        password = input("Enter new password: ").strip()
                        if len(password) >= 8:
                            break
                        print_colored("Password must be at least 8 characters long", "yellow")
                
                user_data = {
                    "email": sso_user["email"],
                    "name": f"{sso_user['first_name']} {sso_user['last_name']}",
                    "password": password
                }
                
                try:
                    # Make the API request
                    response = requests.post(
                        f"{api_url}/api/auth/users",
                        json=user_data,
                        headers={
                            "Authorization": f"Bearer {sso_token}",
                            "Content-Type": "application/json"
                        }
                    )
                    
                    # Check if the request was successful
                    print("response API: ", response)
                    if response.status_code == 200:
                        print_colored("✅ User and API Key created successfully!", "green")
                        return 0
                    else:
                        print_colored(f"❌ Error creating user: {response.text}", "red")
                        return 1
                        
                except requests.exceptions.RequestException as e:
                    print_colored(f"❌ Error connecting to API: {str(e)}", "red")
                    return 1
                
            else:
                print_colored("❌ Could not create the user or the API KEY.", "red")
                return 1
        
        # Test SSO authentication
        if args.test_auth:
            sso_url = args.sso_url or os.environ.get("COREBRAIN_SSO_URL") or DEFAULT_SSO_URL
            
            print_colored("Testing SSO authentication...", "blue")
            
            # Authentication configuration
            auth_config = {
                'GLOBODAIN_SSO_URL': sso_url,
                'GLOBODAIN_CLIENT_ID': SSO_CLIENT_ID,
                'GLOBODAIN_CLIENT_SECRET': SSO_CLIENT_SECRET,
                'GLOBODAIN_REDIRECT_URI': f"http://localhost:{DEFAULT_PORT}/auth/sso/callback",
                'GLOBODAIN_SUCCESS_REDIRECT': f"http://localhost:{DEFAULT_PORT}/auth/sso/callback"
            }
            
            try:
                # Instantiate authentication client
                sso_auth = GlobodainSSOAuth(config=auth_config)
                
                # Get login URL
                login_url = sso_auth.get_login_url()
                
                print_colored(f"Login URL: {login_url}", "blue")
                print_colored("Opening browser for login...", "blue")
                
                # Open browser
                webbrowser.open(login_url)
                
                print_colored("Please complete the login process in the browser.", "blue")
                input("\nPress Enter when you've completed the process or to cancel...")
                
                print_colored("✅ SSO authentication test completed!", "green")
                return 0
            except Exception as e:
                print_colored(f"❌ Error during test: {str(e)}", "red")
                return 1
        
        # Login via SSO
        if args.login:
            sso_url = args.sso_url or os.environ.get("COREBRAIN_SSO_URL") or DEFAULT_SSO_URL
            api_key, user_data, api_token = authenticate_with_sso_and_api_key_request(sso_url)
            
            if api_token:
                # Save the general token for future use
                os.environ["COREBRAIN_API_TOKEN"] = api_token
            
            if api_key:
                # Save the specific API key for future use
                os.environ["COREBRAIN_API_KEY"] = api_key
                print_colored("✅ API Key successfully saved. You can use the SDK now.", "green")
                
                # If configuration was also requested, continue with the process
                if args.configure:
                    api_url = args.api_url or os.environ.get("COREBRAIN_API_URL") or DEFAULT_API_URL
                    configure_sdk(api_token, api_key, api_url, sso_url, user_data)
                
                return 0
            else:
                print_colored("❌ Could not obtain an API Key via SSO.", "red")
                if api_token:
                    print_colored("A general API token was obtained, but not a specific API Key.", "yellow")
                    print_colored("You can create an API Key in the Corebrain dashboard.", "yellow")
                return 1
            
        

        if args.woami:
            try:
                #downloading user data
                sso_url = args.sso_url or os.environ.get("COREBRAIN_SSO_URL") or DEFAULT_SSO_URL
                token_arg = args.api_key if args.api_key else args.token

                #using saved data about user 
                api_key, user_data, api_token = get_api_credential(token_arg, sso_url)
                #printing user data
                if user_data:
                    print_colored("User Data:", "blue")
                    for k, v in user_data.items():
                        print(f"{k}: {v}")
                else:
                    print_colored("❌ Can't find data about user, be sure that you are logged into  --login.", "red")
                    return 1

                return 0
            except Exception as e:
                print_colored(f"❌ Error when downloading data about user {str(e)}", "red")
                return 1
        
        
        
        
        if args.test_connection:
            # Test connection to the Corebrain API
            api_url = args.api_url or os.environ.get("COREBRAIN_API_URL", DEFAULT_API_URL)
            sso_url = args.sso_url or os.environ.get("COREBRAIN_SSO_URL", DEFAULT_SSO_URL)
            
            try:
                # Retrieve API credentials
                api_key, user_data, api_token = get_api_credential(args.token, sso_url)
            except Exception as e:
                print_colored(f"Error while retrieving API credentials: {e}", "red")
                return 1

            if not api_key:
                print_colored(
                    "Error: An API key is required. You can generate one at dashboard.corebrain.com.",
                    "red"
                )
                return 1

            try:
                # Test the connection
                from corebrain.db.schema_file import test_connection
                test_connection(api_key, api_url)
                print_colored("Successfully connected to Corebrain API.", "green")
            except Exception as e:
                print_colored(f"Failed to connect to Corebrain API: {e}", "red")
                return 1




        if args.gui:
            import subprocess
            from pathlib import Path

            def run_cmd(cmd, cwd=None):
                print_colored(f"▶ {cmd}", "yellow")
                subprocess.run(cmd, shell=True, cwd=cwd, check=True)

            print("Checking GUI setup...")

            commands_path = Path(__file__).resolve()
            corebrain_root = commands_path.parents[1]

            cli_ui_path = corebrain_root / "CLI-UI"
            client_path = cli_ui_path / "client"
            server_path = cli_ui_path / "server"
            api_path = corebrain_root / "wrappers" / "csharp_cli_api"

            # Path validation
            if not client_path.exists():
                print_colored(f"Folder {client_path} does not exist!", "red")
                sys.exit(1)
            if not server_path.exists():
                print_colored(f"Folder {server_path} does not exist!", "red")
                sys.exit(1)
            if not api_path.exists():
                print_colored(f"Folder {api_path} does not exist!", "red")
                sys.exit(1)

            # Setup client
            if not (client_path / "node_modules").exists():
                print_colored("Installing frontend (React) dependencies...", "cyan")
                run_cmd("npm install", cwd=client_path)
                run_cmd("npm install history", cwd=client_path)
                run_cmd("npm install --save-dev vite", cwd=client_path)
                run_cmd("npm install concurrently --save-dev", cwd=client_path)

            # Setup server
            if not (server_path / "node_modules").exists():
                print_colored("Installing backend (Express) dependencies...", "cyan")
                run_cmd("npm install", cwd=server_path)
                run_cmd("npm install --save-dev ts-node-dev", cwd=server_path)

            # Start GUI: CLI UI + Corebrain API
            print("Starting GUI (CLI-UI + Corebrain API)...")

            def run_in_background_silent(cmd, cwd):
                return subprocess.Popen(
                    cmd,
                    cwd=cwd,
                    shell=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )

            run_in_background_silent("dotnet run", cwd=api_path)
            run_in_background_silent(
                'npx concurrently "npm --prefix server run dev" "npm --prefix client run dev"',
                cwd=cli_ui_path
            )

            url = "http://localhost:5173/"
            print_colored(f"GUI: {url}", "cyan")
            webbrowser.open(url)
        










        else:
            # If no option was specified, show help
            parser.print_help()
            print_colored("\nTip: Use 'corebrain --login' to login via SSO.", "blue")
        
        return 0
    except Exception as e:
        print_colored(f"Error: {str(e)}", "red")
        import traceback
        traceback.print_exc()
        return 1