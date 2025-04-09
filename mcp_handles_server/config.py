import os

# Get the root directory of the project (parent of mcp_handles_server)
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Define the database path relative to the root directory
DB_PATH = os.path.join(ROOT_DIR, "handles_db.sqlite") 