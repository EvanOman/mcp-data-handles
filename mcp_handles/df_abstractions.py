"""
This module contains a simple MCP server that exposes several basic DataFrame operations along with a mock query_database tool to simulate a database.
"""

import pandas as pd
import uuid
from typing import Dict

from mcp.server.fastmcp import FastMCP

# --- In-memory storage for our demo ---
# Simulate a database with a couple of tables
sample_db: Dict[str, pd.DataFrame] = {
    "users": pd.DataFrame({
        "user_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "city": ["New York", "London", "Paris", "London", "Tokyo"]
    }),
    "orders": pd.DataFrame({
        "order_id": [101, 102, 103, 104, 105, 106],
        "user_id": [1, 2, 1, 3, 5, 2],
        "product": ["Laptop", "Keyboard", "Mouse", "Monitor", "Webcam", "Desk"],
        "amount": [1200, 75, 25, 300, 50, 250]
    })
}

# Store the DataFrames generated during the session, mapped by handle
data_handles: Dict[str, pd.DataFrame] = {}

# --- MCP Server Setup ---
handler_mcp = FastMCP(
    "df-abstractions-handler-demo",
    # You can add server info and capabilities here if needed
    # server_info=T.Implementation(version="1.0.0"),
    # server_options=T.ServerOptions(capabilities=T.ServerCapabilities(...))
)

print("DataFrame Handler Server starting...")

# --- Tool Definitions ---

@handler_mcp.tool()
def query_database(table_name: str) -> str:
    """
    Queries the sample database for a table and returns a handle to the resulting DataFrame.

    Args:
        table_name: The name of the table to query (e.g., 'users', 'orders').

    Returns:
        A unique handle (string) representing the loaded DataFrame, or an error message.

    Available Tables:
        users:
            - user_id (int): Primary key (1-5)
            - name (str): User's name
            - city (str): User's city

        orders:
            - order_id (int): Primary key (101-106)
            - user_id (int): Foreign key referencing users.user_id
            - product (str): Name of the purchased product
            - amount (float): Price of the product in dollars

    Example:
        handle = query_database('users')  # Returns a handle to access the users table
    """
    print(f"Received query_database request for table: {table_name}")
    if table_name in sample_db:
        df_copy = sample_db[table_name].copy()
        handle = str(uuid.uuid4())
        data_handles[handle] = df_copy
        print(f"Generated handle {handle} for table {table_name}, shape: {df_copy.shape}")
        return handle
    else:
        print(f"Error: Table '{table_name}' not found.")
        return f"Error: Table '{table_name}' not found in sample database."

@handler_mcp.tool()
def combine_columns(handle: str, col1_name: str, col2_name: str, new_col_name: str, sep: str = " ") -> str:
    """
    Combines two string columns in the DataFrame referenced by the handle into a new column.
    Modifies the DataFrame in place.

    Args:
        handle: The handle of the DataFrame to modify.
        col1_name: The name of the first column.
        col2_name: The name of the second column.
        new_col_name: The name for the new combined column.
        sep: The separator to use between the combined values (default is space).

    Returns:
        The original handle if successful, or an error message.
    """
    print(f"Received combine_columns request for handle: {handle}")
    if handle not in data_handles:
        print(f"Error: Handle '{handle}' not found.")
        return f"Error: Handle '{handle}' not found."

    df = data_handles[handle]
    if col1_name not in df.columns or col2_name not in df.columns:
        print(f"Error: One or both columns ('{col1_name}', '{col2_name}') not found.")
        return f"Error: One or both columns ('{col1_name}', '{col2_name}') not found in DataFrame '{handle}'."

    try:
        # Ensure columns are string type before combining
        df[new_col_name] = df[col1_name].astype(str) + sep + df[col2_name].astype(str)
        data_handles[handle] = df # Update the stored DataFrame (though modification happens in place)
        print(f"Successfully combined columns for handle {handle}. New shape: {df.shape}")
        return handle # Return the original handle as it's modified in place
    except Exception as e:
        print(f"Error combining columns for handle {handle}: {e}")
        return f"Error combining columns for handle {handle}: {str(e)}"

@handler_mcp.tool()
def join_dataframes(handle1: str, handle2: str, on_column: str, how: str = 'inner') -> str:
    """
    Joins two DataFrames referenced by their handles on a specified column.
    Returns a handle to the *new* resulting DataFrame.

    Args:
        handle1: The handle of the left DataFrame.
        handle2: The handle of the right DataFrame.
        on_column: The column name to join on.
        how: Type of join ('left', 'right', 'outer', 'inner'). Defaults to 'inner'.

    Returns:
        A *new* unique handle (string) for the joined DataFrame, or an error message.
    """
    print(f"Received join_dataframes request for handles: {handle1}, {handle2} on '{on_column}'")
    if handle1 not in data_handles:
        return f"Error: Handle '{handle1}' not found."
    if handle2 not in data_handles:
        return f"Error: Handle '{handle2}' not found."

    df1 = data_handles[handle1]
    df2 = data_handles[handle2]

    if on_column not in df1.columns or on_column not in df2.columns:
        return f"Error: Join column '{on_column}' not found in one or both DataFrames."

    valid_joins = ['left', 'right', 'outer', 'inner']
    if how not in valid_joins:
        return f"Error: Invalid join type '{how}'. Must be one of {valid_joins}."

    try:
        joined_df = pd.merge(df1, df2, on=on_column, how=how)
        new_handle = str(uuid.uuid4())
        data_handles[new_handle] = joined_df
        print(f"Successfully joined {handle1} and {handle2}. New handle {new_handle}, shape: {joined_df.shape}")
        return new_handle
    except Exception as e:
        print(f"Error joining dataframes {handle1} and {handle2}: {e}")
        return f"Error joining dataframes: {str(e)}"

@handler_mcp.tool()
def get_shape(handle: str) -> str:
    """
    Gets the shape (number of rows, number of columns) of the DataFrame referenced by the handle.

    Args:
        handle: The handle of the DataFrame.

    Returns:
        A string representation of the shape (e.g., "(100, 5)"), or an error message.
    """
    print(f"Received get_shape request for handle: {handle}")
    if handle not in data_handles:
        return f"Error: Handle '{handle}' not found."

    df = data_handles[handle]
    shape_str = str(df.shape)
    print(f"Returning shape for handle {handle}: {shape_str}")
    return shape_str

@handler_mcp.tool()
def get_head(handle: str) -> str:
    """
    Gets the first 5 rows of the DataFrame referenced by the handle as a string.

    Args:
        handle: The handle of the DataFrame.

    Returns:
        A string representation of the first 5 rows, or an error message.
    """
    print(f"Received get_head request for handle: {handle}")
    if handle not in data_handles:
        return f"Error: Handle '{handle}' not found."

    df = data_handles[handle]
    try:
        # Using to_string for a simple, readable representation
        head_str = df.head().to_string()
        print(f"Returning head for handle {handle}:\n{head_str[:200]}...") # Log truncated
        return head_str
    except Exception as e:
        print(f"Error getting head for handle {handle}: {e}")
        return f"Error getting head for handle {handle}: {str(e)}"

@handler_mcp.tool()
def get_top_n_rows(handle: str, n: int) -> str:
    """
    Gets the first N rows of the DataFrame referenced by the handle as a string.

    Args:
        handle: The handle of the DataFrame.
        n: The number of rows to retrieve.

    Returns:
        A string representation of the first N rows, or an error message.
    """
    print(f"Received get_top_n_rows request for handle: {handle}, n={n}")
    if handle not in data_handles:
        return f"Error: Handle '{handle}' not found."
    if not isinstance(n, int) or n <= 0:
        return f"Error: Number of rows 'n' must be a positive integer."

    # Optional: Add a safety cap to prevent asking for too many rows
    MAX_ROWS = 1000
    if n > MAX_ROWS:
        print(f"Warning: Requested rows {n} exceeds limit {MAX_ROWS}. Returning {MAX_ROWS} rows.")
        n = MAX_ROWS

    df = data_handles[handle]
    try:
        # Using to_string for a simple, readable representation
        top_n_str = df.head(n).to_string()
        print(f"Returning top {n} rows for handle {handle}:\n{top_n_str[:200]}...") # Log truncated
        return top_n_str
    except Exception as e:
        print(f"Error getting top {n} rows for handle {handle}: {e}")
        return f"Error getting top {n} rows for handle {handle}: {str(e)}"

# --- Run the server ---
if __name__ == "__main__":
    # This runs the server using stdio transport by default
    # It will print connection info and listen for MCP messages
    handler_mcp.run()
    print("DataFrame Handler Server finished.")