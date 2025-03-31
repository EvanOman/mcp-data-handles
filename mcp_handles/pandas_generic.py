"""
This module contains a simple MCP server that gives a model the ability to run generic pandas DataFrame operations, along with a mock query_database tool to simulate a database.
"""

import pandas as pd
import uuid
from typing import Dict, Any, List
import io
import traceback

from mcp.server.fastmcp import FastMCP

# --- Security Warning ---
print("\n" + "="*60)
print("WARNING: This server uses exec() to run arbitrary Python code.")
print("This is extremely dangerous and should NOT be used in production")
print("without proper sandboxing and security reviews.")
print("This is for demonstration purposes ONLY.")
print("="*60 + "\n")
# --- End Security Warning ---

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
mcp = FastMCP(
    "generic-pandas-handler-demo",
)

print("Generic Pandas Server starting...")

# --- Tool Definitions ---

@mcp.tool()
def query_database(table_name: str) -> str:
    """
    Loads a sample table into a DataFrame and returns a handle.

    Args:
        table_name: The name of the table ('users' or 'orders').

    Returns:
        A unique handle (string) for the loaded DataFrame, or an error message.
    """
    print(f"[Tool: query_database] Args: table_name='{table_name}'")
    if table_name in sample_db:
        # Return a copy to avoid modifying the original 'db'
        df_copy = sample_db[table_name].copy()
        handle = str(uuid.uuid4())
        data_handles[handle] = df_copy
        print(f"  -> Generated handle {handle} for table '{table_name}', shape: {df_copy.shape}")
        return handle
    else:
        print(f"  -> Error: Table '{table_name}' not found.")
        return f"Error: Table '{table_name}' not found."

@mcp.tool()
def execute_pandas_code(
    code: str,
    input_handles: Dict[str, str],
    output_aliases: List[str]
) -> Dict[str, str]:
    """
    Executes arbitrary Python/Pandas code using specified DataFrames as input.
    WARNING: Executes arbitrary code, use with extreme caution!

    Args:
        code: A string containing the Python code to execute. The code can use
              variable names matching the keys in `input_handles` to access the
              corresponding DataFrames. It should assign results to variables
              named in `output_aliases`.
        input_handles: A dictionary mapping variable names (aliases) to use in the code
                       to the corresponding DataFrame handles. Example: {"df1": "handle_uuid_1", "users_df": "handle_uuid_2"}
        output_aliases: A list of variable names expected to hold DataFrame results
                        after the code execution. Example: ["result_df", "summary_df"]

    Returns:
        A dictionary mapping the output aliases to their new handles if successful,
        or a dictionary containing an 'error' key with an error message.
    """
    print(f"[Tool: execute_pandas_code] Input Handles: {input_handles}, Output Aliases: {output_aliases}")
    print(f"  Code to execute:\n---\n{code}\n---")

    local_vars: Dict[str, Any] = {}
    global_vars: Dict[str, Any] = {'pd': pd} # Make pandas available to the code

    # Load input DataFrames into the local execution context
    for alias, handle in input_handles.items():
        if handle not in data_handles:
            error_msg = f"Error: Input handle '{handle}' (alias '{alias}') not found."
            print(f"  -> {error_msg}")
            return {"error": error_msg}
        # Pass copies to prevent unexpected modification of originals via reference
        local_vars[alias] = data_handles[handle].copy()
        print(f"  Mapped alias '{alias}' to handle '{handle}' (shape: {local_vars[alias].shape})")

    # Execute the code - THIS IS THE DANGEROUS PART
    try:
        exec(code, global_vars, local_vars)
        print("  Code execution completed.")
    except Exception as e:
        error_msg = f"Error during code execution: {type(e).__name__}: {e}\n{traceback.format_exc()}"
        print(f"  -> {error_msg}")
        return {"error": error_msg}

    # Process outputs
    output_handles: Dict[str, str] = {}
    for alias in output_aliases:
        if alias not in local_vars:
            error_msg = f"Error: Expected output alias '{alias}' not found after code execution."
            print(f"  -> {error_msg}")
            return {"error": error_msg}

        result_df = local_vars[alias]
        if not isinstance(result_df, pd.DataFrame):
            error_msg = f"Error: Output alias '{alias}' did not result in a DataFrame (type: {type(result_df).__name__})."
            print(f"  -> {error_msg}")
            return {"error": error_msg}

        new_handle = str(uuid.uuid4())
        data_handles[new_handle] = result_df # Store the new DataFrame
        output_handles[alias] = new_handle
        print(f"  Generated handle '{new_handle}' for output alias '{alias}' (shape: {result_df.shape})")

    print(f"  -> Returning output handles: {output_handles}")
    return output_handles


@mcp.tool()
def materialize_dataframe(
    handle: str,
    format: str = "head_string",
    n: int = 5
) -> str:
    """
    Retrieves data from a DataFrame referenced by a handle in a specified format.

    Args:
        handle: The handle of the DataFrame to materialize.
        format: The desired output format. Options:
                'head_string' (default): First n rows as string.
                'tail_string': Last n rows as string.
                'sample_string': Random n rows as string.
                'full_string': Entire DataFrame as string (use with caution!).
                'json_records': DataFrame as JSON string (records orientation).
                'json_split': DataFrame as JSON string (split orientation).
                'csv': DataFrame as CSV string.
        n: The number of rows for 'head_string', 'tail_string', 'sample_string' (default: 5).

    Returns:
        The materialized data as a string, or an error message.
    """
    print(f"[Tool: materialize_dataframe] Args: handle='{handle}', format='{format}', n={n}")
    if handle not in data_handles:
        error_msg = f"Error: Handle '{handle}' not found."
        print(f"  -> {error_msg}")
        return error_msg

    if not isinstance(n, int) or n <= 0:
        n = 5 # Default to 5 if n is invalid

    df = data_handles[handle]
    output_str = ""

    try:
        if format == "head_string":
            output_str = df.head(n).to_string()
        elif format == "tail_string":
             output_str = df.tail(n).to_string()
        elif format == "sample_string":
             output_str = df.sample(min(n, len(df))).to_string() # Sample up to n or df length
        elif format == "full_string":
             # Add safety limit for demo purposes
             MAX_ROWS_FULL = 1000
             if len(df) > MAX_ROWS_FULL:
                 print(f"  Warning: 'full_string' requested for large DF ({len(df)} rows). Truncating to {MAX_ROWS_FULL} rows.")
                 output_str = df.head(MAX_ROWS_FULL).to_string() + f"\n... (truncated to {MAX_ROWS_FULL} rows)"
             else:
                output_str = df.to_string()
        elif format == "json_records":
             output_str = df.to_json(orient="records", indent=2)
        elif format == "json_split":
             output_str = df.to_json(orient="split", indent=2)
        elif format == "csv":
            # Use io.StringIO to capture CSV output as string
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            output_str = csv_buffer.getvalue()
        else:
            error_msg = f"Error: Invalid format '{format}'. Valid formats are: head_string, tail_string, sample_string, full_string, json_records, json_split, csv."
            print(f"  -> {error_msg}")
            return error_msg

        print(f"  -> Materialized {len(output_str)} chars in format '{format}'. Preview:\n{output_str[:200]}...")
        return output_str

    except Exception as e:
        error_msg = f"Error materializing DataFrame '{handle}' in format '{format}': {str(e)}"
        print(f"  -> {error_msg}\n{traceback.format_exc()}")
        return error_msg

@mcp.tool()
def get_shape(handle: str) -> str:
    """
    Gets the shape (rows, columns) of the DataFrame referenced by the handle.

    Args:
        handle: The handle of the DataFrame.

    Returns:
        A string representation of the shape (e.g., "(100, 5)"), or an error message.
    """
    print(f"[Tool: get_shape] Args: handle='{handle}'")
    if handle not in data_handles:
        error_msg = f"Error: Handle '{handle}' not found."
        print(f"  -> {error_msg}")
        return error_msg

    df = data_handles[handle]
    shape_str = str(df.shape)
    print(f"  -> Returning shape: {shape_str}")
    return shape_str

# --- Run the server ---
if __name__ == "__main__":
    mcp.run() # Runs using stdio by default
    print("Generic Pandas Server finished.")