import uuid
import sqlite3
import pickle
import pandas as pd
from mcp.server.fastmcp import FastMCP

def get_db_connection():
    try:
        print("Attempting to connect to database...")
        conn = sqlite3.connect("handles_db.sqlite", check_same_thread=False)
        print("Successfully connected to database")
        conn.execute('''CREATE TABLE IF NOT EXISTS handles (
            handle TEXT PRIMARY KEY,
            dataframe BLOB
        )''')
        conn.commit()
        return conn
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise

# Initialize database connection
conn = get_db_connection()

sample_db = {
    "users": pd.DataFrame({
        "user_id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Alice2"],
        "city": ["New York", "London", "Paris", "London", "Tokyo", "New York"]
    }),
    "orders": pd.DataFrame({
        "order_id": [101, 102, 103, 104, 105, 106],
        "user_id": [1, 2, 1, 3, 5, 2],
        "product": ["Laptop", "Keyboard", "Mouse", "Monitor", "Webcam", "Desk"],
        "amount": [1200, 75, 25, 300, 50, 250]
    })
}

handler_mcp = FastMCP("df-abstractions-handler-demo")

# Helper functions to persist DataFrames
def save_handle(handle: str, df: pd.DataFrame):
    print(f"Saving handle {handle} to database...")
    df_blob = pickle.dumps(df)
    # Get a cursor for this operation
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO handles (handle, dataframe) VALUES (?, ?)", (handle, df_blob))
    conn.commit()
    cursor.close()
    print(f"Successfully saved handle {handle}")
    # Verify the save worked
    verify = load_handle(handle)
    if verify is not None:
        print(f"Verified handle {handle} was saved correctly")
    else:
        print(f"WARNING: Could not verify handle {handle} was saved")


def load_handle(handle: str):
    print(f"Loading handle {handle} from database...")
    try:
        # Get a cursor for this operation
        cursor = conn.cursor()
        cursor.execute("SELECT dataframe FROM handles WHERE handle = ?", (handle,))
        result = cursor.fetchone()
        cursor.close()
        if result:
            print(f"Successfully loaded handle {handle}")
            return pickle.loads(result[0])
        else:
            print(f"Handle {handle} not found in database")
            # List all handles in database for debugging
            cursor = conn.cursor()
            cursor.execute("SELECT handle FROM handles")
            handles = cursor.fetchall()
            cursor.close()
            print(f"Available handles in database: {[h[0] for h in handles]}")
            return None
    except Exception as e:
        print(f"Error loading handle {handle}: {e}")
        raise

@handler_mcp.tool()
def get_db_tables() -> list[str]:
    """
    Get the names of all tables in the database.
    """
    return sample_db.keys()

@handler_mcp.tool()
def query_database(table_name: str) -> str:
    """
    Query a table in the database and return a handle to the DataFrame.
    """
    if table_name in sample_db:
        handle = str(uuid.uuid4())
        save_handle(handle, sample_db[table_name].copy())
        return handle
    else:
        return f"Error: Table '{table_name}' not found."

@handler_mcp.tool()
def combine_columns(handle: str, col1_name: str, col2_name: str, new_col_name: str, sep: str = " ") -> str:
    df = load_handle(handle)
    if df is None:
        return f"Error: Handle '{handle}' not found."

    if col1_name not in df or col2_name not in df:
        return f"Error: Column(s) not found."

    df[new_col_name] = df[col1_name].astype(str) + sep + df[col2_name].astype(str)
    save_handle(handle, df)
    return handle

@handler_mcp.tool()
def join_dataframes(handle1: str, handle2: str, on_column: str, how: str = 'inner') -> str:
    df1 = load_handle(handle1)
    df2 = load_handle(handle2)

    if df1 is None or df2 is None:
        return "Error: One or both handles not found."

    if on_column not in df1 or on_column not in df2:
        return "Error: Join column not found."

    joined_df = pd.merge(df1, df2, on=on_column, how=how)
    new_handle = str(uuid.uuid4())
    save_handle(new_handle, joined_df)
    return new_handle

@handler_mcp.tool()
def select_columns(handle: str, columns: list) -> str:
    df = load_handle(handle)
    if df is None:
        return f"Error: Handle '{handle}' not found."

    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        return f"Error: Columns {missing_cols} not found."

    new_df = df[columns].copy()
    new_handle = str(uuid.uuid4())
    save_handle(new_handle, new_df)
    return new_handle

@handler_mcp.tool()
def filter_rows(handle: str, filter_expr: str) -> str:
    df = load_handle(handle)
    if df is None:
        return f"Error: Handle '{handle}' not found."

    try:
        filtered_df = df.query(filter_expr)
    except Exception as e:
        return f"Error in filtering rows: {str(e)}"

    new_handle = str(uuid.uuid4())
    save_handle(new_handle, filtered_df)
    return new_handle

@handler_mcp.tool()
def drop_columns(handle: str, columns: list) -> str:
    df = load_handle(handle)
    if df is None:
        return f"Error: Handle '{handle}' not found."

    new_df = df.drop(columns=columns, errors='ignore')
    new_handle = str(uuid.uuid4())
    save_handle(new_handle, new_df)
    return new_handle

@handler_mcp.tool()
def remove_duplicates(handle: str) -> str:
    df = load_handle(handle)
    if df is None:
        return f"Error: Handle '{handle}' not found."

    new_df = df.drop_duplicates()
    new_handle = str(uuid.uuid4())
    save_handle(new_handle, new_df)
    return new_handle

@handler_mcp.tool()
def distinct_rows(handle: str, columns: list = None) -> str:
    df = load_handle(handle)
    if df is None:
        return f"Error: Handle '{handle}' not found."
    
    if columns:
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            return f"Error: Columns {missing_cols} not found."
        new_df = df[columns].drop_duplicates()
    else:
        new_df = df.drop_duplicates()
        
    new_handle = str(uuid.uuid4())
    save_handle(new_handle, new_df)
    return new_handle

@handler_mcp.tool()
def get_schema(handle: str) -> str:
    df = load_handle(handle)
    if df is None:
        return f"Error: Handle '{handle}' not found."
    
    schema_info = pd.DataFrame({
        'column': df.columns.tolist(),
        'dtype': df.dtypes.astype(str).values.tolist(),
    })
    schema_info['num_rows'] = len(df)
    
    new_handle = str(uuid.uuid4())
    save_handle(new_handle, schema_info)
    return new_handle

@handler_mcp.tool()
def group_by(handle: str, group_columns: list, agg_dict: dict) -> str:
    """
    Group DataFrame by specified columns and apply aggregation functions.
    
    Args:
        handle: The handle of the DataFrame to group
        group_columns: List of columns to group by
        agg_dict: Dictionary mapping column names to aggregation functions
                 e.g., {"amount": "sum", "order_id": "count"}
    
    Returns:
        New handle for the grouped DataFrame
    """
    df = load_handle(handle)
    if df is None:
        return f"Error: Handle '{handle}' not found."
    
    # Verify group columns exist
    missing_group_cols = [col for col in group_columns if col not in df.columns]
    if missing_group_cols:
        return f"Error: Group columns {missing_group_cols} not found."
    
    # Verify aggregation columns exist
    missing_agg_cols = [col for col in agg_dict.keys() if col not in df.columns]
    if missing_agg_cols:
        return f"Error: Aggregation columns {missing_agg_cols} not found."
    
    try:
        grouped_df = df.groupby(group_columns, as_index=False).agg(agg_dict)
        new_handle = str(uuid.uuid4())
        save_handle(new_handle, grouped_df)
        return new_handle
    except Exception as e:
        return f"Error in grouping: {str(e)}"

if __name__ == "__main__":
    handler_mcp.run()
