import asyncio
import json
import os
import sqlite3
import pickle
import pandas as pd

from agents import Agent, Runner
from agents.mcp import MCPServer, MCPServerStdio
from fire import Fire
from dotenv import load_dotenv
from mcp_handles_server.config import DB_PATH

load_dotenv()

def fetch_dataframe(handle: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("SELECT dataframe FROM handles WHERE handle = ?", (handle,))
    result = cursor.fetchone()
    conn.close()
    return pickle.loads(result[0]) if result else None

async def run(mcp_server: MCPServer, query: str):
    agent = Agent(
        name="Assistant",
        instructions="""Use the tools to read the filesystem and answer questions based on those files. You will only receive and work with handles, never the actual data.
        As a final result, you will only respond with JSON of the following form.
        {
            "handle": "The handle to the dataframe answering the question"
        }

        OR

        {
            "error": "The agent could not answer the question using the available tools, because <reason>."
        }

        You will only respond with the JSON, nothing else (no markdown, no code blocks, no other text).
        """,
        mcp_servers=[mcp_server],
    )

    result = await Runner.run(starting_agent=agent, input=query)
    print("Agent responses:", result.raw_responses)

    print(result.final_output)


    res = json.loads(result.final_output.strip())
    if 'error' in res:
        raise Exception(res['error'])
    try:
        handle = res['handle']
        if not handle:
            print("Error: No handle was returned by the agent.")
            return
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute("SELECT EXISTS(SELECT 1 FROM handles WHERE handle = ?)", (handle,))
        exists = cursor.fetchone()[0]
        conn.close()

        if not exists:
            print(f"Error: Invalid handle '{handle}' - not found in database.")
            return

        df = fetch_dataframe(handle)
        if df is not None:
            # Check if this is a schema result (has specific columns)
            if set(df.columns) == {'column', 'dtype'} and 'num_rows' in df:
                print("\nDataFrame Schema:")
                print(f"Number of rows: {df['num_rows'].iloc[0]}")
                print("\nColumns:")
                for _, row in df.drop('num_rows', axis=1).iterrows():
                    print(f"- {row['column']}: {row['dtype']}")
            else:
                print("\nDataFrame contents:")
                print(df)
        else:
            print("Error: Could not load DataFrame from handle.")
    except Exception as e:
        print(f"Error processing result: {str(e)}")

async def main(query: str):
    current_dir = os.path.dirname(os.path.abspath(__file__))

    async with MCPServerStdio(
        name="Filesystem Server, via uvx",
        params={
            "command": "uv",
            "cwd": current_dir,
            "args": ["run", "-m", "mcp_handles_server.df_abstractions"],
        },
    ) as server:
        await run(server, query)

def entrypoint(query: str):
    """
    Run the MCP server with the given query.
    """
    asyncio.run(main(query))

if __name__ == "__main__":
    Fire(entrypoint)
