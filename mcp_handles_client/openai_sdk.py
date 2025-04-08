import asyncio
import os
import shutil

from agents import Agent, Runner, gen_trace_id, trace
from agents.mcp import MCPServer, MCPServerStdio

from fire import Fire

async def run(mcp_server: MCPServer, query: str):
    agent = Agent(
        name="Assistant",
        instructions="Use the tools to read the filesystem and answer questions based on those files.",
        mcp_servers=[mcp_server],
    )

    result = await Runner.run(starting_agent=agent, input=query)
    print(result.raw_responses)
    print(result.final_output)


async def main(query: str):
    current_dir = os.path.dirname(os.path.abspath(__file__))

    async with MCPServerStdio(
        name="Filesystem Server, via uvx",
        params={
            "command": "uv",
            "cwd": current_dir,
            "args": ["run", current_dir + "/../mcp_handles_server/df_abstractions.py"],
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