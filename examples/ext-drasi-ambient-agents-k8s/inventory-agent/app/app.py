#
# Copyright 2026 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

import asyncio
import logging
import os

from dapr.ext.workflow import DaprMCPClient

from dapr_agents import AgentTool, AgentRunner

from dapr_agents.ext.drasi import enable_drasi, DrasiWorkflowTool

from agent import make_agent

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

AGENT_MCP_COMPONENT = os.getenv("AGENT_MCP_COMPONENT", "agent-mcp")
AGENT_PUBSUB_COMPONENT = os.getenv("AGENT_PUBSUB_COMPONENT", "agent-pubsub")
DRASI_TOPIC = "drasi-events"

async def _load_mcp_tools() -> list[AgentTool]:
    """Load MCP tools and wrap the Drasi subscription tool."""
    client = DaprMCPClient()
    try:
        await client.connect(AGENT_MCP_COMPONENT)
        tools: list[AgentTool] = []
        for tool_def in client.get_all_tools():
            if tool_def.name == "subscribe_drasi_query":
                # TODO: support unsubscribe and list queries
                tool = DrasiWorkflowTool.from_mcp_tool_def(
                    tool_def,
                    topic=DRASI_TOPIC,  # TODO: can we delay this?
                )
            tools.append(tool)
        logger.info(
            f"Loaded Drasi MCP tools from '{AGENT_MCP_COMPONENT}': "
            f"{[tool.name for tool in tools]}"
        )
        return tools
    except Exception as exc:
        logger.exception(f"Failed to load MCP tools from '{AGENT_MCP_COMPONENT}'")
        raise RuntimeError(
            f"Could not load MCP tools from server '{AGENT_MCP_COMPONENT}'"
        ) from exc


def main() -> None:
    tools = asyncio.run(_load_mcp_tools())

    # Get a fresh event loop
    asyncio.set_event_loop(asyncio.new_event_loop())

    agent = make_agent(tools=tools)

    enable_drasi(
        agent,
        mcp_server=AGENT_MCP_COMPONENT,
        pubsub=AGENT_PUBSUB_COMPONENT,
        topic=DRASI_TOPIC,
    )

    runner = AgentRunner()
    try:
        runner.serve(agent)
    finally:
        runner.shutdown(agent)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
