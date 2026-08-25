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
from typing import Any

from dapr_agents import AgentRunner
from dapr_agents.agents.schemas import TriggerAction
from dapr_agents.tool.base import AgentTool
from dapr_agents.tool.mcp import MCPClient

from dapr_agents.ext.drasi import DrasiChangeEvent, drasi_trigger

from agent import make_agent
from tools import DrasiTool

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

AGENT_PUBSUB_COMPONENT = os.getenv("AGENT_PUBSUB_COMPONENT", "agent-pubsub")
DAPR_HTTP_PORT = os.getenv("DAPR_HTTP_PORT", "3500")

DRASI_TOPIC = "drasi-events"


def make_task(event: DrasiChangeEvent, ctx: Any) -> TriggerAction:
    return TriggerAction(
        task=event.model_dump_json()
    )


async def _load_mcp_tools_from_client() -> list[AgentTool]:
    client = MCPClient()
    try:
        await client.connect_streamable_http(
            server_name="drasi-agent-router-mcp",
            url=f"http://localhost:{DAPR_HTTP_PORT}/v1.0/invoke/inventory-events-publisher-reaction.drasi-system/method/mcp",
        )
        tools = client.get_all_tools()
        logger.info(f"Loaded Drasi MCP tools: {[tool.name for tool in tools]}")
        return tools
    except Exception as exc:
        raise RuntimeError("Could not connect to MCP server") from exc
    finally:
        try:
            await client.close()
        except RuntimeError as exc:
            if "Attempted to exit cancel scope" not in str(exc):
                raise


def main() -> None:
    try:
        tools = asyncio.run(_load_mcp_tools_from_client())
    except Exception:
        logging.exception("Failed to load MCP tools via streamable HTTP")
        return

    asyncio.set_event_loop(asyncio.new_event_loop())

    # Convert to Drasi MCP tools
    tools = [
        DrasiTool.from_agent_tool(
            tool=tool,
            topic=DRASI_TOPIC,  # TODO: wire this up in activation
        )
        if tool.name == "drasi-agent-router-mcp_subscribe_drasi_query"  # TODO: support unsubscribe
        else tool
        for tool in tools
    ]

    agent = make_agent(tools=tools)

    # Register Drasi query subscriptions
    # TODO: we don't want static subscriptions, events should arrive through the Drasi inbox
    drasi_trigger(
        agent,
        query_id="critical-stock-event-query",
        topic=DRASI_TOPIC,
        task_mapper=make_task,
        operations=["i", "u", "d"],
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
