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
from typing import Any

from dapr_agents import AgentRunner
from dapr_agents.agents.schemas import TriggerAction
from dapr_agents.tool.mcp import mcp_tool_def_to_workflow_tool
from dapr.ext.workflow.aio import DaprMCPClient

from dapr_agents.ext.drasi import DrasiChangeEvent, drasi_trigger

from agent import make_agent
from tools import DrasiWorkflowTool

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

DRASI_TOPIC = "drasi-events"


def make_task(event: DrasiChangeEvent, ctx: Any) -> TriggerAction:
    return TriggerAction(task=event.model_dump_json())


async def _load_mcp_tools() -> list[DrasiWorkflowTool]:
    client = DaprMCPClient()
    try:
        await client.connect("agent-mcp")
        tools = []
        for tool_def in client.get_all_tools():
            tool = mcp_tool_def_to_workflow_tool(tool_def)
            # TODO: support unsubscribe
            if tool_def.name == "subscribe_drasi_query":
                tool = DrasiWorkflowTool.to_drasi_workflow_tool(tool, topic=DRASI_TOPIC)
            tools.append(tool)
        logger.info(f"Loaded Drasi MCP tools: {[tool.name for tool in tools]}")
        return tools
    except Exception as exc:
        raise RuntimeError("Could not connect to MCP server") from exc


def main() -> None:
    try:
        tools = asyncio.run(_load_mcp_tools())
    except Exception:
        logging.exception("Failed to load MCP tools via streamable HTTP")
        return

    # Get a fresh event loop
    asyncio.set_event_loop(asyncio.new_event_loop())

    agent = make_agent(tools=tools)

    # Register Drasi query subscriptions
    # TODO: we don't want static subscriptions, events should arrive through the Drasi inbox
    drasi_trigger(
        agent,
        query_id="low-stock-event-query",
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
