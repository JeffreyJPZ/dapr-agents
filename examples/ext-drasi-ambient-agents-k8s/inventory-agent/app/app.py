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

from dapr.ext.workflow import DaprMCPClient

from dapr_agents import AgentRunner
from dapr_agents.agents.schemas import TriggerAction
from dapr_agents.hooks import Hooks

from dapr_agents.ext.drasi import DrasiChangeEvent, drasi_trigger

from agent import make_agent
from hooks import inject_subscribe_tool_params, wait_for_drasi_event

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

DAPR_HTTP_PORT = os.getenv("DAPR_HTTP_PORT", 3500)


async def _load_mcp_tools() -> list:
    client = DaprMCPClient()
    mcpserver_name = "agent-mcp"
    try:
        await client.connect(mcpserver_name)
        # TODO: sanitize topic and agent ID tool args
        return client.get_all_tools()
    except Exception as exc:
        raise RuntimeError(f"Could not connect to server '{mcpserver_name}") from exc


def make_task(event: DrasiChangeEvent, ctx: Any) -> TriggerAction:
    return TriggerAction(
        task=(
            f"You are an inventory agent that creates purchase orders, calculating the order quantity dynamically.\n"
            f"Create a purchase order for this '{event.payload.source.queryId}' event.\n"
            f"Use the following data:\n\n"
            f"Stock before: {event.payload.before.model_dump_json() if event.payload.before else 'N/A'}.\n"
            f"Stock after: {event.payload.after.model_dump_json() if event.payload.after else 'N/A'}.\n\n"
            "Respond with exactly the following format, and nothing else:\n\n"
            "Product ID: <productId>\n"
            "Product Name: <productName>\n"
            "Product Description: <productDescription>\n"
            "Order Quantity: <quantity>\n\n"
            "Rules:\n"
            "- Output exactly these 4 lines, in this exact order.\n"
            "- Do not add, remove, rename, or reorder any fields.\n"
            "- Do not include any explanation, preamble, or extra text.\n"
            "- Do not wrap the output in code blocks or markdown formatting.\n"
            "- Replace each <placeholder> with the actual value only — do not include the angle brackets."
        )
    )


def main() -> None:
    try:
        tools = asyncio.run(_load_mcp_tools())
    except Exception:
        logging.exception("Failed to load MCP tools via streamable HTTP")
        return

    asyncio.set_event_loop(asyncio.new_event_loop())

    agent = make_agent(
        tools=tools,
        hooks=Hooks(
            before_llm_call=[wait_for_drasi_event],
            before_tool_call=[inject_subscribe_tool_params],
        )
    )

    # Register Drasi query subscriptions
    drasi_trigger(
        agent,
        query_id="critical-stock-event-query",
        task_mapper=make_task,
        operations=["i", "u", "d"],
    )
    drasi_trigger(
        agent,
        query_id="low-stock-event-query",
        task_mapper=make_task,
        operations=["i", "u", "d"],
    )

    runner = AgentRunner()
    try:
        runner.subscribe(agent)
    finally:
        runner.shutdown(agent)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
