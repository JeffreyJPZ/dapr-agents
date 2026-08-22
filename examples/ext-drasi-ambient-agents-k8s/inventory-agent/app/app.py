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

from dapr.ext.workflow import DaprMCPClient, MCPToolDef
from dapr.ext.workflow.mcp_schema import create_pydantic_model_from_schema

from dapr_agents import AgentRunner
from dapr_agents.agents.schemas import TriggerAction
from dapr_agents.tool.base import AgentTool
from dapr_agents.tool.mcp import MCPClient

from dapr_agents.ext.drasi import DrasiChangeEvent, drasi_trigger

from agent import make_agent

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

AGENT_MCP_COMPONENT = os.getenv("AGENT_MCP_COMPONENT", "agent-mcp")
DAPR_HTTP_PORT = os.getenv("DAPR_HTTP_PORT", "3500")


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

# TODO: figure out how to support MCPServer resource
# TODO: this should be internal
# def _sanitize_drasi_mcp_tools_from_mcpserver(tools: list[MCPToolDef]) -> list[MCPToolDef]:
#     """Adjust the Drasi MCP tool schemas used."""
#     for tool in tools:
#         input_schema = tool.input_schema
#         if not isinstance(input_schema, dict):
#             continue

#         properties = input_schema.get("properties")
#         if not isinstance(properties, dict):
#             properties = {}
#             input_schema["properties"] = properties

#         required = input_schema.get("required")
#         if not isinstance(required, list):
#             required = []
#             input_schema["required"] = required

#         # TODO: remove
#         if tool.name == "subscribe_drasi_query":
#             properties["instructions"] = {
#                 "type": "string",
#                 "description": (
#                     "Detailed, agent-authored instructions for what should happen "
#                     "after this Drasi subscription fires and a Drasi event is received."
#                     "Be specific about the desired follow-up action, the relevant context, "
#                     "and any global rules, constraints, or thresholds "
#                     "you should remember when handling the event later. "
#                     "Write it as durable guidance for your future self, "
#                     "not as a short label."
#                 ),
#             }
#             if "instructions" not in required:
#                 required.append("instructions")

#         if tool.name == "subscribe_drasi_query":
#             properties.pop("topic", None)
#             properties.pop("agent_id", None)
#             required[:] = [field for field in required if field not in {"topic", "agent_id"}]
#         elif tool.name == "unsubscribe_drasi_query":
#             properties.pop("agent_id", None)
#             properties.pop("subscription_id", None)
#             required[:] = [
#                 field for field in required if field not in {"agent_id", "subscription_id"}
#             ]

#     return tools


# async def _load_mcp_tools_from_mcpserver() -> list[MCPToolDef]:
#     client = DaprMCPClient()
#     try:
#         await client.connect(AGENT_MCP_COMPONENT)
#         tools = _sanitize_drasi_mcp_tools_from_mcpserver(client.get_all_tools())
#         logger.info(f"Loaded Drasi MCP tools: {[tool.name for tool in tools]}")
#         return tools
#     except Exception as exc:
#         raise RuntimeError(f"Could not connect to server '{AGENT_MCP_COMPONENT}") from exc


def _sanitize_drasi_mcp_tools_from_client(tools: list[AgentTool]) -> list[AgentTool]:
    """Adjust the Drasi AgentTool schemas used."""
    for tool in tools:
        args_model = getattr(tool, "args_model", None)
        if args_model is None:
            continue

        try:
            input_schema = args_model.model_json_schema()
        except Exception:
            continue

        properties = input_schema.get("properties")
        if not isinstance(properties, dict):
            properties = {}
            input_schema["properties"] = properties

        required = input_schema.get("required")
        if not isinstance(required, list):
            required = []
            input_schema["required"] = required

        if tool.name == "drasi-agent-router-mcp_subscribe_drasi_query":
            properties["instructions"] = {
                "type": "string",
                "description": (
                    "Detailed, agent-authored instructions for what should happen "
                    "after this Drasi subscription fires and a Drasi event is received. "
                    "Be specific about the desired follow-up action, the relevant context, "
                    "and any global rules, constraints, or thresholds you should remember "
                    "when handling the event later. Write it as durable guidance for your "
                    "future self, not as a short label."
                ),
            }
            if "instructions" not in required:
                required.append("instructions")

            properties.pop("topic", None)
            properties.pop("agent_id", None)
            required[:] = [field for field in required if field not in {"topic", "agent_id"}]
        elif tool.name == "drasi-agent-router-mcp_unsubscribe_drasi_query":
            properties.pop("agent_id", None)
            properties.pop("subscription_id", None)
            required[:] = [
                field for field in required if field not in {"agent_id", "subscription_id"}
            ]

        tool.args_model = create_pydantic_model_from_schema(
            input_schema, f"{tool.name}Args"
        )

    return tools


async def _load_mcp_tools_from_client() -> list[AgentTool]:
    client = MCPClient()
    try:
        await client.connect_streamable_http(
            server_name="drasi-agent-router-mcp",
            url=f"http://localhost:{DAPR_HTTP_PORT}/v1.0/invoke/inventory-events-publisher-reaction.drasi-system/method/mcp",
        )
        tools = client.get_all_tools()
        tools = _sanitize_drasi_mcp_tools_from_client(tools)
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

    agent = make_agent(tools=tools)

    # Register Drasi query subscriptions
    # TODO: we don't want static subscriptions, events should arrive through the agent's inbox
    # need a way to accept Drasi payloads as agent inputs
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
        runner.serve(agent)
    finally:
        runner.shutdown(agent)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
