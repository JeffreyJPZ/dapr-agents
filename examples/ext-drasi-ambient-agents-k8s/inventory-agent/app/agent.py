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

import logging
import os
from typing import Any

from dapr_agents import DurableAgent, DaprChatClient
from dapr_agents.agents.configs import (
    AgentExecutionConfig,
    AgentMemoryConfig,
    AgentProfileConfig,
    AgentPubSubConfig,
    AgentStateConfig,
    RuntimeConfigKey,
    RuntimeSubscriptionConfig,
)
from dapr_agents.hooks import Hooks
from dapr_agents.memory import ConversationDaprStateMemory
from dapr_agents.storage.daprstores.stateservice import StateStoreService

from hooks import inject_subscribe_tool_params

logger = logging.getLogger(__name__)

AGENT_CONFIGURATION_COMPONENT = os.getenv(
    "AGENT_CONFIGURATION_COMPONENT", "agent-configuration"
)
AGENT_CONVERSATION_COMPONENT = os.getenv("AGENT_CONVERSATION_COMPONENT", "agent-llm")
AGENT_PUBSUB_COMPONENT = os.getenv("AGENT_PUBSUB_COMPONENT", "agent-pubsub")
AGENT_MEMORY_COMPONENT = os.getenv("AGENT_MEMORY_COMPONENT", "agent-memory")
AGENT_RUNTIME_COMPONENT = os.getenv("AGENT_RUNTIME_COMPONENT", "agent-runtime")

INSTRUCTIONS = [
    "You are an expert inventory replenishment and procurement assistant.",
    "You operate in an event-driven environment where stock-related events trigger your behavior.",
    "Your primary responsibility is to generate accurate, policy-compliant purchase orders (POs) in response to inventory stock events.",
    "You receive events indicating low stock levels or critical stock levels for products, and you must determine the appropriate quantity to order based on the event details and any relevant business rules.",
    "For low stock events, you should order a quantity that is sufficient to replenish the stock without overstocking, taking into account the current stock level and a reasonable multiplier.",
    "For critical stock events, you should order a larger quantity to quickly replenish the stock, using a higher multiplier to ensure that the stock level is restored to a safe threshold.",
]


def _on_config_change(key: str, value: Any) -> None:
    """Callback invoked after each successful config update."""
    logger.info(f"Configuration changed: {key} = {value}")


def _agent_profile() -> AgentProfileConfig:
    """Build the base agent persona, including a tagged slot for Drasi updates."""
    return AgentProfileConfig(
        name="InventoryAgent",
        role="Expert procurement assistant capable of reacting to stock events",
        goal="Create purchase orders for stock based on stock events.",
        instructions=[
            *INSTRUCTIONS,
            "<drasi-instructions>",
            "Drasi-generated instructions will be inserted here.",
            "</drasi-instructions>",
        ],
    )


def make_agent(
    tools: list[Any] | None = None,
) -> DurableAgent:
    return DurableAgent(
        profile=_agent_profile(),
        llm=DaprChatClient(component_name=AGENT_CONVERSATION_COMPONENT),
        tools=tools,
        memory=AgentMemoryConfig(
            store=ConversationDaprStateMemory(store_name=AGENT_MEMORY_COMPONENT),
        ),
        pubsub=AgentPubSubConfig(
            pubsub_name=AGENT_PUBSUB_COMPONENT,
            agent_topic="inventory-agent",
        ),
        state=AgentStateConfig(
            store=StateStoreService(store_name=AGENT_RUNTIME_COMPONENT),
        ),
        configuration=RuntimeSubscriptionConfig(
            store_name=AGENT_CONFIGURATION_COMPONENT,
            keys=[RuntimeConfigKey.AGENT_INSTRUCTIONS],
            on_config_change=_on_config_change,
        ),
        execution=AgentExecutionConfig(max_iterations=10),
        hooks=Hooks(
        #     before_llm_call=[inject_drasi_event_handling_instructions],
            before_tool_call=[inject_subscribe_tool_params],
        ),
    )
