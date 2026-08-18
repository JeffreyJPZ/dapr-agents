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

import json
import os

from dapr.clients import DaprClient

from dapr_agents.hooks import (
    HookDecision,
    LLMHookContext,
    Mutate,
    Proceed,
    ToolHookContext,
)

AGENT_MEMORY_COMPONENT = os.getenv("AGENT_MEMORY_COMPONENT", "agent-memory")
AGENT_ID = "inventory-agent"
SUBSCRIPTION_ID = "low-stock-event-query"
SUBSCRIPTIONS_KEY_PREFIX = "drasi-subscriptions"
SUBSCRIPTION_INSTRUCTIONS_KEY_PREFIX = "subscription_instructions_"


def _read_state_value(response: object) -> object | None:
    if response is None:
        return None
    if hasattr(response, "json"):
        try:
            return response.json()
        except Exception:
            pass
    data = getattr(response, "data", None)
    if data is None:
        return None
    if isinstance(data, bytes):
        text = data.decode("utf-8")
    else:
        text = str(data)
    try:
        return json.loads(text)
    except Exception:
        return text


def wait_for_drasi_event(ctx: LLMHookContext) -> HookDecision:
    """
    Injects instructions for Drasi events
    and determines whether to wait for a Drasi event (currently runs if any subscription exists).
    """
    messages = ctx.payload.get("messages", [])
    if not messages:
        return Proceed()

    # Get instructions keyed by agent ID (TODO: should be obtained from the runtime)
    with DaprClient() as client:
        agent_state = _read_state_value(
            client.get_state(store_name=AGENT_MEMORY_COMPONENT, key=f"{SUBSCRIPTIONS_KEY_PREFIX}{AGENT_ID}")
        )

        if not isinstance(agent_state, dict):
            return Proceed()

        subscription_ids = agent_state.get("subscription_ids")
        if not isinstance(subscription_ids, list) or not subscription_ids:
            return Proceed()

        # TODO: replace this, this is ok for now since we only assume one subscription
        subscription_id = subscription_ids[0]
        if not subscription_id:
            return Proceed()

        instruction_raw_state = client.get_state(
            store_name=AGENT_MEMORY_COMPONENT,
            key=f"{SUBSCRIPTION_INSTRUCTIONS_KEY_PREFIX}{subscription_id}",
        )
        instruction_state = _read_state_value(instruction_raw_state)

    if isinstance(instruction_state, dict):
        instructions = instruction_state.get("instructions")
    else:
        instructions = instruction_state

    if not instructions:
        return Proceed()

    # Enrich messages with instructions for how to handle Drasi events
    messages.append(
        {
            "role": "system",
            "content": (
                "Use the following instructions for the Drasi subscription:\n\n"
                f"{instructions}"
            ),
        }
    )

    return Mutate(payload={"messages": messages})


def inject_subscribe_tool_params(ctx: ToolHookContext) -> HookDecision:
    """Inject the Drasi subscribe tool parameters into the LLM context."""
    if ctx.step_name != "subscribe":
        return Proceed()

    new_payload = dict(ctx.payload)

    # Persist instructions for the LLM so it knows how to act on the Drasi event
    instructions = new_payload.get("instructions")
    if instructions is not None:
        with DaprClient() as client:
            agent_raw_state = client.get_state(store_name=AGENT_MEMORY_COMPONENT, key=f"{SUBSCRIPTIONS_KEY_PREFIX}{AGENT_ID}")
            agent_state = _read_state_value(agent_raw_state)
            if not isinstance(agent_state, dict):
                agent_state = {}

            # TODO: subscription id should be persisted after tool call but currently not supported
            # ok for now since we only have one subscription
            subscription_ids = agent_state.get("subscription_ids")
            if not isinstance(subscription_ids, list):
                subscription_ids = []
            if SUBSCRIPTION_ID not in subscription_ids:
                subscription_ids.append(SUBSCRIPTION_ID)
            agent_state["subscription_ids"] = subscription_ids

            client.save_state(
                store_name=AGENT_MEMORY_COMPONENT,
                key=f"drasi-events-{AGENT_ID}",
                value=json.dumps(agent_state),
                state_metadata={"contentType": "application/json"},
            )
            client.save_state(
                store_name=AGENT_MEMORY_COMPONENT,
                key=f"{SUBSCRIPTION_INSTRUCTIONS_KEY_PREFIX}{SUBSCRIPTION_ID}",
                value=json.dumps({"instructions": instructions}),
                state_metadata={"contentType": "application/json"},
            )

    # Scrub instructions as they are an internal detail
    new_payload.pop("instructions", None)

    # Inject the agent ID and topic into the tool call payload
    # TODO: these should be from the runtime
    new_payload["agent_id"] = AGENT_ID
    new_payload["topic"] = "drasi-events-low-stock-event-query"

    return Mutate(payload=new_payload)
