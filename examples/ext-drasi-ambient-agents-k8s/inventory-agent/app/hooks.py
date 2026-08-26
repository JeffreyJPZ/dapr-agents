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
import logging
import os

from dapr.clients import DaprClient

from dapr_agents.hooks import (
    HookDecision,
    LLMHookContext,
    Mutate,
    Proceed,
    ToolHookContext,
)

logger = logging.getLogger(__name__)

AGENT_CONFIGURATION_COMPONENT = os.getenv(
    "AGENT_CONFIGURATION_COMPONENT", "agent-configuration"
)

AGENT_ID = "inventory-agent"
AGENT_INSTRUCTIONS_KEY = "agent_instructions"
AGENT_DRASI_INSTRUCTIONS_START_TAG = "<drasi-instructions>"
AGENT_DRASI_INSTRUCTIONS_END_TAG = "</drasi-instructions>"
SUBSCRIPTION_ID = "low-stock-event-query"
SUBSCRIPTIONS_KEY_PREFIX = "drasi_subscriptions"
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


def _write_state_value(store_name: str, key: str, value: object) -> None:
    """Persist a JSON-serializable value through Dapr state."""
    with DaprClient() as client:
        client.save_state(
            store_name=store_name,
            key=key,
            value=json.dumps(value),
            state_metadata={"contentType": "application/json"},
        )


def _load_instructions_from_store(store_name: str, key: str) -> list[str]:
    """Load the current agent instructions from the config store."""
    with DaprClient() as client:
        raw_state = client.get_state(store_name=store_name, key=key)
    state_value = _read_state_value(raw_state)
    if isinstance(state_value, dict):
        instructions = state_value.get("instructions")
        if isinstance(instructions, list):
            return [str(item) for item in instructions]
        if isinstance(instructions, str):
            return [line for line in instructions.splitlines() if line.strip()]
        return []
    if isinstance(state_value, list):
        return [str(item) for item in state_value]
    if isinstance(state_value, str):
        return [line for line in state_value.splitlines() if line.strip()]
    return []


def _serialize_instructions(
    instructions: list[str], generated_instructions: object
) -> list[str]:
    """Replace or append the Drasi instruction block."""
    if isinstance(generated_instructions, list):
        generated_text = "\n".join(str(item) for item in generated_instructions)
    elif isinstance(generated_instructions, str):
        generated_text = generated_instructions
    else:
        generated_text = json.dumps(generated_instructions, ensure_ascii=False)
    generated_lines = [line for line in generated_text.splitlines() if line.strip()]

    start_index = None
    end_index = None
    for index, line in enumerate(instructions):
        if line.strip() == AGENT_DRASI_INSTRUCTIONS_START_TAG:
            start_index = index
        if line.strip() == AGENT_DRASI_INSTRUCTIONS_END_TAG:
            end_index = index
            break

    if start_index is None or end_index is None or end_index <= start_index:
        return [
            *instructions,
            AGENT_DRASI_INSTRUCTIONS_START_TAG,
            *generated_lines,
            AGENT_DRASI_INSTRUCTIONS_END_TAG,
        ]

    return [
        *instructions[: start_index + 1],
        *generated_lines,
        *instructions[end_index:],
    ]


def update_agent_instructions_from_drasi_subscription(
    ctx: ToolHookContext,
) -> HookDecision:
    """
    Update the agent instructions in the configuration store on a Drasi subscription so the agent can hot-reload them.
    """
    if ctx.step_name != "subscribe_drasi_query":
        return Proceed()

    # TODO: this should be from the runtime
    configuration_store_name = AGENT_CONFIGURATION_COMPONENT
    instructions = ctx.payload.get("instructions")

    logger.info(
        f"Injecting Drasi subscription tool parameters for agent '{AGENT_ID}' with instructions: {instructions}"
    )

    if instructions is not None:
        current_instructions = _load_instructions_from_store(
            configuration_store_name, AGENT_INSTRUCTIONS_KEY
        )

        logger.info(
            f"Current agent instructions in store '{configuration_store_name}' for key '{AGENT_INSTRUCTIONS_KEY}': {current_instructions}"
        )

        updated_instructions = _serialize_instructions(
            current_instructions, instructions
        )

        logger.info(
            f"Updating agent instructions in store '{configuration_store_name}' for key '{AGENT_INSTRUCTIONS_KEY}' with updated instructions: {updated_instructions}"
        )

        _write_state_value(
            configuration_store_name, AGENT_INSTRUCTIONS_KEY, updated_instructions
        )

        logger.info(
            f"Agent instructions updated successfully in store '{configuration_store_name}' for key '{AGENT_INSTRUCTIONS_KEY}'."
        )

    return Proceed()
