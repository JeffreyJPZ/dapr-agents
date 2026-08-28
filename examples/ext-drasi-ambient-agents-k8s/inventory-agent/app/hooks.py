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

from dapr_agents.hooks import (
    HookDecision,
    Proceed,
    ToolHookContext,
)

from utils import AGENT_ID, AGENT_MEMORY_COMPONENT, write_state_value

logger = logging.getLogger(__name__)

AGENT_INSTRUCTIONS_KEY = "agent_instructions"
AGENT_DRASI_INSTRUCTIONS_START_TAG = "<drasi-instructions>"
AGENT_DRASI_INSTRUCTIONS_END_TAG = "</drasi-instructions>"
SUBSCRIPTIONS_KEY_PREFIX = "drasi_subscriptions"


# TODO: are hooks sufficient for this?
def persist_task_instructions_from_drasi_subscription(
    ctx: ToolHookContext,
) -> HookDecision:
    """
    Persist the Drasi-generated task instructions so the trigger mapper can
    recover them when a matching CloudEvent arrives.
    """
    if ctx.step_name != "subscribe_drasi_query":
        return Proceed()

    instructions = ctx.payload.get("instructions")
    if instructions is None:
        return Proceed()

    subscription_id = ctx.payload.get("subscription_id")
    query_id = ctx.payload.get("query_id") or ctx.payload.get("queryId")

    if isinstance(subscription_id, str) and subscription_id:
        subscription_key = f"{AGENT_ID}:{subscription_id}"
        if isinstance(query_id, str) and query_id:
            subscription_key = f"{query_id}:{subscription_key}"
    else:
        subscription_key = AGENT_ID

    logger.info(
        "Injecting Drasi subscription tool parameters for agent '%s' with "
        "instructions: %s",
        AGENT_ID,
        instructions,
    )

    write_state_value(
        AGENT_MEMORY_COMPONENT,
        subscription_key,
        {"instructions": instructions},
    )

    return Proceed()
