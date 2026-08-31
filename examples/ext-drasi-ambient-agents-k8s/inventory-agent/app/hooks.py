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

from utils import (
    AGENT_ID,
    AGENT_MEMORY_COMPONENT,
    build_subscription_instructions_key,
    write_state_value,
)

logger = logging.getLogger(__name__)


# TODO: are hooks sufficient for this? or should we move this to the tool?
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

    query_id = ctx.payload.get("query_id")
    subscription_id = "inventory-agent"  # TODO: remove hardcoded subscription ID as its injected by the tool

    key = build_subscription_instructions_key(
        f"{query_id}:{AGENT_ID}:{subscription_id}"
    )

    logger.info(
        f"Agent '{AGENT_ID}' subscribing to query '{query_id}' with "
        f"instructions: {instructions} (key={key})"
    )

    write_state_value(
        AGENT_MEMORY_COMPONENT,
        key,
        {"instructions": instructions},
    )

    return Proceed()
