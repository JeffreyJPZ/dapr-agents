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

from dapr_agents.hooks import (
    HookDecision,
    LLMHookContext,
    Mutate,
    Proceed,
    RequireApproval,
    ToolHookContext,
)


def wait_for_drasi_event(ctx: LLMHookContext) -> HookDecision:
    """Decide whether to wait for a Drasi event (currently runs if any subscription exists)."""
    messages = ctx.payload.get("messages", [])
    if not messages:
        return Proceed()

    # TODO: check if any instructions exists

    # TODO: skip if Drasi is not enabled/wired properly

    # TODO: retrieve instructions/replace with hardcoded for now
    instructions = "foo"
    # TODO: yield and wait for data
    data = {"foo": "bar"}
    enriched_messages = [
        *messages,
        {
            "role": "system",
            "content": (
                f"{instructions}\n\n"
            ),
        },
        {
            "role": "system",  # TODO: is this role correct
            "content": (
                f"{data}\n\n"
            ),
        },
    ]

    return Mutate(payload={"messages": enriched_messages})


def inject_subscribe_tool_params(ctx: ToolHookContext) -> HookDecision:
    """Inject the Drasi subscribe tool parameters into the LLM context."""
    # TODO: update tool name depending on what we end up naming the Drasi tool
    if ctx.step_name != "subscribe":
        return Proceed()

    # TODO: check if Drasi is enabled/wired properly

    new_payload = dict(ctx.payload)
    # Persist instructions and scrub them from the new payload
    # TODO: persist instructions — where should dapr client come from
    new_payload.pop("instructions", None)

    # TODO: get these from the runtime instead of hardcoding
    new_payload["agent_id"] = "inventory-agent"
    new_payload["topic"] = "drasi-events-low-stock-event-query"

    return Mutate(payload=new_payload)
