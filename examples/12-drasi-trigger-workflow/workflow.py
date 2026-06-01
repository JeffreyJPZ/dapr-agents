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

from dapr.ext.workflow import DaprWorkflowContext
from dotenv import load_dotenv

from dapr_agents.llm.dapr import DaprChatClient
from dapr_agents.workflow.decorators.decorators import message_router

from models import DrasiUnpackedEvent

load_dotenv()


# Initialize the LLM client and workflow runtime
llm = DaprChatClient(component_name="openai")


@message_router(
    pubsub="notifications-pubsub", topic="low-stock-events", message_model=DrasiUnpackedEvent
)
def low_stock_order_workflow(ctx: DaprWorkflowContext, wf_input: dict) -> str:
    op = wf_input["op"]

    if op == "i":
        item = wf_input["payload"]["after"]

        count = yield ctx.call_activity(create_order_count, input={"stock_level": item["stockLevel"]})
        order = yield ctx.call_activity(create_order, input={"item": item, "count": count})

        return order

    return "Low stock event received, no action needed"


@message_router(
    pubsub="notifications-pubsub", topic="critical-stock-events", message_model=DrasiUnpackedEvent
)
def critical_stock_order_workflow(ctx: DaprWorkflowContext, wf_input: dict) -> str:
    op = wf_input["op"]

    if op == "i":
        item = wf_input["payload"]["after"]

        count = yield ctx.call_activity(create_order_count, input={"stock_level": item["stockLevel"]})
        order = yield ctx.call_activity(create_order, input={"item": item, "count": count})

        return order

    return "Critical stock event received, no action needed"


def create_order_count(ctx, input: Dict[str, Any]) -> int:
    import random

    stock_level = input["stock_level"]

    if stock_level == 0:
        return 100

    return random.randint(1, 10) * stock_level


def create_order(ctx, input: Dict[str, Any]) -> str:
    item = input["item"]
    count = input["count"]

    return str(
        llm.generate(
            messages=(
                f"Create an inventory order of {count} units for the item:\n{item}.\n"
                f"Only output the item ID, the previous item count, and the new item count after the units are added in the format:\n"
                f"Item ID: <item_id>\nPrevious count: <previous_count>\nNew count: <new_count>\n"
            )
        )
    )
