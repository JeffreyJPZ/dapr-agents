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

from copy import deepcopy
from typing import Any, Callable, Type
import uuid

from pydantic import BaseModel, Field, PrivateAttr, create_model

from dapr.ext.workflow.mcp_schema import create_pydantic_model_from_schema

from dapr_agents.tool.base import AgentTool
from dapr_agents.tool.workflow import WorkflowContextInjectedTool


DRASI_INSTRUCTIONS_DESCRIPTION = (
    "Detailed, agent-authored instructions for what should happen "
    "after this Drasi subscription fires and a Drasi event is received. "
    "Be specific about the desired follow-up action, the relevant context, "
    "and any global rules, constraints, or thresholds you should remember "
    "when handling the event later. Write it as durable guidance for your "
    "future self, not as a short label."
)


def _clone_schema_with_instructions(
    args_model: Type[BaseModel] | None,
) -> Type[BaseModel]:
    """Clone an args schema and add the Drasi instructions field."""
    if args_model is None:
        schema: dict[str, Any] = {"type": "object", "properties": {}, "required": []}
        base_name = "DrasiWorkflowToolArgs"
    else:
        schema = deepcopy(args_model.model_json_schema())
        base_name = args_model.__name__

    properties = schema.get("properties")
    if not isinstance(properties, dict):
        properties = {}
        schema["properties"] = properties

    required = schema.get("required")
    if not isinstance(required, list):
        required = []
        schema["required"] = required

    properties["instructions"] = {
        "type": "string",
        "description": DRASI_INSTRUCTIONS_DESCRIPTION,
    }
    if "instructions" not in required:
        required.append("instructions")

    return create_pydantic_model_from_schema(
        schema, f"{base_name}WithDrasiInstructions"
    )


class DrasiWorkflowTool(WorkflowContextInjectedTool):
    """Workflow tool wrapper that injects Drasi runtime details for subscriptions."""

    # TODO: remove agent_name once registered agent workflow name is available
    agent_name: str
    topic: str

    _validation_args_model: Type[BaseModel] | None = PrivateAttr(default=None)

    def __init__(
        self,
        name: str,
        description: str,
        agent_name: str,
        topic: str,
        func: Callable[..., Any] | None = None,
        args_model: Type[BaseModel] | None = None,
    ) -> None:
        super().__init__(
            name=name,
            description=description,
            func=func,
            args_model=args_model,
        )
        self.agent_name = agent_name
        self.topic = topic

    # TODO: should this be in activation instead?
    # TODO: support unsubscribe
    @classmethod
    def from_agent_tool(
        cls,
        agent_tool: AgentTool,
        **kwargs,
    ) -> "DrasiWorkflowTool":
        """Build a Drasi workflow tool from an existing AgentTool."""
        # Create two separate models: one for validation (the original schema) and one for the exposed schema (with instructions added).
        validation_args_model = agent_tool.args_model or create_model(
            f"{agent_tool.name}Args"
        )
        exposed_args_model = _clone_schema_with_instructions(validation_args_model)

        tool = cls(
            name=agent_tool.name,
            description=agent_tool.description,
            agent_name=kwargs.get("agent_name"),
            topic=kwargs.get("topic"),
            func=agent_tool.func or agent_tool._run,
            args_model=exposed_args_model,
            source=getattr(agent_tool, "source", "mcp"),
        )
        tool._validation_args_model = validation_args_model
        return tool

    def _validate_and_prepare_args(
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> dict[str, Any]:
        """
        Remove the internal Drasi instructions before validating, then inject
        the runtime routing values the MCP server expects.
        """
        cleaned_kwargs = dict(kwargs)
        cleaned_kwargs.pop("instructions", None)

        # Accept nested instructions for tools that have a "kwargs" field
        if "kwargs" in cleaned_kwargs and isinstance(cleaned_kwargs["kwargs"], dict):
            inner_kwargs = dict(cleaned_kwargs["kwargs"])
            inner_kwargs.pop("instructions", None)
            cleaned_kwargs["kwargs"] = inner_kwargs

        # TODO: is mutating args_model the best way?
        exposed_args_model = self.args_model
        validation_args_model = self._validation_args_model

        try:
            if validation_args_model is not None:
                self.args_model = validation_args_model
            prepared_kwargs = super()._validate_and_prepare_args(
                func, *args, **cleaned_kwargs
            )
        finally:
            self.args_model = exposed_args_model

        # Inject agent_id and subscription_id from runtime
        # TODO: replace with registered agent workflow name
        prepared_kwargs["agent_id"] = f"dapr.agents.{self.agent_name}.workflow"
        # TODO: replace with context.new_guid() when available for replay safety
        prepared_kwargs["subscription_id"] = uuid.uuid4()
        prepared_kwargs["topic"] = self.topic
        return prepared_kwargs
