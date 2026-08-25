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
import inspect
import uuid
from copy import deepcopy
from typing import Any, Callable, Type

from pydantic import BaseModel, PrivateAttr, create_model

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


class DrasiTool(WorkflowContextInjectedTool):
    """Tool wrapper that injects workflow context for Drasi subscriptions."""

    _topic: str
    _validation_args_model: Type[BaseModel] | None = PrivateAttr(default=None)

    # TODO: should this be in activation instead?
    # TODO: support unsubscribe
    @classmethod
    def from_agent_tool(
        cls,
        tool: AgentTool,
        topic: str,
    ) -> "DrasiTool":
        """Build a Drasi tool from an existing AgentTool."""
        # Create two separate models: one for validation (the original schema)
        # and one for the exposed schema (with instructions added).
        validation_args_model = tool.args_model or create_model(
            f"{tool.name}Args"
        )
        exposed_args_model = _clone_schema_with_instructions(validation_args_model)

        # Tool function should not be ``None``
        assert tool.func is not None, "AgentTool must have a callable func to create a DrasiTool"
        drasi_tool = cls(
            name=tool.name,
            description=tool.description,
            func=cls._make_drasi_tool_func(tool.func),
            args_model=exposed_args_model,
            source=tool.source,
        )
        # TODO: move this to post-init?
        drasi_tool._is_async = inspect.iscoroutinefunction(drasi_tool.func)
        drasi_tool._topic = topic
        drasi_tool._validation_args_model = validation_args_model

        return drasi_tool

    @classmethod
    def _make_drasi_tool_func(cls, func: Callable[..., Any]) -> Callable[..., Any]:
        """
        Strip hidden kwargs on ``WorkflowContextInjectedTool`` before calling the tool function.
        Must return a sync callable as workflow context injected tools are not awaited.
        """
        def _clean_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
            cleaned_kwargs = dict(kwargs)
            cleaned_kwargs.pop("ctx", None)  # Can't use the context_kwarg field here
            cleaned_kwargs.pop("_source_agent", None)
            cleaned_kwargs.pop("_child_instance_id", None)
            return cleaned_kwargs

        if inspect.iscoroutinefunction(func):
            def wrapped(*args: Any, **kwargs: Any) -> Any:
                cleaned_kwargs = _clean_kwargs(kwargs)
                return asyncio.run(func(*args, **cleaned_kwargs))

            return wrapped

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **_clean_kwargs(kwargs))

        return wrapped

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
        assert prepared_kwargs.get("_source_agent") is not None, (
            "DrasiTool requires '_source_agent' to be provided in kwargs"
        )
        # TODO: make this configurable
        prepared_kwargs["agent_id"] = prepared_kwargs.get("_source_agent")
        # TODO: replace with context.new_guid() when available for replay safety
        prepared_kwargs["subscription_id"] = uuid.uuid4()
        prepared_kwargs["topic"] = self._topic

        return prepared_kwargs
