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

import logging
import uuid
from copy import deepcopy
from typing import Any, Callable, Type

from pydantic import BaseModel, PrivateAttr

from dapr.ext.workflow.mcp_schema import create_pydantic_model_from_schema

from dapr_agents.tool.workflow import WorkflowContextInjectedTool
from dapr_agents.types import ToolError

logger = logging.getLogger(__name__)

DRASI_INSTRUCTIONS_DESCRIPTION = (
    "Detailed, agent-authored instructions for what should happen "
    "after this Drasi subscription fires and a Drasi event is received. "
    "Be specific about the desired follow-up action, the relevant context, "
    "and any global rules, constraints, or thresholds you should remember "
    "when handling the event later. Write it as durable guidance for your "
    "future self, not as a short label."
)


class DrasiWorkflowTool(WorkflowContextInjectedTool):
    """A WorkflowContextInjectedTool that is aware of Drasi-specific routing metadata."""

    _topic: str = PrivateAttr()
    # Reference to the original args_model matching the original MCP tool schema
    _validation_args_model: Type[BaseModel] = PrivateAttr()
    # Reference to the args_model that is exposed to the LLM, which includes an ``instructions`` param
    _exposed_args_model: Type[BaseModel] = PrivateAttr()

    @classmethod
    def to_drasi_workflow_tool(
        cls,
        tool: WorkflowContextInjectedTool,
        *,
        topic: str,  # TODO: should this even be passed here?
    ) -> "DrasiWorkflowTool":
        """Wrap a workflow-native MCP tool so it can carry Drasi routing metadata."""
        if tool.func is None:
            raise ToolError(
                f"Tool '{tool.name}' must define a callable function before wrapping"
            )
        if tool.args_model is None:
            raise ToolError(
                f"Tool '{tool.name}' must define an args model before wrapping"
            )

        validation_args_model = tool.args_model
        exposed_args_model = cls._clone_schema_with_instructions(validation_args_model)
        wrapped_tool = cls(
            name=tool.name,
            description=tool.description,
            func=tool.func,
            args_model=exposed_args_model,
            source=tool.source,
        )
        # Set private attributes
        wrapped_tool._topic = topic
        wrapped_tool._validation_args_model = validation_args_model
        wrapped_tool._exposed_args_model = exposed_args_model

        logger.debug(
            "Wrapped workflow tool '%s' for Drasi topic '%s'", tool.name, topic
        )

        return wrapped_tool

    @classmethod
    def _clone_schema_with_instructions(
        cls,
        args_model: Type[BaseModel] | None,
    ) -> Type[BaseModel]:
        """Clone an args schema and add the Drasi instructions field."""
        if args_model is None:
            schema: dict[str, Any] = {
                "type": "object",
                "properties": {},
                "required": [],
            }
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

        return create_pydantic_model_from_schema(schema, f"{base_name}WithInstructions")

    def _clean_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Strip LLM-only fields from a ``kwargs`` dict."""
        cleaned_kwargs = dict(kwargs)
        cleaned_kwargs.pop("instructions", None)

        if "kwargs" in cleaned_kwargs and isinstance(cleaned_kwargs["kwargs"], dict):
            inner_kwargs = dict(cleaned_kwargs["kwargs"])
            inner_kwargs.pop("instructions", None)
            cleaned_kwargs["kwargs"] = inner_kwargs

        return cleaned_kwargs

    def _build_subscription_id(self, agent_id: str) -> str:
        """Build a deterministic subscription ID from agent ID."""
        return str(uuid.uuid5(uuid.NAMESPACE_URL, agent_id))

    def _validate_and_prepare_args(
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> dict[str, Any]:
        """
        Remove the internal Drasi instructions before validating, then inject
        the runtime routing values the MCP server expects.
        """
        cleaned_kwargs = self._clean_kwargs(kwargs)

        # NOTE: this is not the workflow instance ID, but the user-facing agent ID
        # injected for all ``WorkflowContextInjectedTool`` tool calls.
        agent_id = cleaned_kwargs.get("_source_agent")
        assert agent_id is not None, (
            "Drasi workflow tool requires '_source_agent' in kwargs"
        )

        # Inject agent_id, subscription_id, topic from runtime
        cleaned_kwargs["agent_id"] = agent_id
        # TODO: replace with context.new_guid() when available
        cleaned_kwargs["subscription_id"] = self._build_subscription_id(agent_id)
        cleaned_kwargs["topic"] = self._topic

        try:
            # Point args_model to the validation model so we can validate without instructions
            self.args_model = self._validation_args_model
            prepared_kwargs = super()._validate_and_prepare_args(
                func, *args, **cleaned_kwargs
            )
        finally:
            # Restore args_model to the exposed model so the LLM can continue to generate instructions
            self.args_model = self._exposed_args_model

        return prepared_kwargs
