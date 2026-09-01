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

from dapr.ext.workflow.mcp import MCPToolDef
from dapr.ext.workflow.mcp_schema import create_pydantic_model_from_schema

from dapr_agents.tool.mcp.dapr_workflow_client import mcp_tool_def_to_workflow_tool
from dapr_agents.tool.workflow import WorkflowContextInjectedTool
from dapr_agents.types import ToolError

logger = logging.getLogger(__name__)


# TODO: this needs to be improved
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

    # Used by ``subscribe_drasi_query``
    _topic: str = PrivateAttr()
    # Used by ``subscribe_drasi_query`` and ``unsubscribe_drasi_query```
    _subscription_arg: str = PrivateAttr(default="_subscription_id")
    # Reference to the original ``args_model`` matching the original MCP tool schema
    _validation_args_model: Type[BaseModel] = PrivateAttr()
    # Reference to the ``args_model`` that is exposed to the LLM
    _exposed_args_model: Type[BaseModel] = PrivateAttr()

    @classmethod
    def from_mcp_tool_def(
        cls,
        tool: MCPToolDef,  # TODO: is this the right type?
    ) -> "DrasiWorkflowTool":
        """Wrap a framework-agnostic MCP tool definition so it can carry Drasi routing metadata."""
        if tool.func is None:
            raise ToolError(
                f"Tool '{tool.name}' must define a callable function"
            )
        if tool.args_model is None:
            raise ToolError(
                f"Tool '{tool.name}' must define an args model"
            )

        # Convert to ``WorkflowContextInjectedTool``
        workflow_tool = mcp_tool_def_to_workflow_tool(tool)

        drasi_workflow_tool = cls(
            name=workflow_tool.name,
            description=workflow_tool.description,
            func=workflow_tool.func,
            args_model=workflow_tool.args_model,
            source=workflow_tool.source,
        )
        validation_args_model = workflow_tool.args_model
        exposed_args_model = drasi_workflow_tool._create_exposed_args_model(validation_args_model)

        # Set ``args_model`` to the LLM-exposed model
        drasi_workflow_tool.args_model = exposed_args_model

        # Set private attributes. ``_topic`` is assigned when the agent is
        # hosted, after the activation configuration has been resolved.
        drasi_workflow_tool._validation_args_model = validation_args_model
        drasi_workflow_tool._exposed_args_model = exposed_args_model

        logger.debug(f"Created Drasi workflow tool '{drasi_workflow_tool.name}'")

        return drasi_workflow_tool

    def _create_exposed_args_model(
        self,
        args_model: type[BaseModel] | None,
    ) -> Type[BaseModel]:
        """Create the argument model exposed to the LLM for a Drasi tool."""
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

        fields_to_remove: set[str] = set()
        if self.name == "subscribe_drasi_query":
            fields_to_remove = {"agent_id", "subscription_id", "topic"}
            properties["instructions"] = {
                "type": "string",
                "description": DRASI_INSTRUCTIONS_DESCRIPTION,
            }
            if "instructions" not in required:
                required.append("instructions")
        elif self.name == "unsubscribe_drasi_query":
            fields_to_remove = {"agent_id", "subscription_id"}

        for field_name in fields_to_remove:
            properties.pop(field_name, None)
        required[:] = [field for field in required if field not in fields_to_remove]

        return create_pydantic_model_from_schema(schema, f"{base_name}Exposed")  # TODO: is this a good name>

    def _clean_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Strip LLM-only arguments from a ``kwargs`` tool dict."""
        cleaned_kwargs = dict(kwargs)

        # This should only apply to ``subscribe_drasi_query``
        cleaned_kwargs.pop("instructions", None)

        return cleaned_kwargs

    def _build_subscription_id(self, agent_id: str) -> str:
        """Build a deterministic subscription ID from agent ID."""
        return str(uuid.uuid5(uuid.NAMESPACE_URL, agent_id))

    def _validate_and_prepare_args(
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> dict[str, Any]:
        """Strip LLM-only arguments, then inject the runtime/infra arguments the MCP tool expects."""
        cleaned_kwargs = self._clean_kwargs(kwargs)

        # NOTE: this is not the workflow instance ID, but the user-facing agent ID
        # injected for all ``WorkflowContextInjectedTool`` tool calls.
        agent_id = cleaned_kwargs.get("_source_agent")
        assert agent_id is not None, (
            "Drasi workflow tool requires '_source_agent' in kwargs"
        )

        if self.name == "subscribe_drasi_query":
            # Inject agent ID, forwarded subscription ID, topic
            cleaned_kwargs["agent_id"] = agent_id
            cleaned_kwargs["subscription_id"] = cleaned_kwargs[self._subscription_arg]
            cleaned_kwargs["topic"] = self._topic

            # Strip forwarded subscription ID as it has been promoted to a top-level argument
            cleaned_kwargs.pop(self._subscription_arg, None)
        elif self.name == "unsubscribe_drasi_query":
            # Inject agent ID, forwarded subscription ID
            cleaned_kwargs["agent_id"] = agent_id
            cleaned_kwargs["subscription_id"] = cleaned_kwargs[self._subscription_arg]

            # Strip forwarded subscription ID as it has been promoted to a top-level argument
            cleaned_kwargs.pop(self._subscription_arg, None)

        try:
            # Set ``args_model`` to the validation model matching the original MCP tool schema
            self.args_model = self._validation_args_model
            prepared_kwargs = super()._validate_and_prepare_args(
                func, *args, **cleaned_kwargs
            )
        finally:
            # Restore ``args_model`` to the LLM-exposed model
            self.args_model = self._exposed_args_model

        return prepared_kwargs
