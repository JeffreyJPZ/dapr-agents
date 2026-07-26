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
from typing import Optional, Set

from dapr.clients import DaprClient

from dapr_agents.types.activation import ActivationContext
from dapr_agents.utils.dapr_client_factory import DaprClientFactory, default_dapr_client_factory

logger = logging.getLogger(__name__)


# TODO: may or may not need to refactor
def _validate_binding_components(
    dapr_client: DaprClient,
    binding_names: Set[str],
    client_factory: Optional[DaprClientFactory] = None,
) -> None:
    """
    Validate that the required binding components are available in Dapr.

    Args:
        dapr_client: Active Dapr client to query metadata.
        binding_names: Set of binding component names that are required.
        client_factory: Factory used to build the transient metadata-query
            client. Resolves to ``default_dapr_client_factory`` at call time
            when ``None`` so test monkeypatches of the module-level symbol
            take effect.

    Raises:
        ValueError: If any of the required binding components are not registered in Dapr.
    """
    if not binding_names:
        return

    factory = client_factory or default_dapr_client_factory
    try:
        with factory() as client:
            metadata = client.get_metadata()
        registered_components = metadata.registered_components or []

        # Find all registered binding components
        available_bindings: Set[str] = set()
        for component in registered_components:
            if "bindings" in component.type.lower():
                available_bindings.add(component.name)

        # Check if all required binding components are available
        for binding_name in binding_names:
            if binding_name not in available_bindings:
                raise ValueError(
                    f"Required binding component '{binding_name}' is not registered in Dapr. "
                ) # TODO: replace with better error

    except ValueError:
        # Re-raise our custom exception
        raise
    except Exception as e:
        # Log and fail on metadata retrieval errors
        # (e.g., Dapr sidecar might not be fully ready)
        logger.error(
            "Could not validate binding component availability: %s. "
            "Failing startup to prevent silent missing triggers.",
            str(e),
        )
        raise


def subscribe_binding() -> None:
    """
    Subscribe to a Dapr binding component.
    """

    def trigger_agent(ctx: ActivationContext) -> None:

        async def handler(response: dict) -> None:
            """
            Handle the incoming binding event and trigger the agent workflow.

            Args:
                response: The incoming binding event payload.
            """

            await ctx.runner.run(ctx.agent, response, wait=False)

        return handler

    def subscribe(ctx: ActivationContext) -> None:
        binding_name = ctx.agent.binding_name
        if not binding_name:
            # Should not happen
            raise ValueError()

        # Validate that the binding component is available
        _validate_binding_components(ctx.dapr_client, {binding_name})

        # Discover if sidecar is using HTTP or gRPC
        # with ctx.dapr_client as client:
        #     metadata = client.get_metadata().appConnectionProperties

        protocol = "http"  # TODO: detect from metadata

        if protocol == "grpc":
            pass # TODO: implement gRPC binding support
        else:
            ctx.app.add_api_route(
                f"/{binding_name}",
                trigger_agent,
                methods=["POST"],
            )


