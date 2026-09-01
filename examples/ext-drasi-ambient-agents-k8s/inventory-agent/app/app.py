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
import os

from dapr_agents import AgentRunner

from dapr_agents.ext.drasi import enable_drasi

from agent import make_agent

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

AGENT_MCP_COMPONENT = os.getenv("AGENT_MCP_COMPONENT", "agent-mcp")
AGENT_PUBSUB_COMPONENT = os.getenv("AGENT_PUBSUB_COMPONENT", "agent-pubsub")


def main() -> None:
    agent = make_agent()

    # TODO: should users construct Drasi tools themselves?
    enable_drasi(
        agent,
        mcp_server=AGENT_MCP_COMPONENT,
        pubsub=AGENT_PUBSUB_COMPONENT,
        topic="drasi-events",
    )

    runner = AgentRunner()
    try:
        runner.serve(agent)
    finally:
        runner.shutdown(agent)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
