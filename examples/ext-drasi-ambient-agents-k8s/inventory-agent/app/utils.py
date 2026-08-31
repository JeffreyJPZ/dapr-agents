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

import json
import os
from typing import Any, Iterable

from dapr.clients import DaprClient

AGENT_MEMORY_COMPONENT = os.getenv("AGENT_MEMORY_COMPONENT", "agent-memory")
# TODO: remove hardcoded agent ID
AGENT_ID = "InventoryAgent"
DRASI_SUBSCRIPTION_INSTRUCTIONS_KEY_PREFIX = "drasi_subscriptions:"


def build_subscription_instructions_key(identifier: str | None) -> str:
    """Build the state key used to persist Drasi task instructions."""
    return f"{DRASI_SUBSCRIPTION_INSTRUCTIONS_KEY_PREFIX}{identifier or ''}"


def _decode_state_value(response: Any) -> Any:
    if response is None:
        return None
    if hasattr(response, "json"):
        try:
            return response.json()
        except Exception:
            pass
    data = getattr(response, "data", None)
    if data is None:
        return None
    if isinstance(data, bytes):
        text = data.decode("utf-8")
    else:
        text = str(data)
    if not text.strip():
        return None
    try:
        return json.loads(text)
    except Exception:
        return text


def read_state_value(store_name: str, key: str | None) -> Any:
    """Read a JSON-encoded Dapr state value."""
    with DaprClient() as client:
        response = client.get_state(
            store_name=store_name,
            key=key,
        )
        value = _decode_state_value(response)
        if value is not None:
            return value
    return None


def write_state_value(store_name: str, key: str | None, value: Any) -> None:
    """Persist a JSON-serializable value through Dapr state."""
    with DaprClient() as client:
        client.save_state(
            store_name=store_name,
            key=key,
            value=json.dumps(value),
            state_metadata={"contentType": "application/json"},
        )
