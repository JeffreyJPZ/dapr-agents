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
AGENT_ID = "inventory-agent"
SUBSCRIPTION_INSTRUCTIONS_KEY_PREFIX = "subscription_instructions_"


def build_subscription_instructions_key(identifier: str | None) -> str:
    """Build the state key used to persist Drasi task instructions."""
    return f"{SUBSCRIPTION_INSTRUCTIONS_KEY_PREFIX}{identifier or ''}"


def _state_key_variants(identifier: str | None) -> Iterable[str]:
    """Return likely key variants for a Drasi CloudEvent identifier."""
    if identifier is None:
        return []
    normalized = identifier.strip()
    if not normalized:
        return []

    variants = [normalized]
    parts = normalized.split(":")
    if len(parts) >= 5:
        # Full Drasi CloudEvent id: queryId:agent_id:subscription_id:sequence:idx
        variants.append(":".join(parts[:3]))
        variants.append(":".join(parts[1:3]))
    elif len(parts) >= 3:
        variants.append(":".join(parts[:3]))
        variants.append(":".join(parts[1:3]))
    elif len(parts) == 2:
        variants.append(":".join(parts))

    # Preserve order while removing duplicates.
    seen: set[str] = set()
    ordered: list[str] = []
    for variant in variants:
        if variant and variant not in seen:
            seen.add(variant)
            ordered.append(variant)
    return ordered


def _decode_state_value(response: object) -> object | None:
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


def read_state_value(store_name: str, key: str | None) -> object | None:
    """Read a JSON-encoded Dapr state value, trying common Drasi key variants."""
    with DaprClient() as client:
        for candidate in _state_key_variants(key):
            response = client.get_state(
                store_name=store_name,
                key=build_subscription_instructions_key(candidate),
            )
            value = _decode_state_value(response)
            if value is not None:
                return value
    return None


def write_state_value(store_name: str, key: str | None, value: object) -> None:
    """Persist a JSON-serializable value through Dapr state."""
    with DaprClient() as client:
        client.save_state(
            store_name=store_name,
            key=build_subscription_instructions_key(key),
            value=json.dumps(value),
            state_metadata={"contentType": "application/json"},
        )
