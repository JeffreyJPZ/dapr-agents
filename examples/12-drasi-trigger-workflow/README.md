<!--
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Drasi Trigger Workflows (Pub/Sub → Workflow)

This example shows how to trigger Dapr Workflows directly from Drasi CDC (Change Data Capture) events using the `@message_router` decorator. The decorator is applied to the workflow itself, enabling automatic message validation and workflow scheduling. When low or critical stock events are detected, the workflows automatically process orders.

You'll run two processes:

* **App**: subscribes to Drasi event topics and runs the workflow runtime
* **Client**: publishes test Drasi events to those topics

## Key Concept

The `@message_router` decorator is applied **directly to the workflow function**, not to a separate handler. This means:
- The workflow IS the pub/sub handler
- Messages are automatically validated against the Drasi event models
- The workflow is automatically scheduled when events arrive
- No manual workflow scheduling code needed

Supported Drasi event types:
- **Unpacked Events** (`DrasiUnpackedEvent`): Individual CDC records with `op` (i/u/d/x), `before`/`after` state
- **Packed Events** (`DrasiPackedEvent`): Batched query results with `addedResults`, `updatedResults`, `deletedResults`

## Prerequisites

- uv package manager
- OpenAI API key
- Dapr CLI and Docker installed
- Drasi or a service publishing to the notification topics

## Environment Setup

```bash
uv venv
# Activate the virtual environment
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate
uv sync --active
```

## Configuration

The example includes an OpenAI component configuration in the `components` directory. You have two options to configure your API key:

### Option 1: Using Environment Variables (Recommended)

1. Create a `.env` file in the project root and add your OpenAI API key:

```env
OPENAI_API_KEY=your_api_key_here
```

2. When running the examples with Dapr, use the helper script to resolve environment variables:

#### macOS / Linux (Bash)
```bash
# Get the environment variables from the .env file:
export $(grep -v '^#' ../../.env | xargs)

# Create a temporary resources folder with resolved environment variables
temp_resources_folder=$(../resolve_env_templates.py ./components)

# Run your dapr command with the temporary resources
uv run dapr run --app-id dapr-agent-wf --resources-path $temp_resources_folder -- python workflow.py

# Clean up when done
rm -rf $temp_resources_folder
```

#### Windows (PowerShell)
```powershell
# Get the environment variables from the .env file:
Get-Content .env | Where-Object { $_ -and -not $_.StartsWith("#") } | ForEach-Object {
    $name, $value = $_.Split('=', 2)
    [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
}

# Create a temporary resources folder with resolved environment variables
$temp_resources_folder = python ../resolve_env_templates.py ./components

# Run your dapr command with the temporary resources
uv run dapr run --app-id dapr-agent-wf --resources-path $temp_resources_folder -- python workflow.py

# Clean up when done
Remove-Item -Recurse -Force $temp_resources_folder
```

> The temporary resources folder will be automatically deleted when the Dapr sidecar is stopped or when the computer is restarted.

### Option 2: Direct Component Configuration

You can directly update the `key` in [resources/openai.yaml](resources/openai.yaml):
```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: openai
spec:
  type: conversation.openai
  metadata:
    - name: key
      value: "YOUR_OPENAI_API_KEY"
```

Replace `YOUR_OPENAI_API_KEY` with your actual OpenAI API key.

> Many LLM providers are compatible with OpenAI's API (DeepSeek, Google AI, etc.) and can be used with this component by configuring the appropriate parameters. Dapr also has [native support](https://docs.dapr.io/reference/components-reference/supported-conversation/) for other providers like Google AI, Anthropic, Mistral, DeepSeek, etc.

### Additional Components

Make sure Dapr is initialized on your system:

```bash
dapr init
```

The example includes other necessary Dapr components in the `components` directory. For example, the workflow state store component:

Look at the `workflowstate.yaml` file in the `components` directory:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: workflowstatestore
spec:
  type: state.redis
  version: v1
  metadata:
  - name: redisHost
    value: localhost:6379
  - name: redisPassword
    value: ""
  - name: actorStateStore
    value: "true"
```

## Project layout

```text
12-drasi-trigger-workflow/
├─ resources/                 # Dapr components (pubsub, conversation, workflow state)
├─ app.py                     # Starts WorkflowRuntime + registers message routers
├─ workflow.py                # @message_router decorated workflows & activities
└─ message_client.py          # Publishes test Drasi events to the topics
```

## How it works (flow)

* `message_client.py` publishes Drasi-style CDC events to Dapr pubsub topics:
  - `low-stock-events` (unpacked events with low stock records)
  - `critical-stock-events` (unpacked events with critical stock records)
* `app.py` starts the Dapr Workflow runtime, registers the workflows + activities, and calls `register_message_routes()`.
* `register_message_routes()` discovers the `@message_router` decorators, validates incoming messages using Pydantic models (`DrasiUnpackedEvent`, `DrasiPackedEvent`), and automatically schedules workflows when valid events arrive.
* `workflow.py` runs `low_stock_order_workflow` or `critical_stock_order_workflow`:
  - Extracts item data from the Drasi event payload
  - Calls activities to calculate order counts and create orders
  - Uses LLM-backed activities for intelligent decision making

## Code Structure

### Workflows

Two main workflows handle different stock scenarios:

**`low_stock_order_workflow`**
- Topic: `low-stock-events` on pubsub `notifications-pubsub`
- Model: `DrasiUnpackedEvent` with item stock information
- Behavior: Calculates order count and creates order for low-stock items

**`critical_stock_order_workflow`**
- Topic: `critical-stock-events` on pubsub `notifications-pubsub`
- Model: `DrasiUnpackedEvent` with item stock information
- Behavior: Calculates order count and creates order for critical-stock items

### app.py

The application entry point registers the workflows and sets up pub/sub subscriptions.

**Key Points:**
- `runtime.register_workflow(low_stock_order_workflow)` - Register the workflows with the runtime
- `register_message_routes(targets=[low_stock_order_workflow, critical_stock_order_workflow])` - Set up pub/sub subscriptions by discovering the `@message_router` decorators
- The workflow functions are passed as targets
- When events arrive on their respective topics, workflows are automatically validated and scheduled

## Running with Option 1

Start the app (subscriber + workflow runtime)

```bash
dapr run \
  --app-id message-workflow \
  --resources-path $temp_resources_folder \
  -- python app.py
rm -rf $temp_resources_folder
```

Publish a test message (publisher)

```bash
dapr run \
  --app-id message-workflow-client \
  --resources-path $temp_resources_folder \
  -- python message_client.py
rm -rf $temp_resources_folder
```

## Running with Option 2

Start the app (subscriber + workflow runtime)

```bash
dapr run \
  --app-id message-workflow \
  --resources-path resources \
  -- python app.py
```

Publish a test message (publisher)

```bash
dapr run \
  --app-id message-workflow-client \
  --resources-path resources \
  -- python message_client.py

## Publisher configuration (env vars)

You can tweak `message_client.py` using environment variables to publish different Drasi event types:

| Variable             | Default                      | Description                                    |
| -------------------- | ---------------------------- | ---------------------------------------------- |
| `PUBSUB_NAME`        | `notifications-pubsub`       | Pub/Sub component name                         |
| `UNPACKED_TOPIC`     | `low-stock-events`           | Topic for unpacked stock events                |
| `PACKED_TOPIC`       | `product-updates-query`      | Topic for packed query result events           |
| `EVENT_TYPE`         | `unpacked`                   | Event type to publish: `unpacked` or `packed`  |
| `RAW_DATA`           | *(unset)*                    | JSON string that overrides payload (must be object) |
| `CONTENT_TYPE`       | `application/json`           | Content type sent with the event               |
| `CLOUDEVENT_TYPE`    | *(unset)*                    | Optional `cloudevent.type` metadata            |
| `PUBLISH_ONCE`       | `true`                       | If `false`, publish periodically               |
| `INTERVAL_SEC`       | `0`                          | Period (seconds) when `PUBLISH_ONCE=false`     |
| `MAX_ATTEMPTS`       | `8`                          | Retry attempts per publish                     |
| `INITIAL_DELAY`      | `0.5`                        | Initial backoff seconds                        |
| `BACKOFF_FACTOR`     | `2.0`                        | Exponential backoff factor                     |
| `JITTER_FRAC`        | `0.2`                        | ± jitter applied to each delay                 |
| `STARTUP_DELAY`      | `1.0`                        | Sleep before first publish (sidecar warmup)    |

## Event Schemas

### DrasiUnpackedEvent (Low/Critical Stock Events)

Used by `low-stock-events` and `critical-stock-events` topics:

```json
{
  "op": "i",
  "ts_ms": 1678886400200,
  "seq": 0,
  "payload": {
    "source": {
      "queryId": "inventory-alerts-query",
      "ts_ms": 1678886400150
    },
    "after": { 
      "itemId": "SKU789", 
      "stockLevel": 5,
      "status": "low"
    }
  }
}
```

### DrasiPackedEvent (Batch Query Results)

For batched events with multiple added/updated/deleted results:

```json
{
  "queryId": "product-updates-query",
  "sourceTimeMs": 1678886400123,
  "addedResults": [
    { "product_id": "P101", "name": "Laptop X", "price": 1200.00 }
  ],
  "updatedResults": [],
  "deletedResults": [],
  "sequence": 1
}
```

## Integration with Dapr

Dapr Agents workflows leverage Dapr's core capabilities:

- **Durability**: Workflows survive process restarts or crashes
- **State Management**: Workflow state is persisted in a distributed state store
- **Actor Model**: Tasks run as reliable, stateful actors within the workflow
- **Event Handling**: Workflows can react to external events

## Troubleshooting

1. **Docker is Running**: Ensure Docker is running with `docker ps` and verify you have container instances with `daprio/dapr`, `openzipkin/zipkin`, and `redis` images running
2. **Redis Connection**: Ensure Redis is running (automatically installed by Dapr)
3. **Dapr Initialization**: If components aren't found, verify Dapr is initialized with `dapr init`
4. **API Key**: Check your OpenAI API key if authentication fails
5. **gRPC Timeout**: For longer prompts/responses set `DAPR_API_TIMEOUT_SECONDS=300` so the Dapr client waits beyond the 60 s default.
