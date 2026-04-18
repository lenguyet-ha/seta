# Log Service -> Elasticsearch Guide

This document explains how to send logs from microservices to Kafka and store them in Elasticsearch using `log-service`.

## 1) End-to-End Flow

```
┌──────────────────┐
│  auth-service    │──┐
├──────────────────┤  │    Kafka topic     ┌──────────────┐   Index API    ┌───────────────┐
│  user-service    │──┼───── "logs" ──────▶│ log-service  │──────────────▶│ Elasticsearch │
├──────────────────┤  │                    │  (consumer)  │               │               │
│  other services  │──┘                    └──────────────┘               └───────┬───────┘
└──────────────────┘                                                             │
         ▲ publish                         transform &                    ┌──────▼──────┐
         │ log event                       build ES doc                   │   Kibana    │
         │                                                                └─────────────┘
```

1. Any service (`auth-service`, `user-service`, etc.) publishes a log event to Kafka topic `logs`.
2. `log-service` consumes messages from `logs`.
3. `log-service` transforms/normalizes each message into an Elasticsearch document.
4. `log-service` indexes the document into a daily index (e.g. `seta-logs-2026.04.11`).
5. You query logs from Elasticsearch directly or through Kibana.

## 2) Message Contract (Recommended)

Use one consistent JSON structure for log events.

```json
{
  "event_type": "service-log",
  "data": {
    "service": "auth-service",
    "level": "INFO",
    "message": "User login success",
    "trace_id": "4f89ea9e-001",
    "user_id": 123,
    "path": "/graphql",
    "method": "POST",
    "status_code": 200,
    "extra": {
      "ip": "127.0.0.1"
    }
  },
  "timestamp": "2026-04-11T09:10:11.123456+00:00",
  "version": "1.0"
}
```

Notes:
- `event_type` should identify the category, for example `service-log`, `auth-log`, `user-log`.
- `data.level` should be one of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`.
- `timestamp` should be UTC ISO-8601.

## 3) Publish Logs to Kafka

The shared `KafkaProducerService` in `seta_shared.kafka.producer` already supports this envelope shape.

Example (inside any service):

```python
from seta_shared.kafka.producer import get_kafka_producer

async def publish_log_event():
    producer = get_kafka_producer()
    await producer.send_event(
        topic="logs",
        event_type="service-log",
        data={
            "service": "auth-service",
            "level": "INFO",
            "message": "User login success",
            "trace_id": "4f89ea9e-001",
            "user_id": 123,
            "path": "/graphql",
            "method": "POST",
            "status_code": 200,
        },
        key="auth-service",
    )
```

### What happens inside `send_event`

1. `_build_message()` wraps `data` into the standard envelope, auto-adding `timestamp` (UTC) and `version`.
2. `_serialize_value()` converts the envelope to **JSON bytes** (UTF-8).
3. `_serialize_key()` encodes the key (`"auth-service"`) to bytes — Kafka uses this for **partition routing** (same key → same partition → ordered).
4. The message is sent with `acks=all` (waits for all replicas), `enable_idempotence=True` (exactly-once within a partition), and `compression_type=gzip`.

So the raw bytes on the Kafka topic look like:

```json
{
  "event_type": "service-log",
  "data": { "service": "auth-service", "level": "INFO", ... },
  "timestamp": "2026-04-11T09:10:11.123456+00:00",
  "version": "1.0"
}
```

## 4) Log-Service Consumer Behavior

`log-service` uses the shared `KafkaConsumerService` from `seta_shared.kafka.consumer`.

### 4.1) Subscribe and route

1. Create a `KafkaConsumerService` with topic `["logs"]` and `group_id="log-service-group"`.
2. Register a handler for `event_type="service-log"` (and any other event types you define).
3. Call `consumer.consume()` — the shared consumer loop automatically:
   - Deserializes each message from JSON bytes.
   - Extracts `event_type` from the envelope.
   - Dispatches to the matching registered handler.

```python
from seta_shared.kafka.consumer import KafkaConsumerService

consumer = KafkaConsumerService(
    topics=["logs"],
    bootstrap_servers="kafka:9093",
    group_id="log-service-group",
)
consumer.register_handler("service-log", handle_service_log)
await consumer.start()
await consumer.consume()
```

### 4.2) Transform Kafka event → Elasticsearch document

The handler receives the `data` dict from the envelope. It should build a flat ES document:

```python
from datetime import datetime, timezone

async def handle_service_log(data: dict, event: dict, msg) -> None:
    es_doc = {
        # Time field — use event timestamp, fallback to current UTC
        "@timestamp": event.get("timestamp") or datetime.now(timezone.utc).isoformat(),

        # Service fields (from data payload)
        "service":     data.get("service"),
        "level":       data.get("level"),
        "message":     data.get("message"),
        "trace_id":    data.get("trace_id"),
        "user_id":     data.get("user_id"),
        "path":        data.get("path"),
        "method":      data.get("method"),
        "status_code": data.get("status_code"),
        "extra":       data.get("extra", {}),

        # Metadata (from envelope + Kafka message)
        "event_type": event.get("event_type"),
        "version":    event.get("version"),
        "kafka_topic":     msg.topic,
        "kafka_partition":  msg.partition,
        "kafka_offset":     msg.offset,
    }

    # Compute daily index name
    dt = datetime.fromisoformat(es_doc["@timestamp"])
    index_name = f"seta-logs-{dt.strftime('%Y.%m.%d')}"
    # e.g. "seta-logs-2026.04.11"

    await es_client.index(index=index_name, body=es_doc)
```

### 4.3) Index naming strategy

| Strategy | Index name | Pros | Cons |
|----------|-----------|------|------|
| Fixed | `seta-logs` | Simple | Hard to manage retention, index grows forever |
| **Daily (recommended)** | `seta-logs-2026.04.11` | Easy retention (delete old indices), smaller shards | More indices to manage |
| Monthly | `seta-logs-2026.04` | Fewer indices | Less granular retention |

Recommended pattern: `seta-logs-%Y.%m.%d`

## 5) Elasticsearch Mapping (Recommended)

Use an explicit mapping to keep search fast and avoid wrong auto-detected types.

### 5.1) Field type rationale

| Field | ES Type | Why |
|-------|---------|-----|
| `@timestamp` | `date` | Time-based queries, sorting, Kibana time picker |
| `service` | `keyword` | Exact match filter, aggregation (not tokenized) |
| `level` | `keyword` | Exact match: `INFO`, `ERROR`, etc. |
| `message` | `text` | Full-text search (tokenized, analyzed) |
| `trace_id` | `keyword` | Exact match lookup for distributed tracing |
| `event_type` | `keyword` | Filter by log category |
| `version` | `keyword` | Filter by envelope version |
| `status_code` | `integer` | Range queries (`status_code >= 400`) |
| `path` | `keyword` | Exact match on API paths |
| `method` | `keyword` | Exact match: `GET`, `POST`, etc. |
| `extra` | `object` (dynamic) | Flexible nested metadata |

### 5.2) Mapping definition

```json
{
  "mappings": {
    "properties": {
      "@timestamp":  { "type": "date" },
      "service":     { "type": "keyword" },
      "level":       { "type": "keyword" },
      "message":     { "type": "text" },
      "trace_id":    { "type": "keyword" },
      "user_id":     { "type": "integer" },
      "event_type":  { "type": "keyword" },
      "version":     { "type": "keyword" },
      "status_code": { "type": "integer" },
      "path":        { "type": "keyword" },
      "method":      { "type": "keyword" },
      "extra":       { "type": "object", "dynamic": true },
      "kafka_topic":     { "type": "keyword" },
      "kafka_partition":  { "type": "integer" },
      "kafka_offset":     { "type": "long" }
    }
  }
}
```

### 5.3) Create an Index Template (recommended)

Instead of creating mappings per index, use an **index template** so every new daily index gets the mapping automatically:

```bash
curl -X PUT "http://localhost:9200/_index_template/seta-logs-template" \
  -H "Content-Type: application/json" -d '{
  "index_patterns": ["seta-logs-*"],
  "priority": 100,
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 1
    },
    "mappings": {
      "properties": {
        "@timestamp":  { "type": "date" },
        "service":     { "type": "keyword" },
        "level":       { "type": "keyword" },
        "message":     { "type": "text" },
        "trace_id":    { "type": "keyword" },
        "user_id":     { "type": "integer" },
        "event_type":  { "type": "keyword" },
        "version":     { "type": "keyword" },
        "status_code": { "type": "integer" },
        "path":        { "type": "keyword" },
        "method":      { "type": "keyword" },
        "extra":       { "type": "object", "dynamic": true },
        "kafka_topic":     { "type": "keyword" },
        "kafka_partition":  { "type": "integer" },
        "kafka_offset":     { "type": "long" }
      }
    }
  }
}'
```

After this, any index created matching `seta-logs-*` will inherit this mapping + settings.

## 6) Run with Docker Compose

From repository root:

```bash
docker compose up -d kafka zookeeper elasticsearch kibana log-service
```

Optional: run full stack:

```bash
docker compose up -d
```

Check health:

```bash
curl http://localhost:9200
curl http://localhost:5601/api/status
```

## 7) Verify Logs Are Stored in Elasticsearch

Query recent documents:

```bash
curl "http://localhost:9200/seta-logs-*/_search?pretty" -H "Content-Type: application/json" -d '{
  "size": 20,
  "sort": [{"@timestamp": "desc"}]
}'
```

Filter by service:

```bash
curl "http://localhost:9200/seta-logs-*/_search?pretty" -H "Content-Type: application/json" -d '{
  "query": {
    "term": {
      "service": "auth-service"
    }
  }
}'
```

## 8) View in Kibana

1. Open `http://localhost:5601`.
2. Create index pattern: `seta-logs-*`.
3. Select time field: `@timestamp`.
4. Go to Discover and filter by `service`, `level`, or `trace_id`.

## 9) Recommended Environment Variables

For `log-service`:

- `KAFKA_BOOTSTRAP_SERVERS=kafka:9093` (inside Docker network)
- `ELASTICSEARCH_HOSTS=http://elasticsearch:9200`
- `LOG_INDEX_PREFIX=seta-logs`
- `KAFKA_LOG_TOPIC=logs`
- `KAFKA_GROUP_ID=log-service-group`

Note:
- In many Docker Kafka setups, internal services should use `kafka:9093` (internal listener), not host-mapped `localhost:9092`.

## 10) Troubleshooting

- No data in Elasticsearch:
  - Check `log-service` container logs.
  - Verify topic `logs` exists.
  - Verify `KAFKA_BOOTSTRAP_SERVERS` matches Kafka internal listener.
- Data exists but not visible in Kibana:
  - Check index pattern `seta-logs-*`.
  - Check selected time field and time range.
- Mapping conflicts:
  - Recreate index with explicit mapping.
  - Keep payload field types consistent (for example `status_code` should always be integer).

## 11) Minimal Responsibilities by Service

| Service | Responsibility |
|---------|---------------|
| Producer services (`auth-service`, `user-service`, ...) | Emit structured log events to Kafka topic `logs` |
| `log-service` | Consume → validate → transform → index into ES |
| Elasticsearch | Persistent, searchable log storage |
| Kibana | Exploration UI and dashboards |

## 12) Project Structure

```
log-service/
├── Dockerfile
├── requirements.txt
├── README.md
└── app/
    ├── main.py                  # Application entry point (startup/shutdown lifecycle)
    ├── core/
    │   └── config.py            # Environment variables, settings
    ├── kafka/
    │   └── consumer.py          # Kafka consumer setup, handler registration
    ├── elastic/
    │   └── client.py            # Elasticsearch client init, index operations
    └── services/
        └── log_service.py       # Transform logic: Kafka event → ES document
```

### Data flow through the codebase

```
main.py  ──▶  kafka/consumer.py  ──▶  services/log_service.py  ──▶  elastic/client.py
(startup)     (consume loop)          (transform event → doc)       (index into ES)
```

## 13) Index Retention (Optional)

With daily indices, you can delete old logs to save disk space:

```bash
# Delete logs older than 30 days
curl -X DELETE "http://localhost:9200/seta-logs-2026.03.*"

# Or use ILM (Index Lifecycle Management) for automatic retention:
curl -X PUT "http://localhost:9200/_ilm/policy/seta-logs-policy" \
  -H "Content-Type: application/json" -d '{
  "policy": {
    "phases": {
      "hot": {
        "actions": {}
      },
      "delete": {
        "min_age": "30d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}'
```

Then attach the policy to the index template by adding to `template.settings`:

```json
{
  "index.lifecycle.name": "seta-logs-policy"
}
```
