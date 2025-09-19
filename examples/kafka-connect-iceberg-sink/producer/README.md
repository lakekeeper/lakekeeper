# Kafka Avro Producer

A simple Python script to produce dummy trade events to a Kafka topic using an Avro schema stored in a Schema Registry. The script retrieves the schema at startup and generates random events for testing or performance purposes.

## Requirements

* Python 3.12+
* Kafka cluster running and accessible
* Schema Registry accessible
* Python dependencies:

```bash
uv sync
```

## Usage

```bash
python main.py [OPTIONS]
```

### CLI Arguments

| Argument                | Type | Default                 | Description                                                      |
| ----------------------- | ---- | ----------------------- | ---------------------------------------------------------------- |
| `--kafka-broker`        | str  | `localhost:9092`        | Kafka bootstrap server URL (can also use `KAFKA_BROKER` env var) |
| `--topic`               | str  | `events`                | Kafka topic to produce events to (can also use `TOPIC` env var)  |
| `--schema-registry-url` | str  | `http://localhost:8085` | URL of the Schema Registry (env: `SCHEMA_REGISTRY_URL`)          |
| `--schema-subject`      | str  | `events-value`          | Schema Registry subject name (env: `SCHEMA_SUBJECT`)             |
| `--throughput`          | int  | `100000000`             | Target throughput in messages per second (default: unlimited)    |
| `--num-records`         | int  | `100000`                | Total number of records to send                                  |

### Example

```bash
export KAFKA_BROKER=localhost:9092
export TOPIC=events
export SCHEMA_REGISTRY_URL=http://localhost:8085
export SCHEMA_SUBJECT=events-value

python main.py --throughput 1000 --num-records 5000

## Behavior

1. On startup, the script fetches the Avro schema from the Schema Registry.
2. If the schema cannot be retrieved, the script exits with an error.
3. Produces random trade events to the configured Kafka topic.
4. Optionally limits the message throughput and number of records via CLI arguments.

## Event Format

The produced events follow the Avro schema (example fields):

```json
{
  "tradeId": "uuid",
  "symbol": "EURUSD",
  "price": 123.45,
  "quantity": 10,
  "timestamp": 1694698673000
}
```
