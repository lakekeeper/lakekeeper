import time
import uuid
import random
import sys
import os
import argparse
import logging
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# ----------------------
# Configure Logging
# ----------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ----------------------
# CLI Arguments
# ----------------------
parser = argparse.ArgumentParser(description='Kafka Producer Performance Test with Avro')
parser.add_argument('--schema-registry-url', type=str, 
                    default=os.environ.get('SCHEMA_REGISTRY_URL', 'http://localhost:8085'),
                    help='Schema Registry URL')
parser.add_argument('--throughput', type=int, default=100_0000_000, 
                    help='Target throughput in messages per second (default: unlimited)')
parser.add_argument('--num-records', type=int, default=100_000, 
                    help='Total number of records to send (default: 100000)')
parser.add_argument('--kafka-broker',type=str,default=os.environ.get('KAFKA_BROKER', 'localhost:9092'),
                    help='Kafka bootstrap server URL (env: KAFKA_BROKER)'
)
parser.add_argument('--topic', type=str, default=os.environ.get('TOPIC', 'events'),
                    help='Kafka topic to produce events to (env: TOPIC)'
)
parser.add_argument('--schema-subject', type=str, default=os.environ.get('SCHEMA_SUBJECT', 'events-value'),
                    help='Schema Registry subject name (env: SCHEMA_SUBJECT)'
)
parser.add_argument('--log-level', type=str, default='INFO',
                    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                    help='Set the logging level')
args = parser.parse_args()

# Set log level based on argument
logger.setLevel(getattr(logging, args.log_level))

# ----------------------
# Configuration
# ----------------------
KAFKA_BROKER = args.kafka_broker
TOPIC = args.topic
SCHEMA_REGISTRY_URL = args.schema_registry_url
SCHEMA_SUBJECT = args.schema_subject

# Create Schema Registry client
schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

# Fetch schema from registry
try:
    latest_version = schema_registry_client.get_latest_version(SCHEMA_SUBJECT)
    schema_str = latest_version.schema.schema_str
    logger.info(f"Using schema ID {latest_version.schema_id} from {SCHEMA_SUBJECT}")
except Exception as e:
    logger.error(f"Failed to fetch schema: {e}")
    sys.exit(1)

# Create Avro Serializer with fetched schema
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str
)

# Create SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': KAFKA_BROKER,
    'value.serializer': avro_serializer,
    'key.serializer': StringSerializer('utf_8')
})

# ----------------------
# Produce events
# ----------------------
symbols = ["EURUSD", "AAPL", "BTCUSD", "GOOG"]

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# ----------------------
# Main loop
# ----------------------
if __name__ == "__main__":
    interval = 1.0 / args.throughput if args.throughput > 0 else 0
    
    logger.info(f"Starting to produce {args.num_records} records to topic '{TOPIC}'")
    logger.info(f"Configuration: Broker={KAFKA_BROKER}, Schema Registry={SCHEMA_REGISTRY_URL}")
    
    start_time = time.time()
    
    for i in range(args.num_records):
        event = {
            "tradeId": str(uuid.uuid4()),
            "symbol": random.choice(symbols),
            "price": round(random.uniform(10, 1000), 2),
            "quantity": random.randint(1, 100),
            "timestamp": int(time.time() * 1000)
        }
        
        producer.produce(
            topic=TOPIC,
            # key=event["tradeId"],
            value=event,
            on_delivery=delivery_report
        )
        
        if (i + 1) % 1000 == 0:
            producer.poll(0)
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            logger.info(f"Produced {i + 1} messages... (Rate: {rate:.2f} msg/s)")
        
        if interval > 0:
            time.sleep(interval)
    
    producer.flush()
    
    total_time = time.time() - start_time
    avg_rate = args.num_records / total_time if total_time > 0 else 0
    
    logger.info(f"Successfully produced {args.num_records} messages")
    logger.info(f"Total time: {total_time:.2f}s, Average rate: {avg_rate:.2f} msg/s")
