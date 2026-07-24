# Apache Kafka
---

Apache Kafka is **a distributed event streaming platform used to build real-time data pipelines and streaming applications**. It ingests, stores, and processes massive streams of data (like financial transactions or user clicks) across multiple servers, ensuring speed, fault tolerance, and high availability.

## 1.1 Why Docker for Kafka

Docker is introduced as a **containerization platform** that packages Apache Kafka and all its dependencies into isolated, reproducible runtime units called containers.

- Installing Kafka directly on the host machine can lead to dependency conflicts, environment mismatches, and cleanup issues.
- Docker solves this by providing:
    - **Isolation**: Kafka runs inside its own container, separate from the host system.
    - **Portability**: The same container image can run across different environments.
    - **Reproducibility**: Ensures consistent behavior across development, testing, and production.
    - **Ease of cleanup**: Containers can be stopped and removed without polluting the host system.

**Conceptual Flow:**

```
Host Machine
   └── Docker Engine
         └── Kafka Container
               ├── Kafka Broker
               ├── Dependencies (Java, configs)
               └── Networking (ports exposed)
```

**docker-compose.yml skeleton:**

```yaml
services:
	kafka:
		image: apache/kafka:latest
		container_name: kafka-broker
		ports:
			- "9092:9092"
		environment:
			KAFKA_BROKER_ID: 1
			KAFKA_LISTENERS: PLAINTEXT://:9092
			KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

## 1.2 Why KRaft and not ZooKeeper

Traditionally, Kafka relied on **ZooKeeper** for cluster metadata management.

- **Drawbacks of ZooKeeper:**
    - Extra dependency outside Kafka.
    - Complexity in managing cluster state.
    - Harder scaling and maintenance.

**KRaft (Kafka Raft Metadata Mode):**

- Introduced in Kafka 2.8+, now the default in newer versions.
- Removes ZooKeeper dependency.
- Uses **Raft consensus algorithm** for metadata quorum.

**Advantages:**

- Simplified architecture (Kafka-only).
- Better scalability.
- Unified deployment (no external ZooKeeper cluster).
- Faster startup and failover.

**Conceptual Diagram:**

```
ZooKeeper-based Kafka (Old)
   ├── ZooKeeper Cluster
   └── Kafka Brokers (depend on ZooKeeper)

KRaft-based Kafka (New)
   └── Kafka Brokers (self-manage metadata via Raft quorum)
```

## 1.3 Kafka Installation with Docker and KRaft

**Steps to install Kafka with Docker and KRaft:**

1. **Pull Kafka image with KRaft support:**
    
    ```bash
    docker pull apache/kafka:latest
    ```
    
2. **docker-compose.yml example:**
    
    ```yaml
    services:
      kafka:
        image: apache/kafka:latest
        container_name: kafka-kraft
        ports:
          - "9092:9092"
        environment:
          KAFKA_PROCESS_ROLES: broker,controller
          KAFKA_NODE_ID: 1
          KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
          KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
          KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
          KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
    ```
    
3. **Start container:**
    
    ```bash
    docker-compose up -d
    ```
    
4. **Verify broker logs:**
    
    ```bash
    docker logs kafka-kraft
    ```
    

**Flow Diagram:**

```
Docker Compose
   └── Kafka Service
         ├── Broker Role
         ├── Controller Role (Raft quorum)
         └── Exposed Ports (9092 client, 9093 controller)
```

## 2.1 What is Apache Kafka?

Apache Kafka is a distributed event streaming platform built on the concept of a commit log. Producers write events, consumers read them. Kafka ensures durability, scalability, replayability, and fault tolerance. It is used for building real‑time pipelines and applications.

## 2.2 Kafka vs Traditional Message Queues

Traditional queues deliver messages once and then delete them. Kafka retains messages for a configurable retention period, allowing multiple consumer groups to read the same data independently. Kafka supports replay by resetting offsets. Traditional queues are point‑to‑point, Kafka is optimized for streaming and fan‑out.

## 2.3 Topics

Topics are logical categories of events. They are immutable and append‑only. Each topic is split into partitions for scalability.

**CLI Example:**

```bash
kafka-topics.sh --create --topic orders \
--bootstrap-server localhost:9092 \
--partitions 3 --replication-factor 2
```

**Describe Topic:**

```bash
kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

## 2.4 Partitions

Partitions are the unit of parallelism. Ordering is guaranteed within a partition but not across partitions. Assignment is round‑robin/random if no key is provided, or hash‑based if a key is specified. Keys ensure deterministic routing.

## 2.5 Offsets

Offsets are sequential IDs for events in a partition. Consumers use offsets to track progress. Kafka stores committed offsets in the internal topic `__consumer_offsets`. This enables replay and crash recovery. Offsets are metadata, not general event data.

**Python Consumer Commit Example:**

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer('orders', group_id='group1',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest', # latest, none
                         enable_auto_commit=False)

for message in consumer:
    print(f"Offset={message.offset}, Key={message.key}, Value={message.value}")
    consumer.commit()
```

## 2.6 Producers

Producers send events to topics. They can specify keys for partitioning and configure delivery semantics (`acks`).

- `acks=0`: fire‑and‑forget.
- `acks=1`: leader acknowledgment.
- `acks=all`: leader + ISR replicas acknowledgment.

**CLI Example:**

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

**Python Producer Example:**

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=5,
    linger_ms=10
)
producer.send('orders', key=b'orderId', value=b'Order Created')
producer.flush()
```

## 2.7 Consumers

Consumers subscribe to topics and poll events. The four steps are:

1. Subscribe.
2. Poll.
3. Process.
4. Commit offsets.

**CLI Example:**

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning
```

**Python Consumer Example:**

```python
consumer = KafkaConsumer('orders',
                         group_id='group1',
                         bootstrap_servers='localhost:9092')

for record in consumer:
    print(f"offset={record.offset}, key={record.key}, value={record.value}")
```

## 2.8 Consumer Groups

Consumer groups allow scaling. Kafka ensures each partition is consumed by exactly one consumer in the group. If partitions exceed consumers, some consumers handle multiple partitions. Group coordination is managed by Kafka’s group coordinator.

**Create Consumer Group (CLI):**

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic orders --group group1 --from-beginning
```

**Clear Offset of a Consumer Group (CLI)**
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
--group order-group --topic orders --reset-offsets --to-earliest --execute
```

**Describe Consumer Group (CLI):**

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
--describe --group group1
```

## 2.9 Internal Components

- **Broker**: Kafka server storing data and serving clients.
- **Cluster**: Multiple brokers working together.
- **ZooKeeper / KRaft**: Metadata management and leader election.
- **Replication**: Leader and follower partitions (ISR).
- **Controller**: Manages partition leadership and failover.
- **Log Segments**: Physical files storing events.
- **Retention Policy**: Defines how long events are kept.

**CLI Example:**

```
kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

## 3.1 Message Keys

Message keys are used to control how events are distributed across partitions. If no key is provided, Kafka distributes messages in a round‑robin fashion. If a key is provided, Kafka uses a hash of the key to consistently route all events with the same key to the same partition. This ensures ordering for events with the same key. Keys are critical when you want related events (like all orders from a single user) to be processed in sequence.

**Python Producer with Key (production style):**

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=5,
    linger_ms=10,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: v.encode('utf-8')
)

producer.send('orders', key='user123', value='Order Created for User 123')
producer.send('orders', key='user123', value='Order Shipped for User 123')
producer.flush()
```
**CLI Producer with Key:**
```bash
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092 \
--property "parse.key=true" --property "key.separator=:"
```

Then type messages like in producer console:

```
user123:Order Created
user123:Order Shipped
```

## 3.2 Serialization

Serialization is required because Kafka messages are byte arrays. Producers must serialize keys and values before sending, and consumers must deserialize them when reading.

Common serializers:

- **StringSerializer / StringDeserializer** → for plain text.
- **JSONSerializer / JSONDeserializer** → for structured data.
- **Avro / Protobuf** → for schema‑based serialization.

Serialization ensures that structured data can be transmitted reliably across Kafka topics, while deserialization allows consumers to reconstruct the original objects.

**Python Producer with JSON Serialization (production style):**

```python
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=5,
    linger_ms=10,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send('orders', key='user456', value={'order_id': 101, 'status': 'created'})
producer.flush()
```

**Python Consumer with JSON Deserialization (production style):**

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='group1',
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in consumer:
    print(f"Key={message.key}, Value={message.value}")
```

**CLI Consumer with Key and Value:**

```bash
kafka-console-consumer.sh --topic orders --bootstrap-server localhost:9092 \
--from-beginning --property "print.key=true" --property "key.separator=:"
```

**Avro Serialization Example (Python with Confluent Kafka):**

```python
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

value_schema_str = """
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "int"},
    {"name": "status", "type": "string"}
  ]
}
"""

key_schema_str = """
{
  "type": "string"
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)

producer = AvroProducer(
    {'bootstrap.servers': 'localhost:9092', 'schema.registry.url': 'http://localhost:8081'},
    default_key_schema=key_schema,
    default_value_schema=value_schema
)

producer.produce(topic='orders', key='user789', value={"order_id": 202, "status": "confirmed"})
producer.flush()
```

**Protobuf Serialization Example (Python with Confluent Kafka):**

```python
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.protobuf import ProtobufSerializer
import order_pb2  # generated from .proto file

protobuf_serializer = ProtobufSerializer(order_pb2.Order, {'schema.registry.url': 'http://localhost:8081'})

producer = SerializingProducer({
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': protobuf_serializer
})

order = order_pb2.Order(order_id=303, status="dispatched")
producer.produce(topic='orders', key='user999', value=order)
producer.flush()
```

## 4.1 Consumer Commits

- **Definition**: A commit is the act of a consumer recording the offset of the last message it has successfully processed. This ensures that if the consumer restarts, it knows where to resume.
- **Mechanism**:
    - Consumers read messages from partitions.
    - After processing, they commit the offset to Kafka (or an external store).
    - This commit is a guarantee of progress tracking.
- **Types of commits**:
    - **Automatic commits**: Kafka periodically commits offsets in the background.
    - **Manual commits**: The consumer explicitly commits offsets after processing.
- **Trade-offs**:
    - Auto commits are simpler but risk committing before actual processing.
    - Manual commits give control but require careful handling to avoid duplicates or data loss.
- **Code Example (Python with Kafka-Python)**:
    
    ```python
    from kafka import KafkaConsumer
    
    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers='localhost:9092',
        enable_auto_commit=False,
        group_id='order-service'
    )
    
    for message in consumer:
        process_order(message.value)
        consumer.commit()  # Explicit commit after processing
    ```
    
- **Reliability aspect**: Commits ensure consumers can recover from crashes without reprocessing the entire log.

## 4.2 Idempotent Producers

- **Definition**: Idempotence means producing the same message multiple times results in only one copy being stored in Kafka.
- **Problem solved**: Without idempotence, retries can cause duplicate messages in topics.
- **How Kafka ensures idempotence**:
    - Each producer session gets a unique **Producer ID (PID)**.
    - Each message has a monotonically increasing sequence number.
    - Broker ensures only one copy of each sequence number is stored.
- **Guarantee**: Exactly-once delivery to a partition.
- **Code Example (Python)**:
    
    ```python
    from kafka import KafkaProducer
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        acks='all',
        enable_idempotence=True  # Ensures idempotent producer
    )
    
    producer.send('payments', b'payment-123')
    producer.flush()
    ```
    
- **Reliability aspect**: Prevents duplicates during retries, critical for financial transactions or inventory updates.

## 4.3 Delivery Semantics

- **Definition**: Delivery semantics describe how Kafka guarantees message delivery between producers and consumers.
- **Types**:
    1. **At-most-once**:
        - Messages may be lost but never duplicated.
        - Commit happens before processing.
        - Fast but unreliable.
    2. **At-least-once**:
        - Messages are never lost but may be duplicated.
        - Commit happens after processing.
        - Reliable but requires deduplication logic downstream.
    3. **Exactly-once**:
        - Each message is processed once and only once.
        - Achieved by combining idempotent producers with transactional writes and consumer commits.
- **Transactional API**:
    - Kafka supports transactions across multiple partitions.
    - Ensures atomicity: either all writes succeed or none.
- **Code Example (Python)**:
    
    ```python
    from kafka import KafkaProducer
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        acks='all',
        enable_idempotence=True,
        transactional_id='txn-1'
    )
    
    producer.init_transactions()
    try:
        producer.begin_transaction()
        producer.send('orders', b'order-456')
        producer.send('inventory', b'decrement-item-456')
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()
    ```
    
- **Reliability aspect**:
    - At-most-once: fastest, least reliable.
    - At-least-once: reliable, but duplicates possible.
    - Exactly-once: strongest guarantee, requires more overhead.

## 5.1 Retention Policies

Kafka stores events in logs for a configurable duration or until a size threshold is reached.

- **Definition**: Retention policies determine how long Kafka keeps messages before deleting them.
- **Types**:
    - **Time-based retention**: Messages are retained for a fixed duration (e.g., 7 days).
    - **Size-based retention**: Messages are retained until the log reaches a configured size.
- **Use cases**:
    - Short retention for ephemeral data (e.g., clickstream).
    - Long retention for audit logs or replay scenarios.

**CLI Example**:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic user-events \
--config retention.ms=604800000 \
--config retention.bytes=1073741824
```

This creates a topic with **7 days retention** and **1 GB size limit**.

## 5.2 Log Compaction

Log compaction ensures Kafka retains the **latest value for each key**, rather than deleting old records purely by time or size.

- **Definition**: Compaction rewrites logs to keep only the most recent record per key.
- **Purpose**: Useful for stateful data (e.g., user profile updates).
- **Behavior**:
    - Older records with the same key are removed.
    - Guarantees that consumers can reconstruct the latest state.

**CLI Example**:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic user-profiles \
--config cleanup.policy=compact
```

This topic will **compact logs** instead of deleting them by retention.

## 5.3 Kafka Streams

Kafka Streams is a **Java library** for building real-time applications on top of Kafka. Since the video doesn’t show code, here’s a Python‑style equivalent using **Quix Streams**, which is designed for streaming data pipelines in Python.

- **Concepts**:
    - **Stream processing**: Continuous computation over data streams.
    - **Operators**: Map, filter, join, aggregate.
    - **State stores**: Maintain local state for aggregations.

**Python Example with Quix Streams**:

```python
from quixstreams import Application

# Create a Quix Streams application
app = Application(broker_address="localhost:9092", consumer_group="orders-app")

# Define input and output topics
orders_topic = app.topic("orders")
high_value_topic = app.topic("high-value-orders")

# Stream pipeline
def process_order(stream):
    return stream.filter(lambda order: float(order["amount"]) > 1000)

# Attach pipeline
app.dataframe(orders_topic).apply(process_order).to_topic(high_value_topic)

# Run the application
app.run()
```

This mimics Kafka Streams’ **filtering and processing** logic in Python.

## 5.4 Kafka Connect

Kafka Connect is a **framework for integrating Kafka with external systems** (databases, cloud storage, etc.).

- **Definition**: Provides source and sink connectors to move data in/out of Kafka.
- **Modes**:
    - **Standalone mode**: Single process, simple deployments.
    - **Distributed mode**: Clustered, fault-tolerant deployments.
- **Use cases**:
    - Ingesting data from MySQL into Kafka.
    - Exporting Kafka topics to Elasticsearch or S3.

**Connector Example (JSON config)**:

```json
{
  "name": "mysql-source-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://localhost:3306/shop",
    "connection.user": "root",
    "connection.password": "password",
    "table.whitelist": "orders",
    "mode": "incrementing",
    "incrementing.column.name": "id",
    "topic.prefix": "mysql-"
  }
}
```

This connector streams **MySQL orders table** into Kafka topics.

## 6.1 Scaling Kafka

Scaling Kafka in production means expanding cluster capacity to handle higher throughput and more partitions. Kafka scales horizontally by adding brokers to the cluster. Each broker manages a subset of partitions, and replication ensures fault tolerance.

- **Broker Addition**: New brokers can be added seamlessly; partitions are rebalanced across brokers.
- **Partition Scaling**: Topics can be configured with more partitions to increase parallelism.
- **Replication Factor**: Adjusting replication ensures durability but increases storage and network overhead.
- **Cluster Balancing**: Tools like `kafka-reassign-partitions.sh` redistribute partitions across brokers.

**CLI Example — Increase partitions for a topic**:

```bash
kafka-topics.sh --alter --topic orders \
--bootstrap-server localhost:9092 \
--partitions 10
```

**Production Note**: Scaling is not just about adding brokers; it requires monitoring disk usage, network bandwidth, and ensuring replication lag stays minimal.

## 6.2 Monitoring & Lag

Monitoring Kafka is critical for production reliability. The most important metric is **consumer lag** — the difference between the latest offset in a partition and the consumer’s committed offset.

- **Lag Definition**: High lag means consumers are falling behind producers.
- **Metrics to Monitor**:
    - Broker health (CPU, memory, disk I/O).
    - Topic partition distribution.
    - Replication status (ISR consistency).
    - Consumer group lag.
- **Tools**:
    - Kafka’s built-in JMX metrics.
    - External monitoring with **Prometheus + Grafana**.
    - Confluent Control Center for enterprise setups.

**CLI Example — Describe consumer group lag**:

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
--describe --group group1
```

**CLI Example — Describe topic details**:

```bash
kafka-topics.sh --describe --topic orders \
--bootstrap-server localhost:9092
```

**Production Note**: Lag must be monitored continuously. If lag grows, either consumers need scaling (more instances in the group) or partitions must be increased.

## 6.3 Kafka in Real Data Engineering Pipelines

Kafka is the backbone of modern data engineering pipelines, acting as the central event bus.

- **Integration with ETL/ELT**: Kafka streams events into warehouses (Snowflake, BigQuery, Redshift) via **Kafka Connect**.
- **Streaming Analytics**: Real-time dashboards powered by Kafka Streams or Flink.
- **Microservices Communication**: Services publish/subscribe to Kafka topics instead of direct API calls.
- **Data Lake Ingestion**: Kafka pipelines push raw events into HDFS, S3, or GCS for long-term storage.
- **Monitoring Pipelines**: Lag metrics ensure downstream systems are keeping up.

**Python Producer (Production-Grade)**:

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['broker1:9092','broker2:9092'],
    acks='all',
    retries=5,
    linger_ms=10,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event = {"order_id": 123, "status": "created"}
producer.send('orders', key=b'order123', value=event)
producer.flush()
```

**Python Consumer (Production-Grade)**:

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    group_id='order-service',
    bootstrap_servers=['broker1:9092','broker2:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in consumer:
    print(f"Offset={message.offset}, Key={message.key}, Value={message.value}")
```

**Pipeline Example**:

1. Producers publish order events.
2. Kafka Connect streams events into a warehouse.
3. Kafka Streams aggregates real-time metrics.
4. Consumers (microservices) react to events independently.