from kafka import KafkaConsumer
consumer=KafkaConsumer(
    'transaction-events',
    bootstrap_servers='localhost:9092',
    enable_auto_commit=True,
    auto_offset_reset='earliest',
    group_id='transaction-events-group'
)

for record in consumer:
    print(f"offset={record.offset}, key={record.key}, value={record.value}")
