from kafka import KafkaConsumer
consumer=KafkaConsumer(
    'idem-orders',
    bootstrap_servers='localhost:9092',
    enable_auto_commit=True,
    auto_offset_reset='latest',
    group_id='idem-orders-group'
)

for record in consumer:
    print(f"offset={record.offset}, key={record.key}, value={record.value}")