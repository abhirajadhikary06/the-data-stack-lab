from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'loop-orders',
    bootstrap_servers='localhost:9092',
    group_id='loop-orders-group',
    auto_offset_reset='earliest'
)

for record in consumer:
    print(f"offset={record.offset}, key={record.key}, value={record.value}")