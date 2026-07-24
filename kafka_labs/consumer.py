from kafka import KafkaConsumer
consumer = KafkaConsumer(
    'orders',
    group_id='orders-group',
    bootstrap_servers='localhost:9092'
)

for record in consumer:
    print(f"offset={record.offset}, key={record.key}, value={record.value}")