from kafka import KafkaConsumer
consumer=KafkaConsumer(
    'serial-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    group_id='serial-orders-group',
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    value_deserializer=lambda v: v.decode('utf-8') if v else 'NaN'
)

for record in consumer:
    print(f"offset={record.offset}, key={record.key}, value={record.value}")
