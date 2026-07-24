from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=5,
    linger_ms=10
)

event = producer.send('orders', key=b'orderId', value=b'Order Created')
record_metadata = event.get(timeout=10)

print(f"Message sent successfully!")
print(f"  Topic     : {record_metadata.topic}")
print(f"  Partition : {record_metadata.partition}")
print(f"  Offset    : {record_metadata.offset}")

producer.flush()
producer.close()
