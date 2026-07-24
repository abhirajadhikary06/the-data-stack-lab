from kafka import KafkaProducer
producer=KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=3,
    linger_ms=5
)
for e in range(21,31,2):
    key=f"ie_{e}".encode()
    if e%2==0:
        event = producer.send('loop-orders', key=key, value=b'order_placed')
        metadata = event.get(timeout=10)
        print(f"topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset}")
    else:
        event = producer.send('loop-orders', key=key, value=b'order_cancelled')
        metadata = event.get(timeout=10)
        print(f"topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset}")
producer.flush()
producer.close()