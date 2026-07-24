from kafka import KafkaProducer
producer=KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=5,
    linger_ms=5,
    enable_idempotence=True,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: v.encode('utf-8')
)
event=producer.send('idem-orders', key='user-102', value='payment processing')
event=producer.send('idem-orders', key='user-102', value='payment done')
metadata=event.get(timeout=10)
print(f"topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset}")
