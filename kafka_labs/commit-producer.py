from kafka import KafkaProducer
import json
producer=KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks=1,
    retries=3,
    linger_ms=5,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event=producer.send('commit-orders', key='abhiraj', value={'user-score':90, 'total-orders':5, 'avg-order-val':2599})
metadata=event.get(timeout=5)
print(f"topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset}")

producer.flush()
producer.close()