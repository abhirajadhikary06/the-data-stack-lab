from kafka import KafkaProducer
import json
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=3,
    linger_ms=10,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event=producer.send('serial-orders', key='abhirajadhikary', value={'order_id': 101, 'status':'order-created', 'amt': 500})
event=producer.send('serial-orders', key='anik', value={'order_id': 102, 'status':'order-placed', 'amt': 999})
event=producer.send('serial-orders', key='abhirajadhikary', value={'order_id': 101, 'status':'order-shipped', 'amt': 499})
event=producer.send('serial-orders', key='anik', value={'order_id': 103, 'status':'order-cancelled', 'amt': 199})

producer.flush()
producer.close()
