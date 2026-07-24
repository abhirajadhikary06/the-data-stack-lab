from kafka import KafkaProducer
producer=KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks=1,
    retries=3,
    linger_ms=10
)

producer.send('mk', key=b'user-123', value=b'Order Created')
producer.send('mk', key=b'user-123', value=b'Order Placed')
producer.send('mk', key=b'user-125', value=b'Order Created')
producer.send('mk', key=b'user-124', value=b'Order Delivered')
producer.send('mk', key=b'user-125', value=b'Order Cancelled')
producer.send('mk', key=b'user-124', value=b'Order Returned')

producer.flush()
producer.close()