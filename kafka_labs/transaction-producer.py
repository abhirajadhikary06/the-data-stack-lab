from kafka import KafkaProducer
producer=KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=3,
    linger_ms=10,
    enable_idempotence=True,
    transactional_id='txn-producer-1'
)

producer.init_transactions()
try:
    producer.begin_transaction()
    producer.send('transaction-events', b'order-500')
    producer.commit_transaction()
except:
    producer.abort_transaction()