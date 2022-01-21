from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="127.0.0.1:29092")
for i in range(100):
    producer.send(topic='dantopic', value=bytes(i))