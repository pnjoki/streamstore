from confluent_kafka import Consumer
import json

Consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'tracker_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(Consumer_config)
consumer.subscribe(['orders'])
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        order = json.loads(msg.value().decode('utf-8'))
        print(f"📨Received order: {order}") 
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
