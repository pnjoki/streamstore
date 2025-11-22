from confluent_kafka import Producer
import json
import uuid

Producer_config = {
    'bootstrap.servers': 'localhost:9092'
}  

producer = Producer(Producer_config)


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        print(f"Message delivered successfully: {msg.value().decode('utf-8')}"  
              
              
)
        

order = {
    "order_id": str(uuid.uuid4()),
    "user": "john_doe",
    "product": "laptop",
    "quantity": 5

}

value = json.dumps(order).encode('utf-8')

producer.produce(
    topic='orders', 
    value=value,
    callback=delivery_report
)

producer.flush()
