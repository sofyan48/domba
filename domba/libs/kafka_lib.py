from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads, dumps

def get_kafka_consumer(broker, topic, group_id):
    try:
        consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=[broker],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: loads(x.decode('utf-8')))
    except Exception as e:
        print(e)
    else:
        return consumer


def get_kafka_producer(broker):
    try:
        producer = KafkaProducer(bootstrap_servers=[broker],
                    value_serializer=lambda x: dumps(x).encode('utf-8'))
    except Exception as e:
        print(e)
    else:
        return producer


def producer_send(broker, topic, data):
    producer = get_kafka_producer(broker)
    producer.send(topic, data)
    producer.flush()