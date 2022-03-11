# Protobuf generated class; resides at ./user_pb2.py
import pb_syscall_pb2
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer


protobuf_deserializer = ProtobufDeserializer(pb_syscall_pb2.Syscall,
                                             {'use.deprecated.format': False})
string_deserializer = StringDeserializer('utf_8')

consumer_conf = {'bootstrap.servers': "broker:9092",
                 'key.deserializer': string_deserializer,
                 'value.deserializer': protobuf_deserializer,
                 'group.id': "mygroup",
                 'auto.offset.reset': "earliest"}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe("test")

while True:
    try:
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        syscall = msg.value()
        if syscall is not None:
            print(msg.key(syscall.timestamp, syscall.user_id, syscall.process_id, syscall.process_name, syscall.thread_id, syscall.syscall_name, syscall.direction, syscall.params.begin))
    except KeyboardInterrupt:
        break

consumer.close()


