import pb_syscall_pb2
from confluent_kafka import Consumer

"""Kafka Consumer receiving serialized messages through Protocol Buffer."""

c = Consumer({
    "bootstrap.servers": "broker:9092",
    "group.id": "mygroup",
    'auto.offset.reset': 'earliest'
})

c.subscribe(["test"])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    # creating protobuf object for incoming syscall and deserialize
    pb_syscall = pb_syscall_pb2.Syscall()

    syscall_line_received = pb_syscall.ParseFromString(msg.value())
    print(syscall_line_received)


c.close()
