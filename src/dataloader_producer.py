from confluent_kafka import Producer
from dataloader import dataloader_factory
from dataloader.direction import Direction

data_base_path = "/home/eschulze/LID-DS-2021-no-relative-time"
dataloader = dataloader_factory(data_base_path, direction=Direction.CLOSE)
data_types = [dataloader.validation_data(), dataloader.test_data(), dataloader.training_data()]

# create producer client
configs = {"bootstrap.servers": "broker:9092"}
p = Producer(configs)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


for data in data_types:
    for recording in data:
        for syscall in recording.syscalls():
            print(syscall.syscall_line)
            """p.poll(0)

            p.produce("test", syscall.syscall_line.encode('utf-8'), callback=delivery_report)

p.flush()"""