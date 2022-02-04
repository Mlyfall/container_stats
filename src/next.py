from confluent_kafka import Producer
from dataloader.dataloader_factory import dataloader_factory
from dataloader.direction import Direction
import os
import time

""""# create producer client
configs = {"bootstrap.servers": "broker:9092"}
p = Producer(configs)"""


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


# data_base_path = "../DS"
data_base_path = "/home/eschulze/Projects/LID-DS-2021-fixed-exploit-time"
scenario_names = os.listdir(data_base_path)
scenario_path_example = os.path.join(data_base_path, scenario_names[0])
#test_path_marco = "/home/scadspc14/PycharmProjects/CVE-2017-7529"

dataloader = dataloader_factory(data_base_path, direction=Direction.BOTH)
data_type_list = iter([dataloader.training_data(), dataloader.validation_data(), dataloader.test_data()])
recs_of_current_type = iter(next(data_type_list))
syscalls_of_current_rec = next(recs_of_current_type).syscalls()
current_sys = next(syscalls_of_current_rec)
current_timestamp = current_sys.timestamp_unix_in_ns()

t_0 = time.time_ns()
last_timestamp = current_timestamp

while True:
    syscall_batch = []
    t_help = time.time_ns()
    t_delta = t_help - t_0

    while current_timestamp <= t_delta + last_timestamp:
        syscall_batch.append(current_sys.syscall_line)
        try:
            current_sys = next(syscalls_of_current_rec)
            current_timestamp = current_sys.timestamp_unix_in_ns()
        except StopIteration:
            try:
                syscalls_of_current_rec = next(recs_of_current_type).syscalls()
                current_sys = next(syscalls_of_current_rec)
                current_timestamp = current_sys.timestamp_unix_in_ns()
            except StopIteration:
                recs_of_current_type = iter(next(data_type_list))
                syscalls_of_current_rec = next(recs_of_current_type).syscalls()
                current_sys = next(syscalls_of_current_rec)
                current_timestamp = current_sys.timestamp_unix_in_ns()

    if len(syscall_batch) > 0:
        print("hurray")
        for syscall in syscall_batch:
            print(syscall)
            """p.poll(0)
               p.produce("test", syscall.encode("utf-8"), callback=delivery_report)
        p.flush()"""

    last_timestamp = current_timestamp
    t_0 = t_help





