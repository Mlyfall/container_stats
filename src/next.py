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

dataloader = dataloader_factory(scenario_path_example, direction=Direction.BOTH)
data_type_list = iter([dataloader.training_data(), dataloader.validation_data(), dataloader.test_data()])
recs_of_current_type = iter(next(data_type_list))
syscalls_of_current_rec = next(recs_of_current_type).syscalls()
current_sys = next(syscalls_of_current_rec)
current_timestamp = current_sys.timestamp_unix_in_ns()
t_0 = time.time_ns()

while True:
    syscall_batch = []
    t_help = time.time_ns()
    t_delta = t_help - t_0

    while current_timestamp <= t_delta + current_timestamp:
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

        #sending in batches rather than separately?
        for syscall in syscall_batch:
            print(syscall)

    t_0 = t_help




"""for data in data_types:
        for recording in data:
            for syscall in recording.syscalls():

                p.poll(0)
    
                p.produce("test", syscall.syscall_line.encode('utf-8'), callback=delivery_report)
    
    p.flush()"""

