from confluent_kafka import Producer
from dataloader.dataloader_factory import dataloader_factory
from dataloader.direction import Direction
import os
import time

configs = {"bootstrap.servers": "broker:9092"}
p = Producer(configs)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


# loading data
data_base_path = "/DS"
# data_base_path = "/home/emmely/PycharmProjects/LID-DS-2021-fixed-exploit-time"
scenario_names = os.listdir(data_base_path)
scenario_path_example = os.path.join(data_base_path, scenario_names[0])
dataloader = dataloader_factory(scenario_path_example, direction=Direction.BOTH)

# getting first syscall of scenario using next()
data_type_iterator = iter([dataloader.training_data(), dataloader.validation_data(), dataloader.test_data()])
recordings_of_current_type = iter(next(data_type_iterator))
syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
current_syscall = next(syscalls_of_current_recording)
timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()

system_time_start = time.time_ns()
timestamp_last_syscall = timestamp_current_syscall

# generating syscall batches with more realistic timing taking computing time into account
while True:
    syscall_batch = []
    system_time_now = time.time_ns()
    t_delta = system_time_now - system_time_start

#  appending syscall batch list
    while timestamp_current_syscall <= t_delta + timestamp_last_syscall:
        syscall_batch.append(current_syscall.syscall_line)
        try:
            current_syscall = next(syscalls_of_current_recording)
            timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()
        except StopIteration:
            try:
                syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
                current_syscall = next(syscalls_of_current_recording)
                timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()
            except StopIteration:
                recordings_of_current_type = iter(next(data_type_iterator))
                syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
                current_syscall = next(syscalls_of_current_recording)
                timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()

# producing kafka messages with syscall batch
    if len(syscall_batch) > 0:
        for syscall in syscall_batch:
            print(syscall)
            p.poll(0)
            p.produce("test", syscall.encode("utf-8"), callback=delivery_report)
        p.flush()

# setting new time variables for new batch loop
    timestamp_last_syscall = timestamp_current_syscall
    system_time_start = system_time_now
