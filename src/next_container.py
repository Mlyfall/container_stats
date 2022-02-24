from confluent_kafka import Producer
from dataloader.dataloader_factory import dataloader_factory
from dataloader.direction import Direction
import os
import time


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def next_syscall(syscalls_of_current_recording, recordings_of_current_type, data_type_iterator, end):
    """Returns the next syscall of the recording, the next recording, the next datatype and stop flag.
       If end of recording is reached, continues with syscalls of subsequent recording,
       analogous behaviour for end of data_type.
       If end of recording/datatype is reached stop variable is set True so the current batch will be closed."""

    try:
        next_syscall = next(syscalls_of_current_recording)
        stop = False

    except StopIteration:
        try:
            syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
            next_syscall = next(syscalls_of_current_recording)
            stop = True

        except StopIteration:
            try:
                recordings_of_current_type = iter(next(data_type_iterator))
                syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
                next_syscall = next(syscalls_of_current_recording)
                stop = True

            except StopIteration:
                next_syscall = None
                stop = True
                end = True

    return next_syscall, syscalls_of_current_recording, recordings_of_current_type, data_type_iterator, stop, end


def send_batch_to_kafka(syscall_batch):
    """Checks for the batch to be non-empty and sends each syscall to broker."""

    if len(syscall_batch) > 0:
        print("Sending new batch!")
        for syscall in syscall_batch:
            #print(syscall)
            p.poll(0)
            p.produce("test", syscall.encode("utf-8"), callback=delivery_report)
        p.flush()


if __name__ == '__main__':

    configs = {"bootstrap.servers": "broker:9092"}
    p = Producer(configs)

    # loading data
    data_base_path = "/DS"
    # scenario_names = os.listdir(data_base_path)
    scenario_name = "CVE-2017-7529"
    scenario_path = os.path.join(data_base_path, scenario_name)
    dataloader = dataloader_factory(scenario_path, direction=Direction.BOTH)

    # getting first syscall of scenario using next()
    data_type_iterator = iter([dataloader.training_data(), dataloader.validation_data(), dataloader.test_data()])
    recordings_of_current_type = iter(next(data_type_iterator))
    syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
    current_syscall = next(syscalls_of_current_recording)
    timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()

    loopend = time.time_ns()
    timestamp_last_syscall = timestamp_current_syscall
    end = False

    # generating syscall batches with more realistic timing taking computing time of following while loop into account:
    # looptime referring to the time interval of the last loop
    # jumptime referring to the time needed for jumping to the start of the new while loop and creating empty batch
    while end is False:
        syscall_batch = []
        loopstart = time.time_ns()
        jumptime = loopstart - loopend
        looptime = 0

        # appending syscall batch list if its timestamp is within time interval
        while timestamp_current_syscall <= timestamp_last_syscall + jumptime + looptime:
            try:
                syscall_batch.append(current_syscall.syscall_line)
            except AttributeError:
                print("AttributeError for current_syscall.syscall_line : Something went wrong.")
                break

            # getting new syscall, opening new recording or datatype if necessary
            current_syscall, syscalls_of_current_recording, recordings_of_current_type, data_type_iterator, stop, end = next_syscall(
                syscalls_of_current_recording,
                recordings_of_current_type,
                data_type_iterator, end)

            if current_syscall is not None:
                timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()

            # stops creating of current batch and leaves while loop
            if stop is True:
                break

        # sends batch to kafka broker if batch is non-empty
        send_batch_to_kafka(syscall_batch)

        # stopping looptime and setting new time variable for next loop
        loopend = time.time_ns()
        looptime = loopend - loopstart
        timestamp_last_syscall = timestamp_last_syscall + looptime

    print("End of scenario.")