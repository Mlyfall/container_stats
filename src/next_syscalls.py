import os
import time
from dataloader.dataloader_factory import dataloader_factory
from dataloader.direction import Direction

"""Script for testing sending process without Kafka/Docker. """


def next_syscall(system_time_now, syscalls_of_current_recording, recordings_of_current_type, data_type_iterator, end):
    """Returns the next syscall of the recording.
       If end of recording is reached, continues with syscalls of subsequent recording,
       analogous behaviour for end of data_type."""

    try:
        next_syscall = next(syscalls_of_current_recording)
        stop = False

    except StopIteration:
        try:
            syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
            print("Opened next recording.")
            next_syscall = next(syscalls_of_current_recording)
            system_time_now = time.time_ns()
            stop = True

        except StopIteration:
            try:
                recordings_of_current_type = iter(next(data_type_iterator))
                print("Opened next datatype.")
                syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
                next_syscall = next(syscalls_of_current_recording)
                system_time_now = time.time_ns()
                stop = True

            except StopIteration:
                next_syscall = None
                end = True
                stop = True

    return next_syscall, syscalls_of_current_recording, recordings_of_current_type, data_type_iterator, system_time_now, stop, end


def print_and_count(syscall_batch, looptime):

    first_time = int(syscall_batch[0].split(" ")[0])
    last_time = int(syscall_batch[-1].split(" ")[0])
    batch_interval = last_time - first_time
    print(len(syscall_batch))


if __name__ == '__main__':

    # loading data
    data_base_path = "/home/emmely/PycharmProjects/LID-DS-2021-fixed-exploit-time"
    # scenario_names = os.listdir(data_base_path)
    scenario_name = "CVE-2017-7529"
    scenario_path_example = os.path.join(data_base_path, scenario_name)
    dataloader = dataloader_factory(scenario_path_example, direction=Direction.BOTH)

    # getting first syscall of scenario using next()
    data_type_iterator = iter([dataloader.training_data(), dataloader.validation_data(), dataloader.test_data()])
    recordings_of_current_type = iter(next(data_type_iterator))
    syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
    current_syscall = next(syscalls_of_current_recording)
    timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()

    system_time_start = time.time_ns()
    timestamp_last_syscall = timestamp_current_syscall

    end = False
    # generating syscall batches with more realistic timing taking computing time into account
    while end is False:
        syscall_batch = []
        system_time_now = time.time_ns()
        looptime = system_time_now - system_time_start

        # appending syscall batch list if its timestamp is within time interval
        while timestamp_current_syscall <= timestamp_last_syscall + looptime:
            try:
                syscall_batch.append(current_syscall.syscall_line)
            except AttributeError:
                break

            current_syscall, syscalls_of_current_recording, recordings_of_current_type, data_type_iterator, system_time_now, stop, end = next_syscall(
                system_time_now,
                syscalls_of_current_recording,
                recordings_of_current_type,
                data_type_iterator, end)

            if current_syscall is not None:
                timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()

            if stop is True:
                system_time_now = time.time_ns()
                break

        if len(syscall_batch) > 0:
            print_and_count(syscall_batch, looptime)

        # setting new time variables for new batch loop
        timestamp_last_syscall = timestamp_current_syscall
        system_time_start = system_time_now

    print("End of scenario")


