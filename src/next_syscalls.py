import os
import time
from dataloader.dataloader_factory import dataloader_factory
from dataloader.direction import Direction

"""Script for testing sending process without Kafka/Docker. """


def next_syscall(syscalls_of_current_recording, recordings_of_current_type, data_type_iterator, end):
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
            stop = True

        except StopIteration:
            try:
                recordings_of_current_type = iter(next(data_type_iterator))
                print("Opened next datatype.")
                syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
                next_syscall = next(syscalls_of_current_recording)
                stop = True

            except StopIteration:
                next_syscall = None
                end = True
                stop = True

    return next_syscall, syscalls_of_current_recording, recordings_of_current_type, data_type_iterator, stop, end


def write_batch_to_file(syscall_batch):
    print(len(syscall_batch))
    with open("syscalls_of_" + scenario_name + ".sc", "a") as file:
        for syscall in syscall_batch:
            file.write(syscall + "\n")


if __name__ == '__main__':

    # loading data
    data_base_path = "/home/emmely/PycharmProjects/LID-DS-2021-fixed-exploit-time"
    # scenario_names = os.listdir(data_base_path)
    scenario_name = "CVE-2017-7529"
    scenario_path = os.path.join(data_base_path, scenario_name)
    dataloader = dataloader_factory(scenario_path, direction=Direction.BOTH)

    # getting first syscall of scenario using next()
    data_type_iterator = iter([dataloader.training_data(), dataloader.validation_data(), dataloader.test_data()])
    recordings_of_current_type = iter(next(data_type_iterator))
    first_recording = next(recordings_of_current_type)
    first_recording_name = first_recording.name
    syscalls_of_current_recording = first_recording.syscalls()
    current_syscall = next(syscalls_of_current_recording)
    timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()

    loopend = time.time_ns()
    timestamp_last_syscall = timestamp_current_syscall

    end = False
    print(f"starting with recording {first_recording_name}")

    # generating syscall batches with more realistic timing taking computing time into account
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

            current_syscall, syscalls_of_current_recording, recordings_of_current_type, data_type_iterator, stop, end = next_syscall(
                syscalls_of_current_recording,
                recordings_of_current_type,
                data_type_iterator, end)

            if current_syscall is not None:
                timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()

            if stop is True:
                break

        if len(syscall_batch) > 0:
            write_batch_to_file(syscall_batch)

        # setting new time variables for new batch loop
        loopend = time.time_ns()
        looptime = loopend - loopstart
        timestamp_last_syscall = timestamp_last_syscall + looptime

    print(f"End of scenario {scenario_name}")
