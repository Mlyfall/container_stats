import os
import time
from uuid import uuid4

# Protobuf generated class; resides at ./user_pb2.py
import pb_syscall_pb2
from dataloader.direction import Direction
from dataloader.dataloader_factory import dataloader_factory
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


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


def send_batch_to_kafka(syscall_batch: list):
    """Checks for the batch to be non-empty and sends each syscall to broker."""

    if len(syscall_batch) > 0:
        print(f"Sending new batch of length {len(syscall_batch)}!")
        for syscall in syscall_batch:
            # Serve on_delivery callbacks from previous calls to produce()
            p.poll(0.0)
            try:
                # construct protobuf syscall object
                pb_syscall = pb_syscall_pb2.Syscall()
                pb_syscall.timestamp = syscall.split(" ")[0]
                pb_syscall.user_id = syscall.split(" ")[1]
                pb_syscall.process_id = syscall.split(" ")[2]
                pb_syscall.process_name = syscall.split(" ")[3]
                pb_syscall.thread_id = syscall.split(" ")[4]
                pb_syscall.syscall_name = syscall.split(" ")[5]
                pb_syscall.direction = syscall.split(" ")[6]
                pb_syscall.params_begin = " ".join(syscall.split(" ")[7:])
                p.produce(topic="test", key=str(uuid4()), value=pb_syscall,
                          on_delivery=delivery_report)
            except (KeyboardInterrupt, EOFError):
                break
            except ValueError:
                print("Invalid input, discarding record...")
                continue
        p.flush()


if __name__ == '__main__':

    schema_registry_conf = {'url': "localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    protobuf_serializer = ProtobufSerializer(pb_syscall_pb2.Syscall,
                                             schema_registry_client,
                                             {'use.deprecated.format': True})

    configs = {"bootstrap.servers": "broker:9092",
               'key.serializer': StringSerializer('utf_8'),
               'value.serializer': protobuf_serializer}

    p = SerializingProducer(configs)

    # loading data
    data_base_path = "/DS"
    # scenario_names = os.listdir(data_base_path)
    scenario_name = "Juice-Shop"
    scenario_path = os.path.join(data_base_path, scenario_name)
    dataloader = dataloader_factory(scenario_path, direction=Direction.BOTH)

    # getting first syscall of scenario using next()
    data_type_iterator = iter([dataloader.training_data(), dataloader.validation_data(), dataloader.test_data()])
    recordings_of_current_type = iter(next(data_type_iterator))
    first_recording = next(recordings_of_current_type)
    first_recording_name = first_recording.name
    syscalls_of_current_recording = next(recordings_of_current_type).syscalls()
    current_syscall = next(syscalls_of_current_recording)
    timestamp_current_syscall = current_syscall.timestamp_unix_in_ns()

    loopend = time.time_ns()
    timestamp_last_syscall = timestamp_current_syscall
    end = False
    print(f"starting with recording {first_recording_name}")

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


