import docker
import time
import csv
from tqdm import tqdm

"""This script 
        runs kafka producer and consumer container from existing kafka_client image (see Dockerfile)
        reads producer logs which occur every second and writes them to csv (in this case network data only)
        calculates the average traffic in B/MB per second transmitted by producer.
        
        Make sure broker and zookeeper containers are running."""


def save_stats(container: docker.models.containers.Container, all_stats=False):
    with open(str(container.name) + "_stats.csv", "w") as container_stats:
        writer = csv.writer(container_stats)
        writer.writerow(["transmitted data in bytes", "timestamp"])

        first_status = True

        # counter for shorter running periods for testing code
        counter_until_break = 0
        number_stats_testing = 50

        if all_stats is False:
            for stat in tqdm(container.stats(decode=True)):
                if counter_until_break <= number_stats_testing:
                    if first_status is True:
                        transmitted_data = stat["networks"]["eth0"]["tx_bytes"]
                        current_timestamp = time.time()
                        writer.writerow([transmitted_data, current_timestamp])
                        old_tx = transmitted_data
                        counter_until_break += 1
                        first_status = False
                    else:
                        transmitted_data = stat["networks"]["eth0"]["tx_bytes"]
                        current_timestamp = time.time()
                        counter_until_break += 1
                        if transmitted_data != old_tx:
                            writer.writerow([transmitted_data, current_timestamp])
                else:
                    break

        if all_stats is True:
            for stat in tqdm(container.stats(decode=True)):
                if counter_until_break <= number_stats_testing:
                    transmitted_data = stat["networks"]["eth0"]["tx_bytes"]
                    current_timestamp = time.time()
                    writer.writerow([transmitted_data, current_timestamp])
                    counter_until_break += 1
                else:
                    break

    return str(container.name + "_stats.csv")


def calc_average_traffic(file: str):
    sum_bytes = 0
    with open(file, "r") as container_stats:
        reader = csv.reader(container_stats, delimiter=",")

        for counter, row in enumerate(reader, start=1):
            if counter == 1:
                continue
            else:
                sum_bytes += int(row[0])

        average_traffic_per_sec = sum_bytes / counter
        print(f"average bytes per second : {average_traffic_per_sec}, "
              f"average MB per second : {average_traffic_per_sec / 1024 ** 2}")


if __name__ == '__main__':
    # instantiate docker client
    client = docker.from_env()

    # build image for producer/consumer
    # kafka_client_image = client.images.build(dockerfile="/home/emmely/PycharmProjects/test/Dockerfile", tag="kafka_client") ????

    # run producer and consumer
    producer_volume_database = ["/home/emmely/PycharmProjects/LID-DS-2021-fixed-exploit-time/:/DS/:ro"]
    producer_entrypoint = "python3 /work/next.py"
    producer = client.containers.run(detach=True,
                                     image="kafka_client",
                                     network="net_kafka",
                                     name="producer", volumes=producer_volume_database,
                                     entrypoint=producer_entrypoint)

    consumer_entrypoint = "python3 /work/simple_consumer.py"
    consumer = client.containers.run(detach=True,
                                     image="kafka_client",
                                     network="net_kafka",
                                     name="consumer",
                                     entrypoint=consumer_entrypoint)

    # check for all containers to be running
    setup = client.containers.list()
    for container in setup:
        print(f"{container.name} : {container.status}")

    # saving producer stats and calculating average
    # if all stats is set true, every status is written to csv even if tx_bytes value didn't change from previous status
    all_stats = True
    try:
        stats_file = save_stats(producer, all_stats)
        calc_average_traffic(stats_file)

    except docker.errors.APIError:
        print("Docker Server Error: Check if producer is running properly.")
