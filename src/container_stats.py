import docker
import time
import csv
from tqdm import tqdm

"""This script 
        runs kafka producer and consumer container from existing kafka_client image (see Dockerfile)
        reads producer logs which occur every second and writes them to csv (in this case network data only)
        calculates the average traffic in B/MB per second transmitted by producer.
        
        Make sure broker and zookeeper containers are running."""


def save_stats(container: docker.models.containers.Container, final_time, all_stats=False):
    with open(str(container.name) + "_stats.csv", "w") as container_stats:
        writer = csv.writer(container_stats)
        writer.writerow(["transmitted data in bytes", "timestamp"])

        first_status = True

        if all_stats is False:
            for stat in tqdm(container.stats(decode=True), unit="logs"):

                if first_status is True:
                    try:
                        transmitted_data = stat["networks"]["eth0"]["tx_bytes"]
                        current_timestamp = time.time()
                        writer.writerow([transmitted_data, current_timestamp])
                        old_tx = transmitted_data

                        first_status = False
                    except KeyError:
                        break
                else:
                    try:
                        transmitted_data = stat["networks"]["eth0"]["tx_bytes"]
                        current_timestamp = time.time()

                        if transmitted_data != old_tx:
                            writer.writerow([transmitted_data, current_timestamp])
                    except KeyError:
                        break

        if all_stats is True:
            while time.time() < final_time:
                for stat in tqdm(container.stats(decode=True), unit="logs"):
                    try:
                        transmitted_data = stat["networks"]["eth0"]["tx_bytes"]
                        current_timestamp = time.time()
                        writer.writerow([transmitted_data, current_timestamp])

                    except KeyError:
                        print("KeyError in stats.")
                        break
            print(f"Reached defined runtime.")

    return str(container.name + "_stats.csv")


def calc_traffic(file: str):
    with open(file, "r") as container_stats:
        row_strings = container_stats.readlines()
        # reader = csv.reader(container_stats, delimiter=",")

        final_row = row_strings[-1]
        counter = len(row_strings)

        total_bytes = int(final_row.split(",")[0])
        total_mb = total_bytes / 1024 ** 2
        mb_per_sec = total_mb / counter
        print(f"total bytes sent: {total_bytes}, "
              f"total MB sent : {total_mb},"
              f"MB per second: {mb_per_sec}")


if __name__ == '__main__':
    # instantiate docker client
    client = docker.from_env()

    # build image for producer/consumer
    # kafka_client_image = client.images.build(dockerfile="/home/emmely/PycharmProjects/test/Dockerfile", tag="kafka_client") ????

    # run producer and consumer
    #producer_volume_database = ["/home/emmely/PycharmProjects/LID-DS-2021-fixed-exploit-time/:/DS/:ro"]
    producer_volume_database = ["/home/emmely/Projects/Datensatz/:/DS/:ro"]
    producer_entrypoint = "python3 /work/next_container.py"
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

    # setting timer
    runtime_total = 60
    timer_start = time.time()
    final_time = runtime_total + timer_start

    # if all stats is set true, every status is written to csv even if tx_bytes value didn't change from previous status
    all_stats = True
    try:
        stats_file = save_stats(producer, final_time, all_stats)
        calc_traffic(stats_file)

    except docker.errors.APIError:
        print("Docker Server Error: Check if producer is running properly.")
