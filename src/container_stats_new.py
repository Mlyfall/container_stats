import docker
import time
import csv
from tqdm import tqdm
from urllib3.connectionpool import xrange

"""This script 
        runs kafka producer and consumer container from existing kafka_client image (see Dockerfile)
        reads producer logs which occur every second and writes them to list (in this case network data only)
        calculates the average traffic in B/MB per second transmitted by producer.
        
        Make sure kafka broker and zookeeper containers are running."""


def save_stats(container: docker.models.containers.Container, final_time):
    """Writes container network stats to list until defined endtime is reached.
    """
    list_of_stats = []

    for stat in tqdm(container.stats(decode=True), unit="logs"):
        if time.time() < final_time:
            try:
                transmitted_data = stat["networks"]["eth0"]["tx_bytes"]
                current_timestamp = time.time()
                list_of_stats.append((int(transmitted_data), float(current_timestamp)))

            except KeyError:
                print("KeyError in stats.")
                break
        else:
            print("Reached defined runtime.")
            break

    return list_of_stats


def calc_traffic(stats_list: list):
    """Calculates average traffic from input list of status values.
    """
    number_of_stats = len(stats_list)
    total_bytes = stats_list[-1][0]

    total_kb = total_bytes * 0.001
    kb_per_sec = total_kb / number_of_stats
    print(f"total bytes sent in {number_of_stats} seconds: {total_bytes},"
          f"total KB sent : {total_kb}, "
          f"KB per second: {kb_per_sec}")

    return total_kb, number_of_stats, kb_per_sec


if __name__ == '__main__':
    # instantiate docker client
    client = docker.from_env()

    # run producer and consumer
    producer_volume_database = ["/home/emmely/PycharmProjects/LID-DS-2021-fixed-exploit-time/:/DS/:ro"]
    # producer_volume_database = ["/home/emmely/Projects/Datensatz/:/DS/:ro"]
    producer_entrypoint = "python3 /work/pb_next_producer.py"
    producer = client.containers.run(detach=True,
                                     image="kafka_client",
                                     network="net_kafka",
                                     name="producer", volumes=producer_volume_database,
                                     entrypoint=producer_entrypoint)

    consumer_entrypoint = "python3 /work/pb_consumer.py"
    consumer = client.containers.run(detach=True,
                                     image="kafka_client",
                                     network="net_kafka",
                                     name="consumer",
                                     entrypoint=consumer_entrypoint)

    # check for all containers to be running
    setup = client.containers.list()
    for container in setup:
        print(f"{container.name} : {container.status}")

    # checking first logs of container
    starting_logs_producer = [x for _, x in zip(xrange(3), producer.logs(stream=True))]
    for log in starting_logs_producer:
        print(log.decode('UTF-8'))

    scenario = starting_logs_producer[1].decode('UTF-8').split(" ")[1].split("/")[2]

    # setting timer
    runtime_total_seconds = 300
    timer_start = time.time()
    final_time = timer_start + runtime_total_seconds

    # save producer stats to list with timestamp
    try:
        stats_list = save_stats(producer, final_time)
        print(stats_list)
    except docker.errors.APIError:
        print("Docker Server Error: Check if producer is running properly.")

    # calculate average traffic from list
    scenario_result = calc_traffic(stats_list)

    # saving scenario average to csv
    with open("../overview_scenario_stats.csv", "a") as overview:
        writer = csv.writer(overview)
        writer.writerow([scenario.strip("\n").replace('"', ''), float(scenario_result[0]), int(scenario_result[1]),
                         float(scenario_result[2])])
