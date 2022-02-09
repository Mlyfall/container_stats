import docker
import time
import csv
from tqdm import tqdm

# make sure broker and zookeeper are running using the compose file


def save_stats(container: docker.models.containers.Container, all_stats=False):
    with open(str(container.name) + "_stats.csv", "w") as container_stats:
        writer = csv.writer(container_stats)
        writer.writerow(["transmitted data in bytes", "timestamp"])

        first_status = True
        counter_until_break = 0
        test_number_syscalls = 30

        if all_stats is False:
            for stat in tqdm(container.stats(decode=True)):
                if counter_until_break <= test_number_syscalls:
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
                if counter_until_break <= test_number_syscalls:
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
            sum_bytes += int(row[0])

        average_traffic_per_sec = sum_bytes / counter
        print(average_traffic_per_sec)


if __name__ == '__main__':
    # instantiate docker client
    client = docker.from_env()

    # build image for producer/consumer
    #kafka_client_image = client.images.build(dockerfile="/home/emmely/PycharmProjects/test/Dockerfile", tag="kafka_client") ????

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

    # saving producer stats
    all_stats = True
    try:
        stats_file = save_stats(producer)
    except:
        "Check producer status."

    # calculating average traffic after saving stats is finished
    calc_average_traffic(stats_file)





