import docker
import time
import csv

# make sure broker and zookeeper are running using the compose file

if __name__ == '__main__':
    # instantiate docker client
    client = docker.from_env()
    # build image for producer/consumer
    #kafka_client_image = client.images.build(dockerfile="/home/emmely/PycharmProjects/test/Dockerfile", tag="kafka_client") ????

    # run producer and consumer
    producer_volume_database = ["/home/emmely/PycharmProjects/LID-DS-2021-fixed-exploit-time/:/DS/:ro"]
    producer_entrypoint = "bash -c  python3 /work/next.py"
    producer = client.containers.run(image="kafka_client",
                                     network="net_kafka",
                                     name="producer", volumes=producer_volume_database,
                                     entrypoint=producer_entrypoint)

    consumer_entrypoint = "bash -c python3 /work/simple_consumer.py"
    consumer = client.containers.run(image="kafka_client",
                                     network="net_kafka",
                                     name="consumer",
                                     entrypoint=consumer_entrypoint)

    # check for all containers to be running
    setup = client.containers.list()
    for container in setup:
        print(f"{container.name} : {container.status}")

    # save producer stats
    with open("producer_stats.csv", "w") as csv_producer_stats:
        writer = csv.writer(csv_producer_stats)
        writer.writerow(["transmitted data in bytes", "timestamp"])

        first_status = True
        for stat in producer.stats(decode=True):
            print(stat)
            if first_status is True:
                transmitted_data = stat[network][tx_bytes]
                current_timestamp = time.time()
                writer.writerow([transmitted_data, current_timestamp])
                old_tx = transmitted_data
                old_time = current_timestamp
                first_status = False
            else:
                transmitted_data = status[network][tx_bytes]
                current_timestamp = time.time()
                if transmitted_data != old_tx:
                    writer.writerow([transmitted_data, current_timestamp])







