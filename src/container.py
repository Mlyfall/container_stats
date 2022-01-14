import docker
import time

# docker build client image and docker compose before running this script


# check if containers are running
def get_containers(client):
    container_setup = []
    try:
        broker = client.containers.get("broker")
        container_setup.append(broker)
    except docker.errors.NotFound:
        print("Broker container does not exist.")
    except docker.errors.APIError:
        print("Server Error Code.")

    try:
        zookeeper = client.containers.get("zookeeper")
        container_setup.append(zookeeper)
    except docker.errors.NotFound:
        print("Zookeeper container does not exist.")
    except docker.errors.APIError:
        print("Server Error Code.")

    try:
        producer = client.containers.get("producer")
        container_setup.append(producer)
    except docker.errors.NotFound:
        print("Producer container does not exist.")
    except docker.errors.APIError:
        print("Server Error Code.")

    try:
        consumer = client.containers.get("consumer")
        container_setup.append(consumer)
    except docker.errors.NotFound:
        print("Consumer container does not exist.")
    except docker.errors.APIError:
        print("Server Error Code.")

    return container_setup

if __name__ == '__main__':
    # instantiate docker client
    client = docker.from_env()
    setup = get_containers(client)

    # getting containers and starting them
    for container in setup:
        if container.name == "broker":
            broker = container
        if container.name == "zookeeper":
            zookeeper = container
        if container.name == "producer":
            producer = container
        if container.name == "consumer":
            consumer = container

        container.start()
    print(producer.stats(decode=True))

    time.sleep(10)





