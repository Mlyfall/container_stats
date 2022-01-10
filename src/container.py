import docker

# docker build client image and docker compose before running this script


# check if containers are running
def get_containers(client):
    try:
        broker = client.containers.get("broker")
    except Exception:
        print("Broker container missing.")

    try:
        zookeeper = client.containers.get("zookeeper")
    except Exception:
        print("Zookeeper container missing.")

    try:
        producer = client.containers.get("producer")
    except Exception:
        print("Producer container missing.")

    try:
        consumer = client.containers.get("consumer")
    except Exception:
        print("Consumer container missing.")

    if broker and zookeeper and producer and consumer:
        return broker, zookeeper, producer, consumer


if __name__ == '__main__':
    # instantiate docker client
    client = docker.from_env()
    setup = get_containers(client)

    for container in setup:
        container.start()

        print(container.status)
