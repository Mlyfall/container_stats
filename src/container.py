import docker
import time

client = docker.from_env()
containers = client.containers.list(all=True)
for container in containers:
    print(container.name)

broker = client.containers.get("broker")
zookeeper = client.containers.get("zookeeper")
producer = client.containers.get("producer")
consumer = client.containers.get("consumer")

print(producer, consumer)


