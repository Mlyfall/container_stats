import docker
from pprint import pprint

client = docker.from_env()

consumer = client.containers.get("consumer")
for stat in consumer.stats(decode=True):
    pprint(stat)


