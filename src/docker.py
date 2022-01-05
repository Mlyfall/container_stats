import docker

client = docker.from_env()

client.containers.list(all=True)

