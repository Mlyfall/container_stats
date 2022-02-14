docker stop producer consumer
docker rm producer consumer
docker rmi kakfa_client
docker build . -t kafka_client
