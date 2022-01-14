docker stop producer consumer zookeeper broker
docker rm producer consumer zookeeper broker
docker rmi test/test

docker build . --tag test/test
docker-compose up -d