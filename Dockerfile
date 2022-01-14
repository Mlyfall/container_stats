FROM ubuntu:latest

RUN apt-get update && apt-get install -y python3 python3-dev pip

# Install the Confluent Kafka python library
RUN pip install confluent_kafka

# Add script
RUN mkdir /work
ADD ./src /work
WORKDIR /work
RUN pip install -r requirements.txt
RUN pip install -e .

ENTRYPOINT ["python3"]

