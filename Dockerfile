FROM python:3


RUN apt-get update
RUN apt-get install -y netcat

# Install the Confluent Kafka python library
RUN pip install confluent_kafka

# Add our script
RUN mkdir /work
ADD ./src /work

