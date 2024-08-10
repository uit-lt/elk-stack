# docker-elasticsearch-logstash-kibana

Containerized Elastic Stack (Elasticsearch, Logstash, and Kibana) with Docker Compose.

## Welcome to the **Elastic Stack** Docker images 🐳

This repository contains the source for building an immutable Docker image for the Elastic Stack. The images are published on [Docker Hub](https://hub.docker.com/u/elastic) and can be used as the base image for running the Elastic Stack in a containerized environment.

## Requirements

- [Docker Engine](https://docs.docker.com/install/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Configuration

The Elastic Stack services can be configured using environment variables in the `.env` file. The following variables are available:

- `STACK_VERSION`: The version of the Elastic Stack to use. The default value is `8.14.3`.
- `ELASTICSEARCH_HTTP_PORT`: The port on which Elasticsearch listens for incoming connections. The default port is `9200`.
- `ELASTIC_PASSWORD`: The password for the `elastic` user. This password is used to authenticate with Elasticsearch and Kibana.
- `KIBANA_PORT`: The port on which Kibana listens for incoming connections. The default port is `5601`.
- `LOGSTASH_PORT`: The port on which Logstash listens for incoming connections. The default port is `5044`.
- `ELASTICSEARCH_JAVA_OPTS`: The Java options for Elasticsearch. The default value is `-Xmx1g -Xms1g`.
- `LS_JAVA_OPTS`: The Java options for Logstash. The default value is `-Xmx1g -Xms1g`.

## Quick Start

To get started, please clone this repository to the local machine:

```shell
git clone git@github.com:tanhongit/docker-elasticsearch-logstash-kibana.git
```

Change the directory to the cloned repository:

```shell
cd docker-elasticsearch-logstash-kibana
```

Create a `.env` file from the `.env.example` file:

```shell
cp .env.example .env
```

> Change the value of the `ELASTIC_PASSWORD` and any other variables in the `.env` file as needed.

Start the Elastic Stack services:

```shell
docker-compose up -d
```

Access the Kibana web interface by navigating to [http://localhost:5601](http://localhost:5601) in a web browser. (Use your custom `KIBANA_PORT` if you have changed it in the `.env` file.)

## Start in Mac with arm64

To run as amd64. You need to set the default platform to `linux/amd64`:

```shell
export DOCKER_DEFAULT_PLATFORM=linux/amd64
```
