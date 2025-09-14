# docker-elasticsearch-logstash-kibana

Containerized Elastic Stack (Elasticsearch, Logstash, and Kibana) with Docker Compose.

## 🐳 Welcome to the **Elastic Stack** Docker images 🐳

This repository contains the source for building an immutable Docker image for the Elastic Stack. The images are published on [Docker Hub](https://hub.docker.com/u/elastic) and can be used as the base image for running the Elastic Stack in a containerized environment.

## Requirements

- [Docker Engine](https://docs.docker.com/install/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## 🛠️ Configuration

The Elastic Stack services can be configured using environment variables in the `.env` file. The following variables are available:

- `STACK_VERSION`: The version of the Elastic Stack to use. The default value is `8.14.3`.
- `ELASTICSEARCH_HTTP_PORT`: The port on which Elasticsearch listens for incoming connections. The default port is `9200`.
- `ELASTIC_PASSWORD`: The password for the `elastic` user. This password is used to authenticate with Elasticsearch and Kibana.
- `KIBANA_PORT`: The port on which Kibana listens for incoming connections. The default port is `5601`.
- `LOGSTASH_PORT`: The port on which Logstash listens for incoming connections. The default port is `5044`.
- `ELASTICSEARCH_JAVA_OPTS`: The Java options for Elasticsearch. The default value is `-Xmx1g -Xms1g`.
- `LS_JAVA_OPTS`: The Java options for Logstash. The default value is `-Xmx1g -Xms1g`.

## 🛩️ Quick Start

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

## 💻 Start in Mac with arm64

To run as amd64. You need to set the default platform to `linux/amd64`:

```shell
export DOCKER_DEFAULT_PLATFORM=linux/amd64
```
### For x86_64/amd64 systems (Linux/Windows)
The default configuration is set for `linux/amd64`. No changes needed.

### For ARM64 systems (Mac M1/M2, ARM servers)
If you encounter `exec /bin/tini: exec format error`, you need to change the platform: `platform: ${PLATFORM:-linux/arm64}`

## 🏂 Usage

### Import Data into Elasticsearch using Logstash

This is an example of how to import data (csv file) into Elasticsearch using Logstash:

1. Create a `logstash.conf` file with the following content:

```conf
input {
  file {
    path => "/usr/share/logstash/data/employees.csv"
    start_position => "beginning"
    sincedb_path => "/dev/null"
  }
}

filter {
  csv {
    separator => ","
    columns => ["id", "name", "code", "salary"]
  }

  mutate {
    convert => {
      "id" => "integer"
      "name" => "string"
      "code" => "integer"
      "salary" => "float"
    }
  }
}

output {
  elasticsearch {
    hosts => "${ELASTIC_HOSTS}"
    user => "elastic"
    password => "${ELASTIC_PASSWORD}"
    index => "employees"
  }
  stdout { codec => rubydebug }
}
```

> Note:
> - The `logstash.conf` file reads data from the `employees.csv` file and imports it into Elasticsearch.
> - The `employees.csv` file should be placed in the `logstash/data` directory.
> - The `ELASTIC_HOSTS` and `ELASTIC_PASSWORD` environment variables are used to connect to Elasticsearch.
> - The `employees` index is created in Elasticsearch.
> - The `rubydebug` codec is used to output the data to the console.
> - The `sincedb_path` is set to `/dev/null` to avoid saving the state of the file.

2. Create a `employees.csv` file with the following content:

```csv
id,name,code,salary
1,Alice,1001,50000
2,Bob,1002,60000
3,Charlie,1003,70000
4,Dave,1004,80000
5,Eve,1005,90000
```

3. Update **docker-compose.yml** to include the Logstash service:

```yml
  logstash:
    build:
      context: logstash
      args:
        STACK_VERSION: ${STACK_VERSION:-8.14.3}
    container_name: "${COMPOSE_PROJECT_NAME}-logstash"
    environment:
      NODE_NAME: "logstash"
      LS_JAVA_OPTS: "${LS_JAVA_OPTS}"
      ELASTIC_USERNAME: "elastic"
      ELASTIC_PASSWORD: "${ELASTIC_PASSWORD}"
      ELASTIC_HOSTS: "http://elasticsearch:9200"
    volumes:
      - ./logstash/logstash.conf:/usr/share/logstash/pipeline/logstash.conf
      - ./logstash/logstash.yml:/usr/share/logstash/config/logstash.yml
      - ./logstash/data/employees.csv:/usr/share/logstash/data/employees.csv # Add this line
      ...
```

4. Start the Logstash service:

```shell
docker-compose up -d logstash
```

5. Accessing the Elasticsearch API

You can access the Elasticsearch API using `curl` or tools like Postman. Here are some examples:

```shell
curl -X GET "localhost:9200/employees/_search?pretty"
```

> Note:
> - Replace `employees` with the name of the index you want to query.
> - Change the port number if you have modified the `ELASTICSEARCH_HTTP_PORT` in the `.env` file.

❤️‍🔥 **_Check this branch to see the full example_: [[feat/import-csv-with-logstash/docker-compose](https://github.com/tanhongit/docker-elasticsearch-logstash-kibana/blob/feat/import-csv-with-logstash/logstash/logstash.conf)]** ❤️‍🔥

---

# How to Use the Makefile to Run Spark Tests in Docker

## Overview

This Makefile helps you quickly run Python Spark test scripts inside Docker containers (`spark-master` or `spark-worker`), without remembering the long `docker exec` and `spark-submit` command syntax.

***

## Usage

### 1. Command syntax

```bash
make run [CONTAINER=<spark-master|spark-worker>] [TEST_FILE=<test_file.py>]
```

- `CONTAINER` (optional): The name of the Docker container to run the Spark job. Default is `spark-master`.
- `TEST_FILE` (optional): The name of the Python test file inside `/opt/bitnami/spark/tests` in the container. Default is `test_duplicate_people_names.py`.

***

### 2. Examples

- Run the default test file on the `spark-master` container:

```bash
make run
```

- Run `test_employees.py` on the `spark-master` container:

```bash
make run TEST_FILE=test_employees
```

- Run the default test on the `spark-worker` container:

```bash
make run CONTAINER=spark-worker
```

- Run `test_connections.py` on the `spark-worker` container:

```bash
make run CONTAINER=spark-worker TEST_FILE=test_connections.py
```

***

### 3. Workflow

1. Identify running containers using `docker ps`.
2. Write or update Python test files in the `spark/tests` folder.
3. Use the `make run` command with appropriate parameters.
4. Check the test results printed in the terminal.

***
