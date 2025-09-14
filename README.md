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

## 🚀 Run Spark

Start Spark services:

```shell
docker-compose up -d spark-master spark-worker
```

Access UIs:
- Spark Master UI: http://localhost:${SPARK_MASTER_WEBUI_PORT:-8080}
- Spark Worker UI: http://localhost:${SPARK_WORKER_WEBUI_PORT:-8081}

Submit an example job:

```shell
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --class org.apache.spark.examples.SparkPi \
  examples/jars/spark-examples_2.12-3.5.1.jar 10
```

Use PySpark with Elasticsearch connector (optional):

```shell
docker-compose exec -it spark-master pyspark \
  --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.1
```

Python example to write to Elasticsearch:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
(
  df.write
    .format("org.elasticsearch.spark.sql")
    .option("es.nodes", "elasticsearch")   # Docker service name
    .option("es.port", "9200")             # Match ELASTICSEARCH_HTTP_PORT
    # If security enabled, add:
    # .option("es.net.http.auth.user", "elastic")
    # .option("es.net.http.auth.pass", "<ELASTIC_PASSWORD>")
    .save("people_index")
)
```

Notes:
- If ports 8080/8081 are in use, change them in `.env` (SPARK_MASTER_WEBUI_PORT, SPARK_WORKER_WEBUI_PORT).
- Elasticsearch security is disabled by default in `.env.example`. If you enable it, include auth options above.

