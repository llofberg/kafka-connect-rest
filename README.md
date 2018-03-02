Kafka Connect REST connector
===


Building and running Spring example in docker
---

    mvn clean install
    cd examples/spring/gs-rest-service
    mvn clean install
    cd ..
    docker-compose up -d
    
    docker exec -it spring_connect_1 bash -c \
     "kafka-topics --zookeeper zookeeper \
       --topic restSourceDestinationTopic --create \
       --replication-factor 1 --partitions 1"
    
    curl -X POST \
       -H 'Host: connect.example.com' \
       -H 'Accept: application/json' \
       -H 'Content-Type: application/json' \
      http://localhost:8083/connectors -d @config/sink.json
    
    curl -X POST \
       -H 'Host: connect.example.com' \
       -H 'Accept: application/json' \
       -H 'Content-Type: application/json' \
      http://localhost:8083/connectors -d @config/source.json

    docker exec -it spring_connect_1 bash -c \
     "kafka-avro-console-consumer --bootstrap-server kafka:9092 \
      --topic restSourceDestinationTopic --from-beginning \
      --property schema.registry.url=http://schema_registry:8081/"

    docker-compose down

Change CONNECT_VALUE_CONVERTER in the docker-compose.yml
to org.apache.kafka.connect.storage.StringConverter if you don't want to use Avro.

    docker exec -it spring_connect_1 bash -c \
     "kafka-console-consumer --bootstrap-server kafka:9092 \
      --topic restSourceDestinationTopic --from-beginning"

Building and running Google Cloud Function example in docker
---

You will need gcloud installed and a GCP project with payments enabled.

    mvn clean install
    cd examples/gcf
    
Replace '\<REGION>' and '\<PROJECTID>' in rest.source.url in config/source.json.

  "rest.source.url": "https://\<REGION>-\<PROJECTID>.cloudfunctions.net/hello",
    
    gcloud beta functions deploy hello --trigger-http
    
    curl -X POST http://https://<REGION>-<PROJECTID>.cloudfunctions.net/hello -d 'name=Kafka Connect'
    
    docker-compose up -d
    
    docker exec -it gcf_connect_1 bash -c \
     "kafka-topics --zookeeper zookeeper \
       --topic restSourceDestinationTopic --create \
       --replication-factor 1 --partitions 1"
    
    curl -X POST \
       -H 'Host: connect.example.com' \
       -H 'Accept: application/json' \
       -H 'Content-Type: application/json' \
      http://localhost:8083/connectors -d @config/source.json

    docker exec -it spring_connect_1 bash -c \
     "kafka-avro-console-consumer --bootstrap-server kafka:9092 \
      --topic restSourceDestinationTopic --from-beginning \
      --property schema.registry.url=http://schema_registry:8081/"

    docker-compose down
