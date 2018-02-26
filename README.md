Kafka Connect REST connector
===


Building and running in docker
---

    mvn clean install
    cd examples/spring/gs-rest-service
    mvn clean install
    cd ..
    docker-compose up -d
    
    docker exec -it spring_connect_1 bash -c \
     "kafka-topics --zookeeper zookeeper --topic restSourceDestinationTopic --create --replication-factor 1 --partitions 1"
    
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
     "kafka-console-consumer --bootstrap-server kafka:9092 --topic restSourceDestinationTopic --from-beginning"

    docker-compose down
