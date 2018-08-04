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

    docker logs -f spring_webservice_1

    docker-compose down
    cd ../..

**_Note: The transformations in this repository should probably be implemented using 
Kafka [Converter](https://github.com/apache/kafka/blob/trunk/connect/api/src/main/java/org/apache/kafka/connect/storage/Converter.java)
and [HeaderConverter](https://github.com/apache/kafka/blob/trunk/connect/api/src/main/java/org/apache/kafka/connect/storage/HeaderConverter.java)
interfaces. See [JsonConverter](https://github.com/apache/kafka/blob/trunk/connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java)
for an example. PR's are welcome._**

To try out the Velocity converter replace the sink above with 

    curl -X POST \
       -H 'Host: connect.example.com' \
       -H 'Accept: application/json' \
       -H 'Content-Type: application/json' \
      http://localhost:8083/connectors -d @config/velocity_sink.json


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
    
    
### Interpolation and payload modification

It often happens that sink connector request or source connector response requires modifications (headers, payload etc)
Using interpolation you can inject into HTTP calls different environment variables, or utility features like time or data or uuid etc.

Here is an sample of kafka-connect-rest sink connector configuration with interpolations 

```
{
  "name": "PetServiceConnector",
  "config": {
    "connector.class": "com.tm.kafka.connect.rest.RestSinkConnector",
    "rest.sink.payload.converter.class": "com.tm.kafka.connect.rest.converter.sink.SinkJSONPayloadConverter",
    "tasks.max": "1",
    "topics": "pets",    
    "rest.sink.headers": "Content-Type:application/json,Correlation-Id:${payload:correlationId},Server-Name:${env:SERVER_NAME}",
    "rest.sink.url": "http://pet-service.com/${payload:petName}?api-key=${property:/configs/config.properties:api.key}",
    "rest.sink.payload.remove": "correlationId,petId",
    "rest.sink.method": "POST"
  }
}
```

If you take a look at rest.sink.headers and rest.sink.url fields, you can notice things like ${payload.perId}, ${payload.correlationId}, ${property:/configs/config.properties:api.key}. 
These values will be injected into HTTP call at runtime using payload content, local property files, environment variables etc. 

Things like payload and property in above examples are interpolation sources. Currently there are 4 of them:

payload - means the value will be taken from Kafka message payload (for sink connectors. For source connectors it will be HTTP response)
property - takes value from properties file in the Kafka Connect filesystem. Useful for sensitive information injection like api-keys, secrets, etc.  
env - environment variables which live in the Kafka Connect host
util - utility features, like current timestamp or date (use it like ${util.timestamp} or ${util:date} )

#### Why not kafka connect transformations?

We find it difficult to maintain multiple transformations in the same sink or source configuration. Interpolation is more readable and maintainable. 
Additionally it would be impossible to maange dynamic HTTP headers using kafka connect transformations.   

### Sink connector configuration options

Here is list of configuration options for REST Sink connector

```
rest.sink.method - HTTP method
rest.sink.headers - comma separated HTTP headers
rest.sink.url - HTTP URL
rest.sink.payload.converter.class - should be com.tm.kafka.connect.rest.converter.sink.SinkJSONPayloadConverter most of the time
rest.sink.payload.replace - String contains comma separated patterns for payload replacements. Interpolation accepted
rest.sink.payload.remove - String contains comma separated list of payload fields to be removed
rest.sink.payload.add - String contains comma separated list of fields to be added to the payload. Interpolation accepted
rest.sink.retry.backoff.ms - The retry backoff in milliseconds. In case of failed HTTP call, connector will sleep rest.sink.retry.backoff.ms and then retry.
rest.http.connection.connection.timeout - HTTP connection timeout in milliseconds, default is 2000
rest.http.connection.read.timeout - HTTP read timeout in milliseconds, default is 2000
rest.http.connection.keep.alive.ms - For how long keep HTTP connection should be keept alive in milliseconds, default is 30000 (5 minutes)
rest.http.connection.max.idle - How many idle connections per host can be kept opened, default is 5
rest.http.max.retries - Number of times to retry request in case of failure. Negative means infinite number of retries, default is -1
rest.http.codes.whitelist - Regex for HTTP codes which are considered as successful. Request will be retried infinitely if response code from the server does not match the regex. Default value is ^[2-4]{1}\\d{1}\\d{1}$
rest.http.codes.blacklist - Regex for HTTP codes which are considered as unsuccessful. Request will be retried infinitely if response code from the server does match the regex, default is empty string
rest.http.executor.class - HTTP request executor. Default is OkHttpRequestExecutor
```

### Source connector configuration options

And here is list of configuration options for REST Source connector

```
rest.source.poll.interval.ms - The innterval in milliseconds between HTTP calls from source connector to your rest.source.url
rest.source.method - HTTP method
rest.source.headers - comma separated HTTP headers
rest.source.url - HTTP URL
rest.source.data - Data to be sent with HTTP request (usually POST body)
rest.source.destination.topics - The list of topics separated by comma. Source connector will push response body from HTTP requeset to these topics
rest.source.payload.converter.class - should be com.tm.kafka.connect.rest.converter.source.SinkJSONPayloadConverter most of the time
rest.source.payload.replace - String contains comma separated patterns for payload replacements. Interpolation accepted
rest.source.payload.remove - String contains comma separated list of payload fields to be removed
rest.source.payload.add - String contains comma separated list of fields to be added to the payload. Interpolation accepted
rest.source.topic.selector - Topic selector. Default is com.tm.kafka.connect.rest.selector.SimpleTopicSelector
rest.http.connection.connection.timeout - HTTP connection timeout in milliseconds, default is 2000
rest.http.connection.read.timeout - HTTP read timeout in milliseconds, default is 2000
rest.http.connection.keep.alive.ms - For how long keep HTTP connection should be keept alive in milliseconds, default is 30000 (5 minutes)
rest.http.connection.max.idle - How many idle connections per host can be kept opened, default is 5
rest.http.executor.class - HTTP request executor. Default is OkHttpRequestExecutor
```

