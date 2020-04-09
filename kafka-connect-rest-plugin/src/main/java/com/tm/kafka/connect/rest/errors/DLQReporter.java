package com.tm.kafka.connect.rest.errors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class DLQReporter {
  public static final String DLQ_TOPIC_CONFIG = "errors.deadletterqueue.topic.name";
  public static final String DEFAULT_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
  public static final String HEADER_PREFIX = "__connect.errors.";
  public static final String ERROR_HEADER_ORIG_TOPIC = HEADER_PREFIX + "topic";
  public static final String ERROR_HEADER_ORIG_PARTITION = HEADER_PREFIX + "partition";
  public static final String ERROR_HEADER_ORIG_OFFSET = HEADER_PREFIX + "offset";
  public static final String ERROR_HEADER_CONNECTOR_NAME = HEADER_PREFIX + "connector.name";
  public static final String ERROR_HEADER_TASK_ID = HEADER_PREFIX + "task.id";
  public static final String ERROR_HEADER_STAGE = HEADER_PREFIX + "stage";
  public static final String ERROR_HEADER_EXECUTING_CLASS = HEADER_PREFIX + "class.name";
  public static final String ERROR_HEADER_EXCEPTION = HEADER_PREFIX + "exception.class.name";
  public static final String ERROR_HEADER_EXCEPTION_MESSAGE = HEADER_PREFIX + "exception.message";
  public static final String ERROR_HEADER_EXCEPTION_STACK_TRACE = HEADER_PREFIX + "exception.stacktrace";
  private static final Logger log = LoggerFactory.getLogger(DLQReporter.class);
  String dlqTopic;
  Producer<byte[], byte[]> dlqProducer;

  public DLQReporter(AbstractConfig config) {
    dlqTopic = (String) config.originals().get(DLQ_TOPIC_CONFIG);
    Properties props = new Properties();
    props.putAll(config.originalsWithPrefix("producer."));
    //set default serializers if required
    if (!props.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_SERIALIZER);
    if (!props.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_SERIALIZER);
    dlqProducer = new KafkaProducer<>(props);
  }

  public void reportError(ConnectRecord msg, Exception e) {
    ProducerRecord<byte[], byte[]> producerRecord;
    if (msg.timestamp() == RecordBatch.NO_TIMESTAMP) {
      producerRecord = new ProducerRecord<>(dlqTopic, null, toBytes(msg.key()), toBytes(msg.value()));

    } else {
      producerRecord = new ProducerRecord<>(dlqTopic, null, msg.timestamp(), toBytes(msg.key()), toBytes(msg.value()));
    }
    populateErrorHeaders(producerRecord, e);
    this.dlqProducer.send(producerRecord, (metadata, exception) -> {
      if (exception != null) {
        log.error("Could not produce message to dead letter queue. topic=" + dlqTopic, exception);
      }
    });
  }

  private byte[] toBytes(Object value) {
    if (value != null) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    } else {
      return null;
    }
  }

  void populateErrorHeaders(ProducerRecord<byte[], byte[]> producerRecord, Exception e) {
    Headers headers = producerRecord.headers();

//    if (context.consumerRecord() != null) {
//      headers.add(ERROR_HEADER_ORIG_TOPIC, toBytes(context.consumerRecord().topic()));
//      headers.add(ERROR_HEADER_ORIG_PARTITION, toBytes(context.consumerRecord().partition()));
//      headers.add(ERROR_HEADER_ORIG_OFFSET, toBytes(context.consumerRecord().offset()));
//    }

//    headers.add(ERROR_HEADER_CONNECTOR_NAME, toBytes(connectorTaskId.connector()));
//    headers.add(ERROR_HEADER_TASK_ID, toBytes(String.valueOf(connectorTaskId.task())));
//    headers.add(ERROR_HEADER_STAGE, toBytes(context.stage().name()));
//    headers.add(ERROR_HEADER_EXECUTING_CLASS, toBytes(e.executingClass().getName()));
    if (e != null) {
      headers.add(ERROR_HEADER_EXCEPTION, toBytes(e.getClass().getName()));
      headers.add(ERROR_HEADER_EXCEPTION_MESSAGE, toBytes(e.getMessage()));
      byte[] trace;
      if ((trace = stacktrace(e)) != null) {
        headers.add(ERROR_HEADER_EXCEPTION_STACK_TRACE, trace);
      }
    }
  }

  private byte[] stacktrace(Throwable error) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      PrintStream stream = new PrintStream(bos, true, "UTF-8");
      error.printStackTrace(stream);
      bos.close();
      return bos.toByteArray();
    } catch (IOException e) {
      log.error("Could not serialize stacktrace.", e);
    }
    return null;
  }

}


