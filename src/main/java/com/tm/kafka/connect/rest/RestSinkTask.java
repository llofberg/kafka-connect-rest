package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.converter.SinkRecordToPayloadConverter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class RestSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(RestSinkTask.class);

  private String method;
  private Map<String, String> requestProperties;
  private String url;
  private SinkRecordToPayloadConverter converter;

  private final Map<String, Map<Integer, Long>> offsets = new HashMap<>();

  @Override
  public void start(Map<String, String> map) {
    RestSinkConnectorConfig connectorConfig = new RestSinkConnectorConfig(map);
    context.timeout(connectorConfig.getRetryBackoff());
    method = connectorConfig.getMethod();
    requestProperties = connectorConfig.getRequestProperties();
    url = connectorConfig.getUrl();
    converter = connectorConfig.getSinkRecordToPayloadConverter();
    converter.start(connectorConfig);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    records.forEach(record -> {
      try {
        String data = converter.convert(record);
        String u = url;
        if ("GET".equals(method)) {
          u = u + URLEncoder.encode(data, "UTF-8");
        }
        HttpURLConnection conn = (HttpURLConnection) new URL(u).openConnection();
        requestProperties.forEach(conn::setRequestProperty);
        conn.setRequestMethod(method);
        if ("POST".equals(method)) {
          conn.setDoOutput(true);
          OutputStream os = conn.getOutputStream();
          os.write(data.getBytes());
          os.flush();
        }
        int responseCode = conn.getResponseCode();
        if (log.isTraceEnabled()) {
          log.trace("Response code: {}, Request data: {}", responseCode, data);
        }
        Map<Integer, Long> topicOffsets = offsets.getOrDefault(record.topic(), new HashMap<>());
        topicOffsets.put(record.kafkaPartition(), record.kafkaOffset());
        offsets.put(record.topic(), topicOffsets);
      } catch (Exception e) {
        throw new RetriableException("REST sink put failed.", e);
      }
    });
  }

  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return offsets.entrySet().stream()
      .map(topicEntry -> topicEntry.getValue().entrySet().stream()
        .collect(Collectors.toMap(
          partitionEntry -> new TopicPartition(topicEntry.getKey(), partitionEntry.getKey()),
          partitionEntry -> new OffsetAndMetadata(partitionEntry.getValue()))))
      .flatMap(m -> m.entrySet().stream())
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public void stop() {
    log.debug("Stopping sink task, setting client to null");
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }
}
