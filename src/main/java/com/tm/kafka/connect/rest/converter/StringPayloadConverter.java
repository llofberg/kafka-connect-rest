package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;
import com.tm.kafka.connect.rest.selector.TopicSelector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.System.currentTimeMillis;

public class StringPayloadConverter implements SinkRecordToPayloadConverter, PayloadToSourceRecordConverter {
  private Logger log = LoggerFactory.getLogger(StringPayloadConverter.class);
  private RestSourceConnectorConfig connectorConfig;

  private TopicSelector topicSelector;

  public List<SourceRecord> convert(byte[] bytes) {
    ArrayList<SourceRecord> records = new ArrayList<>();
    log.error("CONFIG: {}", connectorConfig);
    Map<String, String> sourcePartition = Collections.singletonMap("URL", connectorConfig.getUrl());
    Map<String, Long> sourceOffset = Collections.singletonMap("timestamp", currentTimeMillis());
    records.add(new SourceRecord(sourcePartition, sourceOffset,
      topicSelector.getTopic(bytes), Schema.STRING_SCHEMA, new String(bytes)));
    return records;
  }

  @Override
  public void start(Map<String, String> map) {
    connectorConfig = new RestSourceConnectorConfig(map);
    topicSelector = connectorConfig.getTopicSelector();
    topicSelector.start(map);
  }

  public String convert(SinkRecord record) {
    return record.value().toString();
  }
}
