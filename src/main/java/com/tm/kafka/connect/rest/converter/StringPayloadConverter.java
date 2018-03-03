package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
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

public class StringPayloadConverter
    implements SinkRecordToPayloadConverter, PayloadToSourceRecordConverter {
  private Logger log = LoggerFactory.getLogger(StringPayloadConverter.class);

  private String url;
  private TopicSelector topicSelector;

  public String convert(SinkRecord record) {
    if (log.isTraceEnabled()) {
      log.error("SinkRecord: {}", record.toString());
    }

    return record.value().toString();
  }

  public List<SourceRecord> convert(byte[] bytes) {
    ArrayList<SourceRecord> records = new ArrayList<>();
    Map<String, String> sourcePartition = Collections.singletonMap("URL", url);
    Map<String, Long> sourceOffset = Collections.singletonMap("timestamp", currentTimeMillis());
    String topic = topicSelector.getTopic(bytes);
    String value = new String(bytes);
    SourceRecord sourceRecord = new SourceRecord(sourcePartition, sourceOffset, topic,
        Schema.STRING_SCHEMA, value);
    if (log.isTraceEnabled()) {
      log.trace("SourceRecord: {}", sourceRecord);
    }
    records.add(sourceRecord);
    return records;
  }

  @Override
  public void start(RestSourceConnectorConfig config) {
    url = config.getUrl();
    topicSelector = config.getTopicSelector();
    topicSelector.start(config);
  }

  @Override
  public void start(RestSinkConnectorConfig config) {
    url = config.getUrl();
  }
}
