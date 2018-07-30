package com.tm.kafka.connect.rest.converter.source;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;
import com.tm.kafka.connect.rest.converter.PayloadToSourceRecordConverter;
import com.tm.kafka.connect.rest.selector.TopicSelector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.System.currentTimeMillis;

public class SourceStringPayloadConverter implements PayloadToSourceRecordConverter {

  private Logger log = LoggerFactory.getLogger(SourceStringPayloadConverter.class);

  private String url;
  private TopicSelector topicSelector;

  @Override
  public List<SourceRecord> convert(byte[] bytes) {
    ArrayList<SourceRecord> records = new ArrayList<>();
    Map<String, String> sourcePartition = Collections.singletonMap("URL", url);
    Map<String, Long> sourceOffset = Collections.singletonMap("timestamp", currentTimeMillis());
    String topic = topicSelector.getTopic(bytes);
    String value = new String(bytes);
    SourceRecord sourceRecord = new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, value);
    records.add(sourceRecord);
    return records;
  }

  @Override
  public void start(RestSourceConnectorConfig config) {
    url = config.getUrl();
    topicSelector = config.getTopicSelector();
    topicSelector.start(config);
  }

}
