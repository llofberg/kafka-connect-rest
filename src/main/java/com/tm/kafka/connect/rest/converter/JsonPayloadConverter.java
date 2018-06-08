package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JsonPayloadConverter implements SinkRecordToPayloadConverter {
  private Logger log = LoggerFactory.getLogger(JsonPayloadConverter.class);
  private JsonConverter converter = new JsonConverter();

  @Override
  public String convert(SinkRecord record) {
    String topic = record.topic();
    Object value = record.value();
    Schema schema = record.valueSchema();

    byte[] result = converter.fromConnectData(topic, schema, value);
    String resultStr = new String(result);

    if (log.isTraceEnabled()) {
      log.trace(String.format("Record %s -> JSON %s", record.toString(), resultStr));
    }
    return resultStr;
  }

  @Override
  public void start(RestSinkConnectorConfig config) {
    Map<String, String> map = new HashMap<>();
    map.put("schemas.enable", config.getIncludeSchema().toString());
    map.put("converter.type", ConverterType.VALUE.getName());

    converter.configure(map);
  }
}
