package com.tm.kafka.connect.rest.converter.sink;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import com.tm.kafka.connect.rest.converter.SinkRecordToPayloadConverter;
import com.tm.kafka.connect.rest.http.payload.JSONPayload;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class SinkJSONPayloadConverter implements SinkRecordToPayloadConverter {

  private static Logger log = LoggerFactory.getLogger(SinkJSONPayloadConverter.class);

  @Override
  public JSONPayload convert(SinkRecord record) throws Exception {

    JSONPayload payload;

    try {
      Object value = record.value();
      if (value == null) {
        value = "{}";
      }
      payload = new JSONPayload(value.toString());
    } catch (IOException e) {
      log.error(e.getMessage(), e);
      payload = new JSONPayload(Collections.emptyMap());
    }

    return payload;
  }

  @Override
  public void start(RestSinkConnectorConfig config) {}
}
