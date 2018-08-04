package com.tm.kafka.connect.rest.converter.sink;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import com.tm.kafka.connect.rest.converter.SinkRecordToPayloadConverter;
import com.tm.kafka.connect.rest.http.payload.MapPayload;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class SinkMapPayloadConverter implements SinkRecordToPayloadConverter {

  private static Logger log = LoggerFactory.getLogger(SinkMapPayloadConverter.class);

  @Override
  public MapPayload convert(SinkRecord record) throws Exception {

    MapPayload payload;

    try {
      Map<?, ?> map = (Map<?, ?>) record.value();
      payload = new MapPayload(map);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      payload = new MapPayload(Collections.emptyMap());
    }

    return payload;
  }

  @Override
  public void start(RestSinkConnectorConfig config) {}
}
