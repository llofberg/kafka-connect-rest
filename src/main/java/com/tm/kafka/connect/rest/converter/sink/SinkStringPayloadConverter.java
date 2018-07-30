package com.tm.kafka.connect.rest.converter.sink;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import com.tm.kafka.connect.rest.converter.SinkRecordToPayloadConverter;
import com.tm.kafka.connect.rest.http.payload.StringPayload;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkStringPayloadConverter implements SinkRecordToPayloadConverter {

  @Override
  public StringPayload convert(SinkRecord record) {
    Object val = record.value();
    return new StringPayload(val != null ? record.value().toString() : "");
  }

  @Override
  public void start(RestSinkConnectorConfig config) {}
}
