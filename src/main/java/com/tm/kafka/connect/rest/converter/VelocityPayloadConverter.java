package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import com.tm.kafka.connect.rest.converter.sink.SinkVelocityPayloadConverter;
import com.tm.kafka.connect.rest.http.payload.Payload;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

// use com.tm.kafka.connect.rest.converter.sink.SinkVelocityPayloadConverter instead
@Deprecated
public class VelocityPayloadConverter implements SinkRecordToPayloadConverter {

  private SinkVelocityPayloadConverter converter;

  public VelocityPayloadConverter() {
    converter = new SinkVelocityPayloadConverter();
  }

  public Payload<String> convert(SinkRecord record) throws IOException {
    return converter.convert(record);
  }

  @Override
  public void start(RestSinkConnectorConfig config) {
    converter.start(config);
  }

}
