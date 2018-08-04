package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import com.tm.kafka.connect.rest.converter.sink.SinkJSONPayloadConverter;
import com.tm.kafka.connect.rest.http.payload.Payload;
import org.apache.kafka.connect.sink.SinkRecord;

public class JacksonPayloadConverter
  implements SinkRecordToPayloadConverter {

  private SinkJSONPayloadConverter converter;

  // Convert to a String for outgoing REST calls
  public Payload<?> convert(SinkRecord record) throws Exception {
    return converter.convert(record);
  }

  @Override
  public void start(RestSinkConnectorConfig config) {
    converter.start(config);
  }
}
