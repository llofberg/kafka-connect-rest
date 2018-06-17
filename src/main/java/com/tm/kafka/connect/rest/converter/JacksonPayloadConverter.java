package com.tm.kafka.connect.rest.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

public class JacksonPayloadConverter
  implements SinkRecordToPayloadConverter {

  private ObjectMapper mapper = new ObjectMapper();

  // Convert to a String for outgoing REST calls
  public String convert(SinkRecord record) {
    try {
      return mapper.writeValueAsString(record.value());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start(RestSinkConnectorConfig config) {

  }
}
