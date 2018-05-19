package com.tm.kafka.connect.rest.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JacksonPayloadConverter
  implements SinkRecordToPayloadConverter {
  private Logger log = LoggerFactory.getLogger(JacksonPayloadConverter.class);

  private ObjectMapper mapper = new ObjectMapper();

  // Convert to a String for outgoing REST calls
  public String convert(SinkRecord record) {
    try {
      String msg = mapper.writeValueAsString(record.value());
      return msg;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start(RestSinkConnectorConfig config) {

  }
}
