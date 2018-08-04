package com.tm.kafka.connect.rest.http.payload;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tm.kafka.connect.rest.interpolator.source.UtilInterpolationSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class JSONPayload extends Payload<Map> {

  private static Logger log = LoggerFactory.getLogger(UtilInterpolationSource.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  public JSONPayload(String payload) throws IOException {
    this(mapper.readValue(payload, Map.class));
  }

  public JSONPayload(Map payload) {
    super(payload);
  }

  @Override
  public String asString() {
    try {
      return mapper.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      log.error(e.getMessage(), e);
      return "";
    }
  }

  @Override
  public String toString() {
    return asString();
  }
}
