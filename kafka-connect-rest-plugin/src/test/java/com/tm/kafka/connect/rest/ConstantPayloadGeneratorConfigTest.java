package com.tm.kafka.connect.rest;


import com.tm.kafka.connect.rest.http.payload.ConstantPayloadGeneratorConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class ConstantPayloadGeneratorConfigTest {

  @Test
  public void testConfig() {
    Map<String, String> props = new HashMap<>();

    props.put("rest.source.data", "{\"key\":\"val\"}");

    ConstantPayloadGeneratorConfig config = new ConstantPayloadGeneratorConfig(props);

    assertEquals("{\"key\":\"val\"}", config.getPayload());
  }
}
