package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.payload.ConstantPayloadGeneratorConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;


public class TemplatedPayloadGeneratorConfigTest {

  @Test
  public void testConfig() {
    Map<String, Object> props = new HashMap<>();

    props.put("rest.source.body", "This is the body");
    props.put("rest.source.param.names", "key1, key2");
    props.put("rest.source.param.key1.value", "val1");
    props.put("rest.source.param.key2.value", "val2");
    props.put("rest.source.headers", Arrays.asList("Content-Type:application/json", "Accept:application/json"));

    ConstantPayloadGeneratorConfig config = new ConstantPayloadGeneratorConfig(props);

    assertThat(config.getRequestBody(), equalTo("This is the body"));
    assertThat(config.getRequestParameters(), allOf(hasEntry("key1", "val1"), hasEntry("key2", "val2")));
    assertThat(config.getRequestHeaders(), allOf(
      hasEntry("Content-Type", "application/json"), hasEntry("Accept", "application/json")));
  }
}
