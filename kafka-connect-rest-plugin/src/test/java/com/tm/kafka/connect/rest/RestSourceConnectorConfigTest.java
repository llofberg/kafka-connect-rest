package com.tm.kafka.connect.rest;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RestSourceConnectorConfigTest {

  @Test
  public void testConfig() {
    Map<String, String> props = new HashMap<>();

    props.put("rest.source.poll.interval.ms", "60000");
    props.put("rest.source.method", "POST");
    props.put("rest.source.url", "http://test.foobar");

    props.put("rest.source.payload.converter.class", "com.tm.kafka.connect.rest.converter.source.SourceBytesPayloadConverter");
    props.put("rest.source.topic.selector", "com.tm.kafka.connect.rest.selector.SimpleTopicSelector");
    props.put("rest.source.data.generator", "com.tm.kafka.connect.rest.http.payload.ConstantPayloadGenerator");
    props.put("rest.http.executor.class", "com.tm.kafka.connect.rest.http.executor.OkHttpRequestExecutor");

    props.put("rest.source.destination.topics", "test_topic");

    RestSourceConnectorConfig config = new RestSourceConnectorConfig(props);

    assertEquals(60000l, config.getPollInterval());
    assertEquals("POST", config.getMethod());
    assertEquals("http://test.foobar", config.getUrl());

    assertNotNull(config.getTopicSelector());
    assertNotNull(config.getRequestExecutor());
  }

}
