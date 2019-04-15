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
    props.put("rest.source.headers", "Content-Type:application/json, Accept:application/json");
    props.put("rest.source.url", "http://test.foobar");
    props.put("rest.source.data", "{\"key\":\"val\"}");
    props.put("rest.source.destination.topics", "test_topic1, test_topic2");
    props.put("rest.source.payload.replace", "a:b,c:d");
    props.put("rest.source.payload.remove", "z");
    props.put("rest.source.payload.add", "key:val");
    props.put("rest.http.connection.connection.timeout", "2000");
    props.put("rest.http.connection.read.timeout", "5000");
    props.put("rest.http.connection.keep.alive.ms", "10000");
    props.put("rest.http.connection.max.idle", "30000");

    props.put("rest.source.payload.converter.class", "com.tm.kafka.connect.rest.converter.source.SourceBytesPayloadConverter");
    props.put("rest.source.topic.selector", "com.tm.kafka.connect.rest.selector.SimpleTopicSelector");
    props.put("rest.source.data.generator", "com.tm.kafka.connect.rest.http.payload.ConstantPayloadGenerator");
    props.put("rest.http.executor.class", "com.tm.kafka.connect.rest.http.executor.OkHttpRequestExecutor");

    RestSourceConnectorConfig config = new RestSourceConnectorConfig(props);

    Map<String, String> expectedHeaders = new HashMap<>();
    expectedHeaders.put("Content-Type", "application/json");
    expectedHeaders.put("Accept", "application/json");

    List<String> expectedTopics = Arrays.asList("test_topic1", "test_topic2");

    assertEquals(60000l, config.getPollInterval());
    assertEquals("POST", config.getMethod());
    assertEquals(expectedHeaders, config.getRequestProperties());
    assertEquals(expectedHeaders, config.getRequestHeaders());
    assertEquals("http://test.foobar", config.getUrl());
    assertEquals("{\"key\":\"val\"}", config.getData());
    assertEquals(expectedTopics, config.getTopics());
    assertEquals(2000, config.getConnectionTimeout());
    assertEquals(5000, config.getReadTimeout());
    assertEquals(10000, config.getKeepAliveDuration());
    assertEquals(30000, config.getMaxIdleConnections());

    assertNotNull(config.getTopicSelector());
    assertNotNull(config.getRequestExecutor());
  }

}
