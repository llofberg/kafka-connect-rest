package com.tm.kafka.connect.rest;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RestSinkConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(RestSinkConnectorConfig.conf().toRst());
  }

  @Test
  public void testInit() {
    Map<String, String> props = new HashMap<>();

    props.put("rest.sink.method", "POST");
    props.put("rest.sink.headers", "Accept:application/json, Content-Type:application/json");
    props.put("rest.sink.url", "http://test.foobar");
    props.put("rest.sink.payload.replace", "a:b,c:d");
    props.put("rest.sink.payload.remove", "z");
    props.put("rest.sink.payload.add", "key:val");
    props.put("rest.http.connection.connection.timeout", "2000");
    props.put("rest.http.connection.read.timeout", "5000");
    props.put("rest.http.connection.keep.alive.ms", "10000");
    props.put("rest.http.connection.max.idle", "30000");

    props.put("rest.http.max.retries", "3");
    props.put("rest.http.codes.whitelist", "^200$");
    props.put("rest.http.codes.blacklist", "^500$");
    props.put("rest.sink.retry.backoff.ms", "15000");

    props.put("rest.sink.payload.converter.class", "com.tm.kafka.connect.rest.converter.sink.SinkStringPayloadConverter");
    props.put("rest.http.executor.class", "com.tm.kafka.connect.rest.http.executor.OkHttpRequestExecutor");

    RestSinkConnectorConfig config = new RestSinkConnectorConfig(props);

    Map<String, String> expectedHeaders = new HashMap<>();
    expectedHeaders.put("Content-Type", "application/json");
    expectedHeaders.put("Accept", "application/json");

    assertEquals("POST", config.getMethod());
    assertEquals(expectedHeaders, config.getRequestProperties());
    assertEquals(expectedHeaders, config.getRequestHeaders());
    assertEquals("http://test.foobar", config.getUrl());
    assertEquals(2000, config.getConnectionTimeout());
    assertEquals(5000, config.getReadTimeout());
    assertEquals(10000, config.getKeepAliveDuration());
    assertEquals(30000, config.getMaxIdleConnections());
    assertEquals(3, config.getMaxRetries());
    assertTrue(15000 == config.getRetryBackoff());

    assertNotNull(config.getRequestExecutor());
    assertNotNull(config.getResponseHandler());
  }
}
