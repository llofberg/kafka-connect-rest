package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.http.executor.OkHttpRequestExecutorConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OkHttpRequestExecutorConfigTest {

  @Test
  public void testConfig() {
    Map<String, String> props = new HashMap<>();

    props.put("rest.http.connection.connection.timeout", "2000");
    props.put("rest.http.connection.read.timeout", "5000");
    props.put("rest.http.connection.keep.alive.ms", "10000");
    props.put("rest.http.connection.max.idle", "30000");

    OkHttpRequestExecutorConfig config = new OkHttpRequestExecutorConfig(props);

    assertEquals(2000, config.getConnectionTimeout());
    assertEquals(5000, config.getReadTimeout());
    assertEquals(10000, config.getKeepAliveDuration());
    assertEquals(30000, config.getMaxIdleConnections());
  }
}
