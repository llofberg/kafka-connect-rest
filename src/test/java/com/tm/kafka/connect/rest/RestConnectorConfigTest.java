package com.tm.kafka.connect.rest;

import org.junit.Test;

public class RestConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(RestSourceConnectorConfig.conf().toRst());
  }
}
