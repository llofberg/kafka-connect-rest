package com.tm.kafka.connect.rest.http.payload.templated;


import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;


public class XPathResponseValueProviderConfigTest {

  @Test
  public void testConfig() {
    Map<String, Object> props = new HashMap<>();

    props.put("rest.source.response.var.names", "key1, key2");
    props.put("rest.source.response.var.key1.xpath", "/bookstore/book[1]");
    props.put("rest.source.response.var.key2.xpath", "//title[@lang]");

    XPathResponseValueProviderConfig config = new XPathResponseValueProviderConfig(props);

    assertThat(config.getResponseVariableNames(), contains("key1", "key2"));
    assertThat(config.getResponseVariableXPaths(), allOf(
      hasEntry("key1", "/bookstore/book[1]"),
      hasEntry("key2", "//title[@lang]")));
  }
}
