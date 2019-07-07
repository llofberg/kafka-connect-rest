package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.payload.ConstantPayloadGeneratorConfig;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;


public class RegexResponseValueProviderConfigTest {

  @Test
  public void testConfig() {
    Map<String, Object> props = new HashMap<>();

    props.put("rest.source.response.var.names", "key1, key2");
    props.put("rest.source.response.var.key1.regex", ".*");
    props.put("rest.source.response.var.key2.regex", "result: (\\d+)");

    RegexResponseValueProviderConfig config = new RegexResponseValueProviderConfig(props);

    assertThat(config.getResponseVariableNames(), contains("key1", "key2"));
    assertThat(config.getResponseVariableRegexs(), allOf(
      hasEntry("key1", ".*"),
      hasEntry("key2", "result: (\\d+)")));
  }
}
