package com.tm.kafka.connect.rest.selector;

import com.tm.kafka.connect.rest.selector.SimpleTopicSelectorConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SimpleTopicSelectorConfigTest {

  @Test
  public void testConfig() {
    Map<String, String> props = new HashMap<>();

    props.put("rest.source.destination.topics", "test_topic1, test_topic2");

    SimpleTopicSelectorConfig config = new SimpleTopicSelectorConfig(props);

    List<String> expectedTopics = Arrays.asList("test_topic1", "test_topic2");

    assertEquals(expectedTopics, config.getTopics());
  }
}
