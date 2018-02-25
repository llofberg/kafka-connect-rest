package com.tm.kafka.connect.rest.selector;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;

import java.util.Map;

public class SimpleTopicSelector implements TopicSelector {
  private RestSourceConnectorConfig connectorConfig;
  private String topic;

  @Override
  public String getTopic(Object data) {
    return topic;
  }

  @Override
  public void start(Map<String, String> map) {
    connectorConfig = new RestSourceConnectorConfig(map);
    // Always return the first topic in the list
    topic = connectorConfig.getTopics().get(0);
  }
}
