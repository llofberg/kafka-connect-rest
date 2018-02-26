package com.tm.kafka.connect.rest.selector;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;

public class SimpleTopicSelector implements TopicSelector {
  private String topic;

  @Override
  public String getTopic(Object data) {
    return topic;
  }

  @Override
  public void start(RestSourceConnectorConfig config) {
    // Always return the first topic in the list
    topic = config.getTopics().get(0);
  }
}
