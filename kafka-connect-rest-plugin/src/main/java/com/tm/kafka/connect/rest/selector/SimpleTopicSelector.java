package com.tm.kafka.connect.rest.selector;


import org.apache.kafka.common.Configurable;

import java.util.Map;


public class SimpleTopicSelector implements TopicSelector, Configurable {

  private String topic;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleTopicSelectorConfig config = new SimpleTopicSelectorConfig(props);

    // Always use the first topic in the list
    topic = config.getTopics().get(0);
  }

  @Override
  public String getTopic(Object data) {
    return topic;
  }
}
