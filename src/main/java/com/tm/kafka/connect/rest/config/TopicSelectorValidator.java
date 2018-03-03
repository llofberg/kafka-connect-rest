package com.tm.kafka.connect.rest.config;

import com.tm.kafka.connect.rest.selector.TopicSelector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class TopicSelectorValidator implements ConfigDef.Validator {
  @Override
  public void ensureValid(String name, Object topicSelector) {
    if (topicSelector != null && topicSelector instanceof Class
        && TopicSelector.class.isAssignableFrom((Class<?>) topicSelector)) {
      return;
    }
    throw new ConfigException(name, topicSelector, "Class must extend: " + TopicSelector.class);
  }

  @Override
  public String toString() {
    return "Any class implementing: " + TopicSelector.class;
  }
}
