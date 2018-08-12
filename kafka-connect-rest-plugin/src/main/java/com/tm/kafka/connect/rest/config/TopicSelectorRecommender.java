package com.tm.kafka.connect.rest.config;

import com.tm.kafka.connect.rest.selector.SimpleTopicSelector;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TopicSelectorRecommender implements ConfigDef.Recommender {
  @Override
  public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
    return Collections.singletonList(SimpleTopicSelector.class);
  }

  @Override
  public boolean visible(String name, Map<String, Object> connectorConfigs) {
    return true;
  }
}
