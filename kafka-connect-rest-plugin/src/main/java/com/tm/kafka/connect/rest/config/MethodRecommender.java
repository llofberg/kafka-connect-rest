package com.tm.kafka.connect.rest.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MethodRecommender implements ConfigDef.Recommender {
  @Override
  public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
    return Arrays.asList("GET", "POST", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE");
  }

  @Override
  public boolean visible(String name, Map<String, Object> connectorConfigs) {
    return true;
  }
}
