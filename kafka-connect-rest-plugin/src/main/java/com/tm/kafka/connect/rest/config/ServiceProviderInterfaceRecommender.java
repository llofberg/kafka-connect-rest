package com.tm.kafka.connect.rest.config;


import org.apache.kafka.common.config.ConfigDef;

import java.util.*;


public class ServiceProviderInterfaceRecommender<T> implements ConfigDef.Recommender {

  private List<Object> implementations;

  public ServiceProviderInterfaceRecommender(Class<T> clazz) {
    List<Object> implementations = new ArrayList<>();
    ServiceLoader<T> loader = ServiceLoader.load(clazz);
    for (T impl : loader) {
      implementations.add(impl.getClass());
    }
    this.implementations = implementations;
  }

  @Override
  public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
    return implementations;
  }

  @Override
  public boolean visible(String name, Map<String, Object> connectorConfigs) {
    return true;
  }
}
