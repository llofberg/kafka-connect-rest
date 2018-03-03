package com.tm.kafka.connect.rest.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;

public class MethodValidator implements ConfigDef.Validator {
  @Override
  public void ensureValid(String name, Object provider) {
  }

  @Override
  public String toString() {
    return new MethodRecommender().validValues("", new HashMap<>()).toString();
  }
}
