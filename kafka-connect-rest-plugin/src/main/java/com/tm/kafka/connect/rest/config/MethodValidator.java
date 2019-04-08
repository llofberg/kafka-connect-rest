package com.tm.kafka.connect.rest.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;

public class MethodValidator implements ConfigDef.Validator {
  @Override
  public void ensureValid(String name, Object provider) {
  }

  @Override
  public String toString() {
    return new MethodRecommender().validValues("", Collections.emptyMap()).toString();
  }
}
