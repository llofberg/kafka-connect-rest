package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.interpolator.InterpolationContext;

public class EnvironmentVariableInterpolationSource implements InterpolationSource {

  @Override
  public InterpolationSourceType getType() {
    return InterpolationSourceType.env;
  }

  @Override
  public String getValue(String key, InterpolationContext ctx) {
    return System.getenv(key);
  }
}
