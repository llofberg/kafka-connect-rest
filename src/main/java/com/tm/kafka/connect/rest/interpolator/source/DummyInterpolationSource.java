package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.interpolator.InterpolationContext;

public class DummyInterpolationSource implements InterpolationSource {

  public static InterpolationSource INSTANCE = new DummyInterpolationSource();

  @Override
  public InterpolationSourceType getType() {
    return null;
  }

  @Override
  public String getValue(String key, InterpolationContext ctx) {
    return key;
  }
}
