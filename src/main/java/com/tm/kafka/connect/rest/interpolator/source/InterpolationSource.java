package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.interpolator.InterpolationContext;

public interface InterpolationSource {

  InterpolationSourceType getType();

  String getValue(String key, InterpolationContext ctx);
}
