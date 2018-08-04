package com.tm.kafka.connect.rest.interpolator;

public interface Interpolator {

  String interpolate(String path, InterpolationContext ctx);
}
