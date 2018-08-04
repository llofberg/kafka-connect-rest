package com.tm.kafka.connect.rest.interpolator;

import com.tm.kafka.connect.rest.http.payload.Payload;

public class InterpolationContext {

  private Payload payload;

  public Payload getPayload() {
    return payload;
  }

  public static InterpolationContext create(Payload payload) {
    InterpolationContext interpolationContext = new InterpolationContext();
    interpolationContext.payload = payload;
    return interpolationContext;
  }

}
