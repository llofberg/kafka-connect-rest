package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.http.payload.JSONPayload;
import com.tm.kafka.connect.rest.http.payload.MapPayload;
import com.tm.kafka.connect.rest.http.payload.Payload;
import com.tm.kafka.connect.rest.interpolator.InterpolationContext;
import com.tm.kafka.connect.rest.util.StringToMap;

import java.util.Map;

public class PayloadInterpolationSource implements InterpolationSource {

  @Override
  public InterpolationSourceType getType() {
    return InterpolationSourceType.payload;
  }

  @Override
  public String getValue(String key, InterpolationContext ctx) {

    String result = ctx.getPayload().asString();
    Payload payload = ctx.getPayload();

    if (payload instanceof MapPayload || payload instanceof JSONPayload) {
      result = StringToMap.extract(key, (Map<String, Object>) payload.get());
    }

    return result;
  }

}
