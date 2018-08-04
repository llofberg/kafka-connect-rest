package com.tm.kafka.connect.rest.interpolator;

import com.tm.kafka.connect.rest.interpolator.source.DummyInterpolationSource;
import com.tm.kafka.connect.rest.interpolator.source.InterpolationSource;
import com.tm.kafka.connect.rest.interpolator.source.InterpolationSourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultInterpolator implements Interpolator {

  private static Logger log = LoggerFactory.getLogger(DefaultInterpolator.class);

  private Map<InterpolationSourceType, InterpolationSource> interpolationSources;

  public DefaultInterpolator(List<InterpolationSource> sources) {
    this.interpolationSources = new HashMap<>();
    sources.forEach(source -> interpolationSources.put(source.getType(), source));
  }

  public void addInterpolationSource(InterpolationSource source) {
    this.interpolationSources.put(source.getType(), source);
  }

  @Override
  public String interpolate(String str, InterpolationContext ctx) {
    String[] sourceAndKey = str.split(":", 2);
    InterpolationSourceType type = getType(sourceAndKey[0]);

    String result = str;

    if (type != InterpolationSourceType.unknown) {
      InterpolationSource source = interpolationSources.getOrDefault(type, DummyInterpolationSource.INSTANCE);
      result = source.getValue(sourceAndKey[1], ctx);
    }

    return result;
  }

  private InterpolationSourceType getType(String type) {
    try {
      return InterpolationSourceType.valueOf(type);
    } catch (IllegalArgumentException e) {
      log.error(e.getMessage(), e);
      return InterpolationSourceType.unknown;
    }
  }

}
