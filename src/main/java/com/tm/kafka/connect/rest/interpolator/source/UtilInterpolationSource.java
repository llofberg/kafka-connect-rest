package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.interpolator.InterpolationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UtilInterpolationSource implements InterpolationSource {

  private static Logger log = LoggerFactory.getLogger(UtilInterpolationSource.class);

  enum UtilType {
    timestamp, date
  }

  @Override
  public InterpolationSourceType getType() {
    return InterpolationSourceType.util;
  }

  @Override
  public String getValue(String key, InterpolationContext ctx) {

    UtilType type = toUtilType(key);

    switch (type) {
      case timestamp: return Util.getCurrentTimestamp();
      case date:      return Util.getDate();
      default:        return "";
    }

  }

  private UtilType toUtilType(String key) {
    try {
      return UtilType.valueOf(key);
    } catch (IllegalArgumentException e) {
      log.error(e.getMessage(), e);
      return null;
    }
  }

  public static class Util {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss.SSS");

    static String getDate() {
      return DATE_FORMAT.format(new Date());
    }

    static String getCurrentTimestamp() {
      return String.valueOf(System.currentTimeMillis());
    }


  }
}
