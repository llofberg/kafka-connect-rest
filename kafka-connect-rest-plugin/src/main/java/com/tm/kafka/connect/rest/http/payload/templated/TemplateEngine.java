package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;

import java.util.Map;


/**
 * Template engine is responsible for converting a template into a series of outputs, based on a series of context
 * entries.
 * <p>
 * Note: This is a Service Provider Interface (SPI)
 * All implementations should be listed in
 * META-INF/services/com.tm.kafka.connect.rest.http.payload.templated.TemplateEngine
 */
public interface TemplateEngine {

  /**
   * Get a particular interpretation of the template based on the values in the given coutext.
   *
   * @param context The source from which values are taken.
   * @return A completed template where all appliccable placeholders have been replaced with values.
   */
  String renderTemplate(String template, ValueProvider context);
}
