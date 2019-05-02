package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;

import java.util.Map;


/**
 * Lookup values used to populate dynamic payloads.
 * These values will be substituted into the payload template.
 *
 * This implementation looks up values in the System properties and then in environment variables.
 */
public class EnvironmentValueProvider extends AbstractValueProvider {

  @Override
  void extractValues(Request request, Response response) {
    // Do nothing
  }

  /**
   * Returns the value of the given key, which will be looked up first in the System properties and then
   * in environment variables.
   *
   * @return The defined value or null if te key is undefined.
   */
  @Override
  String getValue(String key) {
    String value = System.getProperty(key);
    return (value != null) ? value : System.getenv(key);
  }
}
