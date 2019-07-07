package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;

import java.util.Map;


/**
 * Lookup values used to populate dynamic payloads.
 * These values will be substituted into the payload template.
 * <p>
 * Note: This is a Service Provider Interface (SPI)
 * All implementations should be listed in
 * META-INF/services/com.tm.kafka.connect.rest.http.payload.templated.ValueProvider
 */
public interface ValueProvider {

  /**
   * Update the values being provided in the light of the most recent request and response
   *
   * @param request The last request made.
   * @param response The last response received.
   */
  void update(Request request, Response response);

  /**
   * Returns the value of the given key, or null if the key s undefined.
   *
   * @return The value of the key.
   */
  String lookupValue(String key);

  /**
   * Get the map of keys to values that have been requested since the last update.
   *
   * @return The parameter map.
   */
  Map<String, Object> getParameters();

  /**
   * Set the map of keys to values that will be used to generate the next template.
   * Note that the update method may overwrite some or all of these mappings.
   *
   * @param params The parameter map.
   */
  void setParameters(Map<String, Object> params);
}
