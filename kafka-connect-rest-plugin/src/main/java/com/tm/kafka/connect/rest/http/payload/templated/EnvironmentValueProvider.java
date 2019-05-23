package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;


/**
 * Lookup values used to populate dynamic payloads.
 * These values will be substituted into the payload template.
 *
 * This implementation looks up values in the System properties and then in environment variables.
 */
public class EnvironmentValueProvider extends AbstractValueProvider {

  /**
   * This method does nothing as none of the values used by this class are based on the request or response.
   *
   * @param request The last request made.
   * @param response The last response received.
   */
  @Override
  protected void extractValues(Request request, Response response) {
    // Do nothing
  }

  /**
   * Returns the value of the given key, which will be looked up first in the System properties and then
   * in environment variables.
   *
   * @return The defined value or null if te key is undefined.
   */
  @Override
  protected String getValue(String key) {
    String value = System.getProperty(key);
    return (value != null) ? value : System.getenv(key);
  }
}
