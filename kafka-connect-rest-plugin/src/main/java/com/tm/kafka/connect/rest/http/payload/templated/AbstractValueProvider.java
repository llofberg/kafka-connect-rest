package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * This abstract class is a sensible base for value provider implementations.
 * It tracks the parameter set in use and ensures that the same parameter will only be looked up once per update
 * meaning it will have the same value throughout a payload.
 */
public abstract class AbstractValueProvider implements ValueProvider {

  protected Map<String, String> parameterMap = new HashMap<>();


  /**
   * Extract the values that will be used by the template engine from the last request-response cycle.
   * It is not necessary for parameters used by the value provider to come from the request or response,
   * and in some cases this method may do nothing.
   *
   * @param request The last request made.
   * @param response The last response received.
   */
  protected abstract void extractValues(Request request, Response response);

  /**
   * Get the value for a given key.
   * This method is called if the key cannot be found in the current set of cached parameters.
   *
   * @param key the key to lookup
   * @return the value or null if the key is undefined.
   */
  protected abstract String getValue(String key);

  /**
   * Update the parameter values based on the last request and response.
   *
   * @param request The last request made.
   * @param response The last response received.
   */
  @Override
  public void update(Request request, Response response) {
    parameterMap.clear();
    extractValues(request, response);
  }

  /**
   * Returns the value of the given key.
   *
   * @return The defined value or null if the key is undefined.
   */
  @Override
  public String lookupValue(String key) {
    String value = parameterMap.getOrDefault(key, getValue(key));
    parameterMap.put(key, value);
    return value;
  }

  /**
   * Get the key-value pairs that have been requested since the last update.
   *
   * @return The parameter map.
   */
  @Override
  public Map<String, Object> getParameters() {
    return Collections.unmodifiableMap(parameterMap);
  }

  /**
   * Set the map of keys to values that will be used to generate the next template.
   * Note that the update method will overwrite these mappings.
   * This method would normally be used to set initial state.
   *
   * @param params The parameter map.
   */
  @Override
  public void setParameters(Map<String, Object> params) {
    parameterMap.clear();
    params.forEach((k, v) -> parameterMap.put(k, v.toString()));
  }
}
