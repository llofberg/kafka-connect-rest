package com.tm.kafka.connect.rest.http.payload;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;

import java.util.Map;


/**
 * Get the request that should be made next.
 * This next request might be made immediately or on next poll.
 * <p>
 * This is currently only used by sources,
 * but could be relevant to sinks if something more than the kafka message needed to be sent.
 * <p>
 * Note: This is a Service Provider Interface (SPI)
 * All implementations should be listed in
 * META-INF/services/com.tm.kafka.connect.rest.http.payload.PayloadGenerator
 */
public interface PayloadGenerator {

  /**
   * Update the generator with the request/response that just happened.
   *
   * @param request  The request just made
   * @param response The response just received
   * @return True if another call should be made immediately, false otherwise.
   */
  boolean update(Request request, Response response);

  /**
   * Get the HTTP request body that should be sent with the next request.
   * This is not used for GET requests.
   *
   * @return The body content to be sent to the REST service.
   */
  String getRequestBody();

  /**
   * Get the HTTP request parameters that should be sent with the next request.
   *
   * @return The parameters to be sent to the REST service.
   */
  Map<String, String> getRequestParameters();

  /**
   * Get the HTTP request headers that should be sent with the next request.
   *
   * @return The headers to be sent to the REST service.
   */
  Map<String, String> getRequestHeaders();

  /**
   * Get the input stream offsets for the current payload.
   *
   * @return The offsets.
   */
  Map<String, Object> getOffsets();

  /**
   * Set the input stream offsets.
   *
   * @param offsets The offsets.
   */
  void setOffsets(Map<String, Object> offsets);
}

