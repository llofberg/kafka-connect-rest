package com.tm.kafka.connect.rest.http.payload;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;


/**
 * Get the request that should be made next.
 * This next request might be made immediately or on next poll.
 *
 * Note that this is currently only used by sources,
 * but could be relevant to sinks if something more than the kafka message needed to be sent.
 */
public interface PayloadGenerator {

  /**
   * Update the generator with the request/response that just happened.
   *
   * @param request The request just made
   * @param response The response just received
   * @return True if another call should be made immediately, false otherwise.
   */
  boolean update(Request request, Response response);

  /**
   * Get the payload that should be sent with the next request.
   *
   * @return The payload to be sent to the REST service.
   */
  String getPayload();
}
