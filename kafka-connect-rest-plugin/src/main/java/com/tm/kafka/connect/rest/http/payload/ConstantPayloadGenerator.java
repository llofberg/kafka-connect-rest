package com.tm.kafka.connect.rest.http.payload;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import org.apache.kafka.common.Configurable;

import java.util.Map;


/**
 * This is a payload generator that always returns the same payload.
 * The constant payload is defined in the configuration.
 */
public class ConstantPayloadGenerator implements PayloadGenerator, Configurable {

  private String requestBody;
  private Map<String, String> requestParameters;
  private Map<String, String> requestHeaders;

  @Override
  public void configure(Map<String, ?> props) {
    final ConstantPayloadGeneratorConfig config = new ConstantPayloadGeneratorConfig(props);

    requestBody = config.getRequestBody();
    requestParameters = config.getRequestParameters();
    requestHeaders = config.getRequestHeaders();
  }

  @Override
  public boolean update(Request request, Response response) {
    // False = Wait for the next poll cycle before calling again.
    return false;
  }

  @Override
  public String getRequestBody() {
    return requestBody;
  }

  @Override
  public Map<String, String> getRequestParameters() {
    return requestParameters;
  }

  @Override
  public Map<String, String> getRequestHeaders() {
    return requestHeaders;
  }
}
