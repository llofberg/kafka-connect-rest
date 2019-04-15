package com.tm.kafka.connect.rest.http.payload;


import com.tm.kafka.connect.rest.RestSourceConnectorConfig;
import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;


/**
 * This is a payload generator that always returns the same payload.
 * The constant payload is defined in the configuration.
 */
public class ConstantPayloadGenerator implements PayloadGenerator {

  private String payload;

  public ConstantPayloadGenerator(RestSourceConnectorConfig connectorConfig) {
    payload = connectorConfig.getData();
  }

  @Override
  public boolean update(Request request, Response response) {
    // False = Wait for the next poll cycle before calling again.
    return false;
  }

  @Override
  public String getPayload() {
    return payload;
  }
}
