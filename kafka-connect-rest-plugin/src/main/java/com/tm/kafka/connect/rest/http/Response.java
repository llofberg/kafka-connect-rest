package com.tm.kafka.connect.rest.http;

import java.util.List;
import java.util.Map;

public class Response {

  private int statusCode;
  private String payload;
  private Map<String, List<String>> headers;


  public Response(int statusCode, Map<String, List<String>> headers, String payload) {
    this.statusCode = statusCode;
    this.headers = headers;
    this.payload = payload;
  }

  public String getPayload() {
    return payload;
  }

  public Map<String, List<String>> getHeaders() {
    return headers;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String toString() {
    return "StatusCode=" + getStatusCode() + ", Payload=" + getPayload() + ", Headers=" + getHeaders();
  }
}
