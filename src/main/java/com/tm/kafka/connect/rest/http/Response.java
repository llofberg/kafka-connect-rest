package com.tm.kafka.connect.rest.http;

import com.tm.kafka.connect.rest.http.payload.Payload;

import java.util.List;
import java.util.Map;

public class Response {

  private int statusCode;
  private Payload payload;
  private Map<String, List<String>> headers;

  public Payload getPayload() {
    return payload;
  }

  public Map<String, List<String>> getHeaders() {
    return headers;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public static Response create(int statusCode, Map<String, List<String>> headers, Payload payload) {
    Response response = new Response();
    response.statusCode = statusCode;
    response.headers = headers;
    response.payload = payload;
    return response;
  }

  public String toString() {
    return "StatusCode=" + getStatusCode() + ", Payload=" + getPayload() + ", Headers=" + getHeaders();
  }
}
