package com.tm.kafka.connect.rest.http;

import java.util.Map;

public class Request {

  private String url;
  private String payload;
  private Map<String, String> headers;
  private String method;


  public Request(String url, String method, String payload, Map<String, String> headers) {
    this.url = url;
    this.method = method;
    this.payload = payload;
    this.headers = headers;
  }

  public String getUrl() {
    return url;
  }

  public String getPayload() {
    return payload;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public String getMethod() {
    return method;
  }


  public String toString() {
    return method + "/ URL=" + url + ", Payload=" + getPayload() + ", Headers=" + getHeaders();
  }


  public static class RequestFactory {

    private String url;
    private String method;
    private Map<String, String> headers;

    public RequestFactory(String url, String method, Map<String, String> headers) {
      this.url = url;
      this.method = method;
      this.headers = headers;
    }

    public Request createRequest(String  payload) {
      return new Request(url, method, payload, headers);
    }
  }
}
