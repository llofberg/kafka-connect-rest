package com.tm.kafka.connect.rest.http;

import java.util.Map;

public class Request {

  private String url;
  private String payload;
  private Map<String, String> headers;
  private String method;

  private Request() {}

  public Request(String url, String payload, Map<String, String> headers) {
    this.url = url;
    this.payload = payload;
    this.headers = headers;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUrl() {
    return url;
  }

  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  public String getMethod() {
    return method;
  }

  public void setMethod(String method) {
    this.method = method;
  }

  public String toString() {
    return method + "/ URL=" + url + ", Payload=" + getPayload() + ", Headers=" + getHeaders();
  }

  public static class RequestFactory {

    private String url;
    private Map<String, String> headers;

    public RequestFactory(String url, Map<String, String> headers) {
      this.url = url;
      this.headers = headers;
    }

    public Request createRequest(String  payload, String method) {
      Request request = new Request();
      request.url = this.url;
      request.headers = this.headers;
      request.payload = payload;
      request.method = method;
      return request;
    }
  }
}
