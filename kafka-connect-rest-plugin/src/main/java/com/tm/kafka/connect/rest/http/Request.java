package com.tm.kafka.connect.rest.http;

import java.util.Collections;
import java.util.Map;

public class Request {

  private String url;
  private String body;
  private Map<String, String> parameters;
  private Map<String, String> headers;
  private String method;


  public Request(String url, String method, String body, Map<String, String> parameters, Map<String, String> headers) {
    this.url = url;
    this.method = method;
    this.body = body;
    this.parameters = parameters;
    this.headers = headers;
  }

  public String getUrl() {
    return url;
  }

  public String getBody() {
    return body;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public String getMethod() {
    return method;
  }

  @Override
  public String toString() {
    return "Request{" +
      "method='" + method + '\'' +
      ", url='" + url + '\'' +
      ", parameters=" + parameters +
      ", headers=" + headers +
      ", body='" + body + '\'' +
      '}';
  }

  public static class RequestFactory {

    private String url;
    private String method;
    private Map<String, String> headers;

    public RequestFactory(String url, String method) {
      this.url = url;
      this.method = method;
    }

    public Request createRequest(String body, Map<String, String> parameters, Map<String, String> headers) {
      return new Request(url, method, body, parameters, headers);
    }

    public Request createRequest(String payload, Map<String, String> headers) {
      // TODO - This is a bit of a hack.  How the value is sent to the REST endpoint should be explicitly configured
      //        It is possible that you may still want to send it as a request parameter for a POST request,
      //        and the name(s) of the parameter(s) being sent should be determined by config or the payload its self.
      if("GET".equalsIgnoreCase(method)) {
        return new Request(url, method, null, Collections.singletonMap("value", payload), headers);
      } else {
        return new Request(url, method, payload, Collections.emptyMap(), headers);
      }
    }
  }
}
