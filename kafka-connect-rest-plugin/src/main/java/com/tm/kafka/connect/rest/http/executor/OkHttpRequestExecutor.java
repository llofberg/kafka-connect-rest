package com.tm.kafka.connect.rest.http.executor;

import com.tm.kafka.connect.rest.config.HttpProperties;
import okhttp3.*;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class OkHttpRequestExecutor implements RequestExecutor {

  private static Logger log = LoggerFactory.getLogger(OkHttpRequestExecutor.class);

  private final OkHttpClient client;

  public OkHttpRequestExecutor(HttpProperties props) {
    client = new OkHttpClient.Builder()
      .connectionPool(new ConnectionPool(props.getMaxIdleConnections(), props.getKeepAliveDuration(), TimeUnit.MILLISECONDS))
      .connectTimeout(props.getConnectionTimeout(), TimeUnit.MILLISECONDS)
      .readTimeout(props.getReadTimeout(), TimeUnit.MILLISECONDS)
      .build();
  }

  @Override
  public com.tm.kafka.connect.rest.http.Response execute(com.tm.kafka.connect.rest.http.Request request) throws IOException {

    okhttp3.Request.Builder builder = new okhttp3.Request.Builder()
      .url(request.getUrl())
      .headers(Headers.of(headersToArray(request.getHeaders())));

    if ("GET".equalsIgnoreCase(request.getMethod())) {
      String url = createUrl(request.getUrl(), request.getPayload());
      builder.get().url(url);
    } else {
      builder.method(request.getMethod(), RequestBody.create(
        MediaType.parse(request.getHeaders().getOrDefault("Content-Type", "")),
        request.getPayload())
      );
    }

    okhttp3.Request okRequest = builder.build();

    try (okhttp3.Response okResponse = client.newCall(okRequest).execute()) {

      return com.tm.kafka.connect.rest.http.Response.create(
        okResponse.code(),
        okResponse.headers().toMultimap(),
        okResponse.body() != null ? okResponse.body().string() : null
      );
    } catch (IOException e) {
      throw new RetriableException(e.getMessage(), e);
    }
  }

  private String createUrl(String url, String payload) throws UnsupportedEncodingException {

    if (payload == null || payload.trim().isEmpty()) {
      return url;
    }

    String format;

    if (url.endsWith("?")) {
      format = "%s&%s";
    } else {
      format = "%s?%s";
    }

    return String.format(format, url, URLEncoder.encode(payload, "UTF-8"));
  }

  private String[] headersToArray(final Map<String, String> headers) {
    return headers.entrySet()
      .stream()
      .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
      .toArray(String[]::new);
  }
}
