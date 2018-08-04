package com.tm.kafka.connect.rest.http.executor;

import com.tm.kafka.connect.rest.RestSinkTask;
import com.tm.kafka.connect.rest.config.HttpProperties;
import com.tm.kafka.connect.rest.http.payload.JSONPayload;
import com.tm.kafka.connect.rest.http.payload.Payload;
import com.tm.kafka.connect.rest.http.payload.StringPayload;
import okhttp3.ConnectionPool;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
        request.getPayload().asString())
      );
    }

    okhttp3.Request okRequest = builder.build();

    try (okhttp3.Response okResponse = client.newCall(okRequest).execute()) {

      com.tm.kafka.connect.rest.http.Response response = com.tm.kafka.connect.rest.http.Response.create(
        okResponse.code(),
        okResponse.headers().toMultimap(),
        extractPayload(okResponse)
      );

      return response;
    } catch (IOException e) {
      throw new RetriableException(e.getMessage(), e);
    }
  }

  private String createUrl(String url, Payload payload) throws UnsupportedEncodingException {

    if (payload.asString() == null || payload.asString().trim().isEmpty()) {
      return url;
    }

    String format;

    if (url.endsWith("?")) {
      format = "%s&%s";
    } else {
      format = "%s?%s";
    }

    return String.format(format, url, URLEncoder.encode(payload.asString(), "UTF-8"));
  }

  private String[] headersToArray(final Map<String, String> headers) {
    return headers.entrySet()
      .stream()
      .flatMap(e -> Arrays.asList(e.getKey(), e.getValue()).stream())
      .toArray(String[]::new);
  }

  private Payload extractPayload(final okhttp3.Response okResponse) throws IOException {
    ResponseBody body = okResponse.body();
    String mediaType = Optional.ofNullable(body)
      .flatMap(b -> Optional.ofNullable(b.contentType()))
      .map(ct -> ct.type())
      .orElse("");

    if (mediaType.toLowerCase().startsWith("application/json")) {
      return new JSONPayload(body.string());
    } else {
      return new StringPayload(body.string());
    }
  }
}
