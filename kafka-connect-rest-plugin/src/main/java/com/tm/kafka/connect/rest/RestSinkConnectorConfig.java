package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.config.HttpProperties;
import com.tm.kafka.connect.rest.http.executor.RequestExecutor;
import com.tm.kafka.connect.rest.http.handler.DefaultResponseHandler;
import com.tm.kafka.connect.rest.http.handler.ResponseHandler;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class RestSinkConnectorConfig extends AbstractConfig implements HttpProperties {

  static final String SINK_METHOD_CONFIG = "rest.sink.method";
  private static final String SINK_METHOD_DOC = "The HTTP method for REST sink connector.";
  private static final String SINK_METHOD_DISPLAY = "Sink method";
  private static final String SINK_METHOD_DEFAULT = "POST";

  static final String SINK_HEADERS_LIST_CONFIG = "rest.sink.headers";
  private static final String SINK_HEADERS_LIST_DISPLAY = "Sink request headers";
  private static final String SINK_HEADERS_LIST_DOC = "The request headers for REST sink connector.";

  static final String SINK_URL_CONFIG = "rest.sink.url";
  private static final String SINK_URL_DOC = "The URL for REST sink connector.";
  private static final String SINK_URL_DISPLAY = "URL for REST sink connector.";

  private static final String SINK_HTTP_CONNECTION_TIMEOUT_CONFIG = "rest.http.connection.connection.timeout";
  private static final String SINK_HTTP_CONNECTION_TIMEOUT_DISPLAY = "HTTP connection timeout in milliseconds";
  private static final String SINK_HTTP_CONNECTION_TIMEOUT_DOC = "HTTP connection timeout in milliseconds";
  private static final long SINK_HTTP_CONNECTION_TIMEOUT_DEFAULT = 2000;

  private static final String SINK_HTTP_READ_TIMEOUT_CONFIG = "rest.http.connection.read.timeout";
  private static final String SINK_HTTP_READ_TIMEOUT_DISPLAY = "HTTP read timeout in milliseconds";
  private static final String SINK_HTTP_READ_TIMEOUT_DOC = "HTTP read timeout in milliseconds";
  private static final long SINK_HTTP_READ_TIMEOUT_DEFAULT = 2000;

  private static final String SINK_HTTP_KEEP_ALIVE_DURATION_CONFIG = "rest.http.connection.keep.alive.ms";
  private static final String SINK_HTTP_KEEP_ALIVE_DURATION_DISPLAY = "Keep alive in milliseconds";
  private static final String SINK_HTTP_KEEP_ALIVE_DURATION_DOC = "For how long keep HTTP connection should be keept alive in milliseconds";
  private static final long SINK_HTTP_KEEP_ALIVE_DURATION_DEFAULT = 300000; // 5 minutes

  private static final String SINK_HTTP_MAX_IDLE_CONNECTION_CONFIG = "rest.http.connection.max.idle";
  private static final String SINK_HTTP_MAX_IDLE_CONNECTION_DISPLAY = "Number of idle connections";
  private static final String SINK_HTTP_MAX_IDLE_CONNECTION_DOC = "How many idle connections per host can be keept opened";
  private static final int SINK_HTTP_MAX_IDLE_CONNECTION_DEFAULT = 5;

  private static final String SINK_HTTP_MAX_RETRIES_CONFIG = "rest.http.max.retries";
  private static final String SINK_HTTP_MAX_RETRIES_DISPLAY = "Number of times to retry request in case of failure";
  private static final String SINK_HTTP_MAX_RETRIES_DOC = "Number of times to retry request in case of failure. Negative means infinite number of retries";
  private static final int SINK_HTTP_MAX_RETRIES_DEFAULT = -1;

  private static final String SINK_HTTP_CODES_WHITELIST_CONFIG = "rest.http.codes.whitelist";
  private static final String SINK_HTTP_CODES_WHITELIST_DISPLAY = "HTTP codes whitelist";
  private static final String SINK_HTTP_CODES_WHITELIST_DOC = "Regex for HTTP codes which are considered as successful. "
    + "Request will be retried rest.http.max.retries if response code from the server does not match the regex";
  private static final String SINK_HTTP_CODES_WHITELIST_DEFAULT = "^[2-4]{1}\\d{1}\\d{1}$";

  private static final String SINK_HTTP_CODES_BLACKLIST_CONFIG = "rest.http.codes.blacklist";
  private static final String SINK_HTTP_CODES_BLACKLIST_DISPLAY = "HTTP codes blacklist";
  private static final String SINK_HTTP_CODES_BLACKLIST_DOC = "Regex for HTTP codes which are considered as unsuccessful. "
    + "Request will be retried rest.http.max.retries if response code from the server does match the regex";
  private static final String SINK_HTTP_CODES_BLACKLIST_DEFAULT = "";

  private static final String SINK_REQUEST_EXECUTOR_CONFIG = "rest.http.executor.class";
  private static final String SINK_REQUEST_EXECUTOR_DISPLAY = "HTTP request executor";
  private static final String SINK_REQUEST_EXECUTOR_DOC = "HTTP request executor. Default is OkHttpRequestExecutor";
  private static final String SINK_REQUEST_EXECUTOR_DEFAULT = "com.tm.kafka.connect.rest.http.executor.OkHttpRequestExecutor";

  private static final String SINK_RETRY_BACKOFF_CONFIG = "rest.sink.retry.backoff.ms";
  private static final String SINK_RETRY_BACKOFF_DOC =
    "The retry backoff in milliseconds. This config is used to notify Kafka connect to retry "
      + "delivering a message batch or performing recovery in case of transient exceptions.";
  private static final String SINK_RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";
  private static final long SINK_RETRY_BACKOFF_DEFAULT = 5000L;

  private final Map<String, String> requestProperties;
  private RequestExecutor requestExecutor;

  @SuppressWarnings("unchecked")
  private RestSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
    try {
      requestExecutor = (RequestExecutor)
        getClass(SINK_REQUEST_EXECUTOR_CONFIG).getDeclaredConstructor(HttpProperties.class).newInstance(this);
    } catch (IllegalAccessException | InstantiationException
      | InvocationTargetException | NoSuchMethodException e) {
      throw new ConnectException("Invalid class. Error: " + e.getMessage() + " " + e.getClass().getName(), e);
    }
    requestProperties = getHeaders().stream()
      .map(a -> a.split(":", 2))
      .collect(Collectors.toMap(a -> a[0], a -> a[1]));
  }

  public RestSinkConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    String group = "REST";
    int orderInGroup = 0;
    return new ConfigDef()
      .define(SINK_METHOD_CONFIG,
        Type.STRING,
        SINK_METHOD_DEFAULT,
        new MethodValidator(),
        Importance.HIGH,
        SINK_METHOD_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SINK_METHOD_DISPLAY,
        new MethodRecommender())

      .define(SINK_HEADERS_LIST_CONFIG,
        Type.LIST,
        Collections.EMPTY_LIST,
        Importance.HIGH,
        SINK_HEADERS_LIST_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SINK_HEADERS_LIST_DISPLAY)

      .define(SINK_URL_CONFIG,
        Type.STRING,
        NO_DEFAULT_VALUE,
        Importance.HIGH,
        SINK_URL_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SINK_URL_DISPLAY)

      .define(SINK_RETRY_BACKOFF_CONFIG,
        Type.LONG,
        SINK_RETRY_BACKOFF_DEFAULT,
        Importance.LOW,
        SINK_RETRY_BACKOFF_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_RETRY_BACKOFF_DISPLAY)

      .define(SINK_HTTP_CONNECTION_TIMEOUT_CONFIG,
        Type.LONG,
        SINK_HTTP_CONNECTION_TIMEOUT_DEFAULT,
        Importance.LOW,
        SINK_HTTP_CONNECTION_TIMEOUT_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_HTTP_CONNECTION_TIMEOUT_DISPLAY)

      .define(SINK_HTTP_READ_TIMEOUT_CONFIG,
        Type.LONG,
        SINK_HTTP_READ_TIMEOUT_DEFAULT,
        Importance.LOW,
        SINK_HTTP_READ_TIMEOUT_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_HTTP_READ_TIMEOUT_DISPLAY)

      .define(SINK_HTTP_KEEP_ALIVE_DURATION_CONFIG,
        Type.LONG,
        SINK_HTTP_KEEP_ALIVE_DURATION_DEFAULT,
        Importance.LOW,
        SINK_HTTP_KEEP_ALIVE_DURATION_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_HTTP_KEEP_ALIVE_DURATION_DISPLAY)

      .define(SINK_HTTP_MAX_IDLE_CONNECTION_CONFIG,
        Type.INT,
        SINK_HTTP_MAX_IDLE_CONNECTION_DEFAULT,
        Importance.LOW,
        SINK_HTTP_MAX_IDLE_CONNECTION_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_HTTP_MAX_IDLE_CONNECTION_DISPLAY)

      .define(SINK_HTTP_MAX_RETRIES_CONFIG,
        Type.INT,
        SINK_HTTP_MAX_RETRIES_DEFAULT,
        Importance.LOW,
        SINK_HTTP_MAX_RETRIES_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_HTTP_MAX_RETRIES_DISPLAY)

      .define(SINK_HTTP_CODES_WHITELIST_CONFIG,
        Type.STRING,
        SINK_HTTP_CODES_WHITELIST_DEFAULT,
        Importance.LOW,
        SINK_HTTP_CODES_WHITELIST_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_HTTP_CODES_WHITELIST_DISPLAY)

      .define(SINK_HTTP_CODES_BLACKLIST_CONFIG,
        Type.STRING,
        SINK_HTTP_CODES_BLACKLIST_DEFAULT,
        Importance.LOW,
        SINK_HTTP_CODES_BLACKLIST_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_HTTP_CODES_BLACKLIST_DISPLAY)

      .define(SINK_REQUEST_EXECUTOR_CONFIG,
        Type.CLASS,
        SINK_REQUEST_EXECUTOR_DEFAULT,
        Importance.LOW,
        SINK_REQUEST_EXECUTOR_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_REQUEST_EXECUTOR_DISPLAY);
  }

  public String getMethod() {
    return this.getString(SINK_METHOD_CONFIG);
  }

  private List<String> getHeaders() {
    return new ArrayList<>(this.getList(SINK_HEADERS_LIST_CONFIG));
  }

  public String getUrl() {
    return this.getString(SINK_URL_CONFIG);
  }

  public Long getRetryBackoff() {
    return this.getLong(SINK_RETRY_BACKOFF_CONFIG);
  }

  public Map<String, String> getRequestProperties() {
    return requestProperties;
  }

  public Map<String, String> getRequestHeaders() {
    return requestProperties;
  }

  @Override
  public long getReadTimeout() {
    return this.getLong(SINK_HTTP_READ_TIMEOUT_CONFIG);
  }

  @Override
  public long getConnectionTimeout() {
    return this.getLong(SINK_HTTP_CONNECTION_TIMEOUT_CONFIG);
  }

  @Override
  public long getKeepAliveDuration() {
    return this.getLong(SINK_HTTP_KEEP_ALIVE_DURATION_CONFIG);
  }

  @Override
  public int getMaxIdleConnections() {
    return this.getInt(SINK_HTTP_MAX_IDLE_CONNECTION_CONFIG);
  }

  public int getMaxRetries() {
    return this.getInt(SINK_HTTP_MAX_RETRIES_CONFIG);
  }

  public RequestExecutor getRequestExecutor() {
    return requestExecutor;
  }

  public ResponseHandler getResponseHandler() {
    return new DefaultResponseHandler(
      this.getString(SINK_HTTP_CODES_WHITELIST_CONFIG),
      this.getString(SINK_HTTP_CODES_BLACKLIST_CONFIG)
    );
  }

  private static class MethodRecommender implements ConfigDef.Recommender {
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return Arrays.asList("GET", "POST");
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return true;
    }
  }

  private static class MethodValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object provider) {
    }

    @Override
    public String toString() {
      return new MethodRecommender().validValues("", new HashMap<>()).toString();
    }
  }
}
