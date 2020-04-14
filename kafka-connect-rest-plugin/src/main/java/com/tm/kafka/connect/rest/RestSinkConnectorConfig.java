package com.tm.kafka.connect.rest;


import com.tm.kafka.connect.rest.http.executor.RequestExecutor;
import com.tm.kafka.connect.rest.http.handler.DefaultResponseHandler;
import com.tm.kafka.connect.rest.http.handler.ResponseHandler;
import com.tm.kafka.connect.rest.errors.DLQReporter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Properties;

import static com.tm.kafka.connect.rest.errors.DLQReporter.DLQ_TOPIC_CONFIG;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;


public class RestSinkConnectorConfig extends AbstractConfig {

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

  private static final String SINK_DATE_FORMAT_CONFIG = "rest.http.date.format";
  private static final String SINK_DATE_FORMAT_DISPLAY = "Date format for interpolation";
  private static final String SINK_DATE_FORMAT_DOC = "Date format for interpolation. The default is MM-dd-yyyy HH:mm:ss.SSS";
  private static final String SINK_DATE_FORMAT_DEFAULT = "MM-dd-yyyy HH:mm:ss.SSS";

  private static final String SINK_DLQ_KAFKA_ENABLE_CONFIG = "rest.deadletter.kafka.enabled";
  private static final String SINK_DLQ_KAFKA_ENABLE_DISPLAY = "HTTP request error to kafka";
  private static final String SINK_DLQ_KAFKA_ENABLE_DOC = "Enable deadletter queue support for error records";
  private static final Boolean SINK_DLQ_KAFKA_ENABLE_DEFAULT = false;

  private static final long SINK_RETRY_BACKOFF_DEFAULT = 5000L;


  private final Map<String, String> requestHeaders;
  private RequestExecutor requestExecutor;


  protected RestSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);

    requestExecutor = this.getConfiguredInstance(SINK_REQUEST_EXECUTOR_CONFIG, RequestExecutor.class);

    requestHeaders = getHeaders().stream()
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
        SINK_REQUEST_EXECUTOR_DISPLAY)

      .define(SINK_DATE_FORMAT_CONFIG,
        Type.STRING,
        SINK_DATE_FORMAT_DEFAULT,
        Importance.LOW,
        SINK_DATE_FORMAT_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_DATE_FORMAT_DISPLAY)

      .define(SINK_DLQ_KAFKA_ENABLE_CONFIG,
        Type.BOOLEAN,
        SINK_DLQ_KAFKA_ENABLE_DEFAULT,
        Importance.LOW,
        SINK_DLQ_KAFKA_ENABLE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_DLQ_KAFKA_ENABLE_DISPLAY)
      ;
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

  public Map<String, String> getRequestHeaders() {
    return requestHeaders;
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

  public Boolean isDlqKafkaEnabled() {
    return this.getBoolean(SINK_DLQ_KAFKA_ENABLE_CONFIG);
  }

  public DLQReporter getDLQReporter() {
    String topic = (String) this.originals().get(DLQ_TOPIC_CONFIG);
    Properties props = new Properties();
    props.putAll(this.originalsWithPrefix("producer."));
    return new DLQReporter(topic, props);
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
