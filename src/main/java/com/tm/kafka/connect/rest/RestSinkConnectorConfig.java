package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.config.HttpProperties;
import com.tm.kafka.connect.rest.config.RequestTransformationFields;
import com.tm.kafka.connect.rest.converter.BytesPayloadConverter;
import com.tm.kafka.connect.rest.converter.SinkRecordToPayloadConverter;
import com.tm.kafka.connect.rest.converter.StringPayloadConverter;
import com.tm.kafka.connect.rest.http.executor.RequestExecutor;
import com.tm.kafka.connect.rest.http.handler.DefaultResponseHandler;
import com.tm.kafka.connect.rest.http.handler.ResponseHandler;
import com.tm.kafka.connect.rest.http.transformer.DefaultRequestTransformer;
import com.tm.kafka.connect.rest.http.transformer.RequestTransformer;
import com.tm.kafka.connect.rest.interpolator.DefaultInterpolator;
import com.tm.kafka.connect.rest.interpolator.Interpolator;
import com.tm.kafka.connect.rest.interpolator.source.EnvironmentVariableInterpolationSource;
import com.tm.kafka.connect.rest.interpolator.source.PayloadInterpolationSource;
import com.tm.kafka.connect.rest.interpolator.source.PropertyInterpolationSource;
import com.tm.kafka.connect.rest.interpolator.source.UtilInterpolationSource;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RestSinkConnectorConfig extends AbstractConfig implements RequestTransformationFields, HttpProperties {

  static final String SINK_METHOD_CONFIG = "rest.sink.method";
  private static final String SINK_METHOD_DOC = "The HTTP method for REST sink connector.";
  private static final String SINK_METHOD_DISPLAY = "Sink method";
  private static final String SINK_METHOD_DEFAULT = "POST";

  @Deprecated // use SINK_HEADERS_LIST_CONFIG instead
  static final String SINK_PROPERTIES_LIST_CONFIG = "rest.sink.properties";
  @Deprecated // use SINK_HEADERS_LIST_DISPLAY instead
  private static final String SINK_PROPERTIES_LIST_DISPLAY = "Sink properties";
  @Deprecated // use SINK_HEADERS_LIST_DOC instead
  private static final String SINK_PROPERTIES_LIST_DOC = "The request properties (headers) for REST sink connector.";

  static final String SINK_HEADERS_LIST_CONFIG = "rest.sink.headers";
  private static final String SINK_HEADERS_LIST_DISPLAY = "Sink request headers";
  private static final String SINK_HEADERS_LIST_DOC = "The request headers for REST sink connector.";

  static final String SINK_URL_CONFIG = "rest.sink.url";
  private static final String SINK_URL_DOC = "The URL for REST sink connector.";
  private static final String SINK_URL_DISPLAY = "URL for REST sink connector.";

  static final String SINK_PAYLOAD_CONVERTER_CONFIG = "rest.sink.payload.converter.class";
  private static final Class<? extends SinkRecordToPayloadConverter> PAYLOAD_CONVERTER_DEFAULT =
    StringPayloadConverter.class;
  private static final String SINK_PAYLOAD_CONVERTER_DOC =
    "Class to be used to convert messages from SinkRecords to Strings for REST calls";
  private static final String SINK_PAYLOAD_CONVERTER_DISPLAY = "Payload converter class";

  private static final String SINK_PAYLOAD_CONVERTER_SCHEMA_CONFIG = "rest.sink.payload.converter.schema";
  private static final String SINK_PAYLOAD_CONVERTER_SCHEMA_DOC = "Include schema in JSON output for JsonPayloadConverter";
  private static final String SINK_PAYLOAD_CONVERTER_SCHEMA_DISPLAY = "Include schema in JSON output (true/false)";
  private static final String SINK_PAYLOAD_CONVERTER_SCHEMA_DEFAULT = "false";

  private static final String SINK_PAYLOAD_REPLACE_CONFIG = "rest.sink.payload.replace";
  private static final String SINK_PAYLOAD_REPLACE_DISPLAY = "Fields to be replaced in payload";
  private static final String SINK_PAYLOAD_REPLACE_DOC = "String contains comma separated patterns for payload replacements. Interpolation accepted";

  private static final String SINK_PAYLOAD_REMOVE_CONFIG= "rest.sink.payload.remove";
  private static final String SINK_PAYLOAD_REMOVE_DISPLAY = "Fields to be removed from payload";
  private static final String SINK_PAYLOAD_REMOVE_DOC = "String contains comma separated list of payload fields to be removed";

  private static final String SINK_PAYLOAD_ADD_CONFIG= "rest.sink.payload.add";
  private static final String SINK_PAYLOAD_ADD_DISPLAY = "Fields to be added to payload";
  private static final String SINK_PAYLOAD_ADD_DOC = "String contains comma separated list of fields to be added. Interpolation accepted";

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

  private static final String SINK_VELOCITY_TEMPLATE_CONFIG = "rest.sink.velocity.template";
  private static final String SINK_VELOCITY_TEMPLATE_DOC =
    "Velocity template file to convert incoming messages to be used in a REST call.";
  private static final String SINK_VELOCITY_TEMPLATE_DEFAULT = "rest.vm";
  private static final String SINK_VELOCITY_TEMPLATE_DISPLAY = "Velocity template";

  static final String SINK_DATE_FORMAT_CONFIG = "rest.http.date.format";
  private static final String SINK_DATE_FORMAT_DISPLAY = "Date format for interpolation";
  private static final String SINK_DATE_FORMAT_DOC = "Date format for interpolation. The default is MM-dd-yyyy HH:mm:ss.SSS";
  private static final String SINK_DATE_FORMAT_DEFAULT = "MM-dd-yyyy HH:mm:ss.SSS";

  private static final long SINK_RETRY_BACKOFF_DEFAULT = 5000L;

  private final SinkRecordToPayloadConverter sinkRecordToPayloadConverter;
  private final Map<String, String> requestProperties;
  private RequestExecutor requestExecutor;

  @SuppressWarnings("unchecked")
  protected RestSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
    try {
      sinkRecordToPayloadConverter = ((Class<? extends SinkRecordToPayloadConverter>)
        getClass(SINK_PAYLOAD_CONVERTER_CONFIG)).getDeclaredConstructor().newInstance();
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

      .define(SINK_PROPERTIES_LIST_CONFIG,
        Type.LIST,
        Collections.EMPTY_LIST,
        Importance.MEDIUM,
        SINK_PROPERTIES_LIST_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SINK_PROPERTIES_LIST_DISPLAY)

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

      .define(SINK_PAYLOAD_CONVERTER_CONFIG,
        Type.CLASS,
        PAYLOAD_CONVERTER_DEFAULT,
        new PayloadConverterValidator(),
        Importance.LOW,
        SINK_PAYLOAD_CONVERTER_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SINK_PAYLOAD_CONVERTER_DISPLAY,
        new PayloadConverterRecommender())

      .define(SINK_PAYLOAD_CONVERTER_SCHEMA_CONFIG,
        Type.BOOLEAN,
        SINK_PAYLOAD_CONVERTER_SCHEMA_DEFAULT,
        new PayloadConverterSchemaValidator(),
        Importance.LOW,
        SINK_PAYLOAD_CONVERTER_SCHEMA_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SINK_PAYLOAD_CONVERTER_SCHEMA_DISPLAY
      )

      .define(SINK_RETRY_BACKOFF_CONFIG,
        Type.LONG,
        SINK_RETRY_BACKOFF_DEFAULT,
        Importance.LOW,
        SINK_RETRY_BACKOFF_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_RETRY_BACKOFF_DISPLAY)

      .define(SINK_PAYLOAD_ADD_CONFIG,
        Type.STRING,
        "",
        Importance.LOW,
        SINK_PAYLOAD_ADD_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_PAYLOAD_ADD_DISPLAY)

      .define(SINK_PAYLOAD_REMOVE_CONFIG,
        Type.STRING,
        "",
        Importance.LOW,
        SINK_PAYLOAD_REMOVE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_PAYLOAD_REMOVE_DISPLAY)

      .define(SINK_PAYLOAD_REPLACE_CONFIG,
        Type.STRING,
        "",
        Importance.LOW,
        SINK_PAYLOAD_REPLACE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_PAYLOAD_REPLACE_DISPLAY)

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
        SINK_REQUEST_EXECUTOR_DISPLAY)

      .define(SINK_VELOCITY_TEMPLATE_CONFIG,
        Type.STRING,
        SINK_VELOCITY_TEMPLATE_DEFAULT,
        Importance.LOW,
        SINK_VELOCITY_TEMPLATE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_VELOCITY_TEMPLATE_DISPLAY)

      .define(SINK_DATE_FORMAT_CONFIG,
        Type.STRING,
        SINK_DATE_FORMAT_DEFAULT,
        Importance.LOW,
        SINK_DATE_FORMAT_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SINK_DATE_FORMAT_DISPLAY)
      ;
  }

  public Interpolator getInterpolator() {
    return new DefaultInterpolator(Arrays.asList(
      new EnvironmentVariableInterpolationSource(),
      new UtilInterpolationSource(this.getString(SINK_DATE_FORMAT_CONFIG)),
      new PayloadInterpolationSource(),
      new PropertyInterpolationSource()
    ));
  }

  public RequestTransformer getRequestTransformer() {
    return new DefaultRequestTransformer(this, getInterpolator());
  }

  public String getMethod() {
    return this.getString(SINK_METHOD_CONFIG);
  }

  private List<String> getHeaders() {
    List<String> oldHeaders = new ArrayList<>(this.getList(SINK_PROPERTIES_LIST_CONFIG));
    List<String> newHeaders = new ArrayList<>(this.getList(SINK_HEADERS_LIST_CONFIG));

    newHeaders.addAll(oldHeaders);

    return newHeaders;
  }

  public String getUrl() {
    return this.getString(SINK_URL_CONFIG);
  }

  public Long getRetryBackoff() {
    return this.getLong(SINK_RETRY_BACKOFF_CONFIG);
  }

  public Boolean getIncludeSchema() {
    return this.getBoolean(SINK_PAYLOAD_CONVERTER_SCHEMA_CONFIG);
  }

  public SinkRecordToPayloadConverter getSinkRecordToPayloadConverter() {
    return sinkRecordToPayloadConverter;
  }

  public Map<String, String> getRequestProperties() {
    return requestProperties;
  }

  public String getVelocityTemplate() {
    return this.getString(SINK_VELOCITY_TEMPLATE_CONFIG);
  }

  public Map<String, String> getRequestHeaders() {
    return requestProperties;
  }

  @Override
  public String getPayloadAdditions() {
    return this.getString(SINK_PAYLOAD_ADD_CONFIG);
  }

  @Override
  public String getPayloadRemovals() {
    return this.getString(SINK_PAYLOAD_REMOVE_CONFIG);
  }

  @Override
  public String getPayloadReplacements() {
    return this.getString(SINK_PAYLOAD_REPLACE_CONFIG);
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

  private static class PayloadConverterRecommender implements ConfigDef.Recommender {
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return Arrays.asList(StringPayloadConverter.class, BytesPayloadConverter.class);
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return true;
    }
  }

  private static class PayloadConverterValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object provider) {
      if (provider instanceof Class
        && SinkRecordToPayloadConverter.class.isAssignableFrom((Class<?>) provider)) {
        return;
      }
      throw new ConfigException(name, provider, "Class must extend: "
        + SinkRecordToPayloadConverter.class);
    }

    @Override
    public String toString() {
      return "Any class implementing: " + SinkRecordToPayloadConverter.class;
    }
  }

  private static class PayloadConverterSchemaRecommender implements ConfigDef.Recommender {
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return Arrays.asList(Boolean.TRUE.toString(), Boolean.FALSE.toString());
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return true;
    }
  }

  private static class PayloadConverterSchemaValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object provider) {
      if (provider instanceof Boolean) {
        Boolean value = (Boolean) provider;
        if (value.equals(true) || (value.equals(false))) {
          return;
        }
      }
      throw new ConfigException(name, provider, "Please provide 'true' or 'false'");
    }

    @Override
    public String toString() {
      return new PayloadConverterSchemaRecommender().validValues("", new HashMap<>()).toString();
    }
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
