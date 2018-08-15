package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.config.*;
import com.tm.kafka.connect.rest.converter.PayloadToSourceRecordConverter;
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
import com.tm.kafka.connect.rest.selector.SimpleTopicSelector;
import com.tm.kafka.connect.rest.selector.TopicSelector;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class RestSourceConnectorConfig extends AbstractConfig implements RequestTransformationFields, HttpProperties {

  static final String SOURCE_POLL_INTERVAL_CONFIG = "rest.source.poll.interval.ms";
  private static final String SOURCE_POLL_INTERVAL_DOC = "How often to poll the source URL.";
  private static final String SOURCE_POLL_INTERVAL_DISPLAY = "Polling interval";
  private static final Long SOURCE_POLL_INTERVAL_DEFAULT = 60000L;

  static final String SOURCE_METHOD_CONFIG = "rest.source.method";
  private static final String SOURCE_METHOD_DOC = "The HTTP method for REST source connector.";
  private static final String SOURCE_METHOD_DISPLAY = "Source method";
  private static final String SOURCE_METHOD_DEFAULT = "POST";

  @Deprecated // use SOURCE_HEADERS_LIST_CONFIG instead
  static final String SOURCE_PROPERTIES_LIST_CONFIG = "rest.source.properties";
  @Deprecated // use SOURCE_HEADERS_LIST_DISPLAY instead
  private static final String SOURCE_PROPERTIES_LIST_DOC = "The request properties (headers) for REST source connector.";
  @Deprecated // use SOURCE_HEADERS_LIST_DOC instead
  private static final String SOURCE_PROPERTIES_LIST_DISPLAY = "Source properties";

  static final String SOURCE_HEADERS_LIST_CONFIG = "rest.source.headers";
  private static final String SOURCE_HEADERS_LIST_DISPLAY = "Source request headers";
  private static final String SOURCE_HEADERS_LIST_DOC = "The request headers for REST source connector.";

  static final String SOURCE_URL_CONFIG = "rest.source.url";
  private static final String SOURCE_URL_DOC = "The URL for REST source connector.";
  private static final String SOURCE_URL_DISPLAY = "URL for REST source connector.";

  static final String SOURCE_DATA_CONFIG = "rest.source.data";
  private static final String SOURCE_DATA_DOC = "The data for REST source connector.";
  private static final String SOURCE_DATA_DISPLAY = "Data for REST source connector.";
  private static final String SOURCE_DATA_DEFAULT = null;

  static final String SOURCE_TOPIC_SELECTOR_CONFIG = "rest.source.topic.selector";
  private static final String SOURCE_TOPIC_SELECTOR_DOC = "The topic selector class for REST source connector.";
  private static final String SOURCE_TOPIC_SELECTOR_DISPLAY = "Topic selector class for REST source connector.";
  private static final Class<? extends TopicSelector> SOURCE_TOPIC_SELECTOR_DEFAULT = SimpleTopicSelector.class;

  static final String SOURCE_TOPIC_LIST_CONFIG = "rest.source.destination.topics";
  private static final String SOURCE_TOPIC_LIST_DOC = "The list of destination topics for the REST source connector.";
  private static final String SOURCE_TOPIC_LIST_DISPLAY = "Source destination topics";

  static final String SOURCE_PAYLOAD_CONVERTER_CONFIG = "rest.source.payload.converter.class";
  private static final Class<? extends PayloadToSourceRecordConverter> PAYLOAD_CONVERTER_DEFAULT = StringPayloadConverter.class;
  private static final String SOURCE_PAYLOAD_CONVERTER_DOC_CONFIG = "Class to be used to convert messages from REST calls to SourceRecords";
  private static final String SOURCE_PAYLOAD_CONVERTER_DISPLAY_CONFIG = "Payload converter class";

  static final String SOURCE_PAYLOAD_REPLACE_CONFIG = "rest.source.payload.replace";
  private static final String SOURCE_PAYLOAD_REPLACE_DISPLAY = "Fields to be replaced in payload";
  private static final String SOURCE_PAYLOAD_REPLACE_DOC = "String contains comma separated patterns for payload replacements. Interpolation accepted";

  static final String SOURCE_PAYLOAD_REMOVE_CONFIG= "rest.source.payload.remove";
  private static final String SOURCE_PAYLOAD_REMOVE_DISPLAY = "Fields to be removed from payload";
  private static final String SOURCE_PAYLOAD_REMOVE_DOC = "String contains comma separated list of payload fields to be removed";

  static final String SOURCE_PAYLOAD_ADD_CONFIG= "rest.source.payload.add";
  private static final String SOURCE_PAYLOAD_ADD_DISPLAY = "Fields to be added to payload";
  private static final String SOURCE_PAYLOAD_ADD_DOC = "String contains comma separated list of fields to be added. Interpolation accepted";

  static final String SOURCE_HTTP_CONNECTION_TIMEOUT_CONFIG = "rest.http.connection.connection.timeout";
  private static final String SOURCE_HTTP_CONNECTION_TIMEOUT_DISPLAY = "HTTP connection timeout in milliseconds";
  private static final String SOURCE_HTTP_CONNECTION_TIMEOUT_DOC = "HTTP connection timeout in milliseconds";
  private static final long SOURCE_HTTP_CONNECTION_TIMEOUT_DEFAULT = 2000;

  static final String SOURCE_HTTP_READ_TIMEOUT_CONFIG = "rest.http.connection.read.timeout";
  private static final String SOURCE_HTTP_READ_TIMEOUT_DISPLAY = "HTTP read timeout in milliseconds";
  private static final String SOURCE_HTTP_READ_TIMEOUT_DOC = "HTTP read timeout in milliseconds";
  private static final long SOURCE_HTTP_READ_TIMEOUT_DEFAULT = 2000;

  static final String SOURCE_HTTP_KEEP_ALIVE_DURATION_CONFIG = "rest.http.connection.keep.alive.ms";
  private static final String SOURCE_HTTP_KEEP_ALIVE_DURATION_DISPLAY = "Keep alive in milliseconds";
  private static final String SOURCE_HTTP_KEEP_ALIVE_DURATION_DOC = "For how long keep HTTP connection should be keept alive in milliseconds";
  private static final long SOURCE_HTTP_KEEP_ALIVE_DURATION_DEFAULT = 300000; // 5 minutes

  static final String SOURCE_HTTP_MAX_IDLE_CONNECTION_CONFIG = "rest.http.connection.max.idle";
  private static final String SOURCE_HTTP_MAX_IDLE_CONNECTION_DISPLAY = "Number of idle connections";
  private static final String SOURCE_HTTP_MAX_IDLE_CONNECTION_DOC = "How many idle connections per host can be keept opened";
  private static final int SOURCE_HTTP_MAX_IDLE_CONNECTION_DEFAULT = 5;

  static final String SOURCE_REQUEST_EXECUTOR_CONFIG = "rest.http.executor.class";
  private static final String SOURCE_REQUEST_EXECUTOR_DISPLAY = "HTTP request executor";
  private static final String SOURCE_REQUEST_EXECUTOR_DOC = "HTTP request executor. Default is OkHttpRequestExecutor";
  private static final String SOURCE_REQUEST_EXECUTOR_DEFAULT = "com.tm.kafka.connect.rest.http.executor.OkHttpRequestExecutor";

  private final TopicSelector topicSelector;
  private final PayloadToSourceRecordConverter payloadToSourceRecordConverter;
  private final Map<String, String> requestProperties;
  private RequestExecutor requestExecutor;

  @SuppressWarnings("unchecked")
  protected RestSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
    try {
      topicSelector = ((Class<? extends TopicSelector>)
        getClass(SOURCE_TOPIC_SELECTOR_CONFIG)).getDeclaredConstructor().newInstance();
      payloadToSourceRecordConverter = ((Class<? extends PayloadToSourceRecordConverter>)
        getClass(SOURCE_PAYLOAD_CONVERTER_CONFIG)).getDeclaredConstructor().newInstance();
      requestExecutor = (RequestExecutor)
        getClass(SOURCE_REQUEST_EXECUTOR_CONFIG).getDeclaredConstructor(HttpProperties.class).newInstance(this);
    } catch (IllegalAccessException | InstantiationException
        | InvocationTargetException | NoSuchMethodException e) {
      throw new ConnectException("Invalid class for: " + SOURCE_PAYLOAD_CONVERTER_CONFIG, e);
    }
    requestProperties = getHeaders().stream()
      .map(a -> a.split(":", 2))
      .collect(Collectors.toMap(a -> a[0], a -> a[1]));

  }

  public RestSourceConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    String group = "REST";
    int orderInGroup = 0;
    return new ConfigDef()
      .define(SOURCE_POLL_INTERVAL_CONFIG,
        Type.LONG,
        SOURCE_POLL_INTERVAL_DEFAULT,
        Importance.LOW,
        SOURCE_POLL_INTERVAL_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_POLL_INTERVAL_DISPLAY)

      .define(SOURCE_METHOD_CONFIG,
        Type.STRING,
        SOURCE_METHOD_DEFAULT,
        new MethodValidator(),
        Importance.HIGH,
        SOURCE_METHOD_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_METHOD_DISPLAY,
        new MethodRecommender())

      .define(SOURCE_PROPERTIES_LIST_CONFIG,
        Type.LIST,
        NO_DEFAULT_VALUE,
        Importance.HIGH,
        SOURCE_PROPERTIES_LIST_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_PROPERTIES_LIST_DISPLAY)

      .define(SOURCE_HEADERS_LIST_CONFIG,
        Type.LIST,
        Collections.EMPTY_LIST,
        Importance.HIGH,
        SOURCE_HEADERS_LIST_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_HEADERS_LIST_DISPLAY)

      .define(SOURCE_URL_CONFIG,
        Type.STRING,
        NO_DEFAULT_VALUE,
        Importance.HIGH,
        SOURCE_URL_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_URL_DISPLAY)

      .define(SOURCE_DATA_CONFIG,
        Type.STRING,
        SOURCE_DATA_DEFAULT,
        Importance.LOW,
        SOURCE_DATA_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_DATA_DISPLAY)

      .define(SOURCE_TOPIC_LIST_CONFIG,
        Type.LIST,
        NO_DEFAULT_VALUE,
        Importance.HIGH,
        SOURCE_TOPIC_LIST_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_TOPIC_LIST_DISPLAY)

      .define(SOURCE_TOPIC_SELECTOR_CONFIG,
        Type.CLASS,
        SOURCE_TOPIC_SELECTOR_DEFAULT,
        new TopicSelectorValidator(),
        Importance.HIGH,
        SOURCE_TOPIC_SELECTOR_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_TOPIC_SELECTOR_DISPLAY,
        new TopicSelectorRecommender())

      .define(SOURCE_PAYLOAD_CONVERTER_CONFIG,
        Type.CLASS,
        PAYLOAD_CONVERTER_DEFAULT,
        new PayloadToSourceRecordConverterValidator(),
        Importance.LOW,
        SOURCE_PAYLOAD_CONVERTER_DOC_CONFIG,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_PAYLOAD_CONVERTER_DISPLAY_CONFIG,
        new PayloadToSourceRecordConverterRecommender())

      .define(SOURCE_PAYLOAD_ADD_CONFIG,
        Type.STRING,
        "",
        Importance.LOW,
        SOURCE_PAYLOAD_ADD_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SOURCE_PAYLOAD_ADD_DISPLAY)

      .define(SOURCE_PAYLOAD_REMOVE_CONFIG,
        Type.STRING,
        "",
        Importance.LOW,
        SOURCE_PAYLOAD_REMOVE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SOURCE_PAYLOAD_REMOVE_DISPLAY)

      .define(SOURCE_PAYLOAD_REPLACE_CONFIG,
        Type.STRING,
        "",
        Importance.LOW,
        SOURCE_PAYLOAD_REPLACE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SOURCE_PAYLOAD_REPLACE_DISPLAY)

      .define(SOURCE_HTTP_CONNECTION_TIMEOUT_CONFIG,
        Type.LONG,
        SOURCE_HTTP_CONNECTION_TIMEOUT_DEFAULT,
        Importance.LOW,
        SOURCE_HTTP_CONNECTION_TIMEOUT_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SOURCE_HTTP_CONNECTION_TIMEOUT_DISPLAY)

      .define(SOURCE_HTTP_READ_TIMEOUT_CONFIG,
        Type.LONG,
        SOURCE_HTTP_READ_TIMEOUT_DEFAULT,
        Importance.LOW,
        SOURCE_HTTP_READ_TIMEOUT_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SOURCE_HTTP_READ_TIMEOUT_DISPLAY)

      .define(SOURCE_HTTP_KEEP_ALIVE_DURATION_CONFIG,
        Type.LONG,
        SOURCE_HTTP_KEEP_ALIVE_DURATION_DEFAULT,
        Importance.LOW,
        SOURCE_HTTP_KEEP_ALIVE_DURATION_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SOURCE_HTTP_KEEP_ALIVE_DURATION_DISPLAY)

      .define(SOURCE_HTTP_MAX_IDLE_CONNECTION_CONFIG,
        Type.INT,
        SOURCE_HTTP_MAX_IDLE_CONNECTION_DEFAULT,
        Importance.LOW,
        SOURCE_HTTP_MAX_IDLE_CONNECTION_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SOURCE_HTTP_MAX_IDLE_CONNECTION_DISPLAY)

      .define(SOURCE_REQUEST_EXECUTOR_CONFIG,
        Type.CLASS,
        SOURCE_REQUEST_EXECUTOR_DEFAULT,
        Importance.LOW,
        SOURCE_REQUEST_EXECUTOR_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SOURCE_REQUEST_EXECUTOR_DISPLAY)
      ;
  }

  public Interpolator getInterpolator() {
    return new DefaultInterpolator(Arrays.asList(
      new EnvironmentVariableInterpolationSource(),
      new UtilInterpolationSource(),
      new PayloadInterpolationSource(),
      new PropertyInterpolationSource()
    ));
  }

  public RequestTransformer getRequestTransformer() {
    return new DefaultRequestTransformer(this, getInterpolator());
  }

  public ResponseHandler getResponseHandler() {
    return new DefaultResponseHandler();
  }

  private List<String> getHeaders() {
    List<String> oldHeaders = new ArrayList<>(this.getList(SOURCE_PROPERTIES_LIST_CONFIG));
    List<String> newHeaders = new ArrayList<>(this.getList(SOURCE_HEADERS_LIST_CONFIG));

    newHeaders.addAll(oldHeaders);

    return newHeaders;
  }

  public RequestExecutor getRequestExecutor() {
    return requestExecutor;
  }

  public long getPollInterval() {
    return this.getLong(SOURCE_POLL_INTERVAL_CONFIG);
  }

  public String getMethod() {
    return this.getString(SOURCE_METHOD_CONFIG);
  }

  public String getUrl() {
    return this.getString(SOURCE_URL_CONFIG);
  }

  public List<String> getTopics() {
    return this.getList(SOURCE_TOPIC_LIST_CONFIG);
  }

  public TopicSelector getTopicSelector() {
    return topicSelector;
  }

  public String getData() {
    return this.getString(SOURCE_DATA_CONFIG);
  }

  public PayloadToSourceRecordConverter getPayloadToSourceRecordConverter() {
    return payloadToSourceRecordConverter;
  }

  public Map<String, String> getRequestProperties() {
    return requestProperties;
  }

  public Map<String, String> getRequestHeaders() {
    return requestProperties;
  }

  @Override
  public String getPayloadAdditions() {
    return this.getString(SOURCE_PAYLOAD_ADD_CONFIG);
  }

  @Override
  public String getPayloadRemovals() {
    return this.getString(SOURCE_PAYLOAD_REMOVE_CONFIG);
  }

  @Override
  public String getPayloadReplacements() {
    return this.getString(SOURCE_PAYLOAD_REPLACE_CONFIG);
  }

  @Override
  public long getReadTimeout() {
    return this.getLong(SOURCE_HTTP_READ_TIMEOUT_CONFIG);
  }

  @Override
  public long getConnectionTimeout() {
    return this.getLong(SOURCE_HTTP_CONNECTION_TIMEOUT_CONFIG);
  }

  @Override
  public long getKeepAliveDuration() {
    return this.getLong(SOURCE_HTTP_KEEP_ALIVE_DURATION_CONFIG);
  }

  @Override
  public int getMaxIdleConnections() {
    return this.getInt(SOURCE_HTTP_MAX_IDLE_CONNECTION_CONFIG);
  }

  private static ConfigDef getConfig() {
    Map<String, ConfigDef.ConfigKey> everything = new HashMap<>(conf().configKeys());
    ConfigDef visible = new ConfigDef();
    for (ConfigDef.ConfigKey key : everything.values()) {
      visible.define(key);
    }
    return visible;
  }

  public static void main(String[] args) {
    System.out.println(VersionUtil.getVersion());
    System.out.println(getConfig().toEnrichedRst());
  }
}
