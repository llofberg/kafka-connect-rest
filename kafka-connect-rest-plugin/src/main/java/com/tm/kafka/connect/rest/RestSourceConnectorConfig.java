package com.tm.kafka.connect.rest;


import com.tm.kafka.connect.rest.config.*;
import com.tm.kafka.connect.rest.http.executor.RequestExecutor;
import com.tm.kafka.connect.rest.http.handler.DefaultResponseHandler;
import com.tm.kafka.connect.rest.http.handler.ResponseHandler;
import com.tm.kafka.connect.rest.http.payload.ConstantPayloadGenerator;
import com.tm.kafka.connect.rest.http.payload.PayloadGenerator;
import com.tm.kafka.connect.rest.selector.SimpleTopicSelector;
import com.tm.kafka.connect.rest.selector.TopicSelector;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;


public class RestSourceConnectorConfig extends AbstractConfig {

  public static final String SOURCE_POLL_INTERVAL_CONFIG = "rest.source.poll.interval.ms";
  private static final String SOURCE_POLL_INTERVAL_DOC = "How often to poll the source URL.";
  private static final String SOURCE_POLL_INTERVAL_DISPLAY = "Polling interval";
  private static final Long SOURCE_POLL_INTERVAL_DEFAULT = 60000L;

  public static final String SOURCE_METHOD_CONFIG = "rest.source.method";
  private static final String SOURCE_METHOD_DOC = "The HTTP method for REST source connector.";
  private static final String SOURCE_METHOD_DISPLAY = "Source method";
  private static final String SOURCE_METHOD_DEFAULT = "POST";

  public static final String SOURCE_URL_CONFIG = "rest.source.url";
  private static final String SOURCE_URL_DOC = "The URL for REST source connector.";
  private static final String SOURCE_URL_DISPLAY = "URL for REST source connector.";

  public static final String SOURCE_PAYLOAD_GENERATOR_CONFIG = "rest.source.data.generator";
  private static final String SOURCE_PAYLOAD_GENERATOR_DOC = "The payload generator class which will produce the HTTP " +
    "request payload to be sent to the REST endpoint.  The payload may be sent as request parameters in the case of a " +
    "GET request, or as the request body in the case of POST";
  private static final String SOURCE_PAYLOAD_GENERATOR_DISPLAY = "Payload Generator class for REST source connector.";
  private static final Class<? extends PayloadGenerator> SOURCE_PAYLOAD_GENERATOR_DEFAULT = ConstantPayloadGenerator.class;

  public static final String SOURCE_TOPIC_SELECTOR_CONFIG = "rest.source.topic.selector";
  private static final String SOURCE_TOPIC_SELECTOR_DOC = "The topic selector class for REST source connector.";
  private static final String SOURCE_TOPIC_SELECTOR_DISPLAY = "Topic selector class for REST source connector.";
  private static final Class<? extends TopicSelector> SOURCE_TOPIC_SELECTOR_DEFAULT = SimpleTopicSelector.class;

  public static final String SOURCE_REQUEST_EXECUTOR_CONFIG = "rest.http.executor.class";
  private static final String SOURCE_REQUEST_EXECUTOR_DISPLAY = "HTTP request executor";
  private static final String SOURCE_REQUEST_EXECUTOR_DOC = "HTTP request executor. Default is OkHttpRequestExecutor";
  private static final String SOURCE_REQUEST_EXECUTOR_DEFAULT = "com.tm.kafka.connect.rest.http.executor.OkHttpRequestExecutor";


  private final TopicSelector topicSelector;
  private final PayloadGenerator payloadGenerator;
  private RequestExecutor requestExecutor;


  protected RestSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
    topicSelector = this.getConfiguredInstance(SOURCE_TOPIC_SELECTOR_CONFIG, TopicSelector.class);
    requestExecutor = this.getConfiguredInstance(SOURCE_REQUEST_EXECUTOR_CONFIG, RequestExecutor.class);
    payloadGenerator = this.getConfiguredInstance(SOURCE_PAYLOAD_GENERATOR_CONFIG, PayloadGenerator.class);
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

      .define(SOURCE_URL_CONFIG,
        Type.STRING,
        NO_DEFAULT_VALUE,
        Importance.HIGH,
        SOURCE_URL_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_URL_DISPLAY)

      .define(SOURCE_PAYLOAD_GENERATOR_CONFIG,
        Type.CLASS,
        SOURCE_PAYLOAD_GENERATOR_DEFAULT,
        new InstanceOfValidator(PayloadGenerator.class),
        Importance.HIGH,
        SOURCE_PAYLOAD_GENERATOR_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_PAYLOAD_GENERATOR_DISPLAY,
        new ServiceProviderInterfaceRecommender<>(PayloadGenerator.class))

      .define(SOURCE_TOPIC_SELECTOR_CONFIG,
        Type.CLASS,
        SOURCE_TOPIC_SELECTOR_DEFAULT,
        new InstanceOfValidator(TopicSelector.class),
        Importance.HIGH,
        SOURCE_TOPIC_SELECTOR_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        SOURCE_TOPIC_SELECTOR_DISPLAY,
        new ServiceProviderInterfaceRecommender<>(TopicSelector.class))

      .define(SOURCE_REQUEST_EXECUTOR_CONFIG,
        Type.CLASS,
        SOURCE_REQUEST_EXECUTOR_DEFAULT,
        new InstanceOfValidator(RequestExecutor.class),
        Importance.LOW,
        SOURCE_REQUEST_EXECUTOR_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.NONE,
        SOURCE_REQUEST_EXECUTOR_DISPLAY,
        new ServiceProviderInterfaceRecommender<>(RequestExecutor.class))
      ;
  }

  public ResponseHandler getResponseHandler() {
    return new DefaultResponseHandler();
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

  public TopicSelector getTopicSelector() {
    return topicSelector;
  }

  public PayloadGenerator getPayloadGenerator() {
    return payloadGenerator;
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
