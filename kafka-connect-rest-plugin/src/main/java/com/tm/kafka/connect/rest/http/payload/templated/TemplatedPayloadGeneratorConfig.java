package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.config.InstanceOfValidator;
import com.tm.kafka.connect.rest.config.ServiceProviderInterfaceRecommender;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class TemplatedPayloadGeneratorConfig extends AbstractConfig {

  public static final String REQUEST_BODY_TEMPLATE_CONFIG = "rest.source.body.template";
  private static final String REQUEST_BODY_TEMPLATE_DOC = "The template used to generate the HTTP request body that will be " +
    "sent with each REST request. This parameter is not appliccable to GET requests.";
  private static final String REQUEST_BODY_TEMPLATE_DISPLAY = "Request body template for REST source connector.";
  private static final String REQUEST_BODY_TEMPLATE_DEFAULT = "";

  public static final String REQUEST_PARAMETER_NAMES_CONFIG = "rest.source.param.names";
  private static final String REQUEST_PARAMETER_NAMES_DOC = "The HTTP request parameter names that will be sent with each " +
    "REST request. The parameter values should each be defined by a rest.source.param.<param_name>.value entry.";
  private static final String REQUEST_PARAMETER_NAMES_DISPLAY = "HTTP request parameter names for REST source connector.";
  private static final List<String> REQUEST_PARAMETER_NAMES_DEFAULT = Collections.EMPTY_LIST;

  public static final String REQUEST_PARAMETER_TEMPLATE_CONFIG = "rest.source.param.%s.template";
  private static final String REQUEST_PARAMETER_TEMPLATE_DOC = "Template used to generate the %s parameter which will " +
    "be passed with each REST request.  This value will be URLEncoded before transmission.";
  private static final String REQUEST_PARAMETER_TEMPLATE_DISPLAY = "Template for %s parameter for REST source connector.";
  private static final Object REQUEST_PARAMETER_TEMPLATE_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;

  public static final String REQUEST_HEADERS_TEMPLATE_CONFIG = "rest.source.headers.template";
  private static final String REQUEST_HEADERS_TEMPLATE_DISPLAY = "The Templates used to generate HTTP request headers " +
    "that will be sent with each REST request. The headers should be of the form 'key:value'.";
  private static final String REQUEST_HEADERS_TEMPLATE_DOC = "Request headers template for REST source connector.";
  private static final List<String> REQUEST_HEADERS_TEMPLATE_DEFAULT = Collections.EMPTY_LIST;

  public static final String VALUE_PROVIDER_CONFIG = "rest.source.payload.value.provider";
  private static final String VALUE_PROVIDER_DOC = "The class that will provide the values to be substituted into " +
    "the payload template by the payload generator.";
  private static final String VALUE_PROVIDER_DISPLAY = "Payload Value Provider class for REST source connector.";
  private static final Class<? extends ValueProvider> VALUE_PROVIDER_DEFAULT =
    EnvironmentValueProvider.class;

  public static final String TEMPLATE_ENGINE_CONFIG = "rest.source.payload.template.engine";
  private static final String TEMPLATE_ENGINE_DOC = "The template engine that will process the template and " +
    "substitute values to generate an actual payload string.";
  private static final String TEMPLATE_ENGINE_DISPLAY = "Payload Template Engine class for REST source connector.";
  private static final Class<? extends TemplateEngine> TEMPLATE_ENGINE_DEFAULT =
    VelocityTemplateEngine.class;

  private final Map<String, String> requestParameterTemplates;
  private final Map<String, String> requestHeaderTemplates;
  private final ValueProvider valueProvider;
  private final TemplateEngine templateEngine;


  protected TemplatedPayloadGeneratorConfig(ConfigDef config, Map<String, ?> unparsedConfig) {
    super(config, unparsedConfig);

    valueProvider = this.getConfiguredInstance(VALUE_PROVIDER_CONFIG, ValueProvider.class);
    templateEngine = this.getConfiguredInstance(TEMPLATE_ENGINE_CONFIG, TemplateEngine.class);

    List<String> paramNames = getRequestParameterNames();
    requestParameterTemplates = new HashMap<>(paramNames.size());
    paramNames.forEach(key -> requestParameterTemplates.put(key, getString(String.format(REQUEST_PARAMETER_TEMPLATE_CONFIG, key))));

    requestHeaderTemplates = getList(REQUEST_HEADERS_TEMPLATE_CONFIG).stream()
      .map(a -> a.split(":", 2))
      .collect(Collectors.toMap(a -> a[0], a -> a[1]));
  }

  public TemplatedPayloadGeneratorConfig(Map<String, ?> unparsedConfig) {
    this(conf(unparsedConfig), unparsedConfig);
  }

  public static ConfigDef conf(Map<String, ?> unparsedConfig) {
    String group = "REST_HTTP";
    int orderInGroup = 0;
    ConfigDef config = new ConfigDef()
      .define(VALUE_PROVIDER_CONFIG,
        Type.CLASS,
        VALUE_PROVIDER_DEFAULT,
        new InstanceOfValidator(ValueProvider.class),
        Importance.HIGH,
        VALUE_PROVIDER_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        VALUE_PROVIDER_DISPLAY,
        new ServiceProviderInterfaceRecommender(ValueProvider.class))

      .define(TEMPLATE_ENGINE_CONFIG,
        Type.CLASS,
        TEMPLATE_ENGINE_DEFAULT,
        new InstanceOfValidator(TemplateEngine.class),
        Importance.HIGH,
        TEMPLATE_ENGINE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        TEMPLATE_ENGINE_DISPLAY,
        new ServiceProviderInterfaceRecommender(TemplateEngine.class))

      .define(REQUEST_BODY_TEMPLATE_CONFIG,
        Type.STRING,
        REQUEST_BODY_TEMPLATE_DEFAULT,
        Importance.LOW,
        REQUEST_BODY_TEMPLATE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        REQUEST_BODY_TEMPLATE_DISPLAY)

      .define(REQUEST_PARAMETER_NAMES_CONFIG,
        Type.LIST,
        REQUEST_PARAMETER_NAMES_DEFAULT,
        Importance.LOW,
        REQUEST_PARAMETER_NAMES_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        REQUEST_PARAMETER_NAMES_DISPLAY)

      .define(REQUEST_HEADERS_TEMPLATE_CONFIG,
        Type.LIST,
        REQUEST_HEADERS_TEMPLATE_DEFAULT,
        Importance.LOW,
        REQUEST_HEADERS_TEMPLATE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        REQUEST_HEADERS_TEMPLATE_DISPLAY)
      ;

    // This is a bit hacky and there may be a better way of doing it, but I don't know it.
    // We need to create config items dynamically, based on the parameter names,
    // so we need a 2 pass parse of the config.
    List<String> paramNames = (List) config.parse(unparsedConfig).get(REQUEST_PARAMETER_NAMES_CONFIG);

    for(String paramName : paramNames) {
      config.define(String.format(REQUEST_PARAMETER_TEMPLATE_CONFIG, paramName),
        Type.STRING,
        REQUEST_PARAMETER_TEMPLATE_DEFAULT,
        Importance.HIGH,
        String.format(REQUEST_PARAMETER_TEMPLATE_DOC, paramName),
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        String.format(REQUEST_PARAMETER_TEMPLATE_DISPLAY, paramName));
    }

    return(config);
  }

  public String getRequestBodyTemplate() {
    return this.getString(REQUEST_BODY_TEMPLATE_CONFIG);
  }

  public List<String> getRequestParameterNames() {
    return this.getList(REQUEST_PARAMETER_NAMES_CONFIG);
  }

  public Map<String, String> getRequestParameterTemplates() {
    return requestParameterTemplates;
  }

  public Map<String, String> getRequestHeaderTemplates() {
    return requestHeaderTemplates;
  }

  public ValueProvider getValueProvider() {
    return valueProvider;
  }

  public TemplateEngine getTemplateEngine() {
    return templateEngine;
  }
}
