package com.tm.kafka.connect.rest.http.payload;


import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class ConstantPayloadGeneratorConfig extends AbstractConfig {

  public static final String REQUEST_BODY_CONFIG = "rest.source.body";
  private static final String REQUEST_BODY_DOC = "The HTTP request body that will be sent with each REST request. " +
    "This parameter is not appliccable to GET requests.";
  private static final String REQUEST_BODY_DISPLAY = "HTTP request body for REST source connector.";
  private static final String REQUEST_BODY_DEFAULT = "";

  public static final String REQUEST_PARAMETER_NAMES_CONFIG = "rest.source.param.names";
  private static final String REQUEST_PARAMETER_NAMES_DOC = "The HTTP request parameter names that will be sent with each " +
    "REST request. The parameter values should each be defined by a rest.source.param.<param_name>.value entry.";
  private static final String REQUEST_PARAMETER_NAMES_DISPLAY = "HTTP request parameter names for REST source connector.";
  private static final List<String> REQUEST_PARAMETER_NAMES_DEFAULT = Collections.EMPTY_LIST;

  public static final String REQUEST_PARAMETER_VALUE_CONFIG = "rest.source.param.%s.value";
  private static final String REQUEST_PARAMETER_VALUE_DOC = "Value for %s parameter which will be passed with each " +
    "REST request.  This value will be URLEncoded before transmission.";
  private static final String REQUEST_PARAMETER_VALUE_DISPLAY = "Value for %s parameter for REST source connector.";
  private static final Object REQUEST_PARAMETER_VALUE_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;

  public static final String REQUEST_HEADERS_CONFIG = "rest.source.headers";
  private static final String REQUEST_HEADERS_DISPLAY = "The HTTP request headers that will be sent with each REST " +
    "request. The headers should be of the form 'key:value'.";
  private static final String REQUEST_HEADERS_DOC = "HTTP request headers for REST source connector.";
  private static final List<String> REQUEST_HEADERS_DEFAULT = Collections.EMPTY_LIST;

  private final Map<String, String> requestParameters;
  private final Map<String, String> requestHeaders;


  protected ConstantPayloadGeneratorConfig(ConfigDef config, Map<String, ?> unparsedConfig) {
    super(config, unparsedConfig);

    List<String> paramNames = getRequestParameterNames();
    requestParameters = new HashMap<>(paramNames.size());
    paramNames.forEach(key -> requestParameters.put(key, getString(String.format(REQUEST_PARAMETER_VALUE_CONFIG, key))));

    requestHeaders = getList(REQUEST_HEADERS_CONFIG).stream()
      .map(a -> a.split(":", 2))
      .collect(Collectors.toMap(a -> a[0], a -> a[1]));
  }

  public ConstantPayloadGeneratorConfig(Map<String, ?> unparsedConfig) {
    this(conf(unparsedConfig), unparsedConfig);
  }



  public static ConfigDef conf(Map<String, ?> unparsedConfig) {
    String group = "REST_HTTP";
    int orderInGroup = 0;
    ConfigDef config =  new ConfigDef()
      .define(REQUEST_BODY_CONFIG,
        Type.STRING,
        REQUEST_BODY_DEFAULT,
        Importance.LOW,
        REQUEST_BODY_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        REQUEST_BODY_DISPLAY)

      .define(REQUEST_PARAMETER_NAMES_CONFIG,
        Type.LIST,
        REQUEST_PARAMETER_NAMES_DEFAULT,
        Importance.HIGH,
        REQUEST_PARAMETER_NAMES_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        REQUEST_PARAMETER_NAMES_DISPLAY)

      .define(REQUEST_HEADERS_CONFIG,
        Type.LIST,
        REQUEST_HEADERS_DEFAULT,
        Importance.HIGH,
        REQUEST_HEADERS_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        REQUEST_HEADERS_DISPLAY)
      ;

    // This is a bit hacky and there may be a better way of doing it, but I don't know it.
    // We need to create config items dynamically, based on the parameter names,
    // so we need a 2 pass parse of the config.
    List<String> paramNames = (List) config.parse(unparsedConfig).get(REQUEST_PARAMETER_NAMES_CONFIG);

    for(String paramName : paramNames) {
      config.define(String.format(REQUEST_PARAMETER_VALUE_CONFIG, paramName),
        Type.STRING,
        REQUEST_PARAMETER_VALUE_DEFAULT,
        Importance.HIGH,
        String.format(REQUEST_PARAMETER_VALUE_DOC, paramName),
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        String.format(REQUEST_PARAMETER_VALUE_DISPLAY, paramName));
    }

    return(config);
  }

  public String getRequestBody() {
    return this.getString(REQUEST_BODY_CONFIG);
  }

  public List<String> getRequestParameterNames() {
    return this.getList(REQUEST_PARAMETER_NAMES_CONFIG);
  }

  public Map<String, String> getRequestParameters() {
    return requestParameters;
  }

  public Map<String, String> getRequestHeaders() {
    return requestHeaders;
  }
}
