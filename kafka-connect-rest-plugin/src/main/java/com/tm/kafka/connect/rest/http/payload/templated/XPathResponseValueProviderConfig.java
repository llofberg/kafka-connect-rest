package com.tm.kafka.connect.rest.http.payload.templated;


import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class XPathResponseValueProviderConfig extends AbstractConfig {

  public static final String RESPONSE_VAR_NAMES_CONFIG = "rest.source.response.var.names";
  private static final String RESPONSE_VAR_NAMES_DOC = "A list of variable names to be used for substitution into the " +
    "HTTP request template.  An XPath expression must be defined for each variable and will be run against the HTTP " +
    "response to extract values for the next HTTP request.";
  private static final String RESPONSE_VAR_NAMES_DISPLAY = "Template variables for REST source connector.";
  private static final List<String> RESPONSE_VAR_NAMES_DEFAULT = Collections.EMPTY_LIST;

  public static final String RESPONSE_VAR_XPATH_CONFIG = "rest.source.response.var.%s.xpath";
  private static final String RESPONSE_VAR_XPATH_DOC = "The XPath expression used to extract the %s variable from the " +
    "HTTP response. This parameter can then be used in the next templated HTTP request.";
  private static final String RESPONSE_VAR_XPATH_DISPLAY = "XPath for %s variable for REST source connector.";
  private static final Object RESPONSE_VAR_XPATH_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;


  private final Map<String, String> responseVariableXPaths;


  protected XPathResponseValueProviderConfig(ConfigDef config, Map<String, ?> unparsedConfig) {
    super(config, unparsedConfig);

    List<String> variableNames = getResponseVariableNames();
    responseVariableXPaths = new HashMap<>(variableNames.size());
    variableNames.forEach(key -> responseVariableXPaths.put(key, getString(String.format(RESPONSE_VAR_XPATH_CONFIG, key))));
  }

  public XPathResponseValueProviderConfig(Map<String, ?> unparsedConfig) {
    this(conf(unparsedConfig), unparsedConfig);
  }

  public static ConfigDef conf(Map<String, ?> unparsedConfig) {
    String group = "REST_HTTP";
    int orderInGroup = 0;
    ConfigDef config = new ConfigDef()
      .define(RESPONSE_VAR_NAMES_CONFIG,
        Type.LIST,
        RESPONSE_VAR_NAMES_DEFAULT,
        Importance.LOW,
        RESPONSE_VAR_NAMES_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        RESPONSE_VAR_NAMES_DISPLAY)
      ;

    // This is a bit hacky and there may be a better way of doing it, but I don't know it.
    // We need to create config items dynamically, based on the parameter names,
    // so we need a 2 pass parse of the config.
    List<String> varNames = (List) config.parse(unparsedConfig).get(RESPONSE_VAR_NAMES_CONFIG);

    for(String varName : varNames) {
      config.define(String.format(RESPONSE_VAR_XPATH_CONFIG, varName),
        Type.STRING,
        RESPONSE_VAR_XPATH_DEFAULT,
        Importance.HIGH,
        String.format(RESPONSE_VAR_XPATH_DOC, varName),
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        String.format(RESPONSE_VAR_XPATH_DISPLAY, varName));
    }

    return(config);
  }

  public List<String> getResponseVariableNames() {
    return this.getList(RESPONSE_VAR_NAMES_CONFIG);
  }

  public Map<String, String> getResponseVariableXPaths() {
    return responseVariableXPaths;
  }
}
