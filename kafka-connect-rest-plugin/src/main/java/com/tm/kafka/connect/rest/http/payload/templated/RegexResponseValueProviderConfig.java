package com.tm.kafka.connect.rest.http.payload.templated;


import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RegexResponseValueProviderConfig extends AbstractConfig {

  public static final String RESPONSE_VAR_NAMES_CONFIG = "rest.source.response.var.names";
  private static final String RESPONSE_VAR_NAMES_DOC = "A list of variable names to be used for substitution into the " +
    "HTTP request template.  A regex must be defined for each variable and will be run against the HTTP response to " +
    "extract values for the next HTTP request.";
  private static final String RESPONSE_VAR_NAMES_DISPLAY = "Template variables for REST source connector.";
  private static final List<String> RESPONSE_VAR_NAMES_DEFAULT = Collections.EMPTY_LIST;

  public static final String RESPONSE_VAR_REGEX_CONFIG = "rest.source.response.var.%s.regex";
  private static final String RESPONSE_VAR_REGEX_DOC = "The regex used to extract the %s variable from the HTTP response. " +
    "This parameter can then be used in the next templated HTTP request.";
  private static final String RESPONSE_VAR_REGEX_DISPLAY = "Regex for %s variable for REST source connector.";
  private static final Object RESPONSE_VAR_REGEX_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;


  private final Map<String, String> responseVariableRegexs;


  protected RegexResponseValueProviderConfig(ConfigDef config, Map<String, ?> unparsedConfig) {
    super(config, unparsedConfig);

    List<String> variableNames = getResponseVariableNames();
    responseVariableRegexs = new HashMap<>(variableNames.size());
    variableNames.forEach(key -> responseVariableRegexs.put(key, getString(String.format(RESPONSE_VAR_REGEX_CONFIG, key))));
  }

  public RegexResponseValueProviderConfig(Map<String, ?> unparsedConfig) {
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
      config.define(String.format(RESPONSE_VAR_REGEX_CONFIG, varName),
        Type.STRING,
        RESPONSE_VAR_REGEX_DEFAULT,
        Importance.HIGH,
        String.format(RESPONSE_VAR_REGEX_DOC, varName),
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        String.format(RESPONSE_VAR_REGEX_DISPLAY, varName));
    }

    return(config);
  }

  public List<String> getResponseVariableNames() {
    return this.getList(RESPONSE_VAR_NAMES_CONFIG);
  }

  public Map<String, String> getResponseVariableRegexs() {
    return responseVariableRegexs;
  }
}
