package com.tm.kafka.connect.rest.http.payload;


import com.tm.kafka.connect.rest.VersionUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.HashMap;
import java.util.Map;


public class ConstantPayloadGeneratorConfig extends AbstractConfig {

  public static final String PAYLOAD_CONFIG = "rest.source.data";
  private static final String PAYLOAD_DOC = "The HTTP request payload that will be sent with each REST request.  " +
    "The payload may be sent as request parameters in the case of a GET request, or as the request body in the case of " +
    "POST.";
  private static final String PAYLOAD_DISPLAY = "Payload for REST source connector.";
  private static final String PAYLOAD_DEFAULT = null;


  @SuppressWarnings("unchecked")
  protected ConstantPayloadGeneratorConfig(ConfigDef config, Map<String, ?> parsedConfig) {
    super(config, parsedConfig);
  }

  public ConstantPayloadGeneratorConfig(Map<String, ?> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    String group = "REST_HTTP";
    int orderInGroup = 0;
    return new ConfigDef()
      .define(PAYLOAD_CONFIG,
        Type.STRING,
        PAYLOAD_DEFAULT,
        Importance.LOW,
        PAYLOAD_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        PAYLOAD_DISPLAY)
      ;
  }

  public String getPayload() {
    return this.getString(PAYLOAD_CONFIG);
  }
}
