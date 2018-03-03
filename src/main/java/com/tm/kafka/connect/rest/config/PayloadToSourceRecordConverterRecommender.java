package com.tm.kafka.connect.rest.config;

import com.tm.kafka.connect.rest.converter.BytesPayloadConverter;
import com.tm.kafka.connect.rest.converter.StringPayloadConverter;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PayloadToSourceRecordConverterRecommender implements ConfigDef.Recommender {
  @Override
  public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
    return Arrays.asList(StringPayloadConverter.class, BytesPayloadConverter.class);
  }

  @Override
  public boolean visible(String name, Map<String, Object> connectorConfigs) {
    return true;
  }
}
