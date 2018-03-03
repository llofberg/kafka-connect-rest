package com.tm.kafka.connect.rest.config;

import com.tm.kafka.connect.rest.converter.PayloadToSourceRecordConverter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class PayloadToSourceRecordConverterValidator implements ConfigDef.Validator {
  @Override
  public void ensureValid(String name, Object provider) {
    if (provider != null && provider instanceof Class
        && PayloadToSourceRecordConverter.class.isAssignableFrom((Class<?>) provider)) {
      return;
    }
    throw new ConfigException(name, provider, "Class must extend: "
        + PayloadToSourceRecordConverter.class);
  }

  @Override
  public String toString() {
    return "Any class implementing: " + PayloadToSourceRecordConverter.class;
  }
}
