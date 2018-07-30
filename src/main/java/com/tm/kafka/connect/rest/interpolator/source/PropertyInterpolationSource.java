package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.interpolator.InterpolationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyInterpolationSource implements InterpolationSource {

  private static final String SYSTEM = "system";

  private static Logger log = LoggerFactory.getLogger(PropertyInterpolationSource.class);

  private final Map<String, Properties> properties;

  public PropertyInterpolationSource() {
    properties = new HashMap<>();
  }

  @Override
  public InterpolationSourceType getType() {
    return InterpolationSourceType.property;
  }

  @Override
  public String getValue(String key, InterpolationContext ctx) {

    String[] fileAndProperty = key.split(":", 2);

    if (fileAndProperty.length == 1) {
      return null;
    }

    String propertySource = fileAndProperty[0];
    String propertyName = fileAndProperty[1];

    if (SYSTEM.equalsIgnoreCase(propertySource) && !properties.containsKey(SYSTEM)) {
      properties.put(SYSTEM, System.getProperties());
    }

    if (!properties.containsKey(propertySource)) {
      properties.put(fileAndProperty[0], loadPropertiesFromClasspath(propertySource));
    }

    Object property = properties.get(propertySource).get(propertyName);
    return property == null ? null : property.toString();
  }

  private Properties loadPropertiesFromClasspath(String propertiesFile) {
    Properties props = new Properties();
    try {
      if (isFilePropertySource(propertiesFile)) {
        // load from file
        InputStream input = new FileInputStream(propertiesFile);
        props.load(input);
      } else {
        // load from classpath
        URL url = ClassLoader.getSystemResource(propertiesFile);
        props.load(url.openStream());
      }
    } catch (IOException | NullPointerException e) {
      log.error("Can't load properties from file '" + propertiesFile + "' " + e.getMessage(), e);
    }
    return props;
  }

  private boolean isFilePropertySource(String propertySource) {
    return propertySource.contains("/");
  }
}
