package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.interpolator.InterpolationContext;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class PropertyInterpolationSourceTest {

  PropertyInterpolationSource subject;

  @Before
  public void setUp() {
    subject = new PropertyInterpolationSource();
  }

  @Test
  public void shouldReturnPropertyTypeWhenRequesting() {
    assertEquals(InterpolationSourceType.property, subject.getType());
  }

  @Test
  public void shouldReturnSystemPropertyWhenRequested() {
    String property = System.getProperties().propertyNames().nextElement().toString();
    String value = subject.getValue("system:" + property, mock(InterpolationContext.class));
    assertEquals(System.getProperty(property), value);
  }

  @Test
  public void shouldReturnClasspathPropertyWhenRequested() {
    String value = subject.getValue("test.properties:property.foo", mock(InterpolationContext.class));
    assertEquals("bar", value);
  }

  @Test
  public void shouldReturnFilePropertyWhenRequested() {
    URL url = ClassLoader.getSystemResource("test.properties");
    String value = subject.getValue(url.getFile() + ":property.foo", mock(InterpolationContext.class));
    assertEquals("bar", value);
  }

  @Test
  public void shouldReturnNullWhenNoPropertyFound() {
    String value = subject.getValue("test.properties:does.not.exist", mock(InterpolationContext.class));
    assertEquals(null, value);
  }

  @Test
  public void shouldReturnNullWhenNoPropertiesFileFound() {
    String value = subject.getValue("no-such-file.properties:property.foo", mock(InterpolationContext.class));
    assertEquals(null, value);
  }

  @Test
  public void shouldReturnNullWhenPropertyDefinedIncorrectly() {
    String value = subject.getValue("some-property", mock(InterpolationContext.class));
    assertEquals(null, value);
  }

}
