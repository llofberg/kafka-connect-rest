package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.interpolator.InterpolationContext;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class EnvironmentVariableInterpolationSourceTest {

  EnvironmentVariableInterpolationSource subject;

  @Before
  public void setUp() {
    subject = new EnvironmentVariableInterpolationSource();
  }

  @Test
  public void shouldReturnEnvSourceType() {
    assertEquals(InterpolationSourceType.env, subject.getType());
  }

  @Test
  public void shouldReturnEnvironmentVariableByName() {
    String envKey = System.getenv().keySet().stream().findFirst().get();
    String var = subject.getValue(envKey, mock(InterpolationContext.class));
    assertEquals(System.getenv(envKey), var);
  }

}
