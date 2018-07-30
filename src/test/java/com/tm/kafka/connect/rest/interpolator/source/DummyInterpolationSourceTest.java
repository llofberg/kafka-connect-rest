package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.interpolator.InterpolationContext;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class DummyInterpolationSourceTest {

  DummyInterpolationSource source;

  @Before
  public void setUp() {
    source = new DummyInterpolationSource();
  }

  @Test
  public void shouldReturnNullAsType() {
    assertNull(source.getType());
  }

  @Test
  public void shouldReturnOriginalKeyWhenGetValue() {
    assertEquals("test", source.getValue("test", mock(InterpolationContext.class)));
  }

}
