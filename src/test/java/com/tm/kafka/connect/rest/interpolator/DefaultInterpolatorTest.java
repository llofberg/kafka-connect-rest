package com.tm.kafka.connect.rest.interpolator;

import com.tm.kafka.connect.rest.http.payload.Payload;
import com.tm.kafka.connect.rest.interpolator.source.InterpolationSource;
import com.tm.kafka.connect.rest.interpolator.source.InterpolationSourceType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultInterpolatorTest {


  private InterpolationSource source;
  private DefaultInterpolator subject;

  @Before
  public void setUp() {
    source = mock(InterpolationSource.class);
    when(source.getType()).thenReturn(InterpolationSourceType.util);
    subject = new DefaultInterpolator(Arrays.asList(source));
  }

  @Test
  public void shouldCallCorrespondingInterpolationSource() {
    when(source.getValue(eq("test"), any())).thenReturn("result");

    String interpolated = subject.interpolate("util:test", InterpolationContext.create(mock(Payload.class)));

    assertEquals("result", interpolated);
  }

  @Test
  public void shouldReturnOriginalStringWhenNoSuchInterpoloationSource() {
    String interpolated = subject.interpolate("no-such-source:test", InterpolationContext.create(mock(Payload.class)));
    assertEquals("no-such-source:test", interpolated);
  }

}
