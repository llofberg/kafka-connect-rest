package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.interpolator.InterpolationContext;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class UtilInterpolationSourceTest {

  private UtilInterpolationSource subject;

  @Before
  public void setUp() {
    subject = new UtilInterpolationSource("MM-dd-yyyy HH:mm:ss.SSS");
  }

  @Test
  public void shouldReturnUtilType() {
    assertEquals(InterpolationSourceType.util, subject.getType());
  }

  @Test
  public void shouldReturnTimestampWhenRequested() {
    String timestamp = subject.getValue("timestamp", mock(InterpolationContext.class));
    assertTrue(Long.parseLong(timestamp) > 1523998769);
  }

  @Test
  public void shouldReturnDateWhenRequested() throws ParseException {
    String dateStr = subject.getValue("date", mock(InterpolationContext.class));
    Date date = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss.SSS").parse(dateStr);
    assertTrue(date.getTime() > 1523998769);
  }

}
