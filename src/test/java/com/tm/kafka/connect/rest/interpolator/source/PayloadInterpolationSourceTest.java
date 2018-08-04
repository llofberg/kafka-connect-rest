package com.tm.kafka.connect.rest.interpolator.source;

import com.tm.kafka.connect.rest.http.payload.MapPayload;
import com.tm.kafka.connect.rest.http.payload.Payload;
import com.tm.kafka.connect.rest.http.payload.StringPayload;
import com.tm.kafka.connect.rest.interpolator.InterpolationContext;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PayloadInterpolationSourceTest {

  PayloadInterpolationSource subject;

  @Before
  public void setUp() {
    subject = new PayloadInterpolationSource();
  }

  @Test
  public void shouldExtractDataFromPayload() {
    Map<String, Object> level3 = new HashMap<>();
    level3.put("bar", "value");
    Map<String, Object> level2 = new HashMap<>();
    level2.put("foo", level3);
    Map<String, Object> data = new HashMap<>();
    data.put("test", level2);

    Payload payload = new MapPayload(data);

    String value = subject.getValue("test.foo.bar", InterpolationContext.create(payload));

    assertEquals("value", value);
  }


  @Test
  public void shouldreturnOriginalPayloadIfPayloadIsNotMapOrJSON() {
    Payload payload = new StringPayload("test");

    String value = subject.getValue("key", InterpolationContext.create(payload));

    assertEquals("test", value);
  }

}
