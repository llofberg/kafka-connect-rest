package com.tm.kafka.connect.rest.converter.sink;

import com.tm.kafka.connect.rest.http.payload.StringPayload;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SinkStringPayloadConverterTest {

  SinkStringPayloadConverter subject;

  @Before
  public void setUp() {
    subject = new SinkStringPayloadConverter();
  }

  @Test
  public void shouldConvertBytes() {
    SinkRecord record = mock(SinkRecord.class);
    when(record.value()).thenReturn("test");

    StringPayload converted = subject.convert(record);

    assertEquals("test", converted.asString());
  }

}
