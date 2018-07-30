package com.tm.kafka.connect.rest.converter.sink;

import com.tm.kafka.connect.rest.http.payload.JSONPayload;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SinkJSONPayloadConverterTest {

  SinkJSONPayloadConverter subject;

  @Before
  public void setUp() {
    subject = new SinkJSONPayloadConverter();
  }

  @Test
  public void shouldConvertJsonMessage() throws Exception {
    String json = "{\"key1\":\"val1\",\"key2\":\"val2\"}";
    SinkRecord record = mock(SinkRecord.class);
    when(record.value()).thenReturn(json);

    JSONPayload converted = subject.convert(record);

    assertEquals(json, converted.asString());
  }

  @Test
  public void shouldReturnEmptyJsonObjectIfStringIsUnparsable() throws Exception {
    String json = "{ \"corrupted json\" ";
    SinkRecord record = mock(SinkRecord.class);
    when(record.value()).thenReturn(json);

    JSONPayload converted = subject.convert(record);

    assertEquals("{}", converted.asString());
  }

}
