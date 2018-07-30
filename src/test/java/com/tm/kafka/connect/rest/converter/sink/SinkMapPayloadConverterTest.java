package com.tm.kafka.connect.rest.converter.sink;

import com.tm.kafka.connect.rest.http.payload.MapPayload;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SinkMapPayloadConverterTest {

  SinkMapPayloadConverter subject;

  @Before
  public void setUp() {
    subject = new SinkMapPayloadConverter();
  }

  @Test
  public void shouldConvertSinkRecordToMap() throws Exception {

    Map<String, Object> map = new HashMap<>();
    map.put("key1", "val1");
    map.put("key2", "val2");

    SinkRecord record = mock(SinkRecord.class);
    when(record.value()).thenReturn(map);

    MapPayload converted = subject.convert(record);

    assertEquals(map, converted.get());
  }

  @Test
  public void shouldReturnEmptyMapIFRecordIsNotAMap() throws Exception {
    SinkRecord record = mock(SinkRecord.class);
    when(record.value()).thenReturn("string");

    MapPayload converted = subject.convert(record);

    assertEquals(Collections.emptyMap(), converted.get());
  }
}
