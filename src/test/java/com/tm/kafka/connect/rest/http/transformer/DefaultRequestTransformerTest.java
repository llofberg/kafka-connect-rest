package com.tm.kafka.connect.rest.http.transformer;

import com.tm.kafka.connect.rest.config.RequestTransformationFields;
import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.payload.MapPayload;
import com.tm.kafka.connect.rest.http.payload.Payload;
import com.tm.kafka.connect.rest.http.payload.StringPayload;
import com.tm.kafka.connect.rest.interpolator.Interpolator;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultRequestTransformerTest {

  @Test
  public void shouldTransformRequest() {
    Interpolator interpolator = mock(Interpolator.class);
    RequestTransformationFields transformationFields = mock(RequestTransformationFields.class);

    when(transformationFields.getPayloadAdditions()).thenReturn("foo:${mock:key1},bar:${mock:key2}");
    when(transformationFields.getPayloadRemovals()).thenReturn("remove1");
    when(transformationFields.getPayloadReplacements()).thenReturn("key1:old-${mock:key1},key2:old-${mock:key2}");

    RequestTransformer subject = new DefaultRequestTransformer(transformationFields, interpolator);

    String url = "http://test.com/${mock:url}/abc";

    Map<String, Object> data = new HashMap<>();
    data.put("key1", "val1");
    data.put("key2", "val2");
    data.put("remove1", "to-be-removed");

    Map<String, String> properties = new HashMap<>();
    properties.put("prop1", "${mock:prop1}");
    properties.put("prop2", "${mock:prop2}");
    properties.put("prop3", "val3");

    Payload payload = new MapPayload(data);
    Request request = new Request(url, payload, properties);

    when(interpolator.interpolate(eq("mock:url"), any())).thenReturn("interpolated-url");
    when(interpolator.interpolate(eq("mock:key1"), any())).thenReturn("interpolated-key1");
    when(interpolator.interpolate(eq("mock:key2"), any())).thenReturn("interpolated-key2");
    when(interpolator.interpolate(eq("mock:prop1"), any())).thenReturn("interpolated-prop1");
    when(interpolator.interpolate(eq("mock:prop2"), any())).thenReturn("interpolated-prop2");

    subject.transform(request);

    Map<String, Object> expectedProperites = new HashMap<>();
    expectedProperites.put("prop1", "interpolated-prop1");
    expectedProperites.put("prop2", "interpolated-prop2");
    expectedProperites.put("prop3", "val3");

    Map<String, Object> expectedPayload = new HashMap<>();
    expectedPayload.put("key1", "old-interpolated-key1");
    expectedPayload.put("key2", "old-interpolated-key2");
    expectedPayload.put("foo", "interpolated-key1");
    expectedPayload.put("bar", "interpolated-key2");

    assertEquals("http://test.com/interpolated-url/abc", request.getUrl());
    assertEquals(expectedProperites, request.getHeaders());
    assertEquals(expectedPayload, request.getPayload().get());
  }

  @Test
  public void shouldPassWithEmptyFields() {
    RequestTransformationFields rtf = mock(RequestTransformationFields.class);
    Interpolator interpolator = mock(Interpolator.class);

    RequestTransformer subject = new DefaultRequestTransformer(rtf, interpolator);
    Request request = mock(Request.class);
    when(request.getUrl()).thenReturn("http://test.com");
    when(request.getPayload()).thenReturn(new MapPayload(new HashMap()));

    subject.transform(request);
  }

  @Test
  public void shouldReplaceDataForStringPayload() {
    RequestTransformationFields rtf = mock(RequestTransformationFields.class);
    Interpolator interpolator = mock(Interpolator.class);

    RequestTransformer subject = new DefaultRequestTransformer(rtf, interpolator);
    when(interpolator.interpolate(eq("foo.bar"), any())).thenReturn("interpolated");
    Request request = new Request("http://test.com", new StringPayload("test/${foo.bar}"), mock(Map.class));

    subject.transform(request);

    assertEquals("test/interpolated", request.getPayload().asString());
  }

  @Test
  public void shouldReplaceWithEmptyStringIfInterpolationValueWasNotFound() {
    Interpolator interpolator = mock(Interpolator.class);
    RequestTransformer subject = new DefaultRequestTransformer(mock(RequestTransformationFields.class), interpolator);
    String url = "http://test.com/abc/${mock:url}";
    Request request = new Request(url, mock(Payload.class), new HashMap<>());
    when(interpolator.interpolate(eq("mock:url"), any())).thenReturn(null);

    subject.transform(request);

    assertEquals("http://test.com/abc/", request.getUrl());
  }

}
