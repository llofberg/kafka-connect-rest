package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AbstractValueProviderTest {

  Request request = mock(Request.class);
  Response response = mock(Response.class);

  AbstractValueProvider provider = new TestValueProvider();

  @Test
  public void update() {
    when(response.getPayload()).thenReturn("OK");
    provider.update(request, response);
    Map<String, Object> parameters = provider.getParameters();
    assertThat(parameters, hasEntry("body", "OK"));
    assertThat(parameters.keySet(), hasSize(1));
  }

  @Test
  public void lookupValue_alreadyFound() {
    assertThat(provider.lookupValue("parameter"), equalTo("value"));
  }

  @Test
  public void lookupValue_canBeFound() {
    assertThat(provider.lookupValue("test"), equalTo("yeah"));
  }

  @Test
  public void lookupValue_cannotBeFound() {
    assertThat(provider.lookupValue("xxx"), nullValue());
  }


  @Test
  public void getParameters() {
    assertThat(provider.getParameters(), hasEntry("parameter", "value"));
  }

  @Test
  public void setParameters() {
    provider.setParameters(Collections.singletonMap("new-param", "val"));
    Map<String, Object> parameters = provider.getParameters();
    assertThat(parameters, hasEntry("new-param", "val"));
    assertThat(parameters.keySet(), hasSize(1));
  }


  private static class TestValueProvider extends AbstractValueProvider {

    public TestValueProvider() {
      parameterMap.put("parameter", "value");
    }

    @Override
    void extractValues(Request request, Response response) {
      parameterMap.put("body", response.getPayload());
    }

    @Override
    String getValue(String key) {
      return "test".equals(key) ? "yeah" : null;
    }
  }
}
