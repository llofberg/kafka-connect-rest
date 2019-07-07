package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;


public class EnvironmentValueProviderTest {

  Request request = mock(Request.class);
  Response response = mock(Response.class);

  EnvironmentValueProvider provider = new EnvironmentValueProvider();

  @Test
  public void extractValues() {
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), not(hasKey(anything())));
  }

  @Test
  public void getValue() {
    System.setProperty("test", "yeah");
    assertThat(provider.getValue("test"), equalTo("yeah"));
  }

  @Test
  public void getValue_notDefined() {
    System.clearProperty("test");
    assertThat(provider.getValue("test"), nullValue());
  }
}
