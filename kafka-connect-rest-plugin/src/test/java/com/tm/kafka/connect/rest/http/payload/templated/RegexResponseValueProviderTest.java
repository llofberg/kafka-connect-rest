package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class RegexResponseValueProviderTest {

  Request request = mock(Request.class);
  Response response = mock(Response.class);

  RegexResponseValueProvider provider = new RegexResponseValueProvider();

  @Test
  public void extractValuesTest_wholeRegexFoundOnce() {
    provider.setRegexes(Collections.singletonMap("name", ".+"));
    when(response.getPayload()).thenReturn("Hello Big Ears");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), hasEntry("name", "Hello Big Ears"));
  }

  @Test
  public void extractValuesTest_wholeRegexFoundMultiple() {
    provider.setRegexes(Collections.singletonMap("name", "\\w+"));
    when(response.getPayload()).thenReturn("Hello Big Ears");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), hasEntry("name", "Hello,Big,Ears"));
  }

  @Test
  public void extractValuesTest_singleGroupFoundOnce() {
    provider.setRegexes(Collections.singletonMap("name", "Hello (.+)"));
    when(response.getPayload()).thenReturn("Hello Big Ears");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), hasEntry("name", "Big Ears"));
  }

  @Test
  public void extractValuesTest_singleGroupFoundMultiple() {
    provider.setRegexes(Collections.singletonMap("name", "Hello (\\w+)"));
    when(response.getPayload()).thenReturn("Hello Noddy Hello Noddy");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), hasEntry("name", "Noddy,Noddy"));
  }

  @Test
  public void extractValuesTest_multipleGroupsFound() {
    provider.setRegexes(Collections.singletonMap("name", "Hello (\\w+) (\\w+)"));
    when(response.getPayload()).thenReturn("Hello Big Ears");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), hasEntry("name", "Big,Ears"));
  }

  @Test
  public void extractValuesTest_valueNotFound() {
    provider.setRegexes(Collections.singletonMap("name", "Hello (.*)"));
    when(response.getPayload()).thenReturn("Hi Big Ears");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), hasEntry("name", null));
  }

  @Test
  public void lookupValueTest_extracted() {
    provider.setRegexes(Collections.singletonMap("name", "Hello (.*)"));
    when(response.getPayload()).thenReturn("Hello Big Ears");
    provider.extractValues(request, response);
    assertThat(provider.lookupValue("name"), equalTo("Big Ears"));
  }

  @Test
  public void lookupValueTest_fromEnvironment() {
    System.setProperty("test", "yeah");
    assertThat(provider.lookupValue("test"), equalTo("yeah"));
  }

  @Test
  public void lookupValueTest_notDefined() {
    System.clearProperty("test");
    assertThat(provider.lookupValue("test"), nullValue());
  }
}
