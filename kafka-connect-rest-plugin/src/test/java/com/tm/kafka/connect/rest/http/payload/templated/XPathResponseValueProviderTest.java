package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class XPathResponseValueProviderTest {

  Request request = mock(Request.class);
  Response response = mock(Response.class);

  XPathResponseValueProvider provider = new XPathResponseValueProvider();

  @Test
  public void extractValuesTest_oneMatch() {
    provider.setExpressions(Collections.singletonMap("name", "/greeting/name"));
    when(response.getPayload()).thenReturn("<greeting><hail>Hello</hail><name>Big Ears</name></greeting>");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), hasEntry("name", "Big Ears"));
  }

  @Test
  public void extractValuesTest_multipleMatches() {
    provider.setExpressions(Collections.singletonMap("name", "/greeting/name"));
    when(response.getPayload()).thenReturn("<greeting><hail>Hello</hail><name>Big Ears</name><name>Noddy</name></greeting>");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), hasEntry("name", "Big Ears,Noddy"));
  }

  @Test
  public void extractValuesTest_noMatch() {
    provider.setExpressions(Collections.singletonMap("name", "/greeting/title"));
    when(response.getPayload()).thenReturn("<greeting><hail>Hello</hail><name>Big Ears</name></greeting>");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), hasEntry("name", null));
  }

  @Test
  public void extractValuesTest_illegalXPath() {
    provider.setExpressions(Collections.singletonMap("name", "/[/]title"));
    when(response.getPayload()).thenReturn("<greeting><hail>Hello</hail><name>Big Ears</name></greeting>");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), not(hasKey(anything())));
  }

  @Test
  public void extractValuesTest_illegalXML() {
    provider.setExpressions(Collections.singletonMap("name", "/greeting/title"));
    when(response.getPayload()).thenReturn("<greeting<hailHello><");
    provider.extractValues(request, response);
    assertThat(provider.getParameters(), hasEntry("name", null));
  }

  @Test
  public void lookupValueTest_extracted() {
    provider.setExpressions(Collections.singletonMap("name", "/greeting/name"));
    when(response.getPayload()).thenReturn("<greeting><hail>Hello</hail><name>Big Ears</name></greeting>");
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
