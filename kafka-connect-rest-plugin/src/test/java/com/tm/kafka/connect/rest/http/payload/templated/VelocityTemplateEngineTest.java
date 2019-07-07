package com.tm.kafka.connect.rest.http.payload.templated;


import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class VelocityTemplateEngineTest {

  ValueProvider valProvider = mock(ValueProvider.class);

  VelocityTemplateEngine engine = new VelocityTemplateEngine();

  @Test
  public void renderTemplate() {
    when(valProvider.lookupValue("name")).thenReturn("Noddy");
    assertThat(engine.renderTemplate("Hello ${name}", valProvider), equalTo("Hello Noddy"));
  }

  @Test
  public void renderTemplate_undefinedValue() {
    assertThat(engine.renderTemplate("Hello ${name}", valProvider), equalTo("Hello ${name}"));
  }
}
