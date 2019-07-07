package com.tm.kafka.connect.rest.http.payload;


import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;


public class ConstantPayloadGeneratorTest {

  private static final Map<String, String> CONFIG_PROPS = Stream.of(new String[][] {
    { ConstantPayloadGeneratorConfig.REQUEST_BODY_CONFIG, "{\"query\": \"select * from known_stars\"" },
    { ConstantPayloadGeneratorConfig.REQUEST_HEADERS_CONFIG, "Content-Type:application/json, Accept:application/json" },
    { ConstantPayloadGeneratorConfig.REQUEST_PARAMETER_NAMES_CONFIG, "priority, paged" },
    { String.format(ConstantPayloadGeneratorConfig.REQUEST_PARAMETER_VALUE_CONFIG, "priority"), "MAXIMUM" },
    { String.format(ConstantPayloadGeneratorConfig.REQUEST_PARAMETER_VALUE_CONFIG, "paged"), "FALSE" },
  }).collect(Collectors.toMap(d -> d[0], d -> d[1]));

  private ConstantPayloadGenerator generator;


  @Before
  public void before() {
    generator = new ConstantPayloadGenerator();
  }


  @Test
  public void testUpdate() {
    generator.configure(CONFIG_PROPS);
    assertThat(generator.update(null, null), equalTo(false));
  }

  @Test
  public void testGetRequestBody() {
    generator.configure(CONFIG_PROPS);
    assertThat(generator.getRequestBody(), equalTo("{\"query\": \"select * from known_stars\""));
  }

  @Test
  public void testGetRequestBody_configUndefined() {
    generator.configure(Collections.emptyMap());
    assertThat(generator.getRequestBody(), equalTo(""));
  }

  @Test
  public void testGetRequestParameters() {
    generator.configure(CONFIG_PROPS);
    assertThat(generator.getRequestParameters(), allOf(hasEntry("priority", "MAXIMUM"), hasEntry("paged", "FALSE")));
  }

  @Test
  public void testGetRequestParameters_configUndefined() {
    generator.configure(Collections.emptyMap());
    assertThat(generator.getRequestParameters(), not(hasKey(anything())));
  }

  @Test
  public void testGetRequestHeaders() {
    generator.configure(CONFIG_PROPS);
    assertThat(generator.getRequestHeaders(), allOf(
      hasEntry("Content-Type", "application/json"), hasEntry("Accept", "application/json")));
  }

  @Test
  public void testGetRequestHeaders_configUndefined() {
    generator.configure(Collections.emptyMap());
    assertThat(generator.getRequestHeaders(), not(hasKey(anything())));
  }

  @Test
  public void testGetOffsets() {
    generator.configure(CONFIG_PROPS);
    assertThat(generator.getOffsets(), hasEntry(equalTo("timestamp"), instanceOf(Long.class)));
  }

  @Test
  public void testSetOffsets() {
    generator.configure(CONFIG_PROPS);
    generator.setOffsets(null);
    assertThat(generator.getOffsets(), notNullValue());
  }
}
