package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import com.tm.kafka.connect.rest.RestSourceConnectorConfig;
import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import com.tm.kafka.connect.rest.http.payload.ConstantPayloadGenerator;
import com.tm.kafka.connect.rest.http.payload.ConstantPayloadGeneratorConfig;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.currentTimeMillis;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;


public class TemplatedPayloadGeneratorTest {

  private static String REQUEST_BODY_VAL = "{\"query\": \"select * from known_stars where name='${STAR_NAME}'\"";

  private static final Map<String, String> REQUEST_PARAM_VAL = Stream.of(new String[][] {
    { "priority", "MAXIMUM" },
    { "paged", "FALSE" },
  }).collect(Collectors.toMap(d -> d[0], d -> d[1]));

  private static final Map<String, String> REQUEST_HEADER_VAL = Stream.of(new String[][] {
    { "Content-Type", "application/json" },
    { "Accept", "application/json" },
  }).collect(Collectors.toMap(d -> d[0], d -> d[1]));

  private static final Map<String, String> CONFIG_PROPS = Stream.of(new String[][] {
    { TemplatedPayloadGeneratorConfig.REQUEST_BODY_TEMPLATE_CONFIG, REQUEST_BODY_VAL },
    { TemplatedPayloadGeneratorConfig.REQUEST_HEADERS_TEMPLATE_CONFIG, "Content-Type:${CONTENT_TYPE}, Accept:${ACCEPT_TYPE}" },
    { TemplatedPayloadGeneratorConfig.REQUEST_PARAMETER_NAMES_CONFIG, "priority, paged" },
    { String.format(TemplatedPayloadGeneratorConfig.REQUEST_PARAMETER_TEMPLATE_CONFIG, "priority"), "${PRIORITY}" },
    { String.format(TemplatedPayloadGeneratorConfig.REQUEST_PARAMETER_TEMPLATE_CONFIG, "paged"), "${PAGED}" },
  }).collect(Collectors.toMap(d -> d[0], d -> d[1]));

  private static final Request REQUEST = new Request("http://data.stars.org/query", "POST",
    REQUEST_BODY_VAL, REQUEST_PARAM_VAL, REQUEST_HEADER_VAL);

  private static final Response RESPONSE = new Response(200, Collections.emptyMap(), "LOTS of data");

  private TemplatedPayloadGenerator generator;

  @Before
  public void before() {
    generator = new TemplatedPayloadGenerator();
  }


  @Test
  public void testUpdate() {
    generator.configure(CONFIG_PROPS);
    assertThat(generator.update(REQUEST, RESPONSE), Matchers.equalTo(false));
  }

  @Test
  public void testGetRequestBody() {
    System.setProperty("STAR_NAME", "Sol");
    generator.configure(CONFIG_PROPS);
    assertThat(generator.getRequestBody(), Matchers.equalTo("{\"query\": \"select * from known_stars where name='Sol'\""));
  }

  @Test
  public void testGetRequestBody_configUndefined() {
    generator.configure(Collections.emptyMap());
    assertThat(generator.getRequestBody(), Matchers.equalTo(""));
  }

  @Test
  public void testGetRequestParameters() {
    System.setProperty("PRIORITY", "MAXIMUM");
    System.setProperty("PAGED", "FALSE");
    generator.configure(CONFIG_PROPS);
    assertThat(generator.getRequestParameters(), allOf(
      hasEntry("priority", "MAXIMUM"),
      hasEntry("paged", "FALSE")));
  }

  @Test
  public void testGetRequestParameters_configUndefined() {
    generator.configure(Collections.emptyMap());
    assertThat(generator.getRequestParameters(), not(hasKey(anything())));
  }

  @Test
  public void testGetRequestHeaders() {
    System.setProperty("CONTENT_TYPE", "application/json");
    System.setProperty("ACCEPT_TYPE", "application/json");
    generator.configure(CONFIG_PROPS);
    assertThat(generator.getRequestHeaders(), allOf(
      hasEntry("Content-Type", "application/json"),
      hasEntry("Accept", "application/json")));
  }

  @Test
  public void testGetRequestHeaders_configUndefined() {
    generator.configure(Collections.emptyMap());
    assertThat(generator.getRequestHeaders(), not(hasKey(anything())));
  }

  @Test
  public void testGetOffsets() {
    System.setProperty("STAR_NAME", "Vega");
    System.setProperty("PRIORITY", "MINIMUM");
    System.setProperty("PAGED", "TRUE");
    System.setProperty("CONTENT_TYPE", "text/plain");
    System.setProperty("ACCEPT_TYPE", "text/plain");
    generator.configure(CONFIG_PROPS);
    assertThat(generator.getOffsets(), allOf(
      hasEntry("STAR_NAME", "Vega"),
      hasEntry("PRIORITY", "MINIMUM"),
      hasEntry("PAGED", "TRUE"),
      hasEntry("CONTENT_TYPE", "text/plain"),
      hasEntry("ACCEPT_TYPE", "text/plain")));
  }

  @Test
  public void testGetOffsets_configUndefined() {
    generator.configure(Collections.emptyMap());
    assertThat(generator.getOffsets(), not(hasKey(anything())));
  }

  @Test
  public void testSetOffsets() {
    generator.configure(CONFIG_PROPS);
    generator.setOffsets(Collections.singletonMap("STAR_NAME", "Pollux"));
    assertThat(generator.getOffsets(), hasEntry("STAR_NAME", "Pollux"));
    assertThat(generator.getRequestBody(), Matchers.equalTo("{\"query\": \"select * from known_stars where name='Pollux'\""));
  }

  @Test
  public void testSetOffsets_configUndefined() {
    generator.configure(Collections.emptyMap());
    generator.setOffsets(Collections.singletonMap("STAR_NAME", "Pollux"));
    assertThat(generator.getOffsets(), hasEntry("STAR_NAME", "Pollux"));
    assertThat(generator.getRequestBody(), Matchers.equalTo(""));
  }
}
