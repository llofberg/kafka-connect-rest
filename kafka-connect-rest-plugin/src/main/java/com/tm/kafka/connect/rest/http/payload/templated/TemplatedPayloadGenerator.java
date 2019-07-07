package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import com.tm.kafka.connect.rest.http.payload.PayloadGenerator;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * This is a payload generator that attempts to fill in the placeholders in a template using values from a
 * ValueProvider instance.
 */
public class TemplatedPayloadGenerator implements PayloadGenerator, Configurable {

  private static Logger log = LoggerFactory.getLogger(RegexResponseValueProvider.class);

  private String requestBodyTemplate;
  private Map<String, String> requestParameterTemplates;
  private Map<String, String> requestParameterValues;
  private String requestBodyValue;
  private Map<String, String> requestHeaderTemplates;
  private Map<String, String> requestHeaderValues;
  private ValueProvider valueProvider;
  private TemplateEngine templateEngine;


  @Override
  public void configure(Map<String, ?> props) {
    final TemplatedPayloadGeneratorConfig config = new TemplatedPayloadGeneratorConfig(props);

    requestBodyTemplate = config.getRequestBodyTemplate();
    requestParameterTemplates = config.getRequestParameterTemplates();
    requestHeaderTemplates = config.getRequestHeaderTemplates();

    requestParameterValues = new HashMap<>(requestParameterTemplates.size());
    requestHeaderValues = new HashMap<>(requestHeaderTemplates.size());

    valueProvider = config.getValueProvider();
    templateEngine = config.getTemplateEngine();

    populateValues();
  }

  @Override
  public boolean update(Request request, Response response) {
    valueProvider.update(request,response);

    populateValues();

    // False = Wait for the next poll cycle before calling again.
    return false;
  }

  @Override
  public String getRequestBody() {
    return requestBodyValue;
  }

  @Override
  public Map<String, String> getRequestParameters() {
    return requestParameterValues;
  }

  @Override
  public Map<String, String> getRequestHeaders() {
    return requestHeaderValues;
  }

  @Override
  public Map<String, Object> getOffsets() {
    return valueProvider.getParameters();
  }

  @Override
  public void setOffsets(Map<String, Object> offsets) {
    valueProvider.setParameters(offsets);
    populateValues();
  }

  private void populateValues() {
    requestBodyValue = templateEngine.renderTemplate(requestBodyTemplate, valueProvider);
    requestParameterTemplates
      .forEach((k, v) -> requestParameterValues.put(k, templateEngine.renderTemplate(v, valueProvider)));
    requestHeaderTemplates
      .forEach((k, v) -> requestHeaderValues.put(k, templateEngine.renderTemplate(v, valueProvider)));

    log.info("Body to be sent: {}", requestBodyValue);
    log.info("Parameters to be sent: {}", Arrays.toString(requestParameterValues.entrySet().toArray()));
    log.info("Headers to be sent: {}", Arrays.toString(requestHeaderValues.entrySet().toArray()));
  }
}
