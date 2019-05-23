package com.tm.kafka.connect.rest.http.payload.templated;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.xpath.*;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;


/**
 * Lookup values used to populate dynamic payloads.
 * These values will be substituted into the payload template.
 *
 * This implementation uses XPath to extract values from an XML HTTP response,
 * and if not found looks them up in the System properties and then in environment variables.
 */
public class XPathResponseValueProvider extends EnvironmentValueProvider implements Configurable {

  private static Logger log = LoggerFactory.getLogger(XPathResponseValueProvider.class);

  public static final String MULTI_VALUE_SEPARATOR = ",";

  private static final XPathFactory X_PATH_FACTORY = XPathFactory.newInstance();

  private Map<String, XPathExpression> expressions;


  /**
   * Configure this instance after creation.
   *
   * @param props The configuration properties
   */
  @Override
  public void configure(Map<String, ?> props) {
    final XPathResponseValueProviderConfig config = new XPathResponseValueProviderConfig(props);
    setExpressions(config.getResponseVariableXPaths());
  }

  /**
   * Extract values from the response using the XPaths
   *
   * @param request The last request made.
   * @param response The last response received.
   */
  @Override
  protected void extractValues(Request request, Response response) {
    String resp = response.getPayload();
    expressions.forEach((key, expr) -> parameterMap.put(key, extractValue(key, resp, expr)));
  }

  /**
   * Set the XPaths to be used for value extraction.
   *
   * @param xPaths A map of key names to XPath expressions
   */
  protected void setExpressions(Map<String, String> xPaths) {
    expressions = new HashMap<>(xPaths.size());
    parameterMap = new HashMap<>(expressions.size());
    xPaths.forEach(this::addXPath);
  }

  /**
   * Extract the value for a given key.
   * Where the XPath yeilds more than one result a comma seperated list will be returned.
   *
   * @param key The name of the key
   * @param resp The response to extract a value from
   * @param expression The compiled XPath used to find the value
   * @return Return the value, or null if it wasn't found
   */
  private String extractValue(String key, String resp, XPathExpression expression) {
    InputSource inputXML = new InputSource(new StringReader(resp));
    StringBuilder values = new StringBuilder();
    try {
      NodeList nodes = (NodeList) expression.evaluate(inputXML, XPathConstants.NODESET);

      for(int i = 0; i < nodes.getLength(); i++) {
        if(values.length() > 0) {
          values.append(MULTI_VALUE_SEPARATOR);
        }
        Node node = nodes.item(i);
        values.append(node.getTextContent());
      }

      String value = (values.length() != 0) ? values.toString() : null;
      log.info("Variable {} was assigned the value {}", key, value);
      return value;
    } catch (XPathExpressionException ex) {
      log.error("The XPath expression '" + expression.toString() + "' could not be evaluated against: " + resp, ex);
      return null;
    } catch (DOMException ex) {
      log.error("The result(s) were too big when XPath expression '" + expression.toString()
        + "' was evaluated against: " + resp, ex);
      return null;
    }
  }

  private void addXPath(String key, String xPath) {
    try {
      expressions.put(key, X_PATH_FACTORY.newXPath().compile(xPath));
    } catch (XPathExpressionException ex) {
      log.error("The XPath expression '" + xPath + "' could not be compiled", ex);
    }
  }
}
