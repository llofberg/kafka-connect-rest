package com.tm.kafka.connect.rest.http.handler;

import com.tm.kafka.connect.rest.ExecutionContext;
import com.tm.kafka.connect.rest.http.Response;
import com.tm.kafka.connect.rest.metrics.Metrics;
import org.apache.kafka.connect.errors.RetriableException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DefaultResponseHandler implements ResponseHandler {

  private final Pattern allowedCodes;
  private final Pattern forbiddenCodes;

  public DefaultResponseHandler() {
    this(null, null);
  }

  public DefaultResponseHandler(String whitelist, String blacklist) {
    this.allowedCodes = createPattern(whitelist);
    this.forbiddenCodes = createPattern(blacklist);
  }

  @Override
  public List<String> handle(Response response, ExecutionContext ctx) {

    String code = String.valueOf(response.getStatusCode());

    Metrics.increaseCounter(translateHttpCodeToMetricName(code), ctx);

    if (allowedCodes != null) {
      checkCodeIsAllowed(code);
    }

    if (forbiddenCodes != null) {
      checkCodeIsForbidden(code);
    }

    ArrayList<String> records = new ArrayList<>();
    records.add(response.getPayload());
    return records;
  }

  private String translateHttpCodeToMetricName(String code) {
    return String.format("response_code_%sXX", code.substring(0, 1));
  }

  private void checkCodeIsAllowed(String code) {
    Matcher allowed = allowedCodes.matcher(code);
    if (!allowed.find()) {
      throw new RetriableException("HTTP Response code is not whitelisted " + code);
    }
  }

  private void checkCodeIsForbidden(String code) {
    Matcher forbidden = forbiddenCodes.matcher(code);
    if (forbidden.find()) {
      throw new RetriableException("HTTP Response code is in blacklist " + code);
    }
  }

  private Pattern createPattern(String regex) {
    if (regex == null || regex.trim().isEmpty()) {
      return null;
    }
    return Pattern.compile(regex);
  }
}
