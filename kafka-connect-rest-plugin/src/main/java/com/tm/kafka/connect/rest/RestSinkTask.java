package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import com.tm.kafka.connect.rest.http.executor.RequestExecutor;
import com.tm.kafka.connect.rest.http.handler.ResponseHandler;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import static com.tm.kafka.connect.rest.metrics.Metrics.*;

public class RestSinkTask extends SinkTask {

  private static Logger log = LoggerFactory.getLogger(RestSinkTask.class);

  private String method;
  private Long retryBackoff;
  private Integer maxRetries;
  private Request.RequestFactory requestFactory;
  private RequestExecutor executor;
  private ResponseHandler responseHandler;
  private String taskName = "";

  @Override
  public void start(Map<String, String> map) {
    RestSinkConnectorConfig connectorConfig = new RestSinkConnectorConfig(map);
    taskName = map.getOrDefault("name", "unknown");
    requestFactory = new Request.RequestFactory(connectorConfig.getUrl(), connectorConfig.getRequestProperties());
    retryBackoff = connectorConfig.getRetryBackoff();
    maxRetries = connectorConfig.getMaxRetries();
    method = connectorConfig.getMethod();
    responseHandler = connectorConfig.getResponseHandler();
    executor = connectorConfig.getRequestExecutor();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      ExecutionContext ctx = ExecutionContext.create(taskName);
      int retries = maxRetries;
      while (maxRetries < 0 || retries-- >= 0) {
        try {
          String payload = (String) record.value();
          Request request = requestFactory.createRequest(payload, method);

          Map<String, String> headers = request.getHeaders();
          if(record.headers()!=null) {
            for (Header header : record.headers()) {
              headers.put(header.key(), String.valueOf(header.value()));
            }
          }
          if (log.isTraceEnabled()) {
            log.info("Request to: {}, Offset: {}", request.getUrl(), record.kafkaOffset());
          }

          Response response = executor.execute(request);

          if (log.isTraceEnabled()) {
            log.info("Response: {}, Request: {}", response, request);
          }

          responseHandler.handle(response, ctx);

          break;
        } catch (RetriableException e) {
          log.error("HTTP call failed", e);
          increaseCounter(RETRIABLE_ERROR_METRIC, ctx);
          try {
            Thread.sleep(retryBackoff);
            log.error("Retrying");
          } catch (Exception ignored) {
            // Ignored
          }
        } catch (Exception e) {
          log.error("HTTP call execution failed " + e.getMessage(), e);
          increaseCounter(UNRETRIABLE_ERROR_METRIC, ctx);
          break;
        }
      }
    }
  }

  @Override
  public void stop() {
    log.debug("Stopping sink task, setting client to null");
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  void setRetryBackoff(long backoff) {
    this.retryBackoff = backoff;
  }

  void setMaxRetries(int retries) {
    this.maxRetries = retries;
  }
}
