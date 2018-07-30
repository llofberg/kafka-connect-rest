package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.converter.SinkRecordToPayloadConverter;
import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import com.tm.kafka.connect.rest.http.executor.RequestExecutor;
import com.tm.kafka.connect.rest.http.handler.ResponseHandler;
import com.tm.kafka.connect.rest.http.payload.Payload;
import com.tm.kafka.connect.rest.http.transformer.RequestTransformer;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import static com.tm.kafka.connect.rest.metrics.Metrics.RETRIABLE_ERROR_METRIC;
import static com.tm.kafka.connect.rest.metrics.Metrics.UNRETRIABLE_ERROR_METRIC;
import static com.tm.kafka.connect.rest.metrics.Metrics.increaseCounter;

public class RestSinkTask extends SinkTask {

  private static Logger log = LoggerFactory.getLogger(RestSinkTask.class);

  private String method;
  private Long retryBackoff;
  private Integer maxRetries;
  private SinkRecordToPayloadConverter converter;
  private Request.RequestFactory requestFactory;
  private RequestTransformer transformer;
  private RequestExecutor executor;
  private ResponseHandler responseHandler;
  private String taskName = "";
  private Map<String, String> map;

  @Override
  public void start(Map<String, String> map) {
    RestSinkConnectorConfig connectorConfig = new RestSinkConnectorConfig(map);
    taskName = map.getOrDefault("name", "unknown");

    requestFactory = new Request.RequestFactory(connectorConfig.getUrl(), connectorConfig.getRequestProperties());

    retryBackoff = connectorConfig.getRetryBackoff();
    maxRetries = connectorConfig.getMaxRetries();

    method = connectorConfig.getMethod();

    converter = connectorConfig.getSinkRecordToPayloadConverter();
    converter.start(connectorConfig);

    responseHandler = connectorConfig.getResponseHandler();
    transformer = connectorConfig.getRequestTransformer();
    executor = connectorConfig.getRequestExecutor();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      ExecutionContext ctx = ExecutionContext.create(taskName);
      int retries = maxRetries;
      while (maxRetries < 0 || retries-- >= 0) {
        try {
          Payload payload = converter.convert(record);
          Request request = requestFactory.createRequest(payload, method);
          transformer.transform(request);

          if (log.isTraceEnabled()) {
            log.trace("Request to: {}, Offset: {}", request.getUrl(), record.kafkaOffset());
          }

          Response response = executor.execute(request);

          if (log.isTraceEnabled()) {
            log.trace("Response: {}, Request: {}", response, request);
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
