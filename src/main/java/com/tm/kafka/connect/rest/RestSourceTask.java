package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.converter.PayloadToSourceRecordConverter;
import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import com.tm.kafka.connect.rest.http.executor.RequestExecutor;
import com.tm.kafka.connect.rest.http.handler.ResponseHandler;
import com.tm.kafka.connect.rest.http.payload.Payload;
import com.tm.kafka.connect.rest.http.payload.StringPayload;
import com.tm.kafka.connect.rest.http.transformer.RequestTransformer;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.tm.kafka.connect.rest.metrics.Metrics.ERROR_METRIC;
import static com.tm.kafka.connect.rest.metrics.Metrics.increaseCounter;

public class RestSourceTask extends SourceTask {

  private static Logger log = LoggerFactory.getLogger(RestSourceTask.class);

  private Long pollInterval;
  private String method;
  private String data;
  private PayloadToSourceRecordConverter converter;

  private Long lastPollTime = 0L;
  private String taskName;
  private RequestExecutor executor;
  private RequestTransformer transformer;
  private Request.RequestFactory requestFactory;
  private ResponseHandler responseHandler;

  @Override
  public void start(Map<String, String> map) {
    RestSourceConnectorConfig connectorConfig = new RestSourceConnectorConfig(map);

    taskName = map.getOrDefault("name", "unknown");

    pollInterval = connectorConfig.getPollInterval();
    method = connectorConfig.getMethod();
    data = connectorConfig.getData();

    converter = connectorConfig.getPayloadToSourceRecordConverter();
    converter.start(connectorConfig);

    requestFactory = new Request.RequestFactory(connectorConfig.getUrl(), connectorConfig.getRequestProperties());
    responseHandler = connectorConfig.getResponseHandler();
    executor = connectorConfig.getRequestExecutor();
    transformer = connectorConfig.getRequestTransformer();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    long millis = pollInterval - (System.currentTimeMillis() - lastPollTime);
    if (millis > 0) {
      Thread.sleep(millis);
    }

    ExecutionContext ctx = ExecutionContext.create(taskName);

    try {

      Payload payload = new StringPayload(data);
      Request request = requestFactory.createRequest(payload, method);

      transformer.transform(request);

      if (log.isTraceEnabled()) {
        log.trace("{} request to: {} with payload: {}", request.getMethod(), request.getUrl(), request.getPayload());
      }

      Response response = executor.execute(request);

      if (log.isTraceEnabled()) {
        log.trace("Response: {}, Request: {}", response, request);
      }

      responseHandler.handle(response, ctx);

      return converter.convert(
        Optional.ofNullable(response.getPayload())
          .map(Payload::asString)
          .map(String::getBytes)
          .get()
      );
    } catch (Exception e) {
      log.error("HTTP call execution failed " + e.getMessage(), e);
      increaseCounter(ERROR_METRIC, ctx);
    } finally {
      lastPollTime = System.currentTimeMillis();
    }

    return Collections.emptyList();
  }

  @Override
  public void stop() {
    log.debug("Stopping source task");
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }
}
