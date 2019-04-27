package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import com.tm.kafka.connect.rest.http.executor.RequestExecutor;
import com.tm.kafka.connect.rest.http.handler.ResponseHandler;
import com.tm.kafka.connect.rest.http.payload.PayloadGenerator;
import com.tm.kafka.connect.rest.selector.TopicSelector;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.tm.kafka.connect.rest.metrics.Metrics.ERROR_METRIC;
import static com.tm.kafka.connect.rest.metrics.Metrics.increaseCounter;
import static java.lang.System.currentTimeMillis;

public class RestSourceTask extends SourceTask {

  private static Logger log = LoggerFactory.getLogger(RestSourceTask.class);

  private RestSourceConnectorConfig connectorConfig;
  private Long pollInterval;

  private Long lastPollTime = 0L;
  private String taskName;
  private RequestExecutor executor;
  private Request.RequestFactory requestFactory;
  private PayloadGenerator payloadGenerator;
  private ResponseHandler responseHandler;
  private TopicSelector topicSelector;
  private String url;

  @Override
  public void start(Map<String, String> properties) {
    log.info("Starting REST source task");
    try {
      connectorConfig = new RestSourceConnectorConfig(properties);
    } catch (ConfigException ex) {
      throw new ConnectException("Couldn't start RestSourceTask due to configuration error", ex);
    }

    taskName = properties.getOrDefault("name", "unknown");

    pollInterval = connectorConfig.getPollInterval();
    url = connectorConfig.getUrl();
    requestFactory = new Request.RequestFactory(url, connectorConfig.getMethod(),
      connectorConfig.getRequestHeaders());
    payloadGenerator = connectorConfig.getPayloadGenerator();
    responseHandler = connectorConfig.getResponseHandler();
    executor = connectorConfig.getRequestExecutor();
    topicSelector = connectorConfig.getTopicSelector();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    long millis = pollInterval - (currentTimeMillis() - lastPollTime);
    if (millis > 0) {
      Thread.sleep(millis);
    }

    ExecutionContext ctx = ExecutionContext.create(taskName);
    ArrayList<SourceRecord> records = new ArrayList<>();
    boolean makeAnotherRequest = true;

    try {
      while (makeAnotherRequest) {
        Request request = requestFactory.createRequest(payloadGenerator.getPayload());

        if (log.isTraceEnabled()) {
          log.trace("{} request to: {} with payload: {}", request.getMethod(), request.getUrl(), request.getPayload());
        }

        Response response = executor.execute(request);

        if (log.isTraceEnabled()) {
          log.trace("Response: {}, Request: {}", response, request);
        }

        Map<String, String> sourcePartition = Collections.singletonMap("URL", url);
        Map<String, Long> sourceOffset = Collections.singletonMap("timestamp", currentTimeMillis());
        for (String responseRecord : responseHandler.handle(response, ctx)) {
          SourceRecord sourceRecord = new SourceRecord(sourcePartition, sourceOffset,
            topicSelector.getTopic(responseRecord), Schema.STRING_SCHEMA, responseRecord);
          for (Map.Entry<String, List<String>> header : response.getHeaders().entrySet()) {
            sourceRecord.headers().add(header.getKey(), header.getValue(), SchemaBuilder.array(Schema.STRING_SCHEMA).build());
          }
          records.add(sourceRecord);
        }

        makeAnotherRequest = payloadGenerator.update(request, response);
      }
    } catch (Exception e) {
      log.error("HTTP call execution failed " + e.getMessage(), e);
      increaseCounter(ERROR_METRIC, ctx);
    } finally {
      lastPollTime = currentTimeMillis();
    }

    return records;
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
