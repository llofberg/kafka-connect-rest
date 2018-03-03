package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.converter.SinkRecordToPayloadConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Map;

public class RestSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(RestSinkTask.class);

  private String method;
  private Map<String, String> requestProperties;
  private String url;
  private SinkRecordToPayloadConverter converter;
  private Long retryBackoff;

  @Override
  public void start(Map<String, String> map) {
    RestSinkConnectorConfig connectorConfig = new RestSinkConnectorConfig(map);
    retryBackoff = connectorConfig.getRetryBackoff();
    method = connectorConfig.getMethod();
    requestProperties = connectorConfig.getRequestProperties();
    url = connectorConfig.getUrl();
    converter = connectorConfig.getSinkRecordToPayloadConverter();
    converter.start(connectorConfig);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      while (true) {
        try {
          String data = converter.convert(record);
          String u = url;
          if ("GET".equals(method)) {
            u = u + URLEncoder.encode(data, "UTF-8");
          }
          HttpURLConnection conn = (HttpURLConnection) new URL(u).openConnection();
          requestProperties.forEach(conn::setRequestProperty);
          conn.setRequestMethod(method);
          if ("POST".equals(method)) {
            conn.setDoOutput(true);
            OutputStream os = conn.getOutputStream();
            os.write(data.getBytes());
            os.flush();
          }
          int responseCode = conn.getResponseCode();
          if (log.isTraceEnabled()) {
            log.trace("Response code: {}, Request data: {}", responseCode, data);
          }
          break;
        } catch (Exception e) {
          log.error("HTTP call failed", e);
          try {
            Thread.sleep(retryBackoff);
          } catch (Exception ignored) {
            // Ignored
          }
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
}
