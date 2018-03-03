/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.converter.PayloadToSourceRecordConverter;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RestSourceTask extends SourceTask {
  private static Logger log = LoggerFactory.getLogger(RestSourceTask.class);

  private Long pollInterval;
  private String method;
  private Map<String, String> requestProperties;
  private String url;
  private String data;
  private PayloadToSourceRecordConverter converter;

  private Long lastPollTime = 0L;

  @Override
  public void start(Map<String, String> map) {
    RestSourceConnectorConfig connectorConfig = new RestSourceConnectorConfig(map);
    pollInterval = connectorConfig.getPollInterval();
    method = connectorConfig.getMethod();
    requestProperties = connectorConfig.getRequestProperties();
    url = connectorConfig.getUrl();
    data = connectorConfig.getData();
    converter = connectorConfig.getPayloadToSourceRecordConverter();
    converter.start(connectorConfig);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    long millis = pollInterval - (System.currentTimeMillis() - lastPollTime);
    if (millis > 0) {
      Thread.sleep(millis);
    }
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
      requestProperties.forEach(conn::setRequestProperty);
      conn.setRequestMethod(method);
      if (data != null) {
        conn.setDoOutput(true);
        OutputStream os = conn.getOutputStream();
        os.write(data.getBytes());
        os.flush();
      }
      if (log.isTraceEnabled()) {
        log.trace("Response code: {}, Request data: {}", conn.getResponseCode(), data);
      }
      return converter.convert(IOUtils.toByteArray(conn.getInputStream()));
    } catch (Exception e) {
      log.error("REST source connector poll() failed", e);
      return Collections.emptyList();
    } finally {
      lastPollTime = System.currentTimeMillis();
    }
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
