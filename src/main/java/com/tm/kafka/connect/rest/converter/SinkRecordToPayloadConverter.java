package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;

public interface SinkRecordToPayloadConverter {
  String convert(final SinkRecord record) throws Exception;

  void start(RestSinkConnectorConfig config);
}
