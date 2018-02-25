package com.tm.kafka.connect.rest.converter;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public interface SinkRecordToPayloadConverter {
  String convert(final SinkRecord record) throws Exception;

  void start(Map<String, String> map);
}
