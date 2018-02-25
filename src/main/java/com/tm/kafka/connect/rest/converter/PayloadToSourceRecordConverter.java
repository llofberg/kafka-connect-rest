package com.tm.kafka.connect.rest.converter;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

public interface PayloadToSourceRecordConverter {
  List<SourceRecord> convert(final byte[] bytes) throws Exception;

  void start(Map<String, String> map);
}
