package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public interface PayloadToSourceRecordConverter {
  List<SourceRecord> convert(final byte[] bytes) throws Exception;

  void start(RestSourceConnectorConfig config);
}
