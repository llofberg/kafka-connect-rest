package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import com.tm.kafka.connect.rest.http.payload.Payload;
import org.apache.kafka.connect.sink.SinkRecord;

public interface SinkRecordToPayloadConverter {

  Payload<?> convert(final SinkRecord record) throws Exception;

  void start(RestSinkConnectorConfig config);
}
