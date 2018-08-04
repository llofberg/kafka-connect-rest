package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import com.tm.kafka.connect.rest.RestSourceConnectorConfig;
import com.tm.kafka.connect.rest.converter.sink.SinkStringPayloadConverter;
import com.tm.kafka.connect.rest.converter.source.SourceStringPayloadConverter;
import com.tm.kafka.connect.rest.http.payload.StringPayload;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * should be splited into SinkStringPayloadConverter and SourceStringPayloadConverter in future
 * Please use specific converters in com.tm.kafka.connect.rest.converter.sink
 */

@Deprecated
public class StringPayloadConverter
    implements SinkRecordToPayloadConverter, PayloadToSourceRecordConverter {

  private SinkStringPayloadConverter sinkConverter;
  private SourceStringPayloadConverter sourceConverter;

  public StringPayloadConverter() {
    sinkConverter = new SinkStringPayloadConverter();
    sourceConverter = new SourceStringPayloadConverter();
  }

  public StringPayload convert(SinkRecord record) {
    return sinkConverter.convert(record);
  }

  public List<SourceRecord> convert(byte[] bytes) {
    return sourceConverter.convert(bytes);
  }

  @Override
  public void start(RestSourceConnectorConfig config) {
    sourceConverter.start(config);
  }

  @Override
  public void start(RestSinkConnectorConfig config) {
    sinkConverter.start(config);
  }
}
