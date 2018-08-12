package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.util.stream.Collectors;

public class AddHeaders<R extends ConnectRecord<R>> implements Transformation<R> {

  // TODO: Allow removing, modifying headers

  private static final String HEADERS_CONFIG = "headers";
  private static final String HEADERS_DOC = "List of headers.";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(HEADERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, HEADERS_DOC);

  private Map<String, String> headers;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    headers = config.getList(HEADERS_CONFIG)
      .stream()
      .map(s -> s.split(":", 2))
      .collect(Collectors.toMap(ss -> ss[0], ss -> ss[1]));
  }

  @Override
  public R apply(R record) {
    for (Map.Entry<String, String> header : headers.entrySet()) {
      record.headers().add(header.getKey(), header.getValue(), Schema.STRING_SCHEMA);
    }
    return record;
  }

  @Override
  public void close() {
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }
}
