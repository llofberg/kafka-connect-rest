package org.apache.kafka.connect.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import hello.Greeting;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class FromJson<R extends ConnectRecord<R>> implements Transformation<R> {

  private static Logger log = LoggerFactory.getLogger(FromJson.class);

  private static final String CLASS_CONFIG = "message.class";
  private static final String CLASS_DOC = "Java class for the JSON object.";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(CLASS_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, CLASS_DOC);

  private final ObjectMapper mapper = new ObjectMapper();
  private Map<?, ?> configs = new HashMap<>();
  private AvroData avroData = new AvroData(new AvroDataConfig(configs));
  private Class<?> clazz;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    try {
      clazz = Class.forName(config.getString(CLASS_CONFIG));
    } catch (ClassNotFoundException e) {
      throw new ConfigException(CLASS_CONFIG, e);
    }
  }

  @Override
  public R apply(R record) {
    try {
      Object updatedValue = mapper.readValue((String) operatingValue(record), clazz);
      SchemaAndValue s = avroData.toConnectData(Greeting.getClassSchema(), updatedValue);
      return newRecord(record, s.schema(), s.value());
    } catch (IOException e) {
      throw new DataException("", e);
    }
  }

  @Override
  public void close() {
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends FromJson<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(),
        updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends FromJson<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(),
        record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
  }
}
