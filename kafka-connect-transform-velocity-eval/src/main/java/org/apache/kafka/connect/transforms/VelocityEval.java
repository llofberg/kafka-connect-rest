package org.apache.kafka.connect.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

public abstract class VelocityEval<R extends ConnectRecord<R>> implements Transformation<R> {

  private static Logger log = LoggerFactory.getLogger(VelocityEval.class);

  private VelocityContext globalContext;
  private String template;

  private ObjectMapper objectMapper = new ObjectMapper();

  private static final String TEMPLATE_CONFIG = "template";
  private static final String TEMPLATE_DOC = "Velocity template.";

  private static final String CONTEXT_CONFIG = "context";
  private static final String CONTEXT_DOC = "JSON string of key value pairs added to Velocity context. " +
    "E.g. '{\"key1\":\"val1\",\"key2\":2}";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(CONTEXT_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, CONTEXT_DOC)
    .define(TEMPLATE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, TEMPLATE_DOC);

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

    // Workaround Velocity classloader issue
    Thread thread = Thread.currentThread();
    ClassLoader loader = thread.getContextClassLoader();
    thread.setContextClassLoader(this.getClass().getClassLoader());
    try {
      Velocity.init();
    } finally {
      thread.setContextClassLoader(loader);
    }

    globalContext = new VelocityContext();
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = objectMapper.readValue(config.getString(CONTEXT_CONFIG), Map.class);
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        globalContext.put(entry.getKey(), entry.getValue());
      }
    } catch (IOException e) {
      throw new ConfigException("", e);
    }
    template = config.getString(TEMPLATE_CONFIG);
  }

  @Override
  public R apply(R record) {
    StringWriter sw = new StringWriter();

    VelocityContext context = new VelocityContext(globalContext);

    context.put("r", record);
    context.put("topic", record.topic());
    context.put("partition", record.kafkaPartition());
    context.put("key", record.key());
    context.put("timestamp", record.timestamp() == null ? 0 : record.timestamp());
    context.put("schema", operatingSchema(record));
    context.put("value", operatingValue(record));

    Velocity.evaluate(context, sw, "", template);
    return newRecord(record, Schema.STRING_SCHEMA, sw.toString());
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

  public static class Key<R extends ConnectRecord<R>> extends VelocityEval<R> {
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

  public static class Value<R extends ConnectRecord<R>> extends VelocityEval<R> {
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
