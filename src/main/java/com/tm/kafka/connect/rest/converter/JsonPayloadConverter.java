package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;

public class JsonPayloadConverter implements SinkRecordToPayloadConverter {
  private Logger log = LoggerFactory.getLogger(JsonPayloadConverter.class);

  @Override
  public String convert(SinkRecord record) throws Exception {
    Object payload = record.value();

    if (payload instanceof Struct) {
      String result = createJSON((Struct) payload);
      return result;
    }
    else {
      throw new RuntimeException("Cannot convert object of type " + payload.getClass().getName() + " to JSON");
    }
  }

  private String createJSON(Struct payload) {
    Writer writer = new StringWriter();
    Schema schema = payload.schema();

    try(JsonGenerator generator = Json.createGenerator(writer)) {
      writeStructToJSON(generator, null, payload);
    }

    return writer.toString();
  }

  private void writeStructToJSON(JsonGenerator generator, String structName, Struct struct) {
    if (structName != null)
      generator.writeStartObject(structName);
    else
      generator.writeStartObject();

    Schema schema = struct.schema();

    for (Field field : schema.fields()) {
      String name = field.name();
      Schema fieldSchema = field.schema();
      Schema.Type type = fieldSchema.type();
      if (type.isPrimitive()) {
        generator.write(name, struct.get(field).toString());
      } else if (type == Schema.Type.STRUCT) {
        writeStructToJSON(generator, name, struct.getStruct(name));
      } else if (type == Schema.Type.ARRAY) {
        writeArrayToJSON(generator, name, struct.getArray(name));
      } else if (type == Schema.Type.MAP) {
        writeMapToJSON(generator, name, struct.getMap(name));
      }

    }
    generator.writeEnd();
  }

  private void writeArrayToJSON(JsonGenerator generator, String arrayName, List<?> array) {
    generator.writeStartArray(arrayName);

    // TODO: implement arrays

    generator.writeEnd();
  }

  private void writeMapToJSON(JsonGenerator generator, String mapName, Map<?,?> map) {
    generator.writeStartObject(mapName);

    // TODO: implement map as an object

    generator.writeEnd();
  }

  @Override
  public void start(RestSinkConnectorConfig config) {
  }

  public static void main(String[] args) {
    Object provider = JsonPayloadConverter.class;

    if (provider != null && provider instanceof Class
      && SinkRecordToPayloadConverter.class.isAssignableFrom((Class<?>) provider)) {
      System.out.println("Passed");
    }
    else {
      System.out.println("FAILED!!");
    }
  }
}
