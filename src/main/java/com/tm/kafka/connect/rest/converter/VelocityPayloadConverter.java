package com.tm.kafka.connect.rest.converter;

import com.tm.kafka.connect.rest.RestSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.velocity.VelocityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VelocityPayloadConverter
  implements SinkRecordToPayloadConverter {
  private Logger log = LoggerFactory.getLogger(VelocityPayloadConverter.class);

  @Override
  public String convert(SinkRecord record) throws Exception {
    return null;
  }

  @Override
  public void start(RestSinkConnectorConfig config) {
    VelocityContext context = new VelocityContext();
  }
//
//  ///private VelocityContext globalContext;
//  //private Template template;
//
//  private PebbleEngine engine;
//  private PebbleTemplate template;
//
//  // Convert to a String for outgoing REST calls
//  public String convert(SinkRecord record) {
//    StringWriter sw = new StringWriter();
//
//    Map<String, Object> context = new HashMap<>();
//    context.put("websiteTitle", "My First Website");
//    context.put("content", "My Interesting Content");
//    context.put("r", record);
//
//    try {
//      template.evaluate(sw, context);
//    } catch (PebbleException | IOException e) {
//      log.error("Failed to evaluate context", e);
//    }
//
//    //VelocityContext context = new VelocityContext(globalContext);
//    //context.put("r", record);
//    //template.merge(context, sw);
//    //Velocity.evaluate(context, sw, "", "$r.topic $r");
//    return sw.toString();
//  }
//
//  @Override
//  public void start(RestSinkConnectorConfig config) {
//    engine = new PebbleEngine.Builder().build();
//    try {
//      template = engine.getTemplate("/jars/msg.pb");
//    } catch (PebbleException e) {
//      log.error("Failed to load template", e);
//    }
//    //Velocity.init();
//    //globalContext = new VelocityContext();
//    //template = Velocity.getTemplate("mytemplate.vm");
//  }
}
