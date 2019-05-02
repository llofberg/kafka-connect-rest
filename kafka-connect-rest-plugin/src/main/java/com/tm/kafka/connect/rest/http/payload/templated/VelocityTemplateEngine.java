package com.tm.kafka.connect.rest.http.payload.templated;


import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.AbstractContext;

import java.io.StringWriter;


/**
 * This template engine uses MessageFormat to convert a template into a series of outputs, based on a series of context
 * entries.
 */
public class VelocityTemplateEngine implements TemplateEngine {

  private VelocityContextAdapter contextAdapter = new VelocityContextAdapter();


  public VelocityTemplateEngine() {
    Velocity.init();
  }

  /**
   * Get a particular interpretation of the template based on the values in the given context.
   *
   * @param context The source from which values are taken.
   * @return A completed template where all appliccable placeholders have been replaced with values.
   */
  @Override
  public String renderTemplate(String template, ValueProvider context) {
    StringWriter writer = new StringWriter();
    contextAdapter.setValueProvider(context);
    Velocity.evaluate(contextAdapter, writer, "", template);
    return writer.toString();
  }


  /**
   * This is an adapter that allows a ValueProvider to be used as a Velocity Context object.
   */
  private static class VelocityContextAdapter extends AbstractContext {

    private ValueProvider context;


    public void setValueProvider(ValueProvider context) {
      this.context = context;
    }

    @Override
    public Object internalGet(String key) {
      return context.lookupValue(key);
    }

    @Override
    public Object internalPut(String s, Object o) {
      throw new UnsupportedOperationException("An attempt was made to alter to a read-only context");
    }

    @Override
    public boolean internalContainsKey(String key) {
      return context.lookupValue(key) != null;
    }

    @Override
    public String[] internalGetKeys() {
      return new String[0];
    }

    @Override
    public Object internalRemove(String s) {
      throw new UnsupportedOperationException("An attempt was made to alter to a read-only context");
    }
  }
}
