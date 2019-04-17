package com.tm.kafka.connect.rest.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.tm.kafka.connect.rest.ExecutionContext;

public class Metrics {

  public static final String ERROR_METRIC = "error";
  public static final String RETRIABLE_ERROR_METRIC = "retriable_error";
  public static final String UNRETRIABLE_ERROR_METRIC = "unretriable_error";

  static final MetricRegistry metrics = new MetricRegistry();
  static final JmxReporter jmxReporter = JmxReporter.forRegistry(metrics).build();

  static {
    jmxReporter.start();
  }

  public static void increaseCounter(String name, ExecutionContext ctx) {
    metrics.counter(String.format("kafka_connect_rest_%s_%s", name, ctx.getTaskName())).inc();
    metrics.counter(String.format("kafka_connect_rest_%s", name)).inc();
  }
}
