package com.tm.kafka.connect.rest.config;

public interface HttpProperties {

  long getReadTimeout();

  long getConnectionTimeout();

  long getKeepAliveDuration();

  int getMaxIdleConnections();
}
