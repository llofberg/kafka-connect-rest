package com.tm.kafka.connect.rest.selector;


/**
 * A class used to select which topic a source message should be sent to.
 * <p>
 * Note: This is a Service Provider Interface (SPI)
 * All implementations should be listed in
 * META-INF/services/com.tm.kafka.connect.rest.http.payload.PayloadGenerator
 */
public interface TopicSelector {

  String getTopic(Object data);
}
