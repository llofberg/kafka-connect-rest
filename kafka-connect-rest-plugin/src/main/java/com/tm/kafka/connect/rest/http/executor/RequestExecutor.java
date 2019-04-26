package com.tm.kafka.connect.rest.http.executor;


import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;


/**
 * Implementation class should declare constructor with HttpProperties as an argument
 * <p>
 * Note: This is a Service Provider Interface (SPI)
 * All implementations should be listed in
 * META-INF/services/com.tm.kafka.connect.rest.http.executor.RequestExecutor
 */
public interface RequestExecutor {

  Response execute(Request request) throws Exception;

}
