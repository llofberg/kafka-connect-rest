package com.tm.kafka.connect.rest.http.executor;

import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;

/**
 * Implementation class should declare constructor with HttpProperties as an argument
 */
public interface RequestExecutor {

  Response execute(Request request) throws Exception;

}
