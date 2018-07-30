package com.tm.kafka.connect.rest.http.handler;

import com.tm.kafka.connect.rest.ExecutionContext;
import com.tm.kafka.connect.rest.http.Response;

public interface ResponseHandler {

  void handle(Response response, ExecutionContext ctx);
}
