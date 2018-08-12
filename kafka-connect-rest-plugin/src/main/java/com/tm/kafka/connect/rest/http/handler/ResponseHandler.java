package com.tm.kafka.connect.rest.http.handler;

import com.tm.kafka.connect.rest.ExecutionContext;
import com.tm.kafka.connect.rest.http.Response;

import java.util.List;

public interface ResponseHandler {

  List<String> handle(Response response, ExecutionContext ctx);
}
