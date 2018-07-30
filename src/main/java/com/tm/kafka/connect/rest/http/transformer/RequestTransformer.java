package com.tm.kafka.connect.rest.http.transformer;

import com.tm.kafka.connect.rest.http.Request;

public interface RequestTransformer {

  void transform(Request request);
}
