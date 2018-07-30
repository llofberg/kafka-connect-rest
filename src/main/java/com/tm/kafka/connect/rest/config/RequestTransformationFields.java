package com.tm.kafka.connect.rest.config;

public interface RequestTransformationFields {

  String getPayloadAdditions();

  String getPayloadRemovals();

  String getPayloadReplacements();
}
