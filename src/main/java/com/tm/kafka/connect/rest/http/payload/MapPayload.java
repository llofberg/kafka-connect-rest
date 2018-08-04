package com.tm.kafka.connect.rest.http.payload;

import java.util.Map;

public class MapPayload extends Payload<Map> {

  public MapPayload(Map payload) {
    super(payload);
  }

  @Override
  public String asString() {
    return this.toString();
  }

  @Override
  public String toString() {
    return payload == null ? "null" : payload.toString();
  }
}
