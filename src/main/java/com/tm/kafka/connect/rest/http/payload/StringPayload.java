package com.tm.kafka.connect.rest.http.payload;

public class StringPayload extends Payload<String> {

  public StringPayload(String payload) {
    super(payload);
  }

  @Override
  public String asString() {
    return this.toString();
  }

  @Override
  public String toString() {
    return payload == null ? "" : payload;
  }
}
