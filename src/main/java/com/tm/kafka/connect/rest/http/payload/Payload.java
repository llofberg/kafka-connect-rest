package com.tm.kafka.connect.rest.http.payload;

public abstract class Payload<T> {

  T payload;

  public Payload(T payload) {
    this.payload = payload;
  }

  public T get() {
    return payload;
  }

  public abstract String asString();
}
