package com.tm.kafka.connect.rest.selector;

import java.util.Map;

public interface TopicSelector {
  String getTopic(Object data);

  void start(Map<String, String> map);
}
