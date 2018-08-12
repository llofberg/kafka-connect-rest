package com.tm.kafka.connect.rest.selector;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;

public interface TopicSelector {
  String getTopic(Object data);

  void start(RestSourceConnectorConfig config);
}
