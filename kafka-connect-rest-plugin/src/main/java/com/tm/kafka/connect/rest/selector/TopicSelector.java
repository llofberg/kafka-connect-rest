package com.tm.kafka.connect.rest.selector;

import com.tm.kafka.connect.rest.RestSourceConnectorConfig;


/**
 * A class used to select which topic a source message should be sent to.
 *
 * Note that all implementations of this interface need to be added to
 * {@link com.tm.kafka.connect.rest.config.TopicSelectorRecommender TopicSelectorRecommender}
 * in order to be usable in config.
 */
public interface TopicSelector {
  String getTopic(Object data);

  void start(RestSourceConnectorConfig config);
}
