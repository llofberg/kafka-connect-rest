package com.tm.kafka.connect.rest.config;

import com.tm.kafka.connect.rest.http.executor.OkHttpRequestExecutor;
import com.tm.kafka.connect.rest.http.executor.RequestExecutor;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.text.spi.DateFormatProvider;
import java.util.Collections;
import java.util.List;
import java.util.spi.LocaleServiceProvider;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ServiceProviderInterfaceRecommenderTest {

  @Test
  public void validValuesTest_actualSPI() {
    ServiceProviderInterfaceRecommender<RequestExecutor> recommender =
      new ServiceProviderInterfaceRecommender<>(RequestExecutor.class);
    assertThat(recommender.validValues("test", Collections.emptyMap()), hasItem(OkHttpRequestExecutor.class));
  }

  @Test
  public void validValuesTest_notSPI() {
    ServiceProviderInterfaceRecommender<ServiceProviderInterfaceRecommenderTest> recommender =
      new ServiceProviderInterfaceRecommender<>(ServiceProviderInterfaceRecommenderTest.class);
    assertThat(recommender.validValues("test", Collections.emptyMap()), emptyIterable());
  }

  @Test
  public void visible() {
    ServiceProviderInterfaceRecommender<RequestExecutor> recommender =
      new ServiceProviderInterfaceRecommender<>(RequestExecutor.class);
    assertThat(recommender.visible("test", Collections.emptyMap()), equalTo(true));
  }
}
