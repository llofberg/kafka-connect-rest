package com.tm.kafka.connect.rest;

import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.Response;
import com.tm.kafka.connect.rest.http.executor.RequestExecutor;
import com.tm.kafka.connect.rest.http.handler.ResponseHandler;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RestSinkTaskTest {

  @Mock
  Request.RequestFactory requestFactory;

  @Mock
  RequestExecutor executor;

  @Mock
  ResponseHandler responseHandler;

  @Mock
  SinkRecord sinkRecord;

  @Mock
  RestSinkConnectorConfig config;

  @InjectMocks
  RestSinkTask subject;

  @Before
  public void setUp() throws Exception {
    when(requestFactory.createRequest(any())).thenReturn(mock(Request.class));
    when(executor.execute(any())).thenReturn(mock(Response.class));
  }

  @Test
  public void shouldRetryInfinitelyWhenMaxRetriesIsNegative() throws Exception {
    when(executor.execute(any()))
      .thenThrow(new RetriableException("Test"))
      .thenThrow(new RetriableException("Test"))
      .thenThrow(new RetriableException("Test"))
      .thenThrow(new RetriableException("Test"))
      .thenThrow(new RetriableException("Test"))
      .thenReturn(mock(Response.class));

    subject.setMaxRetries(-1);
    subject.put(Arrays.asList(sinkRecord));

    verify(executor, times(6)).execute(any());
  }

  @Test
  public void shouldRetryOnErrorMaxRetriesTimes() throws Exception {
    when(executor.execute(any()))
      .thenThrow(new RetriableException("Test"))
      .thenThrow(new RetriableException("Test"))
      .thenThrow(new RetriableException("Test"))
      .thenThrow(new RetriableException("Test"))
      .thenThrow(new RetriableException("Test"))
      .thenThrow(new RetriableException("Test"))
      .thenThrow(new RetriableException("Test"))
      .thenReturn(mock(Response.class));

    subject.setMaxRetries(3);
    subject.put(Arrays.asList(sinkRecord));

    verify(executor, times(4)).execute(any());
  }

}
