package com.tm.kafka.connect.rest.errors;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DLQReporterTest {
  @Mock
  Producer<byte[], byte[]> producer;

  @Mock
  ConnectRecord record;

  DLQReporter reporter;

  @Before
  public void setUp() {
    reporter = new DLQReporter();
    reporter.setDlqTopic("test-topic");
    reporter.setDlqProducer(producer);
  }

  @Test
  public void populateHeadersAndSendToKafka() throws Exception {
    Exception e = new RetriableException("Test");
    ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
    reporter.reportError(record, e);
    verify(producer).send(captor.capture(), any());
    List<String> HEADERS_KEY = Arrays.asList(DLQReporter.ERROR_HEADER_EXCEPTION, DLQReporter.ERROR_HEADER_EXCEPTION_MESSAGE, DLQReporter.ERROR_HEADER_EXCEPTION_STACK_TRACE);
    ProducerRecord r = captor.getValue();
    Map<String, String> errHeaders = new HashMap<>();
    r.headers().forEach(header -> errHeaders.put(header.key(), new String(header.value())));
    HEADERS_KEY.forEach(key -> Assert.assertTrue(errHeaders.containsKey(key)));
    Assert.assertEquals("org.apache.kafka.connect.errors.RetriableException", errHeaders.get(DLQReporter.ERROR_HEADER_EXCEPTION));
    Assert.assertEquals("Test", errHeaders.get(DLQReporter.ERROR_HEADER_EXCEPTION_MESSAGE));
  }
}
