package com.tm.kafka.connect.rest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;


@RunWith(PowerMockRunner.class)
@PrepareForTest({RestSinkConnectorConfig.class, VersionUtil.class})
public class RestSourceConnectorTest {

  @Mock
  private RestSourceConnectorConfig config;

  @InjectMocks
  private RestSourceConnector subject;

  @Test
  public void shouldReturnListOfConfigsOnStartup() {
    Map<String, String> props = new HashMap<>();
    props.put("key", "val");

    when(config.originalsStrings()).thenReturn(props);

    List<Map<String, String>> maps = subject.taskConfigs(3);

    assertEquals(maps.get(0), props);
    assertEquals(maps.get(1), props);
    assertEquals(maps.get(2), props);
  }

  @Test
  public void shouldReturnVersionWhenRequested() {
    PowerMockito.mockStatic(VersionUtil.class);
    when(VersionUtil.getVersion()).thenReturn("test");

    String version = subject.version();

    assertEquals("test", version);
  }
}
