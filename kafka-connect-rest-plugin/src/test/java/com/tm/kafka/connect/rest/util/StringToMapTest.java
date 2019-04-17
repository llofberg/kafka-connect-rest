package com.tm.kafka.connect.rest.util;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StringToMapTest {

  @Test
  public void testUpdate() {
    Map<String, Object> converted = StringToMap.update("test.foo.bar", "value");

    Map<String, Object> level3 = new HashMap<>();
    level3.put("bar", "value");
    Map<String, Object> level2 = new HashMap<>();
    level2.put("foo", level3);
    Map<String, Object> expected = new HashMap<>();
    expected.put("test", level2);

    assertEquals(expected, converted);
  }


  @Test
  public void testExtract() {
    Map<String, Object> level3 = new HashMap<>();
    level3.put("bar", "value");
    Map<String, Object> level2 = new HashMap<>();
    level2.put("foo", level3);
    Map<String, Object> map = new HashMap<>();
    map.put("test", level2);

    String extracted = StringToMap.extract("test.foo.bar", map);

    assertEquals("value", extracted);
  }

  @Test
  public void testRemove() {
    Map<String, Object> level3 = new HashMap<>();
    level3.put("bar", "value");
    Map<String, Object> level2 = new HashMap<>();
    level2.put("foo", level3);
    Map<String, Object> map = new HashMap<>();
    map.put("test", level2);

    StringToMap.remove("test.foo.bar", map);

    Map<String, Object> level2Expected = new HashMap<>();
    level2Expected.put("foo", new HashMap<>());
    Map<String, Object> mapExpected = new HashMap<>();
    mapExpected.put("test", level2Expected);

    assertEquals(mapExpected, map);
  }

}
