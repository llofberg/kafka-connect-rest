package com.tm.kafka.connect.rest.util;

import java.util.HashMap;
import java.util.Map;

public class StringToMap {

  public static Map<String, Object> update(String path, String update) {
    HashMap<String, Object> map = new HashMap<>();
    update(path.split("\\."), 0, map, update);
    return map;
  }

  public static void update(String path, String update, Map<String, Object> map) {
    update(path.split("\\."), 0, map, update);
  }

  private static void update(String[] strHierarchy, int idx, Map<String, Object> map, String update) {
    String key = strHierarchy[idx];

    if (idx == strHierarchy.length - 1) {
      map.put(key, update);
      return;
    }

    Map<String, Object> embedded;
    if (map.containsKey(key) && map.get(key) instanceof Map) {
      embedded = (Map<String, Object>) map.get(key);
    } else {
      embedded = new HashMap<>();
      map.put(key, embedded);
    }

    update(strHierarchy, ++idx, embedded, update);
  }

  public static String extract(String path, Map<String, Object> map) {
    return extract(path.split("\\."), 0, map);
  }

  private static String extract(String[] strHierarchy, int idx, Map<String, Object> map) {
    String key = strHierarchy[idx];

    if (idx == strHierarchy.length - 1) {
      return map.getOrDefault(key, "").toString();
    }

    if (map.containsKey(key) && map.get(key) instanceof Map) {
      Map<String, Object> embedded = (Map<String, Object>) map.get(key);
      return extract(strHierarchy, ++idx, embedded);
    } else {
      return "";
    }
  }

  public static void remove(String path, Map<String, Object> map) {
    remove(path.split("\\."), 0, map);
  }

  private static void remove(String[] strHierarchy, int idx, Map<String, Object> map) {
    String key = strHierarchy[idx];

    if (idx == strHierarchy.length - 1) {
      map.remove(key);
    }

    if (map.containsKey(key) && map.get(key) instanceof Map) {
      Map<String, Object> embedded = (Map<String, Object>) map.get(key);
      remove(strHierarchy, ++idx, embedded);
    }
  }
}
