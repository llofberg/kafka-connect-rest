package com.tm.kafka.connect.rest.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReplacementExtractor {

  private static final Pattern PATTERN = Pattern.compile("\\$\\{(.+?)\\}");

  private static Map<String, List<String>> replacements = new HashMap<>();
  private static Map<String, String> strips = new HashMap<>();

  public static List<String> getReplacements(String str) {
    if (!replacements.containsKey(str)) {
      Matcher matcher = PATTERN.matcher(str);
      List<String> list = new ArrayList<>();
      
      while (matcher.find()) {
        list.add(matcher.group(0));
      }

      replacements.put(str, list);
    }
    return replacements.get(str);
  }

  public static String strip(String replacement) {
    if (!strips.containsKey(replacement)) {
      Matcher matcher = PATTERN.matcher(replacement);
      if (matcher.find()) {
        strips.put(replacement, matcher.group(1));
      }
    }
    return strips.get(replacement);
  }
}
