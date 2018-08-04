package com.tm.kafka.connect.rest.http.transformer;

import com.tm.kafka.connect.rest.config.RequestTransformationFields;
import com.tm.kafka.connect.rest.http.Request;
import com.tm.kafka.connect.rest.http.payload.JSONPayload;
import com.tm.kafka.connect.rest.http.payload.MapPayload;
import com.tm.kafka.connect.rest.http.payload.Payload;
import com.tm.kafka.connect.rest.http.payload.StringPayload;
import com.tm.kafka.connect.rest.interpolator.InterpolationContext;
import com.tm.kafka.connect.rest.interpolator.Interpolator;
import com.tm.kafka.connect.rest.util.ReplacementExtractor;
import com.tm.kafka.connect.rest.util.StringToMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DefaultRequestTransformer implements RequestTransformer {

  private static Logger log = LoggerFactory.getLogger(DefaultRequestTransformer.class);

  private final Interpolator interpolator;

  private final String fieldsToAdd;
  private final String fieldsToRemove;
  private final String fieldsToReplace;

  public DefaultRequestTransformer(RequestTransformationFields config, Interpolator interpolator) {
    this.interpolator = interpolator;

    this.fieldsToAdd = Optional.ofNullable(config.getPayloadAdditions()).orElse("");
    this.fieldsToRemove = Optional.ofNullable(config.getPayloadRemovals()).orElse("");
    this.fieldsToReplace = Optional.ofNullable(config.getPayloadReplacements()).orElse("");
  }

  @Override
  public void transform(Request request) {
    InterpolationContext ctx = InterpolationContext.create(request.getPayload());
    // url
    String url = transformString(request.getUrl(), ctx);
    request.setUrl(url);
    // request params
    transformParams(request.getHeaders(), ctx);
    // payload
    final Payload payload = request.getPayload();
    if (payload instanceof MapPayload || payload instanceof JSONPayload) {
      Map<String, Object> map = (Map<String, Object>) payload.get();
      replaceFields(map, ctx);
      addFields(map, ctx);
      removeFields(map, ctx);
    } else if (payload instanceof StringPayload) {
      StringPayload newPayload = new StringPayload(transformString(payload.asString(), ctx));
      request.setPayload(newPayload);
    }
  }

  private String transformString(final String str, InterpolationContext ctx) {
    String result = str;
    List<String> replacements = ReplacementExtractor.getReplacements(str);

    for (String replacement : replacements) {
      String path = ReplacementExtractor.strip(replacement);
      String interpolated = interpolator.interpolate(path, ctx);

      if (interpolated == null) {
        interpolated = "";
        log.warn("Interpolator returned null value for the key '" + str + "'");
      }

      result = result.replace(replacement, interpolated);
    }

    return result;
  }

  private void transformParams(Map<String, String> properties, InterpolationContext ctx) {
    for (String key : properties.keySet()) {
      String transformed = transformString(properties.get(key), ctx);
      properties.put(key, transformed);
    }
  }

  private void replaceFields(Map<String, Object> map, InterpolationContext ctx) {

    if (fieldsToReplace.trim().isEmpty()) {
      return;
    }

    String transformed = transformString(fieldsToReplace, ctx);
    String[] keyValues = transformed.split(",");

    for (String keyValue : keyValues) {
      String[] keyAndValue = keyValue.split(":", 2);
      StringToMap.update(keyAndValue[0], keyAndValue[1], map);
    }
  }

  private void addFields(Map<String, Object> map, InterpolationContext ctx) {

    if (fieldsToAdd.trim().isEmpty()) {
      return;
    }

    String transformed = transformString(fieldsToAdd, ctx);
    String[] keyValues = transformed.split(",");

    for (String keyValue : keyValues) {
      String[] keyAndValue = keyValue.split(":", 2);
      StringToMap.update(keyAndValue[0], keyAndValue[1], map);
    }
  }

  private void removeFields(Map<String, Object> map, InterpolationContext ctx) {

    if (fieldsToRemove.trim().isEmpty()) {
      return;
    }

    String transformed = transformString(fieldsToRemove, ctx);
    String[] values = transformed.split(",", 2);

    for (String value : values) {
      StringToMap.remove(value, map);
    }
  }
}
