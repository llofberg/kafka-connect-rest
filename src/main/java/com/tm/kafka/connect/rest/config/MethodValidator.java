/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.tm.kafka.connect.rest.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;

public class MethodValidator implements ConfigDef.Validator {
  @Override
  public void ensureValid(String name, Object provider) {
  }

  @Override
  public String toString() {
    return new MethodRecommender().validValues("", new HashMap<>()).toString();
  }
}
