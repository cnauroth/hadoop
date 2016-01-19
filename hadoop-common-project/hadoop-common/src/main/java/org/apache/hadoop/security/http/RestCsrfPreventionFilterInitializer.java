/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.http;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RestCsrfPreventionFilterInitializer extends FilterInitializer {

  private static final Logger LOG =
      LoggerFactory.getLogger(RestCsrfPreventionFilterInitializer.class);

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    String confPrefix = conf.get("hadoop.http.rest-csrf.prefix",
        "hadoop.http.rest-csrf.");
    Map<String, String> filterConfigMap = getFilterConfigMap(conf, confPrefix);
    LOG.info("cn filterConfigMap = " + filterConfigMap);
    if (Boolean.parseBoolean(filterConfigMap.get("enabled"))) {
      LOG.info("Adding cross-site request forgery (CSRF) protection from "
          + "configuration prefix {}, with configuration {}", confPrefix,
          filterConfigMap);
      container.addGlobalFilter("rest-csrf",
          RestCsrfPreventionFilter.class.getName(), filterConfigMap);
    } else {
      LOG.debug("Cross-site request forgery (CSRF) protection not enabled for "
          + "configuration prefix {}", confPrefix);
    }
  }

  public static void setConfigurationPrefix(Configuration conf,
      String confPrefix) {
  }

  private static Map<String, String> getFilterConfigMap(Configuration conf,
      String confPrefix) {
    Map<String, String> filterConfigMap = new HashMap<>();
    for (Map.Entry<String, String> entry : conf) {
      String name = entry.getKey();
      if (name.startsWith(confPrefix)) {
        String value = conf.get(name);
        name = name.substring(confPrefix.length());
        filterConfigMap.put(name, value);
      }
    }
    return filterConfigMap;
  }
}
