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

/**
 * A {@link FilterInitializer} responsible for initializing the
 * {@link RestCsrfPreventionFilter}.  In typical usage, a component would
 * integrate by calling the {@link #setConfigurationPrefix} method to declare a
 * component-specific prefix for all configuration properties to pass to the
 * filter.  Initialization would then flow as usual through the
 * {@link #initFilter} method.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RestCsrfPreventionFilterInitializer extends FilterInitializer {

  private static final String CONF_ENABLED = "enabled";
  private static final String CONF_PREFIX = "hadoop.http.rest-csrf.prefix";
  private static final String FILTER_NAME = "rest-csrf";

  private static final Logger LOG =
      LoggerFactory.getLogger(RestCsrfPreventionFilterInitializer.class);

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    String confPrefix = conf.getTrimmed(CONF_PREFIX);
    if (confPrefix == null || confPrefix.isEmpty()) {
      LOG.debug("Cross-site request forgery (CSRF) protection not initialized, "
          + "because no configuration prefix is specified in {}.", CONF_PREFIX);
      return;
    }
    Map<String, String> filterConfigMap = getFilterConfigMap(conf, confPrefix);
    if (Boolean.parseBoolean(filterConfigMap.get("enabled"))) {
      LOG.info("Adding cross-site request forgery (CSRF) protection from "
          + "configuration prefix {}, with configuration {}", confPrefix,
          filterConfigMap);
      container.addGlobalFilter(FILTER_NAME,
          RestCsrfPreventionFilter.class.getName(), filterConfigMap);
    } else {
      LOG.debug("Cross-site request forgery (CSRF) protection not enabled for "
          + "configuration prefix {}", confPrefix);
    }
  }

  /**
   * Modifies the configuration by setting the configuration prefix.  All
   * properties that have a name starting with the prefix will then be passed to
   * the filter during initialization.
   *
   * @param conf configuration to modify
   * @param confPrefix prefix of configuration properties to be passed to filter
   *     during initialization
   */
  public static void setConfigurationPrefix(Configuration conf,
      String confPrefix) {
    conf.set(CONF_PREFIX, confPrefix);
  }

  /**
   * Constructs a mapping of configuration properties to be used for filter
   * initialization.  The mapping includes all properties that start with the
   * configuration prefix that was specified by calling
   * {@link #setConfigurationPrefix} on the configuration.  Property names in
   * the mapping are trimmed to remove the configuration prefix.
   *
   * @param conf configuration to read
   * @param confPrefix configuration prefix
   * @return mapping of configuration properties to be used for filter
   *     initialization
   */
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
