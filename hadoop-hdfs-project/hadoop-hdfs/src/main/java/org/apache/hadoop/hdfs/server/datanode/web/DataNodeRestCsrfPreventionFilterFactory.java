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
package org.apache.hadoop.hdfs.server.datanode.web;

import java.util.Enumeration;
import java.util.Map;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.apache.hadoop.security.http.RestCsrfPreventionFilterInitializer;

/**
 * Creates the {@link RestCsrfPreventionFilter} for the DataNode.  Since the
 * DataNode HTTP server is not implemented in terms of the servlet API, it takes
 * some extra effort to obtain an instance of the filter.  This factory takes
 * care of configuration and implementing just enough of the servlet API and
 * related interfaces so that the DataNode can get a fully initialized instance
 * of the filter.
 */
@InterfaceAudience.Private
final class DataNodeRestCsrfPreventionFilterFactory {

  /**
   * Creates the CSRF prevention filter.  If the CSRF prevention filter is in
   * the configured list of filter initializers, then change configuration to
   * specify the correct prefix, build the filter, and return it.
   *
   * @param conf configuration to modify with correct configuration prefix if
   *     the CSRF prevention filter initializer is active
   * @return initialized filter, or null if filter initializer not configured
   */
  public static RestCsrfPreventionFilter create(Configuration conf) {
    RestCsrfPreventionFilterInitializer.setConfigurationPrefix(conf,
        "hadoop.http.filter.initializers", "dfs.datanode.http.rest-csrf.");
    CapturingFilterContainer filterContainer = new CapturingFilterContainer();
    new RestCsrfPreventionFilterInitializer().initFilter(filterContainer, conf);
    return filterContainer.filter;
  }

  /**
   * Creates an exception indicating that an interface method is not
   * implemented.  These should never be seen in practice, because it is only
   * used for methods that are not called by
   * {@link RestCsrfPreventionFilterInitializer} or
   * {@link RestCsrfPreventionFilter}.
   *
   * @return exception indicating method not implemented
   */
  private static UnsupportedOperationException notImplemented(Object source) {
    return new UnsupportedOperationException(source.getClass().getSimpleName()
        + " does not implement this method.");
  }

  /**
   * A minimal {@link FilterContainer} that creates an instance of
   * {@link RestCsrfPreventionFilter}, initializes it, and holds a reference to
   * it so that later code can access it.
   */
  private static final class CapturingFilterContainer
      implements FilterContainer {

    public RestCsrfPreventionFilter filter;

    @Override
    public void addFilter(String name, String classname,
        Map<String, String> parameters) {
      throw notImplemented(this);
    }

    @Override
    public void addGlobalFilter(String name, String classname,
        Map<String, String> parameters) {
      RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
      try {
        filter.init(new MapBasedFilterConfig(name, parameters));
      } catch (ServletException e) {
        throw new IllegalStateException("Filter initialization failed.", e);
      }
      this.filter = filter;
    }
  }

  /**
   * A minimal {@link FilterConfig} implementation backed by a {@link Map}.
   */
  private static final class MapBasedFilterConfig implements FilterConfig {

    private final String filterName;
    private final Map<String, String> parameters;

    /**
     * Creates a new MapBasedFilterConfig.
     *
     * @param filterName filter name
     * @param parameters mapping of filter initialization parameters
     */
    public MapBasedFilterConfig(String filterName,
        Map<String, String> parameters) {
      this.filterName = filterName;
      this.parameters = parameters;
    }

    @Override
    public String getFilterName() {
      return this.filterName;
    }

    @Override
    public String getInitParameter(String name) {
      return this.parameters.get(name);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
      throw notImplemented(this);
    }

    @Override
    public ServletContext getServletContext() {
      throw notImplemented(this);
    }
  }
}
