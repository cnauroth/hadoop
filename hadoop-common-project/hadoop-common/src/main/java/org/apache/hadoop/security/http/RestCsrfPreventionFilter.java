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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

/**
 * This filter provides protection against cross site request forgery (CSRF)
 * attacks for REST APIs. Enabling this filter on an endpoint results in the
 * requirement of all client to send a particular (configurable) HTTP header
 * with every request. In the absense of this header the filter will reject the
 * attempt as a bad request.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RestCsrfPreventionFilter implements Filter {
  public static final String CUSTOM_HEADER_PARAM = "custom-header";
  public static final String CUSTOM_METHODS_TO_IGNORE_PARAM =
      "methods-to-ignore";
  public static final String URL_PATH_PREFIXES_PARAM = "url-path-prefixes";

  static final String HEADER_DEFAULT = "X-XSRF-HEADER";
  static final String  METHODS_TO_IGNORE_DEFAULT = "GET,OPTIONS,HEAD,TRACE";
  private String  headerName = HEADER_DEFAULT;
  private Set<String> methodsToIgnore = null;
  private Set<String> urlPathPrefixes = null;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    String customHeader = filterConfig.getInitParameter(CUSTOM_HEADER_PARAM);
    if (customHeader != null) {
      headerName = customHeader;
    }
    String customMethodsToIgnore =
        filterConfig.getInitParameter(CUSTOM_METHODS_TO_IGNORE_PARAM);
    methodsToIgnore = parseStringSet(filterConfig,
        CUSTOM_METHODS_TO_IGNORE_PARAM, METHODS_TO_IGNORE_DEFAULT);
    urlPathPrefixes = parseStringSet(filterConfig, URL_PATH_PREFIXES_PARAM,
        null);
  }

  /**
   * Returns the configured custom header name.
   *
   * @return custom header name
   */
  public String getHeaderName() {
    return headerName;
  }

  /**
   * Returns whether or not the request is allowed.
   *
   * @param method HTTP Method
   * @param requestUri HTTP request URI
   * @param header value of HTTP header defined by {@link #getHeaderName()}
   * @return true if the request is allowed, otherwise false
   */
  public boolean isRequestAllowed(String method, String requestPath,
      String header) {
    return !isFilteredPath(requestPath) || methodsToIgnore.contains(method) ||
        header != null;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest)request;
    if (isRequestAllowed(httpRequest.getMethod(), httpRequest.getRequestURI(),
        httpRequest.getHeader(headerName))) {
      chain.doFilter(request, response);
    } else {
      ((HttpServletResponse)response).sendError(
          HttpServletResponse.SC_BAD_REQUEST,
          "Missing Required Header for Vulnerability Protection");
    }
  }

  @Override
  public void destroy() {
  }

  /**
   * Returns true if the given URL path must be filtered.  A path must be
   * filtered either if it matches one of the configured prefixes, or there were
   * no prefixes configured, which means all paths must be checked.
   *
   * @param urlPath path to check
   * @return true if the path must be filtered, false otherwise
   */
  private boolean isFilteredPath(String urlPath) {
    if (urlPathPrefixes.isEmpty()) {
      return true;
    }
    for (String urlPathPrefix : urlPathPrefixes) {
      if (urlPath.startsWith(urlPathPrefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parses a comma-delimited string stored in a configuration parameter into a
   * set and returns it.
   *
   * @param filterConfig filter configuration to check
   * @param param configuration parameter to read
   * @param defaultValue default value if not found in configuration
   * @return parsed set, or empty set if value not configured
   */
  private static Set<String> parseStringSet(FilterConfig filterConfig,
      String param, String defaultValue) {
    String value = filterConfig.getInitParameter(param);
    if (value == null) {
      value = defaultValue;
    }
    return new HashSet<String>(StringUtils.getTrimmedStringCollection(value));
  }
}
