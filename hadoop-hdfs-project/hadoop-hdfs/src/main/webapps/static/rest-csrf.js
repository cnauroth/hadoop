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

"use strict";

// Initializes client-side handling of cross-site request forgery (CSRF)
// protection by figuring out the custom HTTP headers that need to be sent in
// requests and which HTTP methods are ignored because they do not require
// CSRF protection.  Protection can be configured separately and independently
// for the NameNode and DataNode.  The common case would be to enable it for
// both, using the same settings for the header and the methods to ignore.
// However, this method gracefully handles other possibilities too.  If the
// NameNode and DataNode are configured to use different headers, then this
// method finds both and sends both in relevant requests.  If the NameNode and
// DataNode are configured with different methods to ignore, then this method
// computes the set intersection.  (Since there may be a redirect involved,
// it's important to determine if either of the processes need the headers
// sent.)  If only one of either NameNode or DataNode is configured with CSRF
// protection, then this method only reads configuration pertaining to the one
// daemon.
(function() {
  var restCsrfCustomHeaders = null;
  var restCsrfMethodsToIgnore = null;

  $.ajax({'url': '/conf', 'dataType': 'xml', 'async': false}).done(
    function(data) {
      function getBooleanValue(element) {
        return ($(element).find('value').text().trim().toLowerCase() === 'true')
      }

      function getTrimmedStringValue(element) {
        return $(element).find('value').text().trim();
      }

      function getTrimmedStringArrayValue(element) {
        var str = $(element).find('value').text().trim();
        var array = [];
        if (str) {
          var splitStr = str.split(',');
          for (var i = 0; i < splitStr.length; i++) {
            array.push(splitStr[i].trim());
          }
        }
        return array;
      }

      // Get all relevant configuration properties.
      var $xml = $(data);
      var nnCsrfEnabled = false, dnCsrfEnabled = false;
      var nnHeader = null, dnHeader = null;
      var nnMethods = [], dnMethods = [];
      $xml.find('property').each(function(idx, element) {
        var name = $(element).find('name').text();
        if (name === 'dfs.namenode.http.rest-csrf.enabled') {
          nnCsrfEnabled = getBooleanValue(element);
        } else if (name === 'dfs.namenode.http.rest-csrf.custom-header') {
          nnHeader = getTrimmedStringValue(element);
        } else if (name === 'dfs.namenode.http.rest-csrf.methods-to-ignore') {
          nnMethods = getTrimmedStringArrayValue(element);
        } else if (name === 'dfs.datanode.http.rest-csrf.enabled') {
          dnCsrfEnabled = getBooleanValue(element);
        } else if (name === 'dfs.datanode.http.rest-csrf.custom-header') {
          dnHeader = getTrimmedStringValue(element);
        } else if (name === 'dfs.datanode.http.rest-csrf.methods-to-ignore') {
          dnMethods = getTrimmedStringArrayValue(element);
        }
      });

      // Finalize values of custom headers and ignored methods.
      if (nnCsrfEnabled && dnCsrfEnabled) {
        restCsrfCustomHeaders = {};
        if (nnHeader != null) {
          restCsrfCustomHeaders[nnHeader] = true;
        }
        if (dnHeader != null) {
          restCsrfCustomHeaders[dnHeader] = true;
        }
        restCsrfMethodsToIgnore = {};
        nnMethods
            .filter(function(method) { return $.inArray(method, dnMethods) > -1; })
            .map(function(method) { restCsrfMethodsToIgnore[method] = true; });
      } else if (nnCsrfEnabled) {
        restCsrfCustomHeaders = {};
        if (nnHeader != null) {
          restCsrfCustomHeaders[nnHeader] = true;
        }
        restCsrfMethodsToIgnore = {};
        nnMethods.map(function(method) { restCsrfMethodsToIgnore[method] = true; });
      } else if (dnCsrfEnabled) {
        restCsrfCustomHeaders = {};
        if (dnHeader != null) {
          restCsrfCustomHeaders[dnHeader] = true;
        }
        restCsrfMethodsToIgnore = {};
        dnMethods.map(function(method) { restCsrfMethodsToIgnore[method] = true; });
      } else {
        restCsrfCustomHeaders = null;
        restCsrfMethodsToIgnore = null;
      }

      // If enabled, set up all subsequent AJAX calls with a pre-send callback
      // that adds the custom headers if necessary.
      if (nnCsrfEnabled || dnCsrfEnabled) {
        $.ajaxSetup({
          beforeSend: addRestCsrfCustomHeaders
        });
      }
    });

  // Adds custom headers to request if necessary.  This is done only for WebHDFS
  // URLs, and only if it's not an ignored method.
  function addRestCsrfCustomHeaders(xhr, settings) {
    if (settings.url == null || !settings.url.startsWith('/webhdfs/')) {
      return;
    }
    var method = settings.type;
    if (restCsrfCustomHeaders != null &&
        !restCsrfMethodsToIgnore[method]) {
      for (var header in restCsrfCustomHeaders) {
        // The value of the header is unimportant.  Only its presence matters.
        xhr.setRequestHeader(header, '""');
      }
    }
  }
})();
