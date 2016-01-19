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

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Enumeration;
import java.util.Map;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.apache.hadoop.security.http.RestCsrfPreventionFilterInitializer;

@InterfaceAudience.Private
final class RestCsrfPreventionFilterHandler
    extends SimpleChannelInboundHandler<HttpRequest>
    implements FilterContainer {

  private RestCsrfPreventionFilter filter;

  public RestCsrfPreventionFilterHandler(Configuration conf) {
    conf.set("hadoop.http.rest-csrf.prefix", "dfs.datanode.http.rest-csrf.");
    new RestCsrfPreventionFilterInitializer().initFilter(this, conf);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req)
      throws Exception {
    if (filter.isRequestAllowed(req.getMethod().name(),
        req.headers().get(filter.getHeaderName()))) {
      System.out.println("cn channelRead0, request is allowed");
      // ctx.pipeline().fireChannelRead(req);
      // ctx.writeAndFlush(req);
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    } else {
      System.out.println("cn channelRead0, request is denied");
      HttpResponseStatus status = new HttpResponseStatus(BAD_REQUEST.code(),
          "Missing Required Header for Vulnerability Protection");
      DefaultHttpResponse resp = new DefaultHttpResponse(HTTP_1_1, status);
      resp.headers().set(CONNECTION, CLOSE);
      ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
    }
  }

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

  private static final class MapBasedFilterConfig implements FilterConfig {

    private final String name;
    private final Map<String, String> parameters;

    public MapBasedFilterConfig(String name, Map<String, String> parameters) {
      this.name = name;
      this.parameters = parameters;
    }

    @Override
    public String getFilterName() {
      return this.name;
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

  private static UnsupportedOperationException notImplemented(Object source) {
    return new UnsupportedOperationException(source.getClass().getSimpleName()
        + " does not implement this method.");
  }
}
