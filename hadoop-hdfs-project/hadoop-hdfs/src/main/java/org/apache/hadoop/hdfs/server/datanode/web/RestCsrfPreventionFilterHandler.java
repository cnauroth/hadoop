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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;

@InterfaceAudience.Private
final class RestCsrfPreventionFilterHandler
    extends SimpleChannelInboundHandler<HttpRequest> {

  private final RestCsrfPreventionFilter restCsrfPreventionFilter;

  public RestCsrfPreventionFilterHandler(
      RestCsrfPreventionFilter restCsrfPreventionFilter) {
    this.restCsrfPreventionFilter = restCsrfPreventionFilter;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req)
      throws Exception {
    if (this.restCsrfPreventionFilter.isRequestAllowed(req.getMethod().name(),
        req.headers().get(this.restCsrfPreventionFilter.getHeaderName()))) {
      System.out.println("cn channelRead0, request is allowed");
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
}
