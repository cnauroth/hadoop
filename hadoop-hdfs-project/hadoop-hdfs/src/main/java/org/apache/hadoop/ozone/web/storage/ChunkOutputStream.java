/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Type;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.chunkInfo;
import org.apache.hadoop.ozone.container.transport.client.XceiverClient;
import org.apache.hadoop.ozone.container.transport.client.XceiverClientManager;

class ChunkOutputStream extends OutputStream {

  private static final int BUFFER_SIZE = 1 * 1024 * 1024; // 1 MB

  private final String key;
  private XceiverClientManager xceiverClientManager;
  private XceiverClient xceiverClient;
  private long chunkOffset;
  private ByteBuffer buffer;

  public ChunkOutputStream(String key,
      XceiverClientManager xceiverClientManager, XceiverClient xceiverClient) {
    this.key = key;
    this.xceiverClientManager = xceiverClientManager;
    this.xceiverClient = xceiverClient;
    this.chunkOffset = 0;
    this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
  }

  @Override
  public synchronized void write(int b) throws IOException {
    checkOpen();
    int rollbackPosition = buffer.position();
    int rollbackLimit = buffer.limit();
    buffer.put((byte)b);
    if (buffer.position() == BUFFER_SIZE) {
      buffer.flip();
      boolean success = false;
      try {
        writeChunk();
        success = true;
      } finally {
        if (success) {
          buffer.clear();
        } else {
          buffer.position(rollbackPosition);
          buffer.limit(rollbackLimit);
        }
      }
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    if (buffer.position() > 0) {
      int rollbackPosition = buffer.position();
      int rollbackLimit = buffer.limit();
      buffer.flip();
      boolean success = false;
      try {
        writeChunk();
        success = true;
      } finally {
        if (success) {
          buffer.clear();
        } else {
          buffer.position(rollbackPosition);
          buffer.limit(rollbackLimit);
        }
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (xceiverClientManager != null && xceiverClient != null &&
        buffer != null) {
      try {
        flush();
      } finally {
        xceiverClientManager.releaseClient(xceiverClient);
        xceiverClientManager = null;
        xceiverClient = null;
        buffer = null;
      }
    }
  }

  private synchronized void checkOpen() throws IOException {
    if (xceiverClient == null) {
      throw new IOException("ChunkOutputStream has been closed.");
    }
  }

  private synchronized void writeChunk() throws IOException {
    ByteString byteString = ByteString.copyFrom(buffer);
    chunkInfo.Builder chunk = chunkInfo
        .newBuilder()
        .setOffset(chunkOffset)
        .setLen(byteString.size());
    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setContainerName(xceiverClient.getPipeline().getContainerName())
        .setKeyName(key)
        .setChunkData(chunk)
        .addData(byteString);
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.WriteChunk)
        .setWriteChunk(writeChunkRequest)
        .build();
    xceiverClient.sendCommand(request);
    ++chunkOffset;
  }
}
