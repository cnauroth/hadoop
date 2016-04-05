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

import static org.apache.hadoop.ozone.OzoneConsts.CHUNK_SIZE;
import static org.apache.hadoop.ozone.web.storage.ContainerProtocolCalls.*;
import static org.apache.hadoop.ozone.web.storage.OzoneContainerTranslation.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyData;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClient;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClientManager;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.response.KeyInfo;

class ChunkOutputStream extends OutputStream {

  private final String containerKey;
  private final KeyInfo key;
  private final UserArgs args;
  private final KeyData.Builder containerKeyData;
  private XceiverClientManager xceiverClientManager;
  private XceiverClient xceiverClient;
  private ByteBuffer buffer;
  private int chunkId;

  public ChunkOutputStream(String containerKey, KeyInfo key,
      XceiverClientManager xceiverClientManager, XceiverClient xceiverClient,
      UserArgs args) {
    this.containerKey = containerKey;
    this.key = key;
    this.args = args;
    this.containerKeyData = fromKeyToContainerKeyDataBuilder(
        xceiverClient.getPipeline().getContainerName(), containerKey, key);
    this.xceiverClientManager = xceiverClientManager;
    this.xceiverClient = xceiverClient;
    this.buffer = ByteBuffer.allocate(CHUNK_SIZE);
    this.chunkId = 0;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    checkOpen();
    int rollbackPosition = buffer.position();
    int rollbackLimit = buffer.limit();
    buffer.put((byte)b);
    if (buffer.position() == CHUNK_SIZE) {
      flushBufferToChunk(rollbackPosition, rollbackLimit);
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    checkOpen();
    if (buffer.position() > 0) {
      int rollbackPosition = buffer.position();
      int rollbackLimit = buffer.limit();
      flushBufferToChunk(rollbackPosition, rollbackLimit);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (xceiverClientManager != null && xceiverClient != null &&
        buffer != null) {
      try {
        if (buffer.position() > 0) {
          writeChunkToContainer();
        }
        createKey(xceiverClient, containerKeyData.build(), args);
      } catch (OzoneException e) {
        throw new IOException("Unexpected OzoneException", e);
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

  private synchronized void flushBufferToChunk(int rollbackPosition,
      int rollbackLimit) throws IOException {
    boolean success = false;
    try {
      writeChunkToContainer();
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

  private synchronized void writeChunkToContainer() throws IOException {
    buffer.flip();
    ByteString data = ByteString.copyFrom(buffer);
    ChunkInfo chunk = ChunkInfo
        .newBuilder()
        .setChunkName(key.getKeyName() + "_chunk_" + ++chunkId)
        .setOffset(0)
        .setLen(data.size())
        .build();
    try {
      writeChunk(xceiverClient, chunk, key.getKeyName(), data, args);
    } catch (OzoneException e) {
      throw new IOException("Unexpected OzoneException", e);
    }
    containerKeyData.addChunks(chunk);
  }
}
