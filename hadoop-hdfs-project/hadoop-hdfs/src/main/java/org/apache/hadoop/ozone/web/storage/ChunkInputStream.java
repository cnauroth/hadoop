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

import static org.apache.hadoop.ozone.web.storage.ContainerProtocolCalls.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClient;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClientManager;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;

class ChunkInputStream extends InputStream {

  private static final int EOF = -1;

  private final String key;
  private final UserArgs args;
  private XceiverClientManager xceiverClientManager;
  private XceiverClient xceiverClient;
  private List<ChunkInfo> chunks;
  private int chunkOffset;
  private List<ByteBuffer> buffers;
  private int bufferOffset;

  public ChunkInputStream(String key, XceiverClientManager xceiverClientManager,
      XceiverClient xceiverClient, List<ChunkInfo> chunks, UserArgs args) {
    this.key = key;
    this.args = args;
    this.xceiverClientManager = xceiverClientManager;
    this.xceiverClient = xceiverClient;
    this.chunks = chunks;
    this.chunkOffset = 0;
    this.buffers = null;
    this.bufferOffset = 0;
  }

  @Override
  public synchronized int read()
      throws IOException {
    checkOpen();

    if (chunks.isEmpty()) {
      // This must be an empty key.
      return EOF;
    }

    // This loop advances through chunks and buffers as needed until it finds a
    // byte to return or EOF.
    for (;;) {
      if (buffers == null) {
        // The first read triggers fetching the first chunk.
        readChunkFromContainer(0);
      } else if (!buffers.isEmpty() &&
          buffers.get(bufferOffset).hasRemaining()) {
        // Data is available from the current buffer.
        return buffers.get(bufferOffset).get();
      } else if (!buffers.isEmpty() &&
          !buffers.get(bufferOffset).hasRemaining() &&
          bufferOffset < buffers.size() - 1) {
        // There are additional buffers available.
        ++bufferOffset;
      } else if (chunkOffset < chunks.size() - 1) {
        // There are additional chunks available.
        readChunkFromContainer(chunkOffset + 1);
      } else {
        // All available input has been consumed.
        return EOF;
      }
    }
  }

  @Override
  public synchronized void close() {
    if (xceiverClientManager != null && xceiverClient != null) {
      xceiverClientManager.releaseClient(xceiverClient);
      xceiverClientManager = null;
      xceiverClient = null;
    }
  }

  private synchronized void checkOpen() throws IOException {
    if (xceiverClient == null) {
      throw new IOException("ChunkInputStream has been closed.");
    }
  }

  private synchronized void readChunkFromContainer(int readChunkOffset)
      throws IOException {
    final ReadChunkResponseProto readChunkResponse;
    try {
      readChunkResponse = readChunk(xceiverClient, chunks.get(readChunkOffset),
          key, args);
    } catch (OzoneException e) {
      throw new IOException("Unexpected OzoneException", e);
    }
    chunkOffset = readChunkOffset;
    ByteString byteString = readChunkResponse.getData();
    buffers = byteString.asReadOnlyByteBufferList();
  }
}
