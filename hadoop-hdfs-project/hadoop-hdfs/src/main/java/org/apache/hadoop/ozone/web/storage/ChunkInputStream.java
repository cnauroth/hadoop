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
import java.util.List;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClient;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClientManager;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;

/**
 * An {@link InputStream} used by the REST service in combination with the
 * {@link DistributedStorageHandler} to read the value of a key from a sequence
 * of container chunks.  All bytes of the key value are stored in container
 * chunks.  Each chunk may contain multiple underlying {@link ByteBuffer}
 * instances.  This class encapsulates all state management for iterating
 * through the sequence of chunks and the sequence of buffers within each chunk.
 */
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

  /**
   * Creates a new ChunkInputStream.
   *
   * @param key chunk key
   * @param xceiverClientManager client manager that controls client
   * @param xceiverClient client to perform container calls
   * @param chunks list of chunks to read
   * @param args container protocol call args
   */
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

    if (chunks == null || chunks.isEmpty()) {
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

  /**
   * Checks if the stream is open.  If not, throws an exception.
   *
   * @throws IOException if stream is closed
   */
  private synchronized void checkOpen() throws IOException {
    if (xceiverClient == null) {
      throw new IOException("ChunkInputStream has been closed.");
    }
  }

  /**
   * Attempts to read the chunk at the specified offset in the chunk list.  If
   * successful, then the data of the read chunk is saved so that its bytes can
   * be returned from subsequent read calls.
   *
   * @param readChunkOffset offset in the chunk list of which chunk to read
   * @throws IOException if there is an I/O error while performing the call
   */
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
