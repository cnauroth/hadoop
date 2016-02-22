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
package org.apache.hadoop.ozone.protocolPB;

import java.io.Closeable;
import java.io.IOException;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.metadata.Bucket;
import org.apache.hadoop.ozone.metadata.Key;
import org.apache.hadoop.ozone.protocol.MetadataProtocol;
import org.apache.hadoop.ozone.protocol.proto.MetadataProtocolProtos.BucketProto;
import org.apache.hadoop.ozone.protocol.proto.MetadataProtocolProtos.GetBucketMetadataRequestProto;
import org.apache.hadoop.ozone.protocol.proto.MetadataProtocolProtos.GetBucketMetadataResponseProto;

/**
 * This class is the client-side translator to translate the requests made on the
 * {@link MetadataProtocol} interface to the RPC server implementing
 * {@link MetadataProtocolPB}.
 */
@InterfaceAudience.Private
public final class MetadataProtocolClientSideTranslatorPB
    implements MetadataProtocol, ProtocolTranslator, Closeable {

  /** RpcController is not used and hence is set to null */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final MetadataProtocolPB rpcProxy;

  /**
   * Creates a new MetadataProtocolClientSideTranslatorPB.
   *
   * @param rpcProxy {@link MetadataProtocolPB} RPC proxy
   */
  public MetadataProtocolClientSideTranslatorPB(MetadataProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public Bucket getBucketMetadata(Key bucketName) throws IOException {
    GetBucketMetadataRequestProto req = GetBucketMetadataRequestProto.newBuilder()
        .setBucketName(ByteString.copyFrom(bucketName.toByteArray()))
        .build();
    final GetBucketMetadataResponseProto resp;
    try {
      resp = rpcProxy.getBucketMetadata(NULL_RPC_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    BucketProto bucketProto = resp.getBucket();
    return new Bucket(bucketProto.getBucketId(), bucketProto.getName(),
        bucketProto.getOwner());
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }
}
