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

import java.io.IOException;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.metadata.Bucket;
import org.apache.hadoop.ozone.metadata.Key;
import org.apache.hadoop.ozone.protocol.MetadataProtocol;
import org.apache.hadoop.ozone.protocol.proto.MetadataProtocolProtos.BucketProto;
import org.apache.hadoop.ozone.protocol.proto.MetadataProtocolProtos.GetBucketMetadataRequestProto;
import org.apache.hadoop.ozone.protocol.proto.MetadataProtocolProtos.GetBucketMetadataResponseProto;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link MetadataProtocolPB} to the {@link MetadataProtocol} server
 * implementation.
 */
@InterfaceAudience.Private
public final class MetadataProtocolServerSideTranslatorPB
    implements MetadataProtocolPB {

  private final MetadataProtocol impl;

  /**
   * Creates a new MetadataProtocolServerSideTranslatorPB.
   *
   * @param impl {@link MetadataProtocol} server implementation
   */
  public MetadataProtocolServerSideTranslatorPB(MetadataProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetBucketMetadataResponseProto getBucketMetadata(RpcController unused,
      GetBucketMetadataRequestProto req) throws ServiceException {
    final Bucket bucket;
    try {
      bucket = impl.getBucketMetadata(new Key(req.getBucketName()
          .toByteArray()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetBucketMetadataResponseProto.newBuilder().setBucket(
        BucketProto.newBuilder()
            .setBucketId(bucket.getBucketId())
            .setName(bucket.getName())
            .setOwner(bucket.getOwner())
            .build())
        .build();
  }
}
