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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.CreateKeyRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerKeyData;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ReadKeyRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ReadKeyResponeProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Type;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.chunkInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClient;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClientManager;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.OzoneConsts;
import org.apache.hadoop.ozone.web.utils.OzoneConsts.Versioning;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

/**
 * A {@link StorageHandler} implementation that distributes object storage
 * across the nodes of an HDFS cluster.
 */
public final class DistributedStorageHandler implements StorageHandler {

  private final XceiverClientManager xceiverClientManager;

  public DistributedStorageHandler(OzoneConfiguration conf,
      StorageContainerLocationProtocolClientSideTranslatorPB storageContainerLocation) {
    this.xceiverClientManager = new XceiverClientManager(conf,
        storageContainerLocation);
  }

  @Override
  public void createVolume(final VolumeArgs args) throws
      IOException, OzoneException {
    String key = buildContainerKey(args.getVolumeName());
    XceiverClient xceiverClient = xceiverClientManager.acquireClient(key);
    try {
      ContainerKeyData.Builder containerKeyData = ContainerKeyData
          .newBuilder()
          .setContainerName(xceiverClient.getPipeline().getContainerName())
          .setName(key)
          .addMetadata(newKeyValue("Key", "VOLUME"))
          .addMetadata(newKeyValue("Created", dateToString(new Date())));

      if (args.getQuota() != null && args.getQuota().sizeInBytes() != -1L) {
        containerKeyData.addMetadata(
            newKeyValue("Quota", args.getQuota().sizeInBytes()));
      }

      if (args.getUserName() != null && !args.getUserName().isEmpty()) {
        containerKeyData.addMetadata(newKeyValue("Owner", args.getUserName()));
      }

      if (args.getAdminName() != null && !args.getAdminName().isEmpty()) {
        containerKeyData.addMetadata(
            newKeyValue("CreatedBy", args.getAdminName()));
      }

      createKey(xceiverClient, containerKeyData);
    } finally {
      xceiverClientManager.releaseClient(xceiverClient);
    }
  }

  @Override
  public void setVolumeOwner(VolumeArgs args) throws
      IOException, OzoneException {

  }

  @Override
  public void setVolumeQuota(VolumeArgs args, boolean remove)
      throws IOException, OzoneException {

  }

  @Override
  public boolean checkVolumeAccess(VolumeArgs args)
      throws IOException, OzoneException {
    return false;
  }

  @Override
  public ListVolumes listVolumes(UserArgs args)
      throws IOException, OzoneException {
    return null;
  }

  @Override
  public void deleteVolume(VolumeArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public VolumeInfo getVolumeInfo(VolumeArgs args)
      throws IOException, OzoneException {
    return null;
  }

  @Override
  public void createBucket(final BucketArgs args)
      throws IOException, OzoneException {
    String key = buildContainerKey(args.getVolumeName(), args.getBucketName());
    XceiverClient xceiverClient = xceiverClientManager.acquireClient(key);
    try {
      ContainerKeyData.Builder containerKeyData = ContainerKeyData
          .newBuilder()
          .setContainerName(xceiverClient.getPipeline().getContainerName())
          .setName(key)
          .addMetadata(newKeyValue("Key", "BUCKET"))
          .addMetadata(newKeyValue("KEY_VOLUME_NAME", args.getVolumeName()));

      if (args.getAddAcls() != null) {
        containerKeyData.addMetadata(newKeyValue("ADD_ACLS",
            StringUtils.join(',', args.getAddAcls())));
      }

      if (args.getRemoveAcls() != null) {
        containerKeyData.addMetadata(newKeyValue("REMOVE_ACLS",
            StringUtils.join(',', args.getRemoveAcls())));
      }

      if (args.getVersioning() != null &&
          args.getVersioning() != Versioning.NOT_DEFINED) {
        containerKeyData.addMetadata(newKeyValue("BUCKET_VERSIONING",
            args.getVersioning().name()));
      }

      if (args.getStorageType() != StorageType.RAM_DISK) {
        containerKeyData.addMetadata(newKeyValue("STORAGE_TYPE",
            args.getStorageType().name()));
      }

      createKey(xceiverClient, containerKeyData);
    } finally {
      xceiverClientManager.releaseClient(xceiverClient);
    }
  }

  @Override
  public void setBucketAcls(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public void setBucketVersioning(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public void setBucketStorageClass(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public void deleteBucket(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public void checkBucketAccess(BucketArgs args)
      throws IOException, OzoneException {

  }

  @Override
  public ListBuckets listBuckets(VolumeArgs args)
      throws IOException, OzoneException {
    return null;
  }

  @Override
  public BucketInfo getBucketInfo(BucketArgs args)
      throws IOException, OzoneException {
    return null;
  }

  /**
   * Writes a key in an existing bucket.
   *
   * @param args KeyArgs
   * @return InputStream
   * @throws OzoneException
   */
  @Override
  public OutputStream newKeyWriter(KeyArgs args) throws IOException,
      OzoneException {
    String key = buildContainerKey(args.getVolumeName(), args.getBucketName(),
        args.getKeyName());
    XceiverClient xceiverClient = xceiverClientManager.acquireClient(key);
    boolean success = false;
    try {
      ContainerKeyData.Builder containerKeyData = ContainerKeyData
          .newBuilder()
          .setContainerName(xceiverClient.getPipeline().getContainerName())
          .setName(key)
          .addMetadata(newKeyValue("Key", "KEY"))
          .addMetadata(newKeyValue("KEY_VOLUME_NAME", args.getVolumeName()))
          .addMetadata(newKeyValue("KEY_BUCKET_NAME", args.getBucketName()));

      if (args.getAddAcls() != null) {
        containerKeyData.addMetadata(newKeyValue("ADD_ACLS",
            StringUtils.join(',', args.getAddAcls())));
      }

      if (args.getRemoveAcls() != null) {
        containerKeyData.addMetadata(newKeyValue("REMOVE_ACLS",
            StringUtils.join(',', args.getRemoveAcls())));
      }

      if (args.getVersioning() != null &&
          args.getVersioning() != Versioning.NOT_DEFINED) {
        containerKeyData.addMetadata(newKeyValue("BUCKET_VERSIONING",
            args.getVersioning().name()));
      }

      if (args.getStorageType() != StorageType.RAM_DISK) {
        containerKeyData.addMetadata(newKeyValue("STORAGE_TYPE",
            args.getStorageType().name()));
      }

      createKey(xceiverClient, containerKeyData);
      success = true;
      return new ChunkOutputStream(key, xceiverClientManager, xceiverClient);
    } finally {
      if (!success) {
        xceiverClientManager.releaseClient(xceiverClient);
      }
    }
  }

  /**
   * Tells the file system that the object has been written out completely and
   * it can do any house keeping operation that needs to be done.
   *
   * @param args   Key Args
   * @param stream
   * @throws IOException
   */
  @Override
  public void commitKey(KeyArgs args, OutputStream stream) throws
      IOException, OzoneException {
  }

  /**
   * Reads a key from an existing bucket.
   *
   * @param args KeyArgs
   * @return LengthInputStream
   * @throws IOException
   */
  @Override
  public LengthInputStream newKeyReader(KeyArgs args) throws IOException,
      OzoneException {
    String key = buildContainerKey(args.getVolumeName(), args.getBucketName(),
        args.getKeyName());
    XceiverClient xceiverClient = xceiverClientManager.acquireClient(key);
    boolean success = false;
    try {
      ContainerKeyData.Builder containerKeyData = ContainerKeyData
          .newBuilder()
          .setContainerName(xceiverClient.getPipeline().getContainerName())
          .setName(key);
      ReadKeyRequestProto.Builder readKeyRequest = ReadKeyRequestProto
          .newBuilder()
          .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
          .setContainerKeyData(containerKeyData);
      ContainerCommandRequestProto request = ContainerCommandRequestProto
          .newBuilder()
          .setCmdType(Type.Readkey)
          .setReadKey(readKeyRequest)
          .build();
      ContainerCommandResponseProto response =
          xceiverClient.sendCommand(request);
      ReadKeyResponeProto readKeyResponse = response.getReadKey();
      long length = 0;
      List<chunkInfo> chunks = readKeyResponse.getChunkDataList();
      for (chunkInfo chunk : chunks) {
        length += chunk.getLen();
      }
      success = true;
      return new LengthInputStream(new ChunkInputStream(
          key, xceiverClientManager, xceiverClient, chunks), length);
    } finally {
      if (!success) {
        xceiverClientManager.releaseClient(xceiverClient);
      }
    }
  }

  /**
   * Deletes an existing key.
   *
   * @param args KeyArgs
   * @throws OzoneException
   */
  @Override
  public void deleteKey(KeyArgs args) throws IOException, OzoneException {

  }

  /**
   * Returns a list of Key.
   *
   * @param args KeyArgs
   * @return BucketList
   * @throws IOException
   */
  @Override
  public ListKeys listKeys(ListArgs args) throws IOException, OzoneException {
    return null;
  }

  private static String buildContainerKey(String... parts) {
    return '/' + StringUtils.join('/', parts);
  }

  private static void createKey(XceiverClient xceiverClient,
      ContainerKeyData.Builder containerKeyData) throws IOException {
    CreateKeyRequestProto.Builder createKeyRequest = CreateKeyRequestProto
        .newBuilder()
        .setPipeline(xceiverClient.getPipeline().getProtobufMessage())
        .setContainerKeyData(containerKeyData);
    ContainerCommandRequestProto request = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.CreateKey)
        .setCreateKey(createKeyRequest)
        .build();
    xceiverClient.sendCommand(request);
  }

  private static String dateToString(Date date) {
    SimpleDateFormat sdf =
        new SimpleDateFormat(OzoneConsts.OZONE_DATE_FORMAT, Locale.US);
    sdf.setTimeZone(TimeZone.getTimeZone(OzoneConsts.OZONE_TIME_ZONE));
    return sdf.format(date);
  }

  private static KeyValue newKeyValue(String key, Object value) {
    return KeyValue.newBuilder().setKey(key).setValue(value.toString()).build();
  }
}
