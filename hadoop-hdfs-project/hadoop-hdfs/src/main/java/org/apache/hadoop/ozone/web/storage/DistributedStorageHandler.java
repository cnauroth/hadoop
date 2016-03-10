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
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerKeyData;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Type;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.helpers.Pipeline;
import org.apache.hadoop.ozone.container.transport.client.XceiverClient;
import org.apache.hadoop.ozone.protocol.LocatedContainer;
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
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

/**
 * A {@link StorageHandler} implementation that distributes object storage
 * across the nodes of an HDFS cluster.
 */
public final class DistributedStorageHandler implements StorageHandler {

  private final OzoneConfiguration conf;
  private final StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocation;

  public DistributedStorageHandler(OzoneConfiguration conf,
      StorageContainerLocationProtocolClientSideTranslatorPB storageContainerLocation) {
    this.conf = conf;
    this.storageContainerLocation = storageContainerLocation;
  }

  @Override
  public void createVolume(final VolumeArgs args) throws
      IOException, OzoneException {
    final String key = buildContainerKey(args.getVolumeName());
    doContainerOperation(key, new Operation<Void>() {
        @Override
        public Void call(XceiverClient xceiverClient, Pipeline pipeline)
            throws IOException, OzoneException {
          ContainerKeyData.Builder containerKeyData = ContainerKeyData
              .newBuilder()
              .setContainerName(pipeline.getContainerName())
              .setName(key)
              .addMetadata(newKeyValue("Key", "VOLUME"))
              .addMetadata(newKeyValue("Created", dateToString(new Date())));

          if (args.getQuota() != null && args.getQuota().sizeInBytes() != -1L) {
            containerKeyData.addMetadata(
                newKeyValue("Quota", args.getQuota().sizeInBytes()));
          }

          if (args.getUserName() != null && !args.getUserName().isEmpty()) {
            containerKeyData.addMetadata(
                newKeyValue("Owner", args.getUserName()));
          }

          if (args.getAdminName() != null && !args.getAdminName().isEmpty()) {
            containerKeyData.addMetadata(
                newKeyValue("CreatedBy", args.getAdminName()));
          }

          createKey(xceiverClient, pipeline, containerKeyData);
          return null;
        }
    });
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
    final String key = buildContainerKey(args.getVolumeName(),
        args.getBucketName());
    doContainerOperation(key, new Operation<Void>() {
        @Override
        public Void call(XceiverClient xceiverClient, Pipeline pipeline)
            throws IOException, OzoneException {
          ContainerKeyData.Builder containerKeyData = ContainerKeyData
              .newBuilder()
              .setContainerName(pipeline.getContainerName())
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

          createKey(xceiverClient, pipeline, containerKeyData);
          return null;
        }
    });
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
    final String key = buildContainerKey(args.getVolumeName(),
        args.getBucketName(), args.getKeyName());
    return doContainerOperation(key, new Operation<OutputStream>() {
        @Override
        public OutputStream call(XceiverClient xceiverClient, Pipeline pipeline)
            throws IOException, OzoneException {
          return null;
        }
    });
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
    final String key = buildContainerKey(args.getVolumeName(),
        args.getBucketName(), args.getKeyName());
    doContainerOperation(key, new Operation<Void>() {
        @Override
        public Void call(XceiverClient xceiverClient, Pipeline pipeline)
            throws IOException, OzoneException {
          return null;
        }
    });
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
    final String key = buildContainerKey(args.getVolumeName(),
        args.getBucketName(), args.getKeyName());
    return doContainerOperation(key, new Operation<LengthInputStream>() {
        @Override
        public LengthInputStream call(XceiverClient xceiverClient,
            Pipeline pipeline) throws IOException, OzoneException {
          return null;
        }
    });
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

  private interface Operation<T> {
    T call(XceiverClient xceiverClient, Pipeline pipeline) throws IOException,
        OzoneException;
  }

  private static String buildContainerKey(String... parts) {
    return '/' + StringUtils.join('/', parts);
  }

  private <T> T doContainerOperation(String key, Operation<T> operation)
      throws IOException, OzoneException {
    Set<LocatedContainer> locatedContainers =
        storageContainerLocation.getStorageContainerLocations(
            new HashSet<>(Arrays.asList(key)));
    Pipeline pipeline = newPipelineFromLocatedContainer(locatedContainers);
    try (XceiverClient xceiverClient = new XceiverClient(pipeline, conf)) {
      try {
        xceiverClient.connect();
      } catch (Exception e) {
        throw new IOException("Exception connecting XceiverClient.", e);
      }
      return operation.call(xceiverClient, pipeline);
    }
  }

  private static void createKey(XceiverClient xceiverClient, Pipeline pipeline,
      ContainerKeyData.Builder containerKeyData) throws IOException {
    CreateKeyRequestProto createKeyRequest = CreateKeyRequestProto
        .newBuilder()
        .setPipeline(pipeline.getProtobufMessage())
        .setContainerKeyData(containerKeyData)
        .build();
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

  private static Pipeline newPipelineFromLocatedContainer(
      Set<LocatedContainer> locatedContainers) {
    LocatedContainer locatedContainer = locatedContainers.iterator().next();
    Set<DatanodeInfo> locations = locatedContainer.getLocations();
    String leaderId = locations.iterator().next().getDatanodeUuid();
    Pipeline pipeline = new Pipeline(leaderId);
    for (DatanodeInfo location : locations) {
      pipeline.addMember(location);
    }
    pipeline.setContainerName(locatedContainer.getContainerName());
    return pipeline;
  }
}
