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

import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyData;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.ozone.OzoneConsts.Versioning;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.response.VolumeOwner;
import org.apache.hadoop.util.StringUtils;

final class OzoneContainerTranslation {

  private static final String ACLS = "ACLS";
  private static final String BUCKET = "BUCKET";
  private static final String BUCKET_NAME = "BUCKET_NAME";
  private static final String CREATED_BY = "CREATED_BY";
  private static final String CREATED_ON = "CREATED_ON";
  private static final String KEY = "KEY";
  private static final String OWNER = "OWNER";
  private static final String QUOTA = "QUOTA";
  private static final String STORAGE_TYPE = "STORAGE_TYPE";
  private static final String TYPE = "TYPE";
  private static final String VERSIONING = "VERSIONING";
  private static final String VOLUME = "VOLUME";
  private static final String VOLUME_NAME = "VOLUME_NAME";

  public static KeyData containerKeyDataForRead(String containerName,
      String containerKey) {
    return KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .build();
  }

  public static KeyData fromBucketToContainerKeyData(
      String containerName, String containerKey, BucketInfo bucket) {
    KeyData.Builder containerKeyData = KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .addMetadata(newKeyValue(TYPE, BUCKET))
        .addMetadata(newKeyValue(VOLUME_NAME, bucket.getVolumeName()))
        .addMetadata(newKeyValue(BUCKET_NAME, bucket.getBucketName()));

    if (bucket.getAcls() != null) {
      containerKeyData.addMetadata(newKeyValue(ACLS,
          StringUtils.join(',', bucket.getAcls())));
    }

    if (bucket.getVersioning() != null &&
        bucket.getVersioning() != Versioning.NOT_DEFINED) {
      containerKeyData.addMetadata(newKeyValue(VERSIONING,
          bucket.getVersioning().name()));
    }

    if (bucket.getStorageType() != StorageType.RAM_DISK) {
      containerKeyData.addMetadata(newKeyValue(STORAGE_TYPE,
          bucket.getStorageType().name()));
    }

    return containerKeyData.build();
  }

  public static BucketInfo fromContainerKeyValueListToBucket(
      List<KeyValue> metadata, BucketArgs args) {
    BucketInfo bucket = new BucketInfo();
    bucket.setVolumeName(args.getVolumeName());
    bucket.setBucketName(args.getBucketName());
    for (KeyValue keyValue : metadata) {
      switch (keyValue.getKey()) {
      case VERSIONING:
        bucket.setVersioning(
            Enum.valueOf(Versioning.class, keyValue.getValue()));
        break;
      case STORAGE_TYPE:
        bucket.setStorageType(
            Enum.valueOf(StorageType.class, keyValue.getValue()));
        break;
      }
    }
    return bucket;
  }

  public static VolumeInfo fromContainerKeyValueListToVolume(
      List<KeyValue> metadata, VolumeArgs args) {
    VolumeInfo volume = new VolumeInfo();
    volume.setVolumeName(args.getVolumeName());
    for (KeyValue keyValue : metadata) {
      switch (keyValue.getKey()) {
      case CREATED_BY:
        volume.setCreatedBy(keyValue.getValue());
        break;
      case CREATED_ON:
        volume.setCreatedOn(keyValue.getValue());
        break;
      case OWNER:
        volume.setOwner(new VolumeOwner(keyValue.getValue()));
        break;
      case QUOTA:
        volume.setQuota(new OzoneQuota(
            Integer.parseInt(keyValue.getValue()), OzoneQuota.Units.BYTES));
        break;
      }
    }
    return volume;
  }

  public static KeyData fromKeyToContainerKeyData(String containerName,
      String containerKey, KeyInfo key) {
    return KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .addMetadata(newKeyValue(TYPE, KEY))
        .build();
  }

  public static KeyData.Builder fromKeyToContainerKeyDataBuilder(
      String containerName, String containerKey, KeyInfo key) {
    return KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .addMetadata(newKeyValue(TYPE, KEY));
  }

  public static KeyData fromVolumeToContainerKeyData(
      String containerName, String containerKey, VolumeInfo volume) {
    KeyData.Builder containerKeyData = KeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
        .addMetadata(newKeyValue(TYPE, VOLUME))
        .addMetadata(newKeyValue(VOLUME_NAME, volume.getVolumeName()))
        .addMetadata(newKeyValue(CREATED_ON, volume.getCreatedOn()));

    if (volume.getQuota() != null && volume.getQuota().sizeInBytes() != -1L) {
      containerKeyData.addMetadata(
          newKeyValue(QUOTA, volume.getQuota().sizeInBytes()));
    }

    if (volume.getOwner() != null && volume.getOwner().getName() != null &&
        !volume.getOwner().getName().isEmpty()) {
      containerKeyData.addMetadata(newKeyValue(OWNER,
          volume.getOwner().getName()));
    }

    if (volume.getCreatedBy() != null && !volume.getCreatedBy().isEmpty()) {
      containerKeyData.addMetadata(
          newKeyValue(CREATED_BY, volume.getCreatedBy()));
    }

    return containerKeyData.build();
  }

  private static KeyValue newKeyValue(String key, Object value) {
    return KeyValue.newBuilder().setKey(key).setValue(value.toString()).build();
  }

  private OzoneContainerTranslation() {
  }
}
