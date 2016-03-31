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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerKeyData;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneConsts.Versioning;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.response.VolumeOwner;
import org.apache.hadoop.util.StringUtils;

final class OzoneContainerTranslation {

  public static ContainerKeyData fromBucketToContainerKeyData(
      String containerName, String containerKey, BucketArgs args) {
    ContainerKeyData.Builder containerKeyData = ContainerKeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
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

    return containerKeyData.build();
  }

  public static BucketInfo fromContainerKeyValueListToBucket(
      List<KeyValue> metadata, BucketArgs args) {
    BucketInfo bucketInfo = new BucketInfo();
    bucketInfo.setVolumeName(args.getVolumeName());
    bucketInfo.setBucketName(args.getBucketName());
    for (KeyValue keyValue : metadata) {
      switch (keyValue.getKey()) {
      case "BUCKET_VERSIONING":
        bucketInfo.setVersioning(
            Enum.valueOf(Versioning.class, keyValue.getValue()));
        break;
      case "STORAGE_TYPE":
        bucketInfo.setStorageType(
            Enum.valueOf(StorageType.class, keyValue.getValue()));
        break;
      }
    }
    return bucketInfo;
  }

  public static VolumeInfo fromContainerKeyValueListToVolume(
      List<KeyValue> metadata, VolumeArgs args) {
    VolumeInfo volumeInfo = new VolumeInfo();
    volumeInfo.setVolumeName(args.getVolumeName());
    for (KeyValue keyValue : metadata) {
      switch (keyValue.getKey()) {
      case "CreatedBy":
        volumeInfo.setCreatedBy(keyValue.getValue());
        break;
      case "Created":
        volumeInfo.setCreatedOn(keyValue.getValue());
        break;
      case "Owner":
        volumeInfo.setOwner(new VolumeOwner(keyValue.getValue()));
        break;
      case "Quota":
        volumeInfo.setQuota(new OzoneQuota(
            Integer.parseInt(keyValue.getValue()), OzoneQuota.Units.BYTES));
        break;
      }
    }
    return volumeInfo;
  }

  public static ContainerKeyData fromKeyToContainerKeyData(String containerName,
      String containerKey, KeyArgs args) {
    ContainerKeyData.Builder containerKeyData = ContainerKeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
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

    return containerKeyData.build();
  }

  public static ContainerKeyData fromVolumeToContainerKeyData(
      String containerName, String containerKey, VolumeArgs args) {
    ContainerKeyData.Builder containerKeyData = ContainerKeyData
        .newBuilder()
        .setContainerName(containerName)
        .setName(containerKey)
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

    return containerKeyData.build();
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

  private OzoneContainerTranslation() {
  }
}
