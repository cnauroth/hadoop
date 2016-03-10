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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_METADATA_RPC_ADDRESS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_METADATA_RPC_ADDRESS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_METADATA_RPC_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_METADATA_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_METADATA_RPC_DEFAULT_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_OBJECTSTORE_TRACE_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_OBJECTSTORE_TRACE_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_RPC_ADDRESS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_RPC_DEFAULT_PORT;
import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS;
import static com.sun.jersey.api.core.ResourceConfig.FEATURE_TRACE;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.BlockingService;

import com.sun.jersey.api.container.ContainerFactory;
import com.sun.jersey.api.core.ApplicationAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.metadata.Bucket;
import org.apache.hadoop.ozone.metadata.Key;
import org.apache.hadoop.ozone.protocol.MetadataProtocol;
import org.apache.hadoop.ozone.protocol.proto.MetadataProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.MetadataProtocolPB;
import org.apache.hadoop.ozone.protocolPB.MetadataProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.ozone.web.handlers.ServiceFilter;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.ObjectStoreApplication;
import org.apache.hadoop.ozone.web.netty.ObjectStoreJerseyContainer;
import org.apache.hadoop.ozone.web.storage.DistributedStorageHandler;
import org.apache.hadoop.ozone.web.localstorage.LocalStorageHandler;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implements object store handling within the DataNode process.  This class is
 * responsible for initializing and maintaining the RPC clients and servers and
 * the web application required for the object store implementation.
 */
public final class ObjectStoreHandler implements MetadataProtocol, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectStoreJerseyContainer.class);

  private final RPC.Server metadataRpcServer;
  private final ObjectStoreJerseyContainer objectStoreJerseyContainer;
  private final StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  /**
   * Creates a new ObjectStoreHandler.
   *
   * @param conf configuration
   * @throws IOException if there is an I/O error
   */
  public ObjectStoreHandler(Configuration conf) throws IOException {
    String shType = conf.getTrimmed(DFS_STORAGE_HANDLER_TYPE_KEY,
        DFS_STORAGE_HANDLER_TYPE_DEFAULT);
    LOG.info("ObjectStoreHandler initializing with {}: {}",
        DFS_STORAGE_HANDLER_TYPE_KEY, shType);
    boolean ozoneTrace = conf.getBoolean(DFS_OBJECTSTORE_TRACE_ENABLED_KEY,
        DFS_OBJECTSTORE_TRACE_ENABLED_DEFAULT);
    final StorageHandler storageHandler;

    // Initialize metadata RPC server.
    if ("distributed".equalsIgnoreCase(shType)) {
      RPC.setProtocolEngine(conf, MetadataProtocolPB.class,
          ProtobufRpcEngine.class);
      MetadataProtocolServerSideTranslatorPB metadataProtoPbTranslator =
          new MetadataProtocolServerSideTranslatorPB(this);
      BlockingService metadataProtoPbService = MetadataProtocolProtos
          .MetadataProtocolService.newReflectiveBlockingService(
          metadataProtoPbTranslator);
      InetSocketAddress metadataRpcAddress = NetUtils.createSocketAddr(
          conf.getTrimmed(DFS_METADATA_RPC_ADDRESS_KEY,
              DFS_METADATA_RPC_ADDRESS_DEFAULT), DFS_METADATA_RPC_DEFAULT_PORT,
              DFS_METADATA_RPC_ADDRESS_KEY);
      String metadataRpcBindHost = conf.getTrimmed(
          DFS_METADATA_RPC_BIND_HOST_KEY, DFS_METADATA_RPC_BIND_HOST_DEFAULT);
      if (metadataRpcBindHost == null || metadataRpcBindHost.isEmpty()) {
        metadataRpcBindHost = metadataRpcAddress.getHostName();
      }
      LOG.info("MetdataProtocol RPC server is binding to {}:{}.",
          metadataRpcBindHost, metadataRpcAddress.getPort());
      this.metadataRpcServer = new RPC.Builder(conf)
          .setProtocol(MetadataProtocolPB.class)
          .setInstance(metadataProtoPbService)
          .setBindAddress(metadataRpcBindHost)
          .setPort(metadataRpcAddress.getPort())
          .setNumHandlers(10)
          .setVerbose(false)
          .setSecretManager(null)
          .build();
      this.metadataRpcServer.start();
    } else {
      this.metadataRpcServer = null;
    }

    // Initialize Jersey container for object store web application.
    if ("distributed".equalsIgnoreCase(shType)) {
      long version = RPC.getProtocolVersion(StorageContainerLocationProtocolPB.class);
      InetSocketAddress address = conf.getSocketAddr(
          DFS_STORAGE_RPC_BIND_HOST_KEY, DFS_STORAGE_RPC_ADDRESS_KEY,
          DFS_STORAGE_RPC_ADDRESS_DEFAULT, DFS_STORAGE_RPC_DEFAULT_PORT);
      this.storageContainerLocationClient =
          new StorageContainerLocationProtocolClientSideTranslatorPB(
              RPC.getProxy(StorageContainerLocationProtocolPB.class, version,
              address, UserGroupInformation.getCurrentUser(), conf,
              NetUtils.getDefaultSocketFactory(conf), Client.getTimeout(conf)));
      storageHandler = new DistributedStorageHandler(new OzoneConfiguration(),
          this.storageContainerLocationClient);
    } else {
      if ("local".equalsIgnoreCase(shType)) {
        storageHandler = new LocalStorageHandler(conf);
        this.storageContainerLocationClient = null;
      } else {
        throw new IllegalArgumentException(
            String.format("Unrecognized value for %s: %s",
                DFS_STORAGE_HANDLER_TYPE_KEY, shType));
      }
    }
    ApplicationAdapter aa =
        new ApplicationAdapter(new ObjectStoreApplication());
    Map<String, Object> settingsMap = new HashMap<>();
    settingsMap.put(PROPERTY_CONTAINER_REQUEST_FILTERS,
        ServiceFilter.class.getCanonicalName());
    settingsMap.put(FEATURE_TRACE, ozoneTrace);
    aa.setPropertiesAndFeatures(settingsMap);
    this.objectStoreJerseyContainer = ContainerFactory.createContainer(
        ObjectStoreJerseyContainer.class, aa);
    this.objectStoreJerseyContainer.setStorageHandler(storageHandler);
  }

  @Override
  public Bucket getBucketMetadata(Key bucketName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the initialized web application container.
   *
   * @return initialized web application container
   */
  public ObjectStoreJerseyContainer getObjectStoreJerseyContainer() {
    return this.objectStoreJerseyContainer;
  }

  @Override
  public void close() {
    LOG.info("Closing ObjectStoreHandler.");
    if (this.storageContainerLocationClient != null) {
      this.storageContainerLocationClient.close();
    }
    if (this.metadataRpcServer != null) {
      try {
        this.metadataRpcServer.join();
      } catch (InterruptedException e) {
        LOG.info("Interrupted while closing ObjectStoreHandler.");
        Thread.currentThread().interrupt();
      }
    }
  }
}
