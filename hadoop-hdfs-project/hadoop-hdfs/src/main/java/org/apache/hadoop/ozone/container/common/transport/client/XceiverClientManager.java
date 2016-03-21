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

package org.apache.hadoop.ozone.container.common.transport.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.protocol.LocatedContainer;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;

/**
 * XceiverClientManager is responsible for the lifecycle of XceiverClient
 * instances.  Callers use this class to acquire an XceiverClient instance
 * connected to the desired container pipeline.  When done, the caller also uses
 * this class to release the previously acquired XceiverClient instance.
 *
 * This class may evolve to implement efficient lifecycle management policies by
 * caching container location information and pooling connected client instances
 * for reuse without needing to reestablish a socket connection.  The current
 * implementation simply allocates and closes a new instance every time.
 */
public class XceiverClientManager {

  private final OzoneConfiguration conf;
  private final StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocation;

  /**
   * Creates a new XceiverClientManager.
   *
   * @param conf configuration
   * @param storageContainerLocation client connected to the
   *     StorageContainerManager for lookup of container location information
   */
  public XceiverClientManager(OzoneConfiguration conf,
      StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocation) {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(storageContainerLocation);
    this.conf = conf;
    this.storageContainerLocation = storageContainerLocation;
  }

  /**
   * Acquires a XceiverClient connected to a container capable of storing the
   * specified key.
   *
   * @param key container key to request
   * @return XceiverClient connected to a container
   * @throws IOException if an XceiverClient cannot be acquired
   */
  public XceiverClient acquireClient(String key) throws IOException {
    Preconditions.checkNotNull(key);
    Preconditions.checkArgument(!key.isEmpty());
    Set<LocatedContainer> locatedContainers =
        storageContainerLocation.getStorageContainerLocations(
            new HashSet<>(Arrays.asList(key)));
    Pipeline pipeline = newPipelineFromLocatedContainer(locatedContainers);
    XceiverClient xceiverClient = new XceiverClient(pipeline, conf);
    try {
      xceiverClient.connect();
    } catch (Exception e) {
      // TODO
      throw new IOException("Exception connecting XceiverClient.", e);
    }
    return xceiverClient;
  }

  /**
   * Releases an XceiverClient after use.
   *
   * @param xceiverClient client to release
   */
  public void releaseClient(XceiverClient xceiverClient) {
    Preconditions.checkNotNull(xceiverClient);
    xceiverClient.close();
  }

  /**
   * Translates a set of container locations, ordered such that the first is the
   * leader, into a corresponding Pipeline object.
   *
   * @param locatedContainers container locations
   */
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
