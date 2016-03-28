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
package org.apache.hadoop.ozone;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_STORAGE_RPC_ADDRESS_KEY;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.storage.StorageContainerManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

public class MiniOzoneCluster extends MiniDFSCluster implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneCluster.class);

  private final OzoneConfiguration conf;
  private final StorageContainerManager scm;

  private MiniOzoneCluster(Builder builder, StorageContainerManager scm)
        throws IOException {
    super(builder);
    this.conf = builder.conf;
    this.scm = scm;
  }

  public static class Builder
      extends org.apache.hadoop.hdfs.MiniDFSCluster.Builder {

    private final OzoneConfiguration conf;

    public Builder(OzoneConfiguration conf) {
      super(conf);
      this.conf = conf;
      this.nnTopology(new MiniDFSNNTopology()); // No NameNode required
    }

    @Override
    public Builder numDataNodes(int val) {
      super.numDataNodes(val);
      return this;
    }

    @Override
    public MiniOzoneCluster build() throws IOException {
      conf.set(FS_DEFAULT_NAME_KEY, "hdfs://127.0.0.1:0");
      conf.set(DFS_STORAGE_RPC_ADDRESS_KEY, "127.0.0.1:0");
      StorageContainerManager scm = new StorageContainerManager(conf);
      scm.start();
      return new MiniOzoneCluster(this, scm);
    }
  }

  @Override
  public void close() {
    shutdown();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    LOG.info("Shutting down the Mini Ozone Cluster");
    if (scm == null) {
      return;
    }
    LOG.info("Shutting down the StorageContainerManager");
    scm.stop();
    scm.join();
  }

  public void waitOzoneReady() {
    long begin = Time.monotonicNow();
    while (scm.getDatanodeReport(DatanodeReportType.LIVE).length <
        numDataNodes) {
      if (Time.monotonicNow() - begin > 20000) {
        throw new IllegalStateException(
            "Timed out waiting for Ozone cluster to become ready.");
      }
      LOG.info("Waiting for Ozone cluster to become ready");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted while waiting for Ozone cluster to become ready.");
      }
    }
  }

  protected StorageContainerLocationProtocolClientSideTranslatorPB
      createStorageContainerLocationClient() throws IOException {
    long version = RPC.getProtocolVersion(
        StorageContainerLocationProtocolPB.class);
    InetSocketAddress address = scm.getStorageContainerLocationRpcAddress();
    return new StorageContainerLocationProtocolClientSideTranslatorPB(
        RPC.getProxy(StorageContainerLocationProtocolPB.class, version,
        address, UserGroupInformation.getCurrentUser(), conf,
        NetUtils.getDefaultSocketFactory(conf), Client.getTimeout(conf)));
  }
}
