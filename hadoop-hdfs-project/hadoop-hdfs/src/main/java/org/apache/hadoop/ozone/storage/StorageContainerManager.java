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

package org.apache.hadoop.ozone.storage;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.StorageContainerConfiguration;
import org.apache.hadoop.ozone.protocol.LocatedContainer;
import org.apache.hadoop.ozone.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.protocol.KeyHash;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.ozone.OzoneConfigKeys.*;

public class StorageContainerManager
    implements DatanodeProtocol, StorageContainerLocationProtocol {

  public static final Logger LOG =
      LoggerFactory.getLogger(StorageContainerManager.class);

  private final Namesystem ns = new StorageContainerNameService();
  private final BlockManager blockManager;

  /** The RPC server that listens to requests from DataNodes */
  private final RPC.Server serviceRpcServer;
  private final InetSocketAddress serviceRPCAddress;

  /** The RPC server that listens to requests from clients */
  protected final RPC.Server clientRpcServer;
  protected final InetSocketAddress clientRpcAddress;

  /** The RPC server that listens to requests from nodes to find containers */
  protected final RPC.Server storageRpcServer;
  protected final InetSocketAddress storageRpcAddress;

  public StorageContainerManager(StorageContainerConfiguration conf)
      throws IOException {
    boolean haEnabled = false;
    this.blockManager = new BlockManager(ns, haEnabled, conf);

    int handlerCount =
        conf.getInt(DFS_NAMENODE_HANDLER_COUNT_KEY,
            DFS_NAMENODE_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(conf, DatanodeProtocolPB.class,
        ProtobufRpcEngine.class);

    DatanodeProtocolServerSideTranslatorPB dnProtoPbTranslator =
        new DatanodeProtocolServerSideTranslatorPB(this);
    BlockingService dnProtoPbService = DatanodeProtocolProtos.DatanodeProtocolService
        .newReflectiveBlockingService(dnProtoPbTranslator);

    WritableRpcEngine.ensureInitialized();

    InetSocketAddress serviceRpcAddr = NameNode.getServiceAddress(conf, false);
    if (serviceRpcAddr != null) {
      String bindHost = conf.getTrimmed(DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY);
      if (bindHost == null || bindHost.isEmpty()) {
        bindHost = serviceRpcAddr.getHostName();
      }
      LOG.info("Service RPC server is binding to " + bindHost + ":" +
          serviceRpcAddr.getPort());

      int serviceHandlerCount =
          conf.getInt(DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY,
              DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT);
      serviceRpcServer = new RPC.Builder(conf)
          .setProtocol(
              org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB.class)
          .setInstance(dnProtoPbService)
          .setBindAddress(bindHost)
          .setPort(serviceRpcAddr.getPort())
          .setNumHandlers(serviceHandlerCount)
          .setVerbose(false)
          .setSecretManager(null)
          .build();

      DFSUtil.addPBProtocol(conf, DatanodeProtocolPB.class, dnProtoPbService,
          serviceRpcServer);

      InetSocketAddress listenAddr = serviceRpcServer.getListenerAddress();
      serviceRPCAddress = new InetSocketAddress(
          serviceRpcAddr.getHostName(), listenAddr.getPort());
      conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          NetUtils.getHostPortString(serviceRPCAddress));
    } else {
      serviceRpcServer = null;
      serviceRPCAddress = null;
    }

    InetSocketAddress rpcAddr = DFSUtilClient.getNNAddress(conf);
    String bindHost = conf.getTrimmed(DFS_NAMENODE_RPC_BIND_HOST_KEY);
    if (bindHost == null || bindHost.isEmpty()) {
      bindHost = rpcAddr.getHostName();
    }
    LOG.info("RPC server is binding to " + bindHost + ":" + rpcAddr.getPort());

    clientRpcServer = new RPC.Builder(conf)
        .setProtocol(
            org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB.class)
        .setInstance(dnProtoPbService)
        .setBindAddress(bindHost)
        .setPort(rpcAddr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(null)
        .build();

    DFSUtil.addPBProtocol(conf, DatanodeProtocolPB.class, dnProtoPbService,
        clientRpcServer);

    // The rpc-server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddr = clientRpcServer.getListenerAddress();
    clientRpcAddress = new InetSocketAddress(
        rpcAddr.getHostName(), listenAddr.getPort());
    conf.set(FS_DEFAULT_NAME_KEY, DFSUtilClient.getNNUri(clientRpcAddress)
        .toString());

    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);

    StorageContainerLocationProtocolServerSideTranslatorPB
        storageProtoPbTranslator =
        new StorageContainerLocationProtocolServerSideTranslatorPB(this);
    BlockingService storageProtoPbService =
        StorageContainerLocationProtocolProtos
        .StorageContainerLocationProtocolService
        .newReflectiveBlockingService(storageProtoPbTranslator);

    storageRpcAddress = NetUtils.createSocketAddr(
        conf.getTrimmed(DFS_STORAGE_RPC_ADDRESS_KEY,
            DFS_STORAGE_RPC_ADDRESS_DEFAULT), -1, DFS_STORAGE_RPC_ADDRESS_KEY);
    String storageRpcBindHost = conf.getTrimmed(DFS_STORAGE_RPC_BIND_HOST_KEY,
        DFS_STORAGE_RPC_BIND_HOST_DEFAULT);
    if (storageRpcBindHost == null || storageRpcBindHost.isEmpty()) {
      storageRpcBindHost = storageRpcAddress.getHostName();
    }
    LOG.info("StorageContainerLocationProtocol RPC server is binding to " +
        storageRpcBindHost + ":" + storageRpcAddress.getPort());

    storageRpcServer = new RPC.Builder(conf)
        .setProtocol(StorageContainerLocationProtocolPB.class)
        .setInstance(storageProtoPbService)
        .setBindAddress(storageRpcBindHost)
        .setPort(storageRpcAddress.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(null)
        .build();

    DFSUtil.addPBProtocol(conf, StorageContainerLocationProtocolPB.class,
        storageProtoPbService, storageRpcServer);
  }

  @Override
  public Set<LocatedContainer> getStorageContainerLocations(Set<KeyHash> hashes)
      throws IOException {
    LOG.trace("getStorageContainerLocations hashes = {}", hashes);
    return Collections.<LocatedContainer>emptySet();
  }

  @Override
  public DatanodeRegistration registerDatanode(DatanodeRegistration registration)
      throws IOException {
    ns.writeLock();
    try {
      blockManager.getDatanodeManager().registerDatanode(registration);
    } finally {
      ns.writeUnlock();
    }
    return registration;
  }

  @Override
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration registration,
     StorageReport[] reports, long dnCacheCapacity, long dnCacheUsed,
     int xmitsInProgress, int xceiverCount, int failedVolumes,
     VolumeFailureSummary volumeFailureSummary,
     boolean requestFullBlockReportLease) throws IOException {
    ns.readLock();
    try {
      long cacheCapacity = 0;
      long cacheUsed = 0;
      int maxTransfer = blockManager.getMaxReplicationStreams()
          - xmitsInProgress;
      DatanodeCommand[] cmds = blockManager.getDatanodeManager()
          .handleHeartbeat(registration, reports, ns.getBlockPoolId(),
              cacheCapacity, cacheUsed, xceiverCount, maxTransfer,
              failedVolumes, volumeFailureSummary);
      long txnId = 234;
      NNHAStatusHeartbeat haState = new NNHAStatusHeartbeat(
          HAServiceProtocol.HAServiceState.ACTIVE, txnId);
      RollingUpgradeInfo rollingUpgradeInfo = null;
      long blockReportLeaseId = requestFullBlockReportLease ?
          blockManager.requestBlockReportLeaseId(registration) : 0;
      return new HeartbeatResponse(cmds, haState, rollingUpgradeInfo,
          blockReportLeaseId);
    } finally {
      ns.readUnlock();
    }
  }

  @Override
  public DatanodeCommand blockReport(DatanodeRegistration registration,
     String poolId, StorageBlockReport[] reports, BlockReportContext context)
      throws IOException {
    for (int r = 0; r < reports.length; r++) {
      final BlockListAsLongs storageContainerList = reports[r].getBlocks();
      blockManager.processReport(registration, reports[r].getStorage(),
          storageContainerList, context, r == (reports.length - 1));
    }
    return null;
  }

  @Override
  public DatanodeCommand cacheReport(DatanodeRegistration registration,
     String poolId, List<Long> blockIds) throws IOException {
    // Centralized Cache Management is not supported
    return null;
  }

  @Override
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
     String poolId, StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks)
      throws IOException {
    for(StorageReceivedDeletedBlocks r : rcvdAndDeletedBlocks) {
      ns.writeLock();
      try {
        blockManager.processIncrementalBlockReport(registration, r);
      } finally {
        ns.writeUnlock();
      }
    }
  }

  @Override
  public void errorReport(DatanodeRegistration registration,
     int errorCode, String msg) throws IOException {
    String dnName =
        (registration == null) ? "Unknown DataNode" : registration.toString();

    if (errorCode == DatanodeProtocol.NOTIFY) {
      LOG.info("Error report from " + dnName + ": " + msg);
      return;
    }

    if (errorCode == DatanodeProtocol.DISK_ERROR) {
      LOG.warn("Disk error on " + dnName + ": " + msg);
    } else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
      LOG.warn("Fatal disk error on " + dnName + ": " + msg);
      blockManager.getDatanodeManager().removeDatanode(registration);
    } else {
      LOG.info("Error report from " + dnName + ": " + msg);
    }
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    ns.readLock();
    try {
      return unprotectedGetNamespaceInfo();
    } finally {
      ns.readUnlock();
    }
  }

  private NamespaceInfo unprotectedGetNamespaceInfo() {
    return new NamespaceInfo(1, "random", "random", 2,
                             NodeType.STORAGE_CONTAINER_SERVICE);
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    // It doesn't make sense to have LocatedBlock in this API.
    ns.writeLock();
    try {
      for (int i = 0; i < blocks.length; i++) {
        ExtendedBlock blk = blocks[i].getBlock();
        DatanodeInfo[] nodes = blocks[i].getLocations();
        String[] storageIDs = blocks[i].getStorageIDs();
        for (int j = 0; j < nodes.length; j++) {
          blockManager.findAndMarkBlockAsCorrupt(blk, nodes[j],
              storageIDs == null ? null: storageIDs[j],
              "client machine reported it");
        }
      }
    } finally {
      ns.writeUnlock();
    }
  }

  /**
   * Start client and service RPC servers.
   */
  void start() {
    clientRpcServer.start();
    if (serviceRpcServer != null) {
      serviceRpcServer.start();
    }
    storageRpcServer.start();
  }

  /**
   * Wait until the RPC servers have shutdown.
   */
  void join() throws InterruptedException {
    clientRpcServer.join();
    if (serviceRpcServer != null) {
      serviceRpcServer.join();
    }
    storageRpcServer.join();
  }

  @Override
  public void commitBlockSynchronization(ExtendedBlock block,
     long newgenerationstamp, long newlength, boolean closeFile,
     boolean deleteblock, DatanodeID[] newtargets, String[] newtargetstorages)
      throws IOException {
    // Not needed for the purpose of object store
    throw new UnsupportedOperationException();
  }

  public static void main(String [] argv) throws IOException {
    StorageContainerConfiguration conf = new StorageContainerConfiguration();
    StorageContainerManager scm = new StorageContainerManager(conf);
    scm.start();
    try {
      scm.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
