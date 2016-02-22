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

package org.apache.hadoop.storagecontainer.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;


/**
 * A StorageContainer extends a block, reusing some fields and qualifying
 * the block with a blockpoolId. Thus it can be considered as the analog to
 * an HDFS ExtendedBlock.
 *
 * BlockPoolId, BlockId and Generation Stamp have the same meanings as they
 * do in an HDFS ExtendedBlock.
 *
 * Block length is not meaningful for storage containers so it cannot be
 * changed or queried. Internally it is always zero.
 * 
 * An additional field 'transactionId' is introduced to deal with pipeline
 * recovery.
 */
@InterfaceAudience.Private
public class StorageContainer extends Block {
  private long transactionId;
  private String blockPoolId;

  public StorageContainer(Block b) {
    super(b);
  }
  
  public StorageContainer(long containerId) {
    this(containerId, 0);
  }

  public StorageContainer(long containerId, long generationStamp) {
    super(containerId, 0, generationStamp);
  }

  public StorageContainer(final long containerId, final long len,
      final long generationStamp, long transactionId, String blockPoolId) {
    super(containerId, len, generationStamp);
    this.blockPoolId = blockPoolId;
    this.transactionId = transactionId;
  }

  /**
   * Return the block Id corresponding to this container. The ID is unique
   * only within a given block pool.
   *
   * @return id of the storage container.
   */
  public long getContainerId() {
    return getBlockId();
  }

  /**
   * Return the block pool that this container belongs to.
   * @return
   */
  public String getBlockPoolId() {
    return blockPoolId;
  }

  /**
   * Return the last successfully completed transaction ID that created
   * or updated this container.
   *
   * @return transaction ID corresponding to last create/update operation.
   */
  public long getTransactionId() {
    return transactionId;
  }

  /**
   * Update the transaction ID after a successful container update.
   *
   * @param transactionId new transaction ID.
   */
  public void setTransactionId(long transactionId) {
    this.transactionId = transactionId;
  }  

  @Override // Object
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (o == null || !(o instanceof StorageContainer)) {
      return false;
    }
    return super.equals(o);
  }

  /**
   * Unnecessary override to avoid warning.
   * @return
   */
  @Override // Object
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public long getNumBytes() {
    throw new UnsupportedOperationException(
        "The length of a storage container is not meaningful.");
  }

  @Override
  public void set(long blkid, long len, long genStamp) {
    throw new UnsupportedOperationException(
        "The length of a storage container cannot be updated.");
  }

  @Override
  public void setNumBytes(long len) {
    throw new UnsupportedOperationException(
        "The length of a storage container cannot be updated.");
  }
}
