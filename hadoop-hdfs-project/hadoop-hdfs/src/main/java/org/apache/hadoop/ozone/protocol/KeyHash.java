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

package org.apache.hadoop.ozone.protocol;

import org.apache.hadoop.hdfs.util.LongBitFormat;

/**
 * A KeyHash identifies the canister that holds an object.  It is a 64-bit value
 * consisting of:
 * <ul>
 * <li>28-bit bucket ID</li>
 * <li>4-bit canister type</li>
 * <li>32-bit canister ID within the bucket</li>
 * </ul>
 */
public final class KeyHash {
  private final long hash;

  /**
   * Creates a KeyHash.
   *
   * @param hash value
   */
  public KeyHash(long hash) {
    this.hash = hash;
  }

  /**
   * Creates a KeyHash.
   *
   * @param hash value
   */
  public KeyHash(int bucketId, CanisterType canisterType, int canisterId) {
    long hash = 0;
    hash = KeyHashFormat.BUCKET_ID.bits.combine(bucketId, hash);
    hash = KeyHashFormat.CANISTER_TYPE.bits.combine(canisterType.ordinal(),
        hash);
    hash = KeyHashFormat.CANISTER_ID.bits.combine(canisterId, hash);
    this.hash = hash;
  }

  /**
   * Returns the key hash value.
   *
   * @return key hash value
   */
  public long toLong() {
    return this.hash;
  }

  /**
   * Returns the bucket ID.
   *
   * @return bucket ID
   */
  public int getBucketId() {
    return (int)KeyHashFormat.BUCKET_ID.bits.retrieve(this.hash);
  }

  /**
   * Returns the canister type.
   *
   * @return canister type
   */
  public CanisterType getCanisterType() {
    return CanisterType.fromOrdinal(
        (byte)KeyHashFormat.CANISTER_TYPE.bits.retrieve(this.hash));
  }

  /**
   * Returns the canister ID.
   *
   * @return canister ID
   */
  public int getCanisterId() {
    return (int)KeyHashFormat.CANISTER_ID.bits.retrieve(this.hash);
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == null) {
      return false;
    }
    if (!(otherObj instanceof KeyHash)) {
      return false;
    }
    KeyHash other = (KeyHash)otherObj;
    return this.hash == other.hash;
  }

  @Override
  public int hashCode() {
    // Same implementation as Long#hashCode, but no boxing.
    return (int)(this.hash ^ (this.hash >>> 32));
  }

  /**
   * Bit format describing how we pack the components of the key hash into a
   * 64-bit long.
   */
  private static enum KeyHashFormat {
    BUCKET_ID(null, 28),
    CANISTER_TYPE(BUCKET_ID.bits, 4),
    CANISTER_ID(CANISTER_TYPE.bits, 32);

    final LongBitFormat bits;

    private KeyHashFormat(LongBitFormat previous, int length) {
      this.bits = new LongBitFormat(name(), previous, length, 0);
    }
  }
}
