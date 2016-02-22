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

import java.util.Set;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * Holds the nodes that currently host the container for an object key hash.
 */
public final class LocatedContainer {
  private final KeyHash keyHash;
  private final Set<DatanodeInfo> locations;

  /**
   * Creates a LocatedContainer.
   *
   * @param keyHash object key hash
   * @param locations nodes that currently host the container
   */
  public LocatedContainer(KeyHash keyHash, Set<DatanodeInfo> locations) {
    this.keyHash = keyHash;
    this.locations = locations;
  }

  /**
   * Returns the object key hash.
   *
   * @return object key hash
   */
  public KeyHash getKeyHash() {
    return this.keyHash;
  }

  /**
   * Returns the nodes that currently host the container.
   *
   * @return Set<DatanodeInfo> nodes that currently host the container
   */
  public Set<DatanodeInfo> getLocations() {
    return this.locations;
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == null) {
      return false;
    }
    if (!(otherObj instanceof LocatedContainer)) {
      return false;
    }
    LocatedContainer other = (LocatedContainer)otherObj;
    return this.keyHash == null ? other.keyHash == null :
        this.keyHash.equals(other.keyHash);
  }

  @Override
  public int hashCode() {
    return keyHash.hashCode();
  }
}
