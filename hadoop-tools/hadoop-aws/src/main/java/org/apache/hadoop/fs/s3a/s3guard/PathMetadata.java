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

package org.apache.hadoop.fs.s3a.s3guard;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;

/**
 * {@code PathMetadata} models path metadata stored in the
 * {@link MetadataStore}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class PathMetadata<T extends FileStatus> {

  private final T fileStatus;

  /**
   * Creates a new {@code PathMetadata} containing given {@code FileStatus}.
   *
   * @param fileStatus file status
   */
  public PathMetadata(T fileStatus) {
    Preconditions.checkNotNull(fileStatus);
    this.fileStatus = fileStatus;
  }

  /**
   * @return {@code FileStatus} contained in this {@code PathMetadata}.
   */
  public final T getFileStatus() {
    return fileStatus;
  }
}
