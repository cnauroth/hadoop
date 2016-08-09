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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Implements an access policy for file system data sourced from S3.  A specific
 * access policy implementation coordinates between {@link S3Store} for access
 * to data sourced from an S3 bucket and optionally other data sources, such as
 * a secondary metadata repository.  The specifics of that coordination control
 * consistency and caching semantics of {@link S3AFileSystem} calls performed by
 * clients.  In general, the methods of this class will mirror the methods
 * defined on {@link org.apache.hadoop.fs.FileSystem} for any method that
 * requires customization to achieve consistency and caching goals.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
abstract class AbstractS3AccessPolicy {

  /**
   * An {@link S3Store} that policy subclasses use to interact with S3.
   */
  protected final S3Store s3Store;

  /**
   * Creates an AbstractS3AccessPolicy.
   *
   * @param s3Store S3Store used to interact with S3
   */
  protected AbstractS3AccessPolicy(S3Store s3Store)
      throws IOException {
    this.s3Store = s3Store;
  }

  public abstract void copyFromLocalFile(boolean delSrc, boolean overwrite,
      Path src, Path dst) throws IOException;

  public abstract FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException;

  public abstract boolean delete(Path f, boolean recursive) throws IOException;

  public abstract S3AFileStatus getFileStatus(Path f) throws IOException;

  public abstract FileStatus[] listStatus(Path f) throws IOException;

  public abstract boolean mkdirs(Path path, FsPermission permission)
      throws IOException;

  public abstract FSDataInputStream open(Path f, int bufferSize)
      throws IOException;

  public abstract boolean rename(Path src, Path dst) throws IOException;

  @Override
  public abstract String toString();
}
