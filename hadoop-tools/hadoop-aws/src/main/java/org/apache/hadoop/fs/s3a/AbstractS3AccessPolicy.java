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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

abstract class AbstractS3AccessPolicy {

  protected final S3Store s3Store;

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
}
