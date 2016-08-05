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

class DirectS3AccessPolicy extends AbstractS3AccessPolicy {

  public DirectS3AccessPolicy(S3Store s3Store)
      throws IOException {
    super(s3Store);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
      Path src, Path dst) throws IOException {
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return s3Store.create(f, permission, overwrite, bufferSize, replication,
        blockSize, progress);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return s3Store.delete(f, recursive);
  }

  @Override
  public S3AFileStatus getFileStatus(Path f) throws IOException {
    return s3Store.getFileStatus(f);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return s3Store.listStatus(f);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission)
      throws IOException {
    return s3Store.mkdirs(path, permission);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize)
      throws IOException {
    return s3Store.open(f, bufferSize);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return s3Store.rename(src, dst);
  }
}
