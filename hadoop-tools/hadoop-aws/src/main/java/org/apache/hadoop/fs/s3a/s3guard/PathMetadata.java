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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

/**
 * {@code PathMetadata} models path metadata stored in the
 * {@link MetadataStore}.  The class provides different factory methods to
 * create paths of different types: directories, files, etc.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
abstract class PathMetadata {

  /**
   * Creates a new {@code PathMetadata} representing a directory.
   *
   * @param path the directory path
   * @return metadata for the directory
   */
  public static PathMetadata newDirectory(Path path) {
    return new DirectoryPathMetadata(path);
  }

  /**
   * Creates a new {@code PathMetadata} representing a file.
   *
   * @param path the file path
   * @return metadata for the file
   */
  public static PathMetadata newFile(Path path) {
    return new FilePathMetadata(path);
  }

  /**
   * Returns path.
   *
   * @return path
   */
  public final Path getPath() {
    return path;
  }

  /**
   * Returns {@code true} if the path is a directory.
   *
   * @return {@code true} if the path is a directory
   */
  public abstract boolean isDirectory();

  @Override
  public final String toString() {
    return new StringBuilder(getClass().getSimpleName())
        .append('{')
        .append("path=").append(path)
        .append('}')
        .toString();
  }

  private final Path path;

  private PathMetadata(Path path) {
    this.path = path;
  }

  private static final class DirectoryPathMetadata extends PathMetadata {

    public DirectoryPathMetadata(Path path) {
      super(path);
    }

    @Override
    public boolean isDirectory() {
      return true;
    }
  }

  private static final class FilePathMetadata extends PathMetadata {

    public FilePathMetadata(Path path) {
      super(path);
    }

    @Override
    public boolean isDirectory() {
      return false;
    }
  }
}
