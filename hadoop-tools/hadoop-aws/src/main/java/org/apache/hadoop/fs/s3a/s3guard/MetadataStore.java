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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

/**
 * {@code MetadataStore} defines the set of operations that any metadata store
 * implementation must provide.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class MetadataStore implements Closeable {

  /**
   * {@link Logger} usable by subclasses.
   */
  protected static final Logger LOG = S3AFileSystem.LOG;

  /**
   * Performs one-time initialization of the metadata store.
   *
   * @param s3afs file system that will use this metadata store
   * @throws IOException if there is an error
   */
  public abstract void initialize(S3AFileSystem s3afs) throws IOException;

  /**
   * Deletes exactly one path.
   *
   * @param path the path to delete
   * @throws IOException if there is an error
   */
  public abstract void delete(Path path) throws IOException;

  /**
   * Deletes the entire sub-tree rooted at the given path.
   *
   * @param path the root of the sub-tree to delete
   * @throws IOException if there is an error
   */
  public abstract void deleteSubtree(Path path) throws IOException;

  /**
   * Gets metadata for a path.
   *
   * @param path the path to get
   * @return metadata for {@code path}, {@code null} if not found
   * @throws IOException if there is an error
   */
  public abstract PathMetadata get(Path path) throws IOException;

  /**
   * Lists metadata for all direct children of a path.
   *
   * @param path the path to list
   * @return metadata for all direct children of {@code path}, not {@code null},
   *     but possibly empty
   * @throws IOException if there is an error
   */
  public abstract List<PathMetadata> listChildren(Path path) throws IOException;

  /**
   * Moves metadata from {@code src} to {@code dst}, including all descendants
   * recursively.
   *
   * @param src the source path
   * @param dst the destination path
   * @throws IOException if there is an error
   */
  public abstract void move(Path src, Path dst) throws IOException;

  /**
   * Saves metadata for exactly one path.  For a deeply nested path, this method
   * will not automatically create the full ancestry.  Callers need to ensure
   * saving the full path ancestry.
   *
   * @param meta the metadata to save
   * @throws IOException if there is an error
   */
  public abstract void put(PathMetadata meta) throws IOException;
}
