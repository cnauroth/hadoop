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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@code DirListingMetadata} models a directory listing stored in a
 * {@link MetadataStore}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DirListingMetadata <T extends FileStatus> {

  private final Path path;

  /** Using a map for fast find / remove with large directories. */
  private Map<Path, T> listMap = new HashMap<>();

  private boolean isAuthoritative;

  /**
   * Create a directory listing metadata container.
   * @param path Path of the directory.
   * @param listing Entries in the directory.
   * @param isAuthoritative true iff listing is the full
   *     contents of the directory, and the calling client reports
   *     that this may be cached as the full and authoritative
   *     listing of all files in the directory.
   */
  public DirListingMetadata(Path path, Collection<T> listing,
      boolean isAuthoritative) {
    this.path = path;
    if (listing != null) {
      for (T entry : listing) {
        listMap.put(entry.getPath(), entry);
      }
    }
    this.isAuthoritative  = isAuthoritative;
  }

  /**
   * @return {@code Path} of the directory that contains this listing.
   */
  public Path getPath() {
    return path;
  }

  /**
   * @return true iff this directory listing is full and authoritative
   * within the scope of the {@code MetadataStore} that returned it.
   */
  public boolean isAuthoritative() {
    return isAuthoritative;
  }

  /**
   * Lookup entry within this directory listing.  This may return null if
   * the {@code MetadataStore} only tracks a partial set of the directory
   * entries. In the case where {@link #isAuthoritative()} is true, however,
   * this function returns null iff the directory is known not to contain the
   * listing at given path (within the scope of the {@code MetadataStore} that
   * returned it).
   *
   * @param path of entry to look for
   * @return entry, or null if it is not present or not being tracked.
   */
  public T get(Path path) {
    return listMap.get(path);
  }

  /**
   * Add a file entry to the directory listing.  If this listing already
   * contains a {@code FileStatus} with the same path, it will be replaced.
   * @param fileStatus File entry to add to this directory listing.
   */
  public void put(T fileStatus) {
    // TODO assert that fileStatus is a proper child of path.
    // TODO unit test for this class?
    listMap.put(fileStatus.getPath(), fileStatus);
  }

  @Override
  public String toString() {
    return "DirListingMetadata{" +
        "path=" + path +
        ", listMap=" + listMap +
        ", isAuthoritative=" + isAuthoritative +
        '}';
  }
}
