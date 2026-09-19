/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.bootstrap;

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Refuses writes that would have to merge records into a {@link BootstrapMode#REGISTER_ONLY} partition.
 *
 * <p>Registering a partition records where its files are without reading them, so Hudi holds no record keys for
 * those rows. An upsert or delete cannot find the record it is meant to replace, and would silently write a
 * duplicate instead. Failing the write is the honest outcome.
 *
 * <p>Operations that replace a partition wholesale are allowed through: they do not need to locate existing
 * records, and they are how a partition is promoted out of this mode once its data is back in warm storage.
 */
public class RegisterOnlyPartitionGuard {

  private static final Set<WriteOperationType> WHOLE_PARTITION_OPERATIONS = Collections.unmodifiableSet(EnumSet.of(
      WriteOperationType.INSERT_OVERWRITE,
      WriteOperationType.INSERT_OVERWRITE_TABLE,
      WriteOperationType.DELETE_PARTITION));

  /**
   * Whether this check can reject anything at all, so callers can skip enumerating partitions when it cannot.
   */
  public static boolean appliesTo(HoodieTable<?, ?, ?, ?> table, WriteOperationType operationType) {
    return table.getMetaClient().getTableConfig().hasRegisterOnlyPartitions()
        && !WHOLE_PARTITION_OPERATIONS.contains(operationType);
  }

  /**
   * @throws HoodieException naming the offending partitions, if the operation would write records into any of them.
   */
  public static void assertWritable(HoodieTable<?, ?, ?, ?> table,
                                    WriteOperationType operationType,
                                    Collection<String> partitionPaths) {
    if (!appliesTo(table, operationType)) {
      return;
    }

    List<String> registerOnly = new ArrayList<>();
    for (String partitionPath : partitionPaths) {
      if (isRegisterOnly(table, partitionPath)) {
        registerOnly.add(partitionPath);
      }
    }
    if (registerOnly.isEmpty()) {
      return;
    }

    throw new HoodieException("Cannot " + operationType.value() + " in REGISTER_ONLY bootstrap partition(s) "
        + registerOnly + ". Those partitions were registered without their records being read, so Hudi cannot "
        + "locate the rows to merge. Re-bootstrap them with FULL_RECORD or METADATA_ONLY mode, or replace them "
        + "with insert_overwrite, to enable writes.");
  }

  /**
   * A partition is register-only when its files were never brought under the table: the metadata table points at
   * them where they already live, outside this table's base path.
   */
  private static boolean isRegisterOnly(HoodieTable<?, ?, ?, ?> table, String partitionPath) {
    StoragePath basePath = table.getMetaClient().getBasePath();
    return table.getBaseFileOnlyView().getLatestBaseFiles(partitionPath)
        .anyMatch(baseFile -> !isUnder(basePath, baseFile.getStoragePath()));
  }

  private static boolean isUnder(StoragePath basePath, StoragePath path) {
    URI base = basePath.toUri();
    URI candidate = path.toUri();
    // A different filesystem is by definition not under this table. Walking parents instead is not an option:
    // StoragePath.getParent() throws at the root rather than returning null.
    if (!Objects.equals(base.getScheme(), candidate.getScheme())
        || !Objects.equals(base.getAuthority(), candidate.getAuthority())) {
      return false;
    }
    String basePrefix = base.getPath().endsWith("/") ? base.getPath() : base.getPath() + "/";
    return candidate.getPath().startsWith(basePrefix);
  }
}
