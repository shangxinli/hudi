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

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Turns the file listing of a {@link BootstrapMode#REGISTER_ONLY} partition into write stats, so that the files
 * are recorded in the metadata table's FILES partition and become visible to queries.
 *
 * <p>Nothing here opens a file. Every stat is derived from the listing alone, which is the point of the mode:
 * the partition may sit in cold storage where reading contents would trigger an expensive retrieval.
 *
 * <p>The files were not written by Hudi and so do not carry Hudi's {@code fileId_writeToken_instantTime} naming.
 * They are recorded using the external-file convention from {@link ExternalFilePathUtil}: the stat path carries a
 * {@code _<commitTime>_hudiext} marker, and the file group id is the file's own name, assigned once here at
 * registration. The read path strips the marker to recover the real path.
 */
@Slf4j
public class RegisterOnlyBootstrapStatBuilder {

  /**
   * Builds one write stat per file across all given partitions.
   *
   * @param partitions  partition path paired with the files discovered in it
   * @param commitTime  instant time of the bootstrap commit registering these files
   * @return write stats ready to be committed
   */
  public static List<HoodieWriteStat> buildStats(List<Pair<String, List<HoodieFileStatus>>> partitions, String commitTime) {
    List<HoodieWriteStat> stats = new ArrayList<>();
    for (Pair<String, List<HoodieFileStatus>> partition : partitions) {
      for (HoodieFileStatus fileStatus : partition.getValue()) {
        // The metadata table rejects zero byte entries, and such a file holds no rows to lose. Skipping it here
        // keeps the bootstrap going rather than failing it deep inside the metadata writer.
        if (fileStatus.getLength() == null || fileStatus.getLength() <= 0) {
          log.warn("Skipping zero byte file {} in REGISTER_ONLY partition {}",
              fileStatus.getPath().getUri(), partition.getKey());
          continue;
        }
        stats.add(buildStat(partition.getKey(), fileStatus, commitTime));
      }
    }
    return stats;
  }

  private static HoodieWriteStat buildStat(String partitionPath, HoodieFileStatus fileStatus, String commitTime) {
    String fileName = new StoragePath(fileStatus.getPath().getUri()).getName();
    long fileSize = fileStatus.getLength();

    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(fileName);
    stat.setPartitionPath(partitionPath);
    stat.setPath(markedPathInTable(partitionPath, fileName, commitTime));
    stat.setFileSizeInBytes(fileSize);
    stat.setTotalWriteBytes(fileSize);
    // Record counts are deliberately left at zero: establishing them would mean reading the file, which is
    // exactly the cost this mode exists to avoid.
    stat.setNumWrites(0);
    stat.setNumInserts(0);
    stat.setNumUpdateWrites(0);
    stat.setNumDeletes(0);
    stat.setTotalWriteErrors(0);
    return stat;
  }

  private static String markedPathInTable(String partitionPath, String fileName, String commitTime) {
    String pathInTable = partitionPath.isEmpty() ? fileName : partitionPath + StoragePath.SEPARATOR + fileName;
    return ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(pathInTable, commitTime);
  }
}
