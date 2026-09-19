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
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRegisterOnlyBootstrapStatBuilder {

  private static final String COMMIT_TIME = "20240115000000";

  private HoodieFileStatus fileStatus(String uri, long length) {
    return HoodieFileStatus.newBuilder()
        .setPath(HoodiePath.newBuilder().setUri(uri).build())
        .setLength(length)
        .build();
  }

  @Test
  public void testStatCarriesExternalMarkerAndFileNameAsFileId() {
    List<HoodieWriteStat> stats = RegisterOnlyBootstrapStatBuilder.buildStats(
        Collections.singletonList(Pair.of("datestr=2022-06-15",
            Collections.singletonList(fileStatus("/src/table/datestr=2022-06-15/part-0.parquet", 4096L)))),
        COMMIT_TIME);

    assertEquals(1, stats.size());
    HoodieWriteStat stat = stats.get(0);
    assertEquals("part-0.parquet", stat.getFileId());
    assertEquals("datestr=2022-06-15", stat.getPartitionPath());
    assertEquals("datestr=2022-06-15/part-0.parquet_" + COMMIT_TIME + "_hudiext", stat.getPath());
    assertTrue(ExternalFilePathUtil.isExternallyCreatedFile(stat.getPath()));
    assertEquals(4096L, stat.getFileSizeInBytes());
    assertEquals(4096L, stat.getTotalWriteBytes());
  }

  @Test
  public void testMarkedPathResolvesBackToTheRealFile() {
    HoodieWriteStat stat = RegisterOnlyBootstrapStatBuilder.buildStats(
        Collections.singletonList(Pair.of("datestr=2022-06-15",
            Collections.singletonList(fileStatus("/src/table/datestr=2022-06-15/part-0.parquet", 1L)))),
        COMMIT_TIME).get(0);

    assertEquals("datestr=2022-06-15/part-0.parquet",
        ExternalFilePathUtil.getFilePathInPartition(stat.getPath()));
  }

  @Test
  public void testRecordCountsAreZeroSinceFilesAreNeverRead() {
    HoodieWriteStat stat = RegisterOnlyBootstrapStatBuilder.buildStats(
        Collections.singletonList(Pair.of("datestr=2022-06-15",
            Collections.singletonList(fileStatus("/src/table/datestr=2022-06-15/part-0.parquet", 1L)))),
        COMMIT_TIME).get(0);

    assertEquals(0, stat.getNumWrites());
    assertEquals(0, stat.getNumInserts());
    assertEquals(0, stat.getNumUpdateWrites());
    assertEquals(0, stat.getNumDeletes());
  }

  @Test
  public void testUnpartitionedTableOmitsPartitionPrefix() {
    HoodieWriteStat stat = RegisterOnlyBootstrapStatBuilder.buildStats(
        Collections.singletonList(Pair.of("", Collections.singletonList(fileStatus("/src/table/part-0.parquet", 1L)))),
        COMMIT_TIME).get(0);

    assertEquals("part-0.parquet_" + COMMIT_TIME + "_hudiext", stat.getPath());
    assertEquals("part-0.parquet", ExternalFilePathUtil.getFilePathInPartition(stat.getPath()));
  }

  @Test
  public void testStatsAreBuiltForEveryFileInEveryPartition() {
    List<HoodieWriteStat> stats = RegisterOnlyBootstrapStatBuilder.buildStats(Arrays.asList(
        Pair.of("datestr=2022-06-15", Arrays.asList(
            fileStatus("/src/t/datestr=2022-06-15/a.parquet", 1L),
            fileStatus("/src/t/datestr=2022-06-15/b.parquet", 2L))),
        Pair.of("datestr=2022-06-16", Collections.singletonList(
            fileStatus("/src/t/datestr=2022-06-16/c.parquet", 3L)))),
        COMMIT_TIME);

    assertEquals(3, stats.size());
    assertEquals(Arrays.asList("a.parquet", "b.parquet", "c.parquet"),
        stats.stream().map(HoodieWriteStat::getFileId).sorted().collect(java.util.stream.Collectors.toList()));
  }

  @Test
  public void testZeroByteAndUnsizedFilesAreSkipped() {
    // The metadata table rejects zero byte entries, so they must not reach it.
    HoodieFileStatus noLength = HoodieFileStatus.newBuilder()
        .setPath(HoodiePath.newBuilder().setUri("/src/t/p/no-length.parquet").build())
        .build();
    List<HoodieWriteStat> stats = RegisterOnlyBootstrapStatBuilder.buildStats(
        Collections.singletonList(Pair.of("p", Arrays.asList(
            noLength,
            fileStatus("/src/t/p/empty.parquet", 0L),
            fileStatus("/src/t/p/real.parquet", 512L)))),
        COMMIT_TIME);

    assertEquals(1, stats.size());
    assertEquals("real.parquet", stats.get(0).getFileId());
  }
}
