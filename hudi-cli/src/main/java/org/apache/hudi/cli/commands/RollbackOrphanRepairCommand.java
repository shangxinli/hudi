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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.rollback.RollbackOrphanDetector;

import lombok.extern.slf4j.Slf4j;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * CLI command to find and (optionally) delete parquet/log files left behind by
 * rollbacks that did not fully complete. See Apache Hudi issue #18783 for the
 * underlying class of bug this addresses.
 *
 * <p>Two scopes are scanned:
 * <ol>
 *   <li>Completed rollback instants in the active timeline.</li>
 *   <li>Completed rollback instants in the archived timeline (LSM in 1.x).
 *       This is the higher-value scope for the one-time-repair use case
 *       because it covers orphans created before the in-line archive
 *       precondition (see hoodie.archive.rollback.orphan.guard.mode) was
 *       enabled — but only while the archived rollback record is still
 *       readable (i.e., before LSM timeline TTL prunes it).</li>
 * </ol>
 *
 * <p>Defaults are conservative: {@code --dryrun=true}, {@code --mode=THOROUGH}.
 */
@ShellComponent
@Slf4j
public class RollbackOrphanRepairCommand {

  @ShellMethod(key = "repair rollback-orphans",
      value = "Find (and optionally delete) parquet/log files left over by partially-failed rollbacks.")
  public String repairRollbackOrphans(
      @ShellOption(value = "--dryrun", defaultValue = "true",
          help = "If true (default), only report orphans; do not delete.") boolean dryRun,
      @ShellOption(value = "--mode", defaultValue = "THOROUGH",
          help = "Detection mode: OFF | LIGHT | THOROUGH. THOROUGH lists partitions.") String modeStr,
      @ShellOption(value = "--include-archived", defaultValue = "true",
          help = "Also scan rollback instants on the archived timeline.") boolean includeArchived,
      @ShellOption(value = "--limit", defaultValue = "100",
          help = "Maximum number of rollback instants to inspect per scope.") int limit) throws IOException {
    RollbackOrphanDetector.Mode mode = RollbackOrphanDetector.Mode.parse(modeStr);
    if (mode == RollbackOrphanDetector.Mode.OFF) {
      return "mode=OFF is a no-op; pass --mode=LIGHT or --mode=THOROUGH.";
    }
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    RollbackOrphanDetector detector = new RollbackOrphanDetector();

    List<String[]> rows = new ArrayList<>();
    int totalOrphans = 0;
    int totalDeleted = 0;
    int activeScanned = 0;
    int archivedScanned = 0;

    List<HoodieInstant> activeRollbacks = metaClient.getActiveTimeline()
        .getRollbackTimeline().filterCompletedInstants().getInstants();
    for (HoodieInstant inst : activeRollbacks) {
      if (activeScanned >= limit) {
        break;
      }
      activeScanned++;
      Set<String> orphans = detector.detectOrphans(metaClient, inst, mode);
      totalOrphans += orphans.size();
      for (String orphan : orphans) {
        boolean deleted = !dryRun && deleteSafely(metaClient, orphan);
        if (deleted) {
          totalDeleted++;
        }
        rows.add(new String[]{"active", inst.requestedTime(), orphan,
            dryRun ? "DRY-RUN" : (deleted ? "DELETED" : "DELETE-FAILED")});
      }
    }

    if (includeArchived) {
      List<HoodieInstant> archivedRollbacks = metaClient.getArchivedTimeline().getInstantsAsStream()
          .filter(i -> HoodieTimeline.ROLLBACK_ACTION.equals(i.getAction()) && i.isCompleted())
          .collect(java.util.stream.Collectors.toList());
      for (HoodieInstant inst : archivedRollbacks) {
        if (archivedScanned >= limit) {
          break;
        }
        archivedScanned++;
        Set<String> orphans = detector.detectOrphans(metaClient, inst, mode);
        totalOrphans += orphans.size();
        for (String orphan : orphans) {
          boolean deleted = !dryRun && deleteSafely(metaClient, orphan);
          if (deleted) {
            totalDeleted++;
          }
          rows.add(new String[]{"archived", inst.requestedTime(), orphan,
              dryRun ? "DRY-RUN" : (deleted ? "DELETED" : "DELETE-FAILED")});
        }
      }
    }

    String table = HoodiePrintHelper.print(
        new String[]{"Scope", "RollbackInstant", "OrphanPath", "Action"},
        rows.toArray(new String[0][]));
    return table + "\n"
        + "Scanned active rollbacks: " + activeScanned + ", archived rollbacks: " + archivedScanned
        + " | orphans found: " + totalOrphans
        + (dryRun ? " (dry-run; no files deleted)" : " | deleted: " + totalDeleted);
  }

  private static boolean deleteSafely(HoodieTableMetaClient metaClient, String orphan) {
    try {
      return metaClient.getStorage().deleteFile(new StoragePath(orphan));
    } catch (IOException e) {
      log.warn("Failed to delete orphan {}", orphan, e);
      return false;
    }
  }
}
