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

package org.apache.hudi.client.bootstrap.selector;

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Assigns a bootstrap mode to each partition based on how old the partition's date is, so that a table whose
 * history spans storage tiers can be onboarded in one pass: recent partitions are rewritten, older ones are
 * bootstrapped from their record keys, and the coldest ones are registered without their contents being read.
 *
 * <p>Partitions within {@code hoodie.bootstrap.mode.selector.days.full_record} days of today are assigned
 * {@link BootstrapMode#FULL_RECORD}, those within
 * {@code hoodie.bootstrap.mode.selector.days.metadata_only} days are assigned
 * {@link BootstrapMode#METADATA_ONLY}, and anything older is assigned {@link BootstrapMode#REGISTER_ONLY}.
 *
 * <p>The date is read from the Hive style partition path segment named by
 * {@code hoodie.bootstrap.mode.selector.partition.date.field}, for example {@code datestr=2024-01-15}. Tables
 * that are not partitioned by a single date field cannot use this selector.
 */
@Slf4j
public class DateBasedBootstrapModeSelector extends BootstrapModeSelector {

  private static final long serialVersionUID = 1L;

  private final int fullRecordDays;
  private final int metadataOnlyDays;
  private final String dateFormat;
  private final String dateField;

  public DateBasedBootstrapModeSelector(HoodieWriteConfig writeConfig) {
    super(writeConfig);
    this.fullRecordDays = writeConfig.getBootstrapDateSelectorFullRecordDays();
    this.metadataOnlyDays = writeConfig.getBootstrapDateSelectorMetadataOnlyDays();
    this.dateFormat = writeConfig.getBootstrapDateSelectorPartitionDateFormat();
    this.dateField = writeConfig.getBootstrapDateSelectorPartitionDateField();
    if (StringUtils.isNullOrEmpty(dateField)) {
      throw new HoodieException("hoodie.bootstrap.mode.selector.partition.date.field must be set to use "
          + DateBasedBootstrapModeSelector.class.getSimpleName());
    }
    if (fullRecordDays > metadataOnlyDays) {
      throw new HoodieException(String.format(
          "hoodie.bootstrap.mode.selector.days.full_record (%d) must not exceed "
              + "hoodie.bootstrap.mode.selector.days.metadata_only (%d)", fullRecordDays, metadataOnlyDays));
    }
    log.info("Bootstrap tiers: FULL_RECORD within {} days, METADATA_ONLY within {} days, REGISTER_ONLY beyond that. "
        + "Partition date field {}, format {}", fullRecordDays, metadataOnlyDays, dateField, dateFormat);
  }

  @Override
  public Map<BootstrapMode, List<String>> select(List<Pair<String, List<HoodieFileStatus>>> partitions) {
    LocalDate today = LocalDate.now();
    return partitions.stream()
        .map(p -> Pair.of(modeFor(p.getKey(), today), p.getKey()))
        .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toList())));
  }

  private BootstrapMode modeFor(String partitionPath, LocalDate today) {
    long ageInDays = ChronoUnit.DAYS.between(parsePartitionDate(partitionPath), today);
    if (ageInDays <= fullRecordDays) {
      return BootstrapMode.FULL_RECORD;
    }
    return ageInDays <= metadataOnlyDays ? BootstrapMode.METADATA_ONLY : BootstrapMode.REGISTER_ONLY;
  }

  /**
   * Pulls the date out of the Hive style segment named by {@code dateField}. Anything this selector cannot
   * date is an error rather than a default, so that a mis-partitioned table fails the bootstrap instead of
   * silently rewriting every partition in the most expensive mode.
   */
  private LocalDate parsePartitionDate(String partitionPath) {
    String prefix = dateField + "=";
    String value = null;
    for (String segment : partitionPath.split("/")) {
      if (segment.startsWith(prefix)) {
        value = segment.substring(prefix.length());
        break;
      }
    }
    if (value == null) {
      throw new HoodieException(String.format(
          "Partition path '%s' has no '%s' segment, so %s cannot assign it a bootstrap mode. This selector only "
              + "supports tables partitioned by a single date field.",
          partitionPath, prefix, DateBasedBootstrapModeSelector.class.getSimpleName()));
    }
    try {
      return LocalDate.parse(value, DateTimeFormatter.ofPattern(dateFormat));
    } catch (DateTimeParseException e) {
      throw new HoodieException(String.format(
          "Partition path '%s' carries date '%s', which does not match the configured format '%s'.",
          partitionPath, value, dateFormat), e);
    }
  }
}
