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
import org.apache.hudi.client.bootstrap.selector.DateBasedBootstrapModeSelector;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDateBasedBootstrapModeSelector {

  private static final String DATE_FIELD = "datestr";

  private HoodieWriteConfig getConfig(int fullRecordDays, int metadataOnlyDays, String dateField) {
    HoodieBootstrapConfig.Builder bootstrapConfig = HoodieBootstrapConfig.newBuilder()
        .withBootstrapDateSelectorFullRecordDays(fullRecordDays)
        .withBootstrapDateSelectorMetadataOnlyDays(metadataOnlyDays)
        .withBootstrapDateSelectorPartitionDateFormat("yyyy-MM-dd");
    if (dateField != null) {
      bootstrapConfig.withBootstrapDateSelectorPartitionDateField(dateField);
    }
    return HoodieWriteConfig.newBuilder().withPath("")
        .withBootstrapConfig(bootstrapConfig.build())
        .forTable("test-trip-table").build();
  }

  private String partitionOfAge(long daysAgo) {
    return DATE_FIELD + "=" + LocalDate.now().minusDays(daysAgo).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
  }

  private List<Pair<String, List<HoodieFileStatus>>> toInput(List<String> partitionPaths) {
    return partitionPaths.stream()
        .map(p -> Pair.<String, List<HoodieFileStatus>>of(p, new ArrayList<>()))
        .collect(Collectors.toList());
  }

  @Test
  public void testPartitionsAreTieredByAge() {
    String hot = partitionOfAge(5);
    String warm = partitionOfAge(100);
    String cold = partitionOfAge(900);
    Map<BootstrapMode, List<String>> result =
        new DateBasedBootstrapModeSelector(getConfig(30, 365, DATE_FIELD))
            .select(toInput(Arrays.asList(hot, warm, cold)));

    assertEquals(Arrays.asList(hot), result.get(BootstrapMode.FULL_RECORD));
    assertEquals(Arrays.asList(warm), result.get(BootstrapMode.METADATA_ONLY));
    assertEquals(Arrays.asList(cold), result.get(BootstrapMode.REGISTER_ONLY));
  }

  @Test
  public void testTierBoundariesAreInclusive() {
    Map<BootstrapMode, List<String>> result =
        new DateBasedBootstrapModeSelector(getConfig(30, 365, DATE_FIELD))
            .select(toInput(Arrays.asList(partitionOfAge(30), partitionOfAge(31), partitionOfAge(365), partitionOfAge(366))));

    assertEquals(Arrays.asList(partitionOfAge(30)), result.get(BootstrapMode.FULL_RECORD));
    assertEquals(Arrays.asList(partitionOfAge(31), partitionOfAge(365)), result.get(BootstrapMode.METADATA_ONLY));
    assertEquals(Arrays.asList(partitionOfAge(366)), result.get(BootstrapMode.REGISTER_ONLY));
  }

  @Test
  public void testDateFieldIsFoundInNestedPartitionPath() {
    String nested = "region=us/" + partitionOfAge(900);
    Map<BootstrapMode, List<String>> result =
        new DateBasedBootstrapModeSelector(getConfig(30, 365, DATE_FIELD)).select(toInput(Arrays.asList(nested)));

    assertEquals(Arrays.asList(nested), result.get(BootstrapMode.REGISTER_ONLY));
    assertFalse(result.containsKey(BootstrapMode.FULL_RECORD));
  }

  @Test
  public void testPartitionWithoutDateFieldIsRejected() {
    DateBasedBootstrapModeSelector selector = new DateBasedBootstrapModeSelector(getConfig(30, 365, DATE_FIELD));
    HoodieException e = assertThrows(HoodieException.class, () -> selector.select(toInput(Arrays.asList("region=us"))));
    assertTrue(e.getMessage().contains("has no 'datestr=' segment"), e.getMessage());
  }

  @Test
  public void testPartitionWithUnparseableDateIsRejected() {
    DateBasedBootstrapModeSelector selector = new DateBasedBootstrapModeSelector(getConfig(30, 365, DATE_FIELD));
    HoodieException e =
        assertThrows(HoodieException.class, () -> selector.select(toInput(Arrays.asList("datestr=20240115"))));
    assertTrue(e.getMessage().contains("does not match the configured format"), e.getMessage());
  }

  @Test
  public void testMissingDateFieldConfigIsRejected() {
    HoodieException e =
        assertThrows(HoodieException.class, () -> new DateBasedBootstrapModeSelector(getConfig(30, 365, null)));
    assertTrue(e.getMessage().contains("partition.date.field must be set"), e.getMessage());
  }

  @Test
  public void testInvertedTierThresholdsAreRejected() {
    HoodieException e =
        assertThrows(HoodieException.class, () -> new DateBasedBootstrapModeSelector(getConfig(400, 365, DATE_FIELD)));
    assertTrue(e.getMessage().contains("must not exceed"), e.getMessage());
  }
}
