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

package org.apache.hudi.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Plumbing tests for the marker-retention config introduced for issue #18783.
 *
 * <p>End-to-end behavior is exercised by the existing rollback and archive
 * tests, which continue to pass with the default (OFF) behavior. This class
 * locks in the config key and default explicitly so accidental renames or
 * default-value changes are caught early.
 */
public class TestRetainRollbackMarkersConfig {

  @Test
  public void defaultIsOff_preservingPriorRollbackBehavior() {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/test-table")
        .build();
    assertFalse(cfg.shouldRetainRollbackMarkersUntilArchive(),
        "Marker retention must default to OFF so existing rollback behavior is unchanged.");
  }

  @Test
  public void canBeEnabledViaProperty() {
    java.util.Properties props = new java.util.Properties();
    props.put(HoodieWriteConfig.RETAIN_ROLLBACK_MARKERS_UNTIL_ARCHIVE.key(), "true");
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/test-table")
        .withProperties(props)
        .build();
    assertTrue(cfg.shouldRetainRollbackMarkersUntilArchive());
  }

  @Test
  public void configKeyIsStable() {
    assertEquals("hoodie.rollback.retain.markers.until.archive",
        HoodieWriteConfig.RETAIN_ROLLBACK_MARKERS_UNTIL_ARCHIVE.key(),
        "Config key is part of the user contract — do not rename without a deprecation alias.");
  }
}
