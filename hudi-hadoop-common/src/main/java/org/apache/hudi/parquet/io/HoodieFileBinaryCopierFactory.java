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

package org.apache.hudi.parquet.io;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieFileBinaryCopier;
import org.apache.hudi.util.HoodieFileMetadataMerger;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating HoodieFileBinaryCopier instances based on configuration.
 */
public class HoodieFileBinaryCopierFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFileBinaryCopierFactory.class);

  /**
   * Creates a HoodieFileBinaryCopier instance based on the configuration.
   *
   * @param config Hudi write configuration
   * @param conf Hadoop configuration
   * @param codecName Compression codec name
   * @param fileMetadataMerger File metadata merger
   * @return HoodieFileBinaryCopier instance
   */
  public static HoodieFileBinaryCopier createBinaryCopier(
      HoodieWriteConfig config,
      Configuration conf,
      CompressionCodecName codecName,
      HoodieFileMetadataMerger fileMetadataMerger) {
    
    String copierClassName = config.getStringOrDefault(HoodieStorageConfig.HOODIE_FILE_BINARY_COPIER_CLASS);
    LOG.info("Creating file binary copier with class: {}", copierClassName);

    try {
      // Check if it's the default HoodieParquetFileBinaryCopier
      if (copierClassName.equals(HoodieParquetFileBinaryCopier.class.getName())) {
        return new HoodieParquetFileBinaryCopier(conf, codecName, fileMetadataMerger);
      }
      
      // Check if it's the custom implementation
      if (copierClassName.equals("org.apache.hudi.parquet.io.LiteCustomFileBinaryCopier")) {
        return (HoodieFileBinaryCopier) ReflectionUtils.loadClass(
            copierClassName,
            new Class<?>[] {},
            new Object[] {});
      }
      
      // For other implementations, try to load with common constructor patterns
      // First try constructor with (Configuration, CompressionCodecName, HoodieFileMetadataMerger)
      try {
        return (HoodieFileBinaryCopier) ReflectionUtils.loadClass(
            copierClassName,
            new Class<?>[] {Configuration.class, CompressionCodecName.class, HoodieFileMetadataMerger.class},
            new Object[] {conf, codecName, fileMetadataMerger});
      } catch (Exception e) {
        LOG.debug("Failed to instantiate with full constructor, trying no-arg constructor", e);
      }
      
      // Try no-arg constructor
      return (HoodieFileBinaryCopier) ReflectionUtils.loadClass(
          copierClassName,
          new Class<?>[] {},
          new Object[] {});
          
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to instantiate file binary copier: " + copierClassName, e);
    }
  }
}