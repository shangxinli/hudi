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

import org.apache.hudi.io.storage.HoodieFileBinaryCopier;
import org.apache.hudi.parquet.HoodieParquetStrictMerge;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Lite implementation of HoodieFileBinaryCopier using HoodieParquetStrictMerge.
 * This implementation uses a simpler approach by delegating to HoodieParquetStrictMerge
 * for strict schema validation and file merging.
 */
public class LiteFileBinaryCopier implements HoodieFileBinaryCopier {

  private static final Logger LOG = LoggerFactory.getLogger(LiteFileBinaryCopier.class);
  private final Configuration conf;
  private final HoodieParquetStrictMerge merger;

  public LiteFileBinaryCopier() {
    this.conf = new Configuration();
    this.merger = new HoodieParquetStrictMerge(conf);
  }

  public LiteFileBinaryCopier(Configuration conf) {
    this.conf = conf;
    this.merger = new HoodieParquetStrictMerge(conf);
  }

  @Override
  public long binaryCopy(List<StoragePath> inputFilePaths, 
                         List<StoragePath> outputFilePath, 
                         MessageType writeSchema, 
                         Properties props) throws IOException {
    if (outputFilePath == null || outputFilePath.isEmpty()) {
      throw new IllegalArgumentException("Output file path cannot be null or empty");
    }
    
    if (inputFilePaths == null || inputFilePaths.isEmpty()) {
      throw new IllegalArgumentException("Input file paths cannot be null or empty");
    }

    LOG.info("Starting binary copy from {} files to {}", inputFilePaths.size(), outputFilePath.get(0));
    
    // Convert StoragePath to Hadoop Path
    List<Path> hadoopInputPaths = inputFilePaths.stream()
        .map(storagePath -> new Path(storagePath.toUri()))
        .collect(Collectors.toList());
    
    Path hadoopOutputPath = new Path(outputFilePath.get(0).toUri());
    
    // Use HoodieParquetStrictMerge to merge the files
    merger.mergeFiles(hadoopInputPaths, hadoopOutputPath);
    
    // Calculate total number of records written
    long totalRecords = 0;
    for (Path inputPath : hadoopInputPaths) {
      ParquetMetadata metadata = ParquetFileReader.readFooter(conf, inputPath);
      for (int i = 0; i < metadata.getBlocks().size(); i++) {
        totalRecords += metadata.getBlocks().get(i).getRowCount();
      }
    }
    
    LOG.info("Successfully merged {} files with {} total records to {}", 
        inputFilePaths.size(), totalRecords, outputFilePath.get(0));
    
    return totalRecords;
  }

  @Override
  public void close() throws IOException {
    // No resources to clean up in this implementation
    LOG.info("Closing LiteFileBinaryCopier");
  }
}