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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.bootstrap.selector.DateBasedBootstrapModeSelector;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End to end coverage for a table bootstrapped across all three tiers, where the coldest partitions are
 * registered without their contents ever being read.
 */
@Tag("functional")
public class TestRegisterOnlyBootstrapRead extends HoodieSparkClientTestBase {

  private static final String DATE_FIELD = "datestr";
  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  @TempDir
  public java.nio.file.Path tmpFolder;

  private String sourcePath;
  private String targetPath;
  private String hotDate;
  private String warmDate;
  private String coldDate;

  @BeforeEach
  public void setUp() throws Exception {
    String uuid = UUID.randomUUID().toString();
    sourcePath = tmpFolder.toAbsolutePath() + "/" + uuid + "/source";
    targetPath = tmpFolder.toAbsolutePath() + "/" + uuid + "/table";
    hotDate = LocalDate.now().minusDays(5).format(DATE_FORMAT);
    warmDate = LocalDate.now().minusDays(100).format(DATE_FORMAT);
    coldDate = LocalDate.now().minusDays(900).format(DATE_FORMAT);
    initSparkContexts(this.getClass().getSimpleName() + uuid);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupSparkContexts();
    cleanupClients();
  }

  /** Writes a plain Hive style parquet table with one row per tier, carrying no Hudi metadata columns. */
  private void writeSourceTable() {
    StructType schema = new StructType()
        .add("_row_key", DataTypes.StringType, false)
        .add("value", DataTypes.StringType, false)
        .add("ts", DataTypes.LongType, false)
        .add(DATE_FIELD, DataTypes.StringType, false);
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create("hot-1", "hot", 1L, hotDate));
    rows.add(RowFactory.create("warm-1", "warm", 2L, warmDate));
    rows.add(RowFactory.create("cold-1", "cold", 3L, coldDate));
    sparkSession.createDataFrame(rows, schema)
        .write().format("parquet").partitionBy(DATE_FIELD).mode(SaveMode.Overwrite).save(sourcePath);
  }

  private Map<String, String> bootstrapOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    options.put(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING().key(), "true");
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    options.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), DATE_FIELD);
    options.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), SimpleKeyGenerator.class.getName());
    options.put(HoodieWriteConfig.TBL_NAME.key(), "register_only_test");
    options.put(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    options.put(HoodieMetadataConfig.ENABLE.key(), "true");
    options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL());
    options.put(HoodieBootstrapConfig.BASE_PATH.key(), sourcePath);
    options.put(HoodieBootstrapConfig.MODE_SELECTOR_CLASS_NAME.key(), DateBasedBootstrapModeSelector.class.getName());
    options.put(HoodieBootstrapConfig.DATE_SELECTOR_FULL_RECORD_DAYS.key(), "30");
    options.put(HoodieBootstrapConfig.DATE_SELECTOR_METADATA_ONLY_DAYS.key(), "365");
    options.put(HoodieBootstrapConfig.DATE_SELECTOR_PARTITION_DATE_FORMAT.key(), "yyyy-MM-dd");
    options.put(HoodieBootstrapConfig.DATE_SELECTOR_PARTITION_DATE_FIELD.key(), DATE_FIELD);
    return options;
  }

  private void runBootstrap() {
    writeSourceTable();
    sparkSession.emptyDataFrame().write().format("hudi")
        .options(bootstrapOptions()).mode(SaveMode.Overwrite).save(targetPath);
  }

  @Test
  public void testSelectStarReturnsRowsFromEveryTier() {
    runBootstrap();
    Dataset<Row> df = sparkSession.read().format("hudi").load(targetPath);
    assertEquals(3, df.count(), "every tier's rows should be visible");
    assertEquals(1, df.filter("value = 'cold'").count(), "the register-only partition should be queryable");
  }

  @Test
  public void testColdPartitionPredicateReturnsItsRows() {
    runBootstrap();
    Dataset<Row> df = sparkSession.read().format("hudi").load(targetPath)
        .filter(DATE_FIELD + " = '" + coldDate + "'");
    assertEquals(1, df.count());
    assertEquals("cold-1", df.collectAsList().get(0).getAs("_row_key"));
  }

  @Test
  public void testMetaColumnsAreNullOnlyForTheRegisterOnlyTier() {
    runBootstrap();
    Dataset<Row> df = sparkSession.read().format("hudi").load(targetPath);

    assertEquals(1, df.filter("value = 'cold' and _hoodie_record_key is null").count(),
        "register-only rows carry no record key");
    assertEquals(0, df.filter("value <> 'cold' and _hoodie_record_key is null").count(),
        "the rewritten and skeleton tiers keep their metadata columns");
  }

  @Test
  public void testTablePropertyRecordsThePresenceOfRegisterOnlyPartitions() {
    runBootstrap();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance()).setBasePath(targetPath).build();
    assertTrue(metaClient.getTableConfig().hasRegisterOnlyPartitions());
  }

  @Test
  public void testRegisteredFilesStayInTheSourceTable() {
    runBootstrap();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance()).setBasePath(targetPath).build();
    assertTrue(metaClient.getTableConfig().hasRegisterOnlyPartitions());
    // Nothing was copied: the cold partition exists only under the source table.
    assertTrue(new java.io.File(sourcePath + "/" + DATE_FIELD + "=" + coldDate).isDirectory());
    assertFalse(new java.io.File(targetPath + "/" + DATE_FIELD + "=" + coldDate).isDirectory());
  }

  /** Options for a normal (non-bootstrap) write into the already bootstrapped table. */
  private Map<String, String> writeOptions(String operation) {
    Map<String, String> options = new HashMap<>();
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    options.put(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING().key(), "true");
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    options.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), DATE_FIELD);
    options.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), SimpleKeyGenerator.class.getName());
    options.put(HoodieWriteConfig.TBL_NAME.key(), "register_only_test");
    options.put(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    options.put(HoodieMetadataConfig.ENABLE.key(), "true");
    options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    options.put(DataSourceWriteOptions.OPERATION().key(), operation);
    return options;
  }

  private void write(String operation, String rowKey, String value, String date) {
    StructType schema = new StructType()
        .add("_row_key", DataTypes.StringType, false)
        .add("value", DataTypes.StringType, false)
        .add("ts", DataTypes.LongType, false)
        .add(DATE_FIELD, DataTypes.StringType, false);
    sparkSession.createDataFrame(Collections.singletonList(RowFactory.create(rowKey, value, 9L, date)), schema)
        .write().format("hudi").options(writeOptions(operation)).mode(SaveMode.Append).save(targetPath);
  }

  @Test
  public void testUpsertIntoARegisterOnlyPartitionFailsFast() {
    runBootstrap();
    Exception e = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
        () -> write(DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL(), "cold-1", "changed", coldDate));
    assertTrue(rootCauseMessage(e).contains("REGISTER_ONLY bootstrap partition"), rootCauseMessage(e));
  }

  @Test
  public void testUpsertIntoAnOrdinaryPartitionStillWorks() {
    runBootstrap();
    write(DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL(), "hot-1", "changed", hotDate);

    Dataset<Row> df = sparkSession.read().format("hudi").load(targetPath);
    assertEquals(3, df.count());
    assertEquals(1, df.filter("value = 'changed'").count());
  }

  @Test
  public void testInsertOverwritePromotesARegisterOnlyPartition() {
    runBootstrap();
    write(DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL(), "cold-2", "restored", coldDate);

    Dataset<Row> df = sparkSession.read().format("hudi").load(targetPath)
        .filter(DATE_FIELD + " = '" + coldDate + "'");
    assertEquals(1, df.count());
    Row row = df.collectAsList().get(0);
    assertEquals("cold-2", row.getAs("_row_key"));
    // The partition is now owned by the table, so its rows carry meta columns again.
    assertEquals("cold-2", row.getAs("_hoodie_record_key"));
  }

  @Test
  public void testRegisteredFileWithHiveStyleUnderscoreNameIsStillReadable() throws IOException {
    // Hive writes files named like 000000_0, and FSUtils.getFileId splits a name on its first underscore.
    // A registered file keeps its original name, so this is the shape most likely to trip the read path.
    writeSourceTable();
    java.io.File coldDir = new java.io.File(sourcePath + "/" + DATE_FIELD + "=" + coldDate);
    java.io.File[] parquetFiles = coldDir.listFiles((dir, name) -> name.endsWith(".parquet"));
    assertEquals(1, parquetFiles.length);
    assertTrue(parquetFiles[0].renameTo(new java.io.File(coldDir, "000000_0.parquet")));

    sparkSession.emptyDataFrame().write().format("hudi")
        .options(bootstrapOptions()).mode(SaveMode.Overwrite).save(targetPath);

    Dataset<Row> df = sparkSession.read().format("hudi").load(targetPath)
        .filter(DATE_FIELD + " = '" + coldDate + "'");
    assertEquals(1, df.count());
    assertEquals("cold-1", df.collectAsList().get(0).getAs("_row_key"));
  }

  @Test
  public void testBootstrapFailsWhenMetadataTableIsDisabled() {
    writeSourceTable();
    Map<String, String> options = bootstrapOptions();
    options.put(HoodieMetadataConfig.ENABLE.key(), "false");
    Exception e = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
        () -> sparkSession.emptyDataFrame().write().format("hudi")
            .options(options).mode(SaveMode.Overwrite).save(targetPath));
    assertTrue(rootCauseMessage(e).contains("metadata table is disabled"), rootCauseMessage(e));
  }

  private String rootCauseMessage(Throwable t) {
    StringBuilder sb = new StringBuilder();
    while (t != null) {
      sb.append(t.getMessage()).append(" | ");
      t = t.getCause();
    }
    return sb.toString();
  }
}
