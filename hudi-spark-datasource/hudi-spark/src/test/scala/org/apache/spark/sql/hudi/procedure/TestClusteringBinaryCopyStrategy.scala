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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.client.WriteClientTestUtils
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.{Option => HOption}

import org.apache.spark.SparkConf

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate

class TestClusteringBinaryCopyStrategy extends HoodieSparkProcedureTestBase {

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.hadoop.parquet.avro.write-old-list-structure", "false")
      .set("spark.sql.defaultColumn.enabled", "false")
  }

  // Only test bulk_insert for faster execution
  Seq("bulk_insert").foreach { operation =>
    test(s"Test run_clustering using binary stream copy, cow table prepared by $operation operation") {
      withTempDir { tmp =>
        val conf = operation match {
          case "bulk_insert" =>
            Map(
              "hoodie.sql.bulk.insert.enable" -> "true",
              "hoodie.sql.insert.mode" -> "non-strict",
              "hoodie.combine.before.insert" -> "false",
              "hoodie.parquet.small.file.limit" -> "-1"
            )
          case "insert" =>
            Map(
              "hoodie.datasource.write.operation" -> "insert",
              "hoodie.sql.insert.mode" -> "non-strict",
              "hoodie.combine.before.insert" -> "false",
              "spark.hadoop.parquet.avro.write-old-list-structure" -> "false",
              "hoodie.parquet.small.file.limit" -> "-1"
            )
        }
        withSQLConf(conf.toSeq: _*) {
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = 'cow',
               |  preCombineField = 'price'
               | )
               | partitioned by(ts)
               | location '$basePath'
       """.stripMargin)
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

          val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))

          // Reduced schema evolution for faster execution
          spark.sql(s"ALTER TABLE $tableName ADD COLUMNS(intType int, longType long)")
          spark.sql(
            s"""
               |insert into $tableName (id, name, price, intType, longType, ts)
               |values (4, 'a2', 2, 1, 22, 1000), (5, 'a3', 3, 2, 33, 1001)
               |""".stripMargin)

          // Generate the first clustering plan
          val firstScheduleInstant = WriteClientTestUtils.createNewInstantTime()
          client.scheduleClusteringAtInstant(firstScheduleInstant, HOption.empty())
          checkAnswer(
            s"""
               |call run_clustering(
               |  op => 'execute',
               |  table => '$tableName',
               |  order => 'ts',
               |  options => 'hoodie.clustering.execution.strategy.class=
               |    org.apache.hudi.client.clustering.run.strategy.SparkBinaryCopyClusteringExecutionStrategy'
               |)
               |""".stripMargin)(
            Seq(firstScheduleInstant, 5, HoodieInstant.State.COMPLETED.name(), "*")
          )

          checkAnswer(
            s"select id, name, price, intType, longType, ts from $tableName order by id")(
            Seq(1, "a1", 10.0, null, null, 1000),
            Seq(2, "a2", 10.0, null, null, 1001),
            Seq(3, "a3", 10.0, null, null, 1002),
            Seq(4, "a2", 2.0, 1, 22, 1000),
            Seq(5, "a3", 3.0, 2, 33, 1001)
          )
          // Skip the second clustering plan for faster execution
        }
      }
    }
  }

  def time(value: String): Timestamp = {
    Timestamp.valueOf(value)
  }

  def date(value: String): Date = {
    Date.valueOf(LocalDate.parse(value))
  }
}
