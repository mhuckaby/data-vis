/**
  * Copyright 2018 Matthew Huckaby
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.mhuckaby.bbs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

// Implicits allow (among other things) $"col_name" access vs col() method
// import spark.implicits._

/**
  * Job to perform analysis on BBS data
  */
object BbsRecordJob {
  val RUN = System.currentTimeMillis()
  val AREA_CODE_STATE = "area_code_state"
  val BBS = "bbs"

  def main(args: Array[String]): Unit = {
    val optionPaths = validateArgs(args)

    if(optionPaths.isEmpty) {
      println("no work completed")
      return
    }

    val pathBbsDataCollection = optionPaths.get(0)
    val pathAreaCode = optionPaths.get(1)
    val pathReport = optionPaths.get(2)
    val spark = SparkSession.builder.appName("bbs_records").getOrCreate()

    createOrReplaceBbsDataGlobalView(spark, pathBbsDataCollection)
    createOrReplaceAreaCodeStateGlobalView(spark, pathAreaCode)

    updateCalculatedFields(spark)

    val bbs = spark.sql(s"select * from global_temp.$BBS")

    reportModifiedBbsDataRawAll(pathReport, bbs)
    reportCountStartsPerYear(pathReport, bbs)
    reportCountStartsPerYearPerState(pathReport, bbs)
  }

  /**
    * Updates the Global Temporary View `bbs` to include calculated fields:
    *   `area_code`   => first 3 digits of phone
    *   `duration`    => end - start
    *   `state_code`  => cross reference area_code with state_code from external (to bbs) data
    *
    *   I originally tried to get the `state` value for records that were missing location-data
    *   by matching them to records with the same area-code that did have location-data.
    *   This produced invalid results because it was based upon incorrect user-supplied data.
    *   Example follows:
    *
    *   scala> state_area_code.filter($"area_code".eqNullSafe("512")).show
    *   +--------------------+---------+
    *   |state_from_area_code|area_code|
    *   +--------------------+---------+
    *   |                  IA|      512|
    *   |                  TX|      512|
    *   +--------------------+---------+
    * @param spark
    */
  def updateCalculatedFields(spark: SparkSession): Unit = {
    val udfDuration = registerUdfStartEndDetermination(spark)
    val intermediateResult = spark
      .sql(s"select * from global_temp.$BBS")
      .withColumn("area_code", substring(col("phone"), 0, 3))
      .withColumn("duration", udfDuration(col("start"), col("end")))

    val correctedCodes = spark.sql(s"select * from global_temp.$AREA_CODE_STATE")
    // Adds a valid `state_cd` field to the BBS data based on this join
    intermediateResult
      .join(correctedCodes, Seq("area_code"))
      .createOrReplaceGlobalTempView(BBS)
  }

  /**
    * Creates the initial area code to state Global Temporary View.
    * References to _c0 and _c1 are necessary because no StructType/StructFields is defined.
    * The Global Temporary View that results will have `area_code` and `state_code` field names.
    * @param spark
    * @param pathAreaCode
    */
  def createOrReplaceAreaCodeStateGlobalView(spark: SparkSession, pathAreaCode: String): Unit = {
    spark
      .read
      .csv(pathAreaCode)
      .selectExpr("_c0 as area_code", "_c1 as state_code")
      .createOrReplaceGlobalTempView(AREA_CODE_STATE)
  }

  /**
    * Creates the initial BBS Global Temporary View
    * @param spark
    * @param pathBbsDataCollection
    */
  def createOrReplaceBbsDataGlobalView(spark: SparkSession, pathBbsDataCollection: String): Unit = {
    val structTypeBbsRecord = getBbsRecordStructType

    spark
      .read
      .schema(structTypeBbsRecord)
      .json(pathBbsDataCollection)
      .createOrReplaceGlobalTempView(BBS)
  }

  /**
    * Produces report consisting of all of the BBS starts per year per state.
    * If the `start` year is not included in the record, the record is not considered for
    * the report.
    * @param pathReport
    * @param dataFrame
    */
  def reportCountStartsPerYear(pathReport: String, dataFrame: DataFrame): Unit = {
    val result = dataFrame
      .select(col("area_code"), col("start"))
      .filter(col("start").isNotNull)
      .groupBy(col("start"))
      .count().orderBy(col("start"))

    result.show(50)
    result.coalesce(1).write.format("json").save(s"$pathReport/$RUN/starts_per_year")
  }

  /**
    * Produces report consisting of all of the BBS starts per year per state.
    * If the `start` year is not included in the record, the record is not considered for
    * the report.
    * @param pathReport
    * @param dataFrame
    */
  def reportCountStartsPerYearPerState(pathReport: String, dataFrame: DataFrame): Unit = {
    val result = dataFrame
      .select(col("state_code"), col("start"))
      .filter(col("start").isNotNull)
      .groupBy(col("start"), col("state_code"))
      .count()
      .orderBy(col("start"))

    result.show()
    result.coalesce(1).write.format("json").save(s"$pathReport/$RUN/starts_per_year_per_state")
  }

  /**
    * Produces report consisting of all of the modified (calculated) BBS data.
    * @param pathReport
    * @param df
    */
  def reportModifiedBbsDataRawAll(pathReport: String, df: DataFrame): Unit = {
    val report = df.coalesce(1)
    report.write.format("json").save(s"$pathReport/$RUN/mod_bbs_area_code_state_duration")
  }

  /**
    * Creates a User Defined Function (UDF) to handle `start` and `end` data.
    * If no `end` value is provided, assume BBS was up for 1 year or less.
    * @param spark SparkSession
    * @return UDF that performs `start` and `end` evaluation
    */
  def registerUdfStartEndDetermination(spark: SparkSession): UserDefinedFunction = {
    spark.udf.register("start_end_determination", (start: Integer, end: Integer) => {
      if(Option(end).isDefined) {
        end - start
      }else {
        1
      }
    })
  }

  /**
    * Verifies that three arguments are supplied and provides console output.
    * Argument[0] = path to input file generated by Groovy data-collection script
    * Argument[1] = path to input file area_code_state.csv
    * Argument[2] = path to output report file
    * TODO Validate existence of input file and non-existence of output file
    * @param args is an Array[String] of size 3 described above
    * @return an Option containing String array of file paths 0 through 3
    */
  def validateArgs(args: Array[String]): Option[Array[String]] = {
    if(args.length != 3) {
      println("Found invalid arguments")
      println("args[0] = path to input file generated by Groovy data-collection script")
      println("args[1] = path to input file area_code_state.csv")
      println("args[2] = path to output report file")

      Option.empty
    }else {
      println(s"reading input file generated by Groovy data-collection script:\n ${args(0)}")
      println(s"reading input file input file area_code_state.csv:\n ${args(1)}")
      println(s"writing input output report file:\n ${args(2)}")

      Option(Array(args(0), args(1), args(2)))
    }
  }

  /**
    * Struct used to map incoming fields to data types
    * @return
    */
  def getBbsRecordStructType: StructType = {
    StructType(
      StructField("city", StringType, true) ::
        StructField("state", StringType, true) ::
        StructField("end", StringType, true) ::
        StructField("start", StringType, true) ::
        StructField("name", StringType, false) ::
        StructField("phone", StringType, false) ::
        StructField("software", StringType, true) ::
        StructField("sysop", StringType, true) ::
        StructField("duration", IntegerType, true) :: Nil
    )
  }
}

