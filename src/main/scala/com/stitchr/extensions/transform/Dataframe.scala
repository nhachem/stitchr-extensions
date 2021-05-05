/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stitchr.extensions.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
// import org.apache.spark.sql.functions.{expr, col, monotonically_increasing_id, explode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType, ArrayType, MapType}

object Dataframe {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  import spark.implicits._

  /**
    * Returns a single row DataFrame "session_run_time" as a unix_timestamp.
    * This column can be easily added to all dataframes and provides a means for simple audit
    * @param sessionRunTimeAttribute
    * @return
    */
  def getSessionRunTimeDf(
      sessionRunTimeAttribute: String = "session_run_time"
  ): (DataFrame, String) = {
    (
      // NH: 2/22/2021 using current unix time stamp to get the reference session time as session id
      spark.sql(s"select unix_timestamp() as $sessionRunTimeAttribute"),
      //  Seq(System.nanoTime()).toDF(sessionRunTimeAttribute),
      sessionRunTimeAttribute
    )
  }

  val (sessionRunTimeDf, sessionRunTimeColumnName) = getSessionRunTimeDf()
  // version to use df.transform

  /**
    * returns a new DataFrame excluding the list of columns columns2Exclude
    * @param dataFrame
    * @param columns2exclude
    * @return
    */
  def selectExclude(
      dataFrame: DataFrame,
      columns2exclude: List[String]
  ): DataFrame = {

    val columnList =
      (dataFrame.schema.fieldNames.toSet &~ columns2exclude.toSet)
      //.map{_.toLowerCase}
      .toList
    import org.apache.spark.sql.functions.col
    dataFrame.select(columnList.map(col): _*)

  }

  /**
    * returns the PK columns + extractions of columns following a named regex pattern
    * @param dataFrame
    * @param pkList
    * @param columnsNamesRegex
    * @return
    */
  def selectGroupOfColumn(
      dataFrame: DataFrame,
      pkList: List[String],
      columnsNamesRegex: String
  ): DataFrame = {

    val embeddedRegex = columnsNamesRegex.r.unanchored
    val schemaColumnList = dataFrame.schema.fieldNames.toList

    val columnList = pkList ::: schemaColumnList.filter { x =>
      x match {
        case embeddedRegex() => true
        case _               => false
      }
    }

    import org.apache.spark.sql.functions.col
    dataFrame.select(columnList.map(col): _*)
  }

  /**
    * drops all columns in dropColumnList
    * @param dropColumnsList
    * @param dataFrame
    * @return
    */
  def dropColumns(
      dropColumnsList: List[String]
  )(dataFrame: DataFrame): DataFrame = {
    // check if any columns are not in the  df schema
    val dfColumns = dataFrame.schema.fieldNames.toSet
    // warn that some columns are not in the list... Or maybe throw an error?
    val colsThatDoNotExist = dropColumnsList.toSet &~ dfColumns
    // get the actual list of columns to drop
    val columns2Remove = (dropColumnsList.toSet &~ colsThatDoNotExist).toList
    // drop
    // Note: * operator for sme reason was throwing an error
    dataFrame.drop(columns2Remove.mkString(","))
  }

  /**
    * Returns a new DatFrame with columns in renameMappingList renamed
    *  Note: renaming columns recursively is not efficient but ok for now
    * @param renameMappingList
    * @param dataFrame
    * @return
    */
  def renameColumns(
      renameMappingList: Map[String, String]
  )(dataFrame: DataFrame): DataFrame = {
    // Assuming all columns are correct... But we better add a check step similar to the drop columns function
    renameMappingList.foldLeft(dataFrame)({
      case (df, (k, v)) => df.withColumnRenamed(k, v)
    }): DataFrame
  }

  implicit class Implicits(dataFrame: DataFrame) {

    /**
      * simple select the columnList
      * @param columnList
      * @return
      */
    def selectList(columnList: List[String]): DataFrame = {
      val cl = s"""`${columnList.mkString("`,`")}`""".split(',')
      dataFrame.select(cl.head, cl.tail: _*)
    }

    /**
      * returns a new DataFrame excluding the list of columns columns2Exclude
      * @param dataFrame
      * @param columns2exclude
      * @return
      */
    def selectExclude(
        dataFrame: DataFrame,
        columns2exclude: List[String]
    ): DataFrame = {

      val columnList =
        (dataFrame.schema.fieldNames.toSet &~ columns2exclude.toSet)
        //.map{_.toLowerCase}
        .toList
      import org.apache.spark.sql.functions.col
      dataFrame.select(columnList.map(col): _*)

    }

    /**
      * returns the PK columns + extractions of columns following a named regex pattern
      * @param dataFrame
      * @param pkList
      * @param columnsNamesRegex
      * @return
      */
    def selectGroupOfColumns(
        dataFrame: DataFrame,
        pkList: List[String],
        columnsNamesRegex: String
    ): DataFrame = {

      val embeddedRegex = columnsNamesRegex.r.unanchored
      val schemaColumnList = dataFrame.schema.fieldNames.toList

      val columnList = pkList ::: schemaColumnList.filter { x =>
        x match {
          case embeddedRegex() => true
          case _               => false
        }
      }
      dataFrame.select(columnList.map(col): _*)
    }

    //need to fix the session runtime attribute to be controlled from one place
    /**
      * addRunTime adds a session runtime column. Used for versioning the data
      * @return dataframe with a sess
      */
    def addRunTimeRef(
        sessionRunTimeAttribute: String = sessionRunTimeColumnName
    ): DataFrame =
      // trivial match
      dataFrame.schema.fieldNames contains sessionRunTimeAttribute match {
        case true  => dataFrame
        case false => sessionRunTimeDf.crossJoin(dataFrame)
      }

    /**
      * diff schema L - R
      * @param rightDF
      * @return
      */
    def diff_schemas(rightDF: DataFrame): List[String] =
      (dataFrame.schema.fieldNames.toSet &~ rightDF.schema.fieldNames.toSet).toList

    /**
      * one-way diffs schemas
      * @param rightDF
      * @param direction left (default)  or right
      * @return
      */
    def diffSchemas(
        rightDF: DataFrame,
        direction: String = "left"
    ): List[String] =
      (dataFrame.schema.fieldNames.toSet &~ rightDF.schema.fieldNames.toSet).toList

    /**
      * drop the list of columns
      * @param dropColumnsList
      * @return
      */
    def dropColumns(dropColumnsList: List[String]): DataFrame = {
      // check if any columns are not in the df schema
      val dfColumns = dataFrame.schema.fieldNames.toSet
      // warn that some columns are not in the list... Or maybe throw an error?
      val colsThatDoNotExist = dropColumnsList.toSet &~ dfColumns
      // get the actual list of columns to drop
      val columns2Remove = (dropColumnsList.toSet &~ colsThatDoNotExist).toList
      dataFrame.drop(columns2Remove.mkString(","))
    }

    /**
      * Returns a new DatFrame with columns in renameMappingList renamed
      * renaming columns recursively is not efficient if we have a large number of columns but ok for now
      * @param renameMappingList
      * @param dataFrame
      * @return
      */
    def renameColumns(renameMappingList: Map[String, String]): DataFrame =
      // Assuming all columns are correct... But we better add a check step similar to the drop columns function
      renameMappingList.foldLeft(dataFrame)({
        case (df, (k, v)) => df.withColumnRenamed(k, v)
      }): DataFrame

    /**
      * takes a comma-delimited string of oldColumns and corresponding newColumns, transforms to a map and applies rename
      * @param oldColumnsString
      * @param newColumnsString
      * @return
      */
    def renameColumns(
        oldColumnsString: String,
        newColumnsString: String
    ): DataFrame = {
      // Assuming all columns are correct... But we better add a check step similar to the drop columns function
      val oldArray: Array[String] = oldColumnsString.split(",")
      val newArray: Array[String] = newColumnsString.split(",")
      val ml =
        oldArray.zip(newArray).map { case (l, r) => Map(l -> r) }.reduce(_ ++ _)
      dataFrame.renameColumns(ml)
    }

    // NH: need to merge the
    /**
      * similar to a simple select list of columns.
      * @param  List of columns to select
      * @return
      */
    def reorderDF(columnNamesList: List[String]): DataFrame = {
      val colOrderString = columnNamesList
        .mkString("`,`")

      // this does not need any temp views to generate the output and does not use sql but relies on functional selects.
      // It will be better as it has no side effects
      val columnOrderList = s"`$colOrderString`".split(',')
      dataFrame.select(columnOrderList.head, columnOrderList.tail: _*)
    }

    /**
      * select a list of columns derived from the input string
      * @param columnNamesString comma delimited list string of columns
      * @return
      */
    def reorderDF(columnNamesString: String): DataFrame = {
      val columnNamesList = columnNamesString
        .split(',')
        .toList

      reorderDF(columnNamesList)
    }

    // assume the pivot columns are key, value
    // provide the list as a parameter as sometimes we do not need a full pivot. null means all and we use the schema to generate the pivoted columns
    /**
      *
      * @param pivotedColumnsList
      * @param fn
      * @return
      */
    def pivot(
        pivotedColumnsList: List[String] = null,
        fn: String = "max"
    ): DataFrame = {
      val pivotColumns =
        if (pivotedColumnsList == null) {
          dataFrame.select("key").distinct.map(r => s"${r(0)}").collect.toList
        } else pivotedColumnsList

      // val l = s"'${pivotColumns.mkString("','")}'"
      // replace "." with "++" in column nameees
      val l = pivotColumns
        .foldLeft("")((head, next) => {
          s"$head'${next}' ${next.replace(".", "__")},"
        })
        .stripSuffix(",")
      dataFrame.createOrReplaceTempView("_tmp")

      val q = s"""SELECT * FROM
                  | (
                  | SELECT *
                  | FROM _tmp
                  | )
                  | PIVOT (
                  | $fn(value)
                  | FOR key in ( $l )
                  | ) """.stripMargin
      spark.sql(q)
    }

    // just get the sql to use in hive ... replacing . with $ in output column names
    // note that _tmp will need to be replaced with source table/view if we use the query to establish a hive view/table
    /**
      *
      * @param pivotedColumnsList
      * @param fn
      * @return
      */
    def genPivotSQL(
        pivotedColumnsList: List[String] = null,
        fn: String = "max"
    ): String = {
      val pivotColumns =
        if (pivotedColumnsList == null)
          dataFrame.select("key").distinct.map(r => s"${r(0)}").collect.toList
        else pivotedColumnsList

      // val l = s"'${pivotColumns.mkString("','")}'"
      val l = pivotColumns
        .foldLeft("")((head, next) => {
          s"$head'${next}' ${next.replace(".", "__")},"
        })
        .stripSuffix(",")
      dataFrame.createOrReplaceTempView("_tmp")

      val q = s"""SELECT * FROM
                 | (
                 | SELECT *
                 | FROM _tmp
                 | )
                 | PIVOT (
                 | max(value)
                 | FOR key in ( $l )
                 | ) """.stripMargin
      q
    }

    /**
      * transforms a dataframe with a string json column columnName into a struc
      * Note: does not handle cleanly columns that are null or have an array of null.
      * @param columnName Json column to transform
      * @return transformed dataframe
      */
    def cast2Json(columnName: String): DataFrame = {
      import org.apache.spark.sql.functions._
      val schema = spark.sqlContext.read
        .json(dataFrame.select(columnName).as[String])
        .schema
      dataFrame
        .withColumn(
          s"${columnName}_jsonString",
          from_json(col(columnName), schema)
        )
        .drop(columnName)
        .withColumnRenamed(s"${columnName}_jsonString", columnName)
    }

    // from https://www.24tutorials.com/spark/flatten-json-spark-dataframe/
    // this is not tail recursive but hopefully will not matter

    /**
      * flatten0 a nested structured schema (like json)
      * adapted from https://www.24tutorials.com/spark/flatten-json-spark-dataframe/
      * this is not tail recursive but hopefully will not matter
      * @return
      */
    def flatten0: DataFrame = {

      val fields = dataFrame.schema.fields
      val fieldNames = fields.map(x => x.name)

      for (i <- fields.indices) {
        val field = fields(i)
        val fieldType = field.dataType
        val fieldName = field.name
        fieldType match {
          case arrayType: ArrayType =>
            val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
            val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(
              s"explode_outer($fieldName) as $fieldName"
            )
            // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
            val explodedDf = dataFrame.selectExpr(fieldNamesAndExplode: _*)
            return explodedDf.flatten0
          case structType: StructType =>
            val childFieldnames = structType.fieldNames.map(childName =>
              fieldName + "." + childName
            )
            val newFieldNames =
              fieldNames.filter(_ != fieldName) ++ childFieldnames
            val renamedcols =
              newFieldNames.map(x => col(x).as(x.replace(".", "__")))
            val explodeDf = dataFrame.select(renamedcols: _*)
            return explodeDf.flatten0
          case _ =>
        }
      }
      dataFrame
    }

    def flatten(mode: String = "full", delimiter: String = "__"): DataFrame = {
      /* 3 options:
      full --> flattens everything
      struct --> flatten only struct
      array --> flattens array + struct
      map --> technically flattens map + struct but read note below on MapType
       */
      val fields = dataFrame.schema.fields
      val fieldNames = fields.map(x => x.name)

      for (i <- fields.indices) {
        val field = fields(i)
        val fieldType = field.dataType
        val fieldName = field.name
        fieldType match {
          case arrayType: ArrayType =>
            if (List("full", "array").contains(mode)) {
              val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
              val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(
                s"explode_outer($fieldName) as $fieldName"
              )
              // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
              return dataFrame
                .selectExpr(fieldNamesAndExplode: _*)
                .flatten(mode, delimiter)
            }
          /*
             seems that maps in python become struct in scala so testing is tricky...
             may need to make up a special test case
             scala reads data as struct instead of maps… a little tricky.
             Maybe in python I can treat map type and structType similarly?
          case
           */
          case mapType: MapType =>
            if (List("full", "map").contains(mode)) {
              val leftDf: DataFrame =
                dataFrame.withColumn("id", monotonically_increasing_id())
              val mappedDf = leftDf
                .select(col("id"), explode(col(fieldName)))
                .withColumn(
                  "key1",
                  concat(lit(s"${fieldName}$delimiter"), col("key"))
                )
                .drop("key")
                .groupBy("id")
                .pivot("key1")
                .agg(first(col("value")))
                .withColumnRenamed("id", "id_right")
              val flatDf: DataFrame = leftDf
                .join(mappedDf, leftDf("id") === mappedDf("id_right"), "inner")
                .drop("id")
                .drop("id_right")
                .drop(fieldName)
              return flatDf
                .flatten(mode, delimiter)
            }
          case structType: StructType =>
            val childFieldnames = structType.fieldNames.map(childName =>
              fieldName + "." + childName
            )
            val newfieldNames =
              fieldNames.filter(_ != fieldName) ++ childFieldnames
            val renamedColumns =
              newfieldNames.map(x => col(x).as(x.replace(".", delimiter)))
            return dataFrame
              .select(renamedColumns: _*)
              .flatten(mode, delimiter)
          case _ =>
        }
      }
      dataFrame
    }

    /**
      * similar to flatten0 but keeps the arrays as is
      * @return
      */
    def flattenNoExplode: DataFrame = {

      val fields = dataFrame.schema.fields
      val fieldNames = fields.map(x => x.name)
      // val length = fields.length

      for (i <- fields.indices) {
        val field = fields(i)
        val fieldtype = field.dataType
        val fieldName = field.name
        fieldtype match {
          // only handle structType and skip arrays
          case structType: StructType =>
            val childFieldnames = structType.fieldNames.map(childname =>
              fieldName + "." + childname
            )
            val newfieldNames =
              fieldNames.filter(_ != fieldName) ++ childFieldnames
            val renamedcols =
              newfieldNames.map(x => col(x).as(x.replace(".", "_")))
            val explodedf = dataFrame.select(renamedcols: _*)
            return explodedf.flattenNoExplode
          case _ =>
        }
      }
      dataFrame
    }

    /*
      final unpivot would take additional key column name and value column name
     */
    /**
      * takes 2 lits of columns, the RHS and LHS. It keeps the LHS as is and rotates the RHS into key/value pairs
      *
      * @param unpivotKeys unpivot keys... the left hand side could be extracted from the schema and the unpivot column list
      * @param unpivotColumnList
      * @param keyColumn
      * @param valueColumn
      * @return
      */
    def unPivot(
        unpivotKeys: List[String],
        unpivotColumnList: List[String],
        keyColumn: String = "key_column",
        valueColumn: String = "value"
    ): DataFrame = {
      // val schemaStruct = dataFrame.schema
      // schemaStruct.toDDL // show the string fields as a DDL representation
      // schemaStruct.fieldNames.mkString("'", "','", "'")
      // val schemaArrayNames = schemaStruct.fieldNames

      val stackFieldsArray =
        unpivotColumnList.toArray // schemaArrayNames.drop(1)
      val stackFields =
        s"stack(${stackFieldsArray.length.toString}" + stackFieldsArray //schemaArrayNames
        //.drop(1)
        .toList
        // note sure why not  .foldLeft("")((head, next) => { s"$head, '$next', `$next` " }) + ")"
          .foldLeft("")((head, next) => {
            s"$head, '${next.replace("`", "")}', $next "
          }) + ")"

      // dataFrame.select(col(s"${unpivotKeys.head}"), expr(s"$stackFields as (key_column,value)"))
      dataFrame.createOrReplaceTempView("_unpivot")
      val q =
        s"""select ${unpivotKeys
          .mkString(",")}, $stackFields as (`$keyColumn`, `$valueColumn`)
           | from _unpivot""".stripMargin
      // println(s"query is: $q")

      spark.sql(q)
    }

    /**
      * rename all columns of the dataFrame so that we can save as a Parquet file
      */
    def rename4Parquet(): DataFrame = {
      // need to add left/right trims and replace multiple __ with one?
      // val r = "[ ,;{}()\n\t=]"
      // added "." and "-" so that we skip using ``
      val r = "[ ,;{}()\n\t=.-]"
      spark.createDataFrame(
        dataFrame.rdd,
        StructType(
          dataFrame.schema
            .map(s =>
              StructField(
                s.name.trim.replaceAll(r, "__"),
                s.dataType,
                s.nullable
              )
            )
        )
      )
    }

    /**
      *
      * @param writeFolder
      * @param fileName
      * @param writeMode
      * @param writeFormat
      * @return
      */
    def persistAsParquet(
        writeFolder: String,
        fileName: String,
        writeMode: String = "overwrite",
        writeFormat: String = "parquet"
    ): DataFrame =
      if (writeFormat == "csv") {
        dataFrame.write
          .format(writeFormat)
          .mode(writeMode)
          .option("header", "true")
          // we should support this .option("maxColumns", maxColumns)
          .save(
            s"$writeFolder/${fileName.split("\\.")(0)}"
          ) // maybe force no suffixes?
        spark.read
          .format(writeFormat)
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"$writeFolder/${fileName.split("\\.")(0)}")
      } else { // assume only 2 options
        dataFrame.write
          .format(writeFormat)
          .mode(writeMode)
          //.option("maxColumns", maxColumns)
          .save(s"""$writeFolder/${fileName.split("\\.")(0)}""")
        spark.read
          .format(writeFormat)
          //.option("maxColumns", maxColumns)
          .load(s"""$writeFolder/${fileName.split("\\.")(0)}""")
      }

  }

}
