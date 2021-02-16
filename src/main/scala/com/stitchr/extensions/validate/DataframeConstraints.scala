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

package com.stitchr.extensions.validate

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

/* issues with referencing implicits from implicits... ide is ok but build fails! maybe it has to do with dependencies?*/
object DataframeConstraints {
  //val spark: SparkSession = SparkSession.builder.getOrCreate()
  //import spark.implicits._
  // import com.stitchr.extensions.transform.Dataframe.Implicits

  /**
    * check if any items in the column list is not a column in the schema
    * @param df
    * @param columnList
    * @return
    */
  def attributeCheck(df: DataFrame, columnList: List[String]): Boolean = {
    val state: Boolean =
      (columnList.toSet &~ df.schema.fieldNames.toSet).toList.isEmpty
    if (!state)
      println(
        s"$columnList does not match what the schema fields are"
      ) // should change to logging
    state
  }

  /**
    *  Apply constraint tests to a dataFrame
    *  such as PK check
    *  FK check
    *  not null
    *  uniqueness ...
    * @param dataFrame
    */
  implicit class Implicits(dataFrame: DataFrame) {

    def selectList(columnList: List[String]): DataFrame = {
      // NH: 2/12/21 following should be made as a function that takes a List makes a string adding ``
      // and resplitting to support funky column names
      val cl: Array[String] = s"""`${columnList.mkString("`,`")}`""".split(',')
      dataFrame.select(cl.head, cl.tail: _*)
    }

    /**
      * logic to check that columnList has unique values (and maybe no nulls)
      * assumes lists have been checked
      * @param columnList list of columns that are expected to constitute the PK
      * @return DataFrame of all errors or empty. Current schema is PK, count if > 0
      */
    def pkCheck(columnNamesList: List[String]): DataFrame = {
      // NH: may need to add `` to the strings for non conventional attributes
      selectList(columnNamesList)
        .groupBy(columnNamesList.head, columnNamesList.tail: _*)
        .count()
        .withColumnRenamed("count", "group_count")
        .filter(col("group_count") > 1)
    }

    /**
      * to develop
      * lookup validation (FK validation): needs set of inputs (usually one column) and a FK table
      *  we can use the columnListSrc ---> FK columnKeyDest (and associated Df) and a similar one
      *  with table references (db1.table1) references db2.table2
      */
    def fkCheck(
        srcColumnNamesList: List[String],
        lookupDf: DataFrame,
        lookupColumnList: List[String]
    ): DataFrame = {
      // NH: may need to add `` to the strings for non conventional attributes
      // if (lookupColumnList.isEmpty)
      val targetColumnList = Option(lookupColumnList)
        .filterNot(_.isEmpty)
        .getOrElse(srcColumnNamesList)
      val tcl = s"""`${targetColumnList.mkString("`,`")}`""".split(',').toList
      val rDf = lookupDf.select(tcl.head, tcl.tail: _*)
      selectList(srcColumnNamesList).except(rDf)
    }

  }
}
