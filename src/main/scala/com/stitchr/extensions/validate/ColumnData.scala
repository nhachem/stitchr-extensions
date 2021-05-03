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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object ColumnData {
  // val spark: SparkSession = SparkSession.builder.getOrCreate()

  /**
    * Apply value constraint tests to columns in a dataframe
    * may not need to be returning dataframes so maybe best as UDFs?
    * schema of output could be reference object, "offending-rule" as json snippet, timestamp(session), offending record
    * all get appending to error ? (we may pass through the dataframe for next steps?)
    * or should we tag those independently?
    *
    * @param dataFrame
    */
  implicit class Implicits(dataFrame: DataFrame) {

    /**
      * checks if the column type is numeric
      * @param columnName
      * @return
      */
    //def isNumericDataType(columnName: String) (implicit spark: SparkSession): Boolean = {
    def isNumericDataType(columnName: String): Boolean = {
      val numericTypeSet = Set(
        ByteType,
        ShortType,
        LongType,
        IntegerType,
        FloatType,
        DoubleType,
        DecimalType,
        BigDecimal
      )
      (Set(dataFrame.schema(columnName).dataType) &~ numericTypeSet
        .asInstanceOf[Set[DataType]]).isEmpty
    }

    import org.apache.spark.sql.functions._

    /* need a is_numeric udf construct
    def isNumeric(columnName: String): DataFrame = {
      dataFrame.withColumn("is_number", expr(s"is_numeric('$columnName')"))
    }
     */

    // NH: this is not working well ...
    // using implicits within implicits seems to blow during the build
    import com.stitchr.extensions.transform.Dataframe.Implicits
    /*
      - all values fall within a range, lower bound, upper bound,
      -- category list that is not available as a database table or from an api
      - values match a regex
     */
    /**
      * logic to check the max value of a column < = upperBound
      *
      * @param columnName
      * @return DataFrame of all errors records or empty.
      */
    def checkUpperBound(columnName: String, value: Double): DataFrame = {
      // NH: may need to add `` to the strings for non conventional attributes
      // make sure the type is numeric
      dataFrame.filter(col(columnName) > value)
    }

    /**
      * checks for lower bound
      *
      * @param columnName
      * @param value
      * @return
      */
    def checkLowerBound(columnName: String, value: Double): DataFrame = {
      // NH: may need to add `` to the strings for non conventional attributes
      dataFrame.filter(col(columnName) < value)
    }

    /**
      * checks for Range
      *
      * @param columnName
      * @param lowValue
      * @param highValue
      * @return
      */
    def checkNumericRange(
        columnName: String,
        lowValue: Double,
        highValue: Double
    ): DataFrame = {
      // NH: may need to add `` to the strings for non conventional attributes
      // assumes we used isNumeric to check that the column is numeric
      checkLowerBound(columnName, lowValue).unionAll(
        checkUpperBound(columnName, highValue)
      )
    }

    /**
      * checks if the column has any negative values
      * assumes we used @isNumeric to check that the column is numeric
      *
      * @param columnName
      * @param value
      * @return
      */
    def checkNegativeNumeric(columnName: String, value: Double): DataFrame = {
      // NH: may need to add `` to the strings for non conventional attributes
      // place holder
      dataFrame
    }

    /**
      * this is a string range type check vs a numeric range check
      * may be able to return a DF of all string that fail of an empty frame?
      *
      * @param columnName
      * @param lowValue
      * @param highValue
      * @return
      */
    def checkStringLength(
        columnName: String,
        lowLength: Int,
        highLength: Int
    ): DataFrame = {
      // NH: may need to add `` to the strings for non conventional attributes
      // place holder
      dataFrame
    }

    /**
      * difficult to do across all columns... one needs to apply .isNull to each column and then collapse to verify
      *
      * @param columnList
      * @return
      */
    def checkAllNull(columnList: List[String]): DataFrame = {
      // here checks are on each individually and returns the list of columns (as a dataframe ?) that are actually nulls
      // if input list is null then all columns are checks. If except is true (default is false) then all columns except in the list are checked.
      // NH: place holder
      dataFrame
    }

    /**
      * to do regex match on a column of multiple regexes (Map(key -> regex_rule)
      * best would be to add a UDF and then use withColumn
      *  this is just a stub here
      */
    import org.apache.spark.sql.expressions.UserDefinedFunction

    /**
      * general pupose function-bsed checker. The function f is applied to all values of a column
      *  and results stored in the new column checkField
      * @param columnName
      * @param f
      * @param checkField
      * @return
      */
    def checkFunction(
        columnName: String,
        f: UserDefinedFunction,
        checkField: String = "check_field"
    ): DataFrame = {
      dataFrame.withColumn(checkField, f(col(columnName)))
    }

    /**
      * TODO: this is a place holder for a function that can for example check if all data in a column
      * follow a specific format ... mainly used for dates
      *
      * @param columnName
      * @param format
      * @return
      */
    def formatCheck(columnName: String, format: String): DataFrame = {
      //  TODO
      dataFrame
    }
  }
}
