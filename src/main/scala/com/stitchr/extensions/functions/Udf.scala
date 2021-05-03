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

package com.stitchr.extensions.functions

import com.stitchr.extensions.functions.Udf.spark
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

// Notes: separating Scala functions from Spark SQL's UDFs allows for easier testing
//        Spark SQL cannot and does not try optimize udfs
object Udf {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  val matchStringLength: UserDefinedFunction = udf((s: String, l: Int) =>
    if (s.length == l) {
      s
    } else null
  )

  // this UDF is used to extract the x/y arrays from the json strings
  val extractBetween: UserDefinedFunction =
    udf((s: String, regexStr: String) => {
      val exp = regexStr.r
      exp findFirstIn s.replace("\n", "").replace("'", "")
    })

  // this UDF is used to extract the x/y arrays from the json strings
  // example "x = [(.*)]"
  val extractBetween0: UserDefinedFunction =
    udf((s: String, regexStr: String) => {
      val exp = regexStr.r
      Seq(s.replace("\n", "").replace("'", "")).collect {
        case exp(x) => x.trim
      }
    })

  // NH: 3/30/21 if we want it in python... simple would be to use isDigit but would not cover decimal or float... and signs
  val isNumeric: UserDefinedFunction =
    udf((s: String) => {
      val regexStr = "-?\\d+\\.?\\d*"
      s.matches(regexStr)
    })

  def initUdfs: Unit = {
    // register all UDFs so that we can use them in SQL
    spark.udf.register("extract_between", extractBetween)
    spark.udf.register("extract_between0", extractBetween0)
    spark.udf.register("match_string_length", matchStringLength)
    spark.udf.register("is_numeric", isNumeric)
  }

}
