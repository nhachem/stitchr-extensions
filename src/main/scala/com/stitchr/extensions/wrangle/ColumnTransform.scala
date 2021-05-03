package com.stitchr.extensions.wrangle

import org.apache.spark.sql.DataFrame

case class ColumnTransform(
    df: DataFrame,
    inputCol: String,
    outputColName: String = null
)
