package com.stitchr.extensions.wrangle

/**
  * To invoke
  * source bash/stitchr_env.sh
  * added the mrpowers jar under ~/jars
  * spark-shell --jars $STITCHR_ROOT/app/target/stitchr-app-$VERSION-jar-with-dependencies.jar --packages mrpowers:spark-daria:0.35.2-s_2.11
  * note the following code does not rely on stitchr if invoked in spark-shell. for that comment out the following line
  */
// need to resolve this issue
import com.github.mrpowers.spark.daria.sql.functions._

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.DataFrame

/*
case class ColumnTransform(
    df: DataFrame,
    inputCol: String,
    outputColName: String = null
)
 */

object DataSetColumn {
  val spark = SparkSession.builder().getOrCreate()

  implicit class Implicits(ct: ColumnTransform) {

    import org.apache.spark.sql.functions._

    def overrideOutputColName(colName: String): ColumnTransform =
      ColumnTransform(
        ct.df,
        ct.inputCol,
        colName
      ) // modify to rename the output...

    def dropColumn(colName: String): ColumnTransform =
      ColumnTransform(ct.df.drop(col(colName)), ct.inputCol, ct.outputColName)

    /**
      * trim spaces and convert to Upper
      * @return
      */
    def trimWhiteSpacesAndUpper: ColumnTransform = {
      val outColName = ct.outputColName match {
        case null => s"${ct.inputCol}_trimmed"
        case _    => ct.outputColName
      }
      ColumnTransform(
        ct.df
          .withColumn(outColName, upper(singleSpace(col(s"${ct.inputCol}")))),
        outColName,
        null
      )
    }

    /**
      * strip punctuation
      * @return
      */
    def replacePunctuation(
        replacementPattern: String = " "
    ): ColumnTransform = {
      val outColName = ct.outputColName match {
        case null => s"${ct.inputCol}_nopunctuation"
        case _    => ct.outputColName
      }
      ColumnTransform(
        ct.df.withColumn(
          outColName,
          regexp_replace(
            col(ct.inputCol),
            """[\p{Punct}]""",
            replacementPattern
          )
        ),
        outColName,
        null
      ) // strip punctuation
    }

    /**
      * takes a dataframe and adds a column "bucket that
      *
      * @param colName
      * @param numberOfBuckets
      * @return
      */
    def bucketDataFrame(
        colName: String,
        numberOfBuckets: Int
    ): ColumnTransform =
      ColumnTransform(
        ct.df.withColumn("bucket", col(colName) % numberOfBuckets),
        ct.inputCol,
        null
      ) // put in buckets

    /**
      * tokenize the column
      * @return
      */
    def tokenizeColumn: ColumnTransform = {
      val outColName = ct.outputColName match {
        case null => s"${ct.inputCol}_tokenized"
        case _    => ct.outputColName
      }
      val tok = new Tokenizer()
      // need to validate against null values... so we add a map translation
      ColumnTransform(
        tok
          .setInputCol(ct.inputCol)
          .setOutputCol(outColName)
          .transform(ct.df.na.fill(Map(s"${ct.inputCol}" -> ""))),
        outColName,
        null
      )
    }

    /**
      * NH: need to move this out to its own
      * those are associated usually with companies.... Some are generic/universal
      *
      */
    val stopWords: Array[String] = Array(
      "the",
      "a",
      "http",
      "i",
      "me",
      "to",
      "what",
      "in",
      "Inc",
      "LLC",
      "L L C",
      "Corp",
      "CORP",
      "LTD",
      "and",
      "&",
      "inc.",
      "LLP.",
      "CORP.",
      "CO",
      "Limited",
      "SDN",
      "BHD",
      "Corporation",
      "inc.."
    )

    /**
      * remove stop words from the text
      *  NH: need to add the stopwords array as an input (maybe even a DF?!)
      *
      * @return
      */
    def removeStopWords(
        stopWordsArray: Array[String] = Array(null)
    ): ColumnTransform = {
      val sw = stopWordsArray match {
        case Array(null) =>
          stopWords
        case _ =>
          stopWordsArray
      }

      val outColName = ct.outputColName match {
        case null => s"normalized_${ct.inputCol}"
        case _    => ct.outputColName
      }
      val remover = new StopWordsRemover()
        .setInputCol(ct.inputCol)
        .setOutputCol("normalized_string_tokens")
        .setStopWords(sw)
      ColumnTransform(
        remover
          .transform(ct.df)
          .withColumn(
            outColName,
            upper(concat_ws(" ", col("normalized_string_tokens")))
          ),
        outColName,
        null
      )
    }

    /*
    |-- name_nopunctuation_trimmed: string (nullable = false)
    |-- name_nopunctuation_trimmed_tokenized: array (nullable = true)
    |    |-- element: string (containsNull = true)
    |-- oututColumnName: string (nullable = false)
     */

    def normalizeAttribute(
        outputColumnName: String = null,
        stopWords: Array[String] = Array(null)
    ): ColumnTransform =
      ct.replacePunctuation()
        .trimWhiteSpacesAndUpper
        .dropColumn(s"${ct.inputCol}_nopunctuation")
        .tokenizeColumn
        .overrideOutputColName(s"$outputColumnName")
        .removeStopWords(stopWords)
        .dropColumn("normalized_string_tokens")
        .dropColumn(s"${ct.inputCol}_tokens")
        .dropColumn(s"${ct.inputCol}_nopunctuation_trimmed")
        .dropColumn(s"${ct.inputCol}_nopunctuation_trimmed_tokenized")
  }

}
