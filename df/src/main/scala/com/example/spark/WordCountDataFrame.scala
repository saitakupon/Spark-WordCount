package com.example.spark

import org.apache.spark.sql.SparkSession

object WordCountDataFrame {
  def main(args: Array[String]) {
    val fileName = "sample/wordcount/sample.txt"
    val spark = SparkSession.builder()
      .appName("WordCount with DataFrame")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.text(fileName)
      .flatMap(row => row.getString(0).split(" "))
      .count()
    println(df)
  }
}
