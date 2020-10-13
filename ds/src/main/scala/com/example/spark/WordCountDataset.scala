package com.example.spark

import org.apache.spark.sql.SparkSession

object WordCountDataset {
  def main(args: Array[String]) {
    val fileName = "sample/wordcount/sample.txt"
    val spark = SparkSession.builder()
      .appName("WordCount with Dataset")
      .getOrCreate()
    import spark.implicits._
    val ds = spark.read.text(fileName).as[String]
      .flatMap(value => value.split(" "))
      .count()
    println(ds)
  }
}
