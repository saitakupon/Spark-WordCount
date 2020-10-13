package com.example.spark

import org.apache.spark.sql.SparkSession

object WordCountRDD {
  def main(args: Array[String]) {
    val fileName = "sample/wordcount/sample.txt"
    val spark = SparkSession.builder()
      .appName("WordCount with RDD")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.textFile(fileName)
      .flatMap(line => line.split(" "))
      .count()
    println(rdd)
  }
}
