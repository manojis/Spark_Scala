package com.sparkTutorial.rdd.count

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    //val textFile = sc.textFile("hdfs:///tmp/spark/word_count.text")
    val textFile = sc.textFile("src/main/resources/input/word_count.txt")

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
    System.out.println("Total words: " + counts.count());
    //counts.saveAsTextFile("hdfs:///tmp/shakespeareWordCount")
    counts.saveAsTextFile("src/main/resources/output/shakespeareWordCount")
  }
}
