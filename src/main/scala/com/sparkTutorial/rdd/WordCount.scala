package com.sparkTutorial.rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._

object WordCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/resources/input/word_count.txt")
    val words = lines.flatMap(line => line.split(" "))
    val wordsMap = words.filter(word => !word.isEmpty).map(word => (word,1))
    wordsMap.foreach(println)

    val wordCounts = wordsMap.reduceByKey((x,y) => x + y)
    for ((word, count) <- wordCounts.collect()) println(word + " : " + count)
  }
}
