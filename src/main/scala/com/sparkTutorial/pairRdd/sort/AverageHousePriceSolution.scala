package com.sparkTutorial.pairRdd.sort

import com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice.AvgCount
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceSolution {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    /*val conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[3]")
    val sc = new SparkContext(conf)*/
    val session = SparkSession.builder().appName("averageHousePriceSolution")
      .master("local[*]").getOrCreate()
    val sc = SparkContext.getOrCreate()

    // val lines = sc.textFile("src/main/resources/input/RealEstate.csv")
    // val realEstateDataset = session.read.option("header", "true").option("inferSchema", value = true).csv("src/main/resources/input/RealEstate.csv")
    val lines = sc.textFile("hdfs:///user/maria_dev/Spark/resources/input/RealEstate.csv")

    val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))
    val housePricePairRdd = cleanedLines.map(
      line => (line.split(",")(3).toInt, AvgCount(1, line.split(",")(2).toDouble)))

    val housePriceTotal = housePricePairRdd.reduceByKey((x, y) => AvgCount(x.count + y.count, x.total + y.total))

    val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount.total / avgCount.count)

    val sortedHousePriceAvg = housePriceAvg.sortByKey()

    for ((bedrooms, avgPrice) <- sortedHousePriceAvg.collect()) println(bedrooms + " : " + avgPrice)

    sortedHousePriceAvg.saveAsTextFile("hdfs:///user/maria_dev/Spark/resources/output/")
    // sortedHousePriceAvg.saveAsTextFile("src/main/resources/output/average/")
  }

}
