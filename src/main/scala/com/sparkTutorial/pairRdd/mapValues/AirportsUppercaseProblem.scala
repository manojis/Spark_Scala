package com.sparkTutorial.pairRdd.mapValues

import org.apache.spark.{SparkConf, SparkContext}

object AirportsUppercaseProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
       being the key and country name being the value. Then convert the country name to uppercase and
       output the pair RDD to out/airports_uppercase.text

       Each row of the input file contains the following columns:

       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "CANADA")
       ("Wewak Intl", "PAPUA NEW GUINEA")
       ...
     */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AirportsUppercaseProblem")
    val sparkContext = new SparkContext(sparkConf)
    val airportsUpperRDD = sparkContext.textFile("src/main/resources/input/airports.txt")
    //create a pairRDD here from the regular RDD, use map function for that
    val airportsUpperPairRDD = airportsUpperRDD.map((line:String) => (line.split(",")(1),line.split(",")(3)))
    airportsUpperPairRDD.foreach(println)
    val summation = airportsUpperPairRDD.mapValues(value => value.toUpperCase())
    println("==============Airports with country in upper Case=========================")
    summation.foreach(println)
  }
}
