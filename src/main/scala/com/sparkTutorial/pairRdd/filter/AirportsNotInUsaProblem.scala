package com.sparkTutorial.pairRdd.filter

import org.apache.spark.{SparkConf, SparkContext}

object AirportsNotInUsaProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text;
       generate a pair RDD with airport name being the key and country name being the value.
       Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located,
       IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "Canada")
       ("Wewak Intl", "Papua New Guinea")
       ...
     */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AirportsNotInUsaProblem")
    val sparkContext = new SparkContext(sparkConf)
    val airportRDD = sparkContext.textFile("src/main/resources/input/airports.txt")

    val nonUSAirports = airportRDD.filter(line => line.split(",")(3) != "\"United States\"")
    val nonUsAirportMap = nonUSAirports.map(line => (line.split(",")(1),line.split(",")(2)))
    nonUsAirportMap.foreach(println)


  }
}
