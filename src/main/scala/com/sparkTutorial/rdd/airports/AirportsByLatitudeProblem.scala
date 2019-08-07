package com.sparkTutorial.rdd.airports

import org.apache.spark.{SparkConf, SparkContext}
import com.sparkTutorial.commons.Utils
import org.apache.log4j.Level
import org.apache.log4j.Logger


object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */
    // Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("Airport in USA")
    val sc = new SparkContext(conf)

    // Load the input data
    val textFile = sc.textFile("src/main/resources/airports.txt")
    // use take(n) to get the first n lines instead of collect
    // textFile.collect.foreach(line => println(line.split(",")(3)))
    val countryWithUsa = textFile.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)
    val usaAirportDetails = countryWithUsa.map(line => line.split(",")(1) + "," + line.split(",")(6))
    //usaAirportDetails.saveAsTextFile("hdfs:///tmp/usaairportdetails")
    usaAirportDetails.saveAsTextFile("/tmp/usaairportdetailsl")
  }
}
