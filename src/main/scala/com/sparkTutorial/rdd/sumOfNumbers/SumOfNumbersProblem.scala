package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    val sparkConf = new SparkConf().setAppName("SumOfNumbersProblem").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    val primeNumRdd = sparkContext.textFile("src/main/resources/input/prime_nums.txt")
    val numbers = primeNumRdd.flatMap(line => line.split("\\s+"))
    val validOnes = numbers.filter(number => !number.isEmpty)
    val validInt = validOnes.map(num => num.toInt)

    println("final result",validInt.reduce((x,y) => x + y))
  }
}
