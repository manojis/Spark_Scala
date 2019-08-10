package com.sparkTutorial.pairRdd.sort

import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SortedWordCountProblem")
  val sparkContext = new SparkContext(sparkConf)
}

