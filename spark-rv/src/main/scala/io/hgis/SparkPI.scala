package io.hgis

/**
  * Created by willtemperley@gmail.com on 13-Nov-15.
  */
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
    println("Pi is approx: " + 4.0 * count / n)
    spark.stop()
  }
}